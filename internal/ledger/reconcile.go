package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ReconcileReport is the result of re-deriving every balance from the journal.
type ReconcileReport struct {
	OK               bool             `json:"ok"`
	CheckedAt        time.Time        `json:"checked_at"`
	Accounts         int64            `json:"accounts"`
	Transactions     int64            `json:"transactions"`
	Entries          int64            `json:"entries"`
	ActiveHolds      int64            `json:"active_holds"`
	TotalsByCurrency map[string]int64 `json:"totals_by_currency"`
	Problems         []string         `json:"problems"`
}

const maxProblems = 100

// Reconcile checks, on one consistent snapshot, that:
//
//  1. every transaction's entries sum to zero per currency,
//  2. every account balance equals the sum of its entries,
//  3. every account balance equals the balance_after of its latest entry,
//  4. every account's held amount equals the sum of its active holds,
//  5. the balances of each currency sum to zero across all accounts,
//  6. no account without overdraft has a negative available balance.
//
// It only reads; it never repairs anything.
func (s *Service) Reconcile(ctx context.Context) (*ReconcileReport, error) {
	r := &ReconcileReport{CheckedAt: s.now().UTC(), TotalsByCurrency: map[string]int64{}, Problems: []string{}}
	add := func(format string, args ...any) {
		if len(r.Problems) < maxProblems {
			r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
		}
	}
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly},
		func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM accounts),
				(SELECT count(*) FROM transactions),
				(SELECT count(*) FROM entries),
				(SELECT count(*) FROM holds WHERE status = 'active')`).
				Scan(&r.Accounts, &r.Transactions, &r.Entries, &r.ActiveHolds); err != nil {
				return err
			}
			checks := []struct {
				sql    string
				report func(rows pgx.Rows) error
			}{
				{`SELECT transaction_id::text, currency, sum(amount)::bigint FROM entries
				  GROUP BY transaction_id, currency HAVING sum(amount) <> 0`,
					func(rows pgx.Rows) error {
						var id, cur string
						var sum int64
						if err := rows.Scan(&id, &cur, &sum); err != nil {
							return err
						}
						add("transaction %s is unbalanced in %s by %d", id, cur, sum)
						return nil
					}},
				{`SELECT a.id::text, a.balance, COALESCE(sum(e.amount), 0)::bigint FROM accounts a
				  LEFT JOIN entries e ON e.account_id = a.id
				  GROUP BY a.id HAVING a.balance <> COALESCE(sum(e.amount), 0)`,
					func(rows pgx.Rows) error {
						var id string
						var balance, sum int64
						if err := rows.Scan(&id, &balance, &sum); err != nil {
							return err
						}
						add("account %s balance is %d but its entries sum to %d", id, balance, sum)
						return nil
					}},
				{`SELECT a.id::text, a.balance, last.balance_after FROM accounts a
				  JOIN LATERAL (SELECT balance_after FROM entries e WHERE e.account_id = a.id
				                ORDER BY e.id DESC LIMIT 1) last ON true
				  WHERE a.balance <> last.balance_after`,
					func(rows pgx.Rows) error {
						var id string
						var balance, after int64
						if err := rows.Scan(&id, &balance, &after); err != nil {
							return err
						}
						add("account %s balance is %d but its latest entry says %d", id, balance, after)
						return nil
					}},
				{`SELECT a.id::text, a.held, COALESCE(sum(h.amount) FILTER (WHERE h.status = 'active'), 0)::bigint
				  FROM accounts a LEFT JOIN holds h ON h.account_id = a.id
				  GROUP BY a.id HAVING a.held <> COALESCE(sum(h.amount) FILTER (WHERE h.status = 'active'), 0)`,
					func(rows pgx.Rows) error {
						var id string
						var held, sum int64
						if err := rows.Scan(&id, &held, &sum); err != nil {
							return err
						}
						add("account %s holds %d but its active holds sum to %d", id, held, sum)
						return nil
					}},
				{`SELECT id::text, balance - held FROM accounts WHERE NOT allow_overdraft AND balance - held < 0`,
					func(rows pgx.Rows) error {
						var id string
						var available int64
						if err := rows.Scan(&id, &available); err != nil {
							return err
						}
						add("account %s has negative available balance %d without overdraft", id, available)
						return nil
					}},
				{`SELECT currency, sum(balance)::bigint FROM accounts GROUP BY currency ORDER BY currency`,
					func(rows pgx.Rows) error {
						var cur string
						var sum int64
						if err := rows.Scan(&cur, &sum); err != nil {
							return err
						}
						r.TotalsByCurrency[cur] = sum
						if sum != 0 {
							add("balances in %s sum to %d instead of 0", cur, sum)
						}
						return nil
					}},
			}
			for _, c := range checks {
				rows, err := tx.Query(ctx, c.sql)
				if err != nil {
					return err
				}
				for rows.Next() {
					if err := c.report(rows); err != nil {
						rows.Close()
						return err
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	r.OK = len(r.Problems) == 0
	return r, nil
}
