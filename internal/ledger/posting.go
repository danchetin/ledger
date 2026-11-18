package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
)

// lockedAccount is an account row locked FOR UPDATE inside the current transaction.
// Changes are made in memory and written back by writeAccounts.
type lockedAccount struct {
	ID             string
	Currency       string
	AllowOverdraft bool
	Balance        int64
	Held           int64
	dirty          bool
}

// lockAccounts locks the given accounts in ascending id order. Every code path
// that locks more than one account goes through here, so two transactions
// touching the same accounts always lock them in the same order and cannot
// deadlock each other.
func lockAccounts(ctx context.Context, tx pgx.Tx, ids []string) (map[string]*lockedAccount, error) {
	unique := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	rows, err := tx.Query(ctx,
		`SELECT id::text, currency, allow_overdraft, balance, held
		 FROM accounts WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE`, unique)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]*lockedAccount, len(unique))
	for rows.Next() {
		var a lockedAccount
		if err := rows.Scan(&a.ID, &a.Currency, &a.AllowOverdraft, &a.Balance, &a.Held); err != nil {
			return nil, err
		}
		out[a.ID] = &a
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range unique {
		if _, ok := out[id]; !ok {
			return nil, errorf(CodeNotFound, "account %s not found", id)
		}
	}
	return out, nil
}

// writeAccounts persists balance/held of every modified locked account in one statement.
func writeAccounts(ctx context.Context, tx pgx.Tx, accounts map[string]*lockedAccount) error {
	var ids []string
	var balances, held []int64
	for _, a := range accounts {
		if a.dirty {
			ids = append(ids, a.ID)
			balances = append(balances, a.Balance)
			held = append(held, a.Held)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx,
		`UPDATE accounts AS a SET balance = v.balance, held = v.held, updated_at = now()
		 FROM unnest($1::uuid[], $2::bigint[], $3::bigint[]) AS v(id, balance, held)
		 WHERE a.id = v.id`, ids, balances, held)
	return err
}

// validateLegs checks the shape of legs before anything is locked.
func validateLegs(legs []Leg) error {
	if len(legs) < 2 || len(legs) > MaxLegs {
		return errorf(CodeValidation, "a transaction needs between 2 and %d legs", MaxLegs)
	}
	seen := make(map[string]bool, len(legs))
	for i, l := range legs {
		if err := validID("legs[].account_id", l.AccountID); err != nil {
			return err
		}
		if l.Amount == 0 {
			return errorf(CodeValidation, "legs[%d].amount must not be zero", i)
		}
		if seen[l.AccountID] {
			return errorf(CodeValidation, "account %s appears in more than one leg", l.AccountID)
		}
		seen[l.AccountID] = true
	}
	return nil
}

func legAccountIDs(legs []Leg) []string {
	ids := make([]string, len(legs))
	for i, l := range legs {
		ids[i] = l.AccountID
	}
	return ids
}

type posting struct {
	kind        string
	description string
	metadata    string
	reversesID  *string
	legs        []Leg
}

// post applies legs to already locked accounts and writes the transaction, its
// entries and the new balances. The caller must have locked every leg's account.
func (s *Service) post(ctx context.Context, tx pgx.Tx, accounts map[string]*lockedAccount, p posting) (*Transaction, error) {
	sums := map[string]int64{}
	for _, l := range p.legs {
		a := accounts[l.AccountID]
		total, ok := addInt64(sums[a.Currency], l.Amount)
		if !ok {
			return nil, errorf(CodeValidation, "amounts overflow")
		}
		sums[a.Currency] = total
	}
	for currency, total := range sums {
		if total != 0 {
			return nil, errorf(CodeUnbalanced, "legs in %s sum to %d, they must sum to 0", currency, total)
		}
	}

	accountIDs := make([]string, len(p.legs))
	currencies := make([]string, len(p.legs))
	amounts := make([]int64, len(p.legs))
	balancesAfter := make([]int64, len(p.legs))
	for i, l := range p.legs {
		a := accounts[l.AccountID]
		next, ok := addInt64(a.Balance, l.Amount)
		if !ok {
			return nil, errorf(CodeValidation, "balance of account %s would overflow", a.ID)
		}
		if l.Amount < 0 && !a.AllowOverdraft && next-a.Held < 0 {
			return nil, errorf(CodeInsufficientFunds,
				"account %s has %d available, needs %d", a.ID, a.Balance-a.Held, -l.Amount)
		}
		a.Balance = next
		a.dirty = true
		accountIDs[i], currencies[i], amounts[i], balancesAfter[i] = a.ID, a.Currency, l.Amount, next
	}

	t := Transaction{Kind: p.kind, Description: p.description, Metadata: json.RawMessage(p.metadata), ReversesID: p.reversesID}
	err := tx.QueryRow(ctx,
		`INSERT INTO transactions (kind, description, metadata, reverses_id)
		 VALUES ($1, $2, $3::jsonb, $4::uuid) RETURNING id::text, created_at`,
		p.kind, p.description, p.metadata, p.reversesID).Scan(&t.ID, &t.CreatedAt)
	if err != nil {
		if isUniqueViolation(err, "transactions_reverses_id_key") {
			return nil, errorf(CodeAlreadyReversed, "transaction %s is already reversed", *p.reversesID)
		}
		return nil, err
	}

	rows, err := tx.Query(ctx,
		`INSERT INTO entries (transaction_id, account_id, currency, amount, balance_after)
		 SELECT $1::uuid, v.account_id, v.currency, v.amount, v.balance_after
		 FROM unnest($2::uuid[], $3::text[], $4::bigint[], $5::bigint[])
		      AS v(account_id, currency, amount, balance_after)
		 RETURNING id, transaction_id::text, account_id::text, currency, amount, balance_after, created_at`,
		t.ID, accountIDs, currencies, amounts, balancesAfter)
	if err != nil {
		return nil, err
	}
	t.Entries, err = scanEntries(rows)
	if err != nil {
		return nil, err
	}
	if err := writeAccounts(ctx, tx, accounts); err != nil {
		return nil, err
	}
	return &t, emit(ctx, tx, EventTransactionPosted, t.ID, &t)
}

func scanEntries(rows pgx.Rows) ([]Entry, error) {
	defer rows.Close()
	entries := []Entry{}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Currency, &e.Amount,
			&e.BalanceAfter, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return entries, nil
}

// ---- transfers ----

// TransferRequest moves money. Use either the simple form (from, to, amount)
// or legs for a multi-party transaction.
type TransferRequest struct {
	FromAccountID string          `json:"from_account_id,omitempty"`
	ToAccountID   string          `json:"to_account_id,omitempty"`
	Amount        int64           `json:"amount,omitempty"`
	Legs          []Leg           `json:"legs,omitempty"`
	Description   string          `json:"description,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

func (r TransferRequest) legs() ([]Leg, error) {
	simple := r.FromAccountID != "" || r.ToAccountID != "" || r.Amount != 0
	if len(r.Legs) > 0 {
		if simple {
			return nil, errorf(CodeValidation, "use either legs or from_account_id/to_account_id/amount, not both")
		}
		return r.Legs, nil
	}
	if r.Amount <= 0 {
		return nil, errorf(CodeValidation, "amount must be a positive integer in minor units")
	}
	if r.FromAccountID == r.ToAccountID {
		return nil, errorf(CodeValidation, "from_account_id and to_account_id must differ")
	}
	return []Leg{{AccountID: r.FromAccountID, Amount: -r.Amount}, {AccountID: r.ToAccountID, Amount: r.Amount}}, nil
}

// TransferTx posts a transfer inside tx.
func (s *Service) TransferTx(ctx context.Context, tx pgx.Tx, req TransferRequest) (*Transaction, error) {
	legs, err := req.legs()
	if err != nil {
		return nil, err
	}
	if err := validateLegs(legs); err != nil {
		return nil, err
	}
	if err := validDescription(req.Description); err != nil {
		return nil, err
	}
	meta, err := normalizeMetadata(req.Metadata)
	if err != nil {
		return nil, err
	}
	accounts, err := lockAccounts(ctx, tx, legAccountIDs(legs))
	if err != nil {
		return nil, err
	}
	return s.post(ctx, tx, accounts, posting{kind: "transfer", description: req.Description, metadata: meta, legs: legs})
}

// Transfer posts a transfer in its own transaction.
func (s *Service) Transfer(ctx context.Context, req TransferRequest) (*Transaction, error) {
	var out *Transaction
	err := s.InTx(ctx, func(tx pgx.Tx) (err error) {
		out, err = s.TransferTx(ctx, tx, req)
		return err
	})
	return out, err
}

// ---- reversals ----

// ReverseRequest is the input for Reverse.
type ReverseRequest struct {
	Description string          `json:"description,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

// ReverseTx posts a transaction that undoes transaction id. A transaction can be
// reversed once; reversals themselves cannot be reversed. The reversal is subject
// to the same balance rules, so it fails if the money has already been spent.
func (s *Service) ReverseTx(ctx context.Context, tx pgx.Tx, id string, req ReverseRequest) (*Transaction, error) {
	if err := validID("transaction id", id); err != nil {
		return nil, err
	}
	if err := validDescription(req.Description); err != nil {
		return nil, err
	}
	meta, err := normalizeMetadata(req.Metadata)
	if err != nil {
		return nil, err
	}
	orig, err := getTransaction(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if orig.Kind == "reversal" {
		return nil, errorf(CodeValidation, "a reversal cannot be reversed")
	}
	legs := make([]Leg, len(orig.Entries))
	for i, e := range orig.Entries {
		legs[i] = Leg{AccountID: e.AccountID, Amount: -e.Amount}
	}
	accounts, err := lockAccounts(ctx, tx, legAccountIDs(legs))
	if err != nil {
		return nil, err
	}
	// Check only after the locks: a concurrent reversal of the same transaction
	// locks the same accounts, so by now it has either committed (and this query
	// sees it) or not started. Checking earlier would let us wait for its locks and
	// then fail with a misleading insufficient_funds. The unique index on
	// reverses_id remains the final guard.
	var reversed bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM transactions WHERE reverses_id = $1::uuid)`, id).
		Scan(&reversed); err != nil {
		return nil, err
	}
	if reversed {
		return nil, errorf(CodeAlreadyReversed, "transaction %s is already reversed", id)
	}
	description := req.Description
	if description == "" {
		description = "Reversal of " + id
	}
	return s.post(ctx, tx, accounts, posting{
		kind: "reversal", description: description, metadata: meta, reversesID: &id, legs: legs,
	})
}

// Reverse reverses a transaction in its own transaction.
func (s *Service) Reverse(ctx context.Context, id string, req ReverseRequest) (*Transaction, error) {
	var out *Transaction
	err := s.InTx(ctx, func(tx pgx.Tx) (err error) {
		out, err = s.ReverseTx(ctx, tx, id, req)
		return err
	})
	return out, err
}

// ---- reads ----

const transactionColumns = `id::text, kind, description, metadata, reverses_id::text, created_at`

func getTransaction(ctx context.Context, q querier, id string) (*Transaction, error) {
	var t Transaction
	err := q.QueryRow(ctx, `SELECT `+transactionColumns+` FROM transactions WHERE id = $1::uuid`, id).
		Scan(&t.ID, &t.Kind, &t.Description, &t.Metadata, &t.ReversesID, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errorf(CodeNotFound, "transaction %s not found", id)
	}
	if err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx,
		`SELECT id, transaction_id::text, account_id::text, currency, amount, balance_after, created_at
		 FROM entries WHERE transaction_id = $1::uuid ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	t.Entries, err = scanEntries(rows)
	return &t, err
}

// GetTransaction returns a transaction with its entries.
func (s *Service) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	if err := validID("transaction id", id); err != nil {
		return nil, err
	}
	return getTransaction(ctx, s.pool, id)
}

// ListEntries returns an account statement, newest first. before is the entry id
// cursor from the previous page (0 for the first page); the returned cursor is 0
// when there are no more entries.
func (s *Service) ListEntries(ctx context.Context, accountID string, limit int, before int64) ([]Entry, int64, error) {
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return nil, 0, err
	}
	n := pageSize(limit)
	rows, err := s.pool.Query(ctx,
		`SELECT id, transaction_id::text, account_id::text, currency, amount, balance_after, created_at
		 FROM entries
		 WHERE account_id = $1::uuid AND ($2::bigint = 0 OR id < $2)
		 ORDER BY id DESC LIMIT $3`, accountID, before, n+1)
	if err != nil {
		return nil, 0, err
	}
	entries, err := scanEntries(rows)
	if err != nil {
		return nil, 0, err
	}
	// scanEntries sorts ascending; a statement reads newest first.
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID > entries[j].ID })
	var next int64
	if len(entries) > n {
		entries = entries[:n]
		next = entries[n-1].ID
	}
	return entries, next, nil
}
