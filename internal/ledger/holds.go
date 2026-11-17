package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Hold lifetimes.
const (
	DefaultHoldTTL = 7 * 24 * time.Hour
	MaxHoldTTL     = 30 * 24 * time.Hour
)

const holdColumns = `id::text, account_id::text, destination_account_id::text, currency, amount,
	captured_amount, status, description, metadata, expires_at, transaction_id::text, created_at, updated_at`

func scanHold(row pgx.Row) (*Hold, error) {
	var h Hold
	if err := row.Scan(&h.ID, &h.AccountID, &h.DestinationAccountID, &h.Currency, &h.Amount,
		&h.CapturedAmount, &h.Status, &h.Description, &h.Metadata, &h.ExpiresAt, &h.TransactionID,
		&h.CreatedAt, &h.UpdatedAt); err != nil {
		return nil, err
	}
	return &h, nil
}

// AuthorizeHoldRequest reserves Amount on AccountID, to be paid to
// DestinationAccountID when captured.
type AuthorizeHoldRequest struct {
	AccountID            string          `json:"account_id"`
	DestinationAccountID string          `json:"destination_account_id"`
	Amount               int64           `json:"amount"`
	ExpiresInSeconds     int64           `json:"expires_in_seconds,omitempty"`
	Description          string          `json:"description,omitempty"`
	Metadata             json.RawMessage `json:"metadata,omitempty"`
}

// AuthorizeHoldTx creates an active hold inside tx. The held amount stops being
// available immediately but stays in the balance until capture.
func (s *Service) AuthorizeHoldTx(ctx context.Context, tx pgx.Tx, req AuthorizeHoldRequest) (*Hold, error) {
	if err := validID("account_id", req.AccountID); err != nil {
		return nil, err
	}
	if err := validID("destination_account_id", req.DestinationAccountID); err != nil {
		return nil, err
	}
	if req.AccountID == req.DestinationAccountID {
		return nil, errorf(CodeValidation, "account_id and destination_account_id must differ")
	}
	if req.Amount <= 0 {
		return nil, errorf(CodeValidation, "amount must be a positive integer in minor units")
	}
	ttl := DefaultHoldTTL
	if req.ExpiresInSeconds != 0 {
		ttl = time.Duration(req.ExpiresInSeconds) * time.Second
		if req.ExpiresInSeconds < 1 || ttl > MaxHoldTTL {
			return nil, errorf(CodeValidation, "expires_in_seconds must be between 1 and %d", int64(MaxHoldTTL/time.Second))
		}
	}
	if err := validDescription(req.Description); err != nil {
		return nil, err
	}
	meta, err := normalizeMetadata(req.Metadata)
	if err != nil {
		return nil, err
	}

	accounts, err := lockAccounts(ctx, tx, []string{req.AccountID, req.DestinationAccountID})
	if err != nil {
		return nil, err
	}
	src, dst := accounts[req.AccountID], accounts[req.DestinationAccountID]
	if src.Currency != dst.Currency {
		return nil, errorf(CodeCurrencyMismatch, "account %s is %s but destination %s is %s",
			src.ID, src.Currency, dst.ID, dst.Currency)
	}
	held, ok := addInt64(src.Held, req.Amount)
	if !ok {
		return nil, errorf(CodeValidation, "held amount would overflow")
	}
	if !src.AllowOverdraft && src.Balance-held < 0 {
		return nil, errorf(CodeInsufficientFunds,
			"account %s has %d available, needs %d", src.ID, src.Balance-src.Held, req.Amount)
	}
	src.Held = held
	src.dirty = true
	if err := writeAccounts(ctx, tx, accounts); err != nil {
		return nil, err
	}
	return scanHold(tx.QueryRow(ctx,
		`INSERT INTO holds (account_id, destination_account_id, currency, amount, description, metadata, expires_at)
		 VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6::jsonb, $7) RETURNING `+holdColumns,
		src.ID, dst.ID, src.Currency, req.Amount, req.Description, meta, s.now().Add(ttl)))
}

// AuthorizeHold creates a hold in its own transaction.
func (s *Service) AuthorizeHold(ctx context.Context, req AuthorizeHoldRequest) (*Hold, error) {
	var out *Hold
	err := s.InTx(ctx, func(tx pgx.Tx) (err error) {
		out, err = s.AuthorizeHoldTx(ctx, tx, req)
		return err
	})
	return out, err
}

func lockHold(ctx context.Context, tx pgx.Tx, id string) (*Hold, error) {
	if err := validID("hold id", id); err != nil {
		return nil, err
	}
	h, err := scanHold(tx.QueryRow(ctx, `SELECT `+holdColumns+` FROM holds WHERE id = $1::uuid FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errorf(CodeNotFound, "hold %s not found", id)
	}
	return h, err
}

func releaseHeld(a *lockedAccount, amount int64) error {
	if a.Held < amount {
		// Cannot happen while the invariants hold; refuse rather than corrupt data.
		return errors.New("ledger: held amount is smaller than the hold being released")
	}
	a.Held -= amount
	a.dirty = true
	return nil
}

// CaptureHoldRequest captures all of a hold, or part of it when Amount is set.
// The uncaptured remainder is released.
type CaptureHoldRequest struct {
	Amount      int64  `json:"amount,omitempty"`
	Description string `json:"description,omitempty"`
}

// CaptureHoldTx turns an active hold into a posted transaction inside tx.
func (s *Service) CaptureHoldTx(ctx context.Context, tx pgx.Tx, id string, req CaptureHoldRequest) (*Hold, error) {
	if err := validDescription(req.Description); err != nil {
		return nil, err
	}
	h, err := lockHold(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if h.Status != HoldActive {
		return nil, errorf(CodeHoldNotActive, "hold %s is %s", h.ID, h.Status)
	}
	if !s.now().Before(h.ExpiresAt) {
		return nil, errorf(CodeHoldExpired, "hold %s expired at %s", h.ID, h.ExpiresAt.Format(time.RFC3339))
	}
	amount := req.Amount
	if amount == 0 {
		amount = h.Amount
	}
	if amount < 0 || amount > h.Amount {
		return nil, errorf(CodeValidation, "capture amount must be between 1 and %d", h.Amount)
	}
	accounts, err := lockAccounts(ctx, tx, []string{h.AccountID, h.DestinationAccountID})
	if err != nil {
		return nil, err
	}
	if err := releaseHeld(accounts[h.AccountID], h.Amount); err != nil {
		return nil, err
	}
	description := req.Description
	if description == "" {
		description = h.Description
	}
	t, err := s.post(ctx, tx, accounts, posting{
		kind:        "capture",
		description: description,
		metadata:    string(h.Metadata),
		legs: []Leg{
			{AccountID: h.AccountID, Amount: -amount},
			{AccountID: h.DestinationAccountID, Amount: amount},
		},
	})
	if err != nil {
		return nil, err
	}
	return scanHold(tx.QueryRow(ctx,
		`UPDATE holds SET status = 'captured', captured_amount = $2, transaction_id = $3::uuid, updated_at = now()
		 WHERE id = $1::uuid RETURNING `+holdColumns, h.ID, amount, t.ID))
}

// CaptureHold captures a hold in its own transaction.
func (s *Service) CaptureHold(ctx context.Context, id string, req CaptureHoldRequest) (*Hold, error) {
	var out *Hold
	err := s.InTx(ctx, func(tx pgx.Tx) (err error) {
		out, err = s.CaptureHoldTx(ctx, tx, id, req)
		return err
	})
	return out, err
}

// VoidHoldTx cancels an active hold and releases the money inside tx.
func (s *Service) VoidHoldTx(ctx context.Context, tx pgx.Tx, id string) (*Hold, error) {
	h, err := lockHold(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if h.Status != HoldActive {
		return nil, errorf(CodeHoldNotActive, "hold %s is %s", h.ID, h.Status)
	}
	accounts, err := lockAccounts(ctx, tx, []string{h.AccountID})
	if err != nil {
		return nil, err
	}
	if err := releaseHeld(accounts[h.AccountID], h.Amount); err != nil {
		return nil, err
	}
	if err := writeAccounts(ctx, tx, accounts); err != nil {
		return nil, err
	}
	return scanHold(tx.QueryRow(ctx,
		`UPDATE holds SET status = 'voided', updated_at = now() WHERE id = $1::uuid RETURNING `+holdColumns, h.ID))
}

// VoidHold voids a hold in its own transaction.
func (s *Service) VoidHold(ctx context.Context, id string) (*Hold, error) {
	var out *Hold
	err := s.InTx(ctx, func(tx pgx.Tx) (err error) {
		out, err = s.VoidHoldTx(ctx, tx, id)
		return err
	})
	return out, err
}

// GetHold returns one hold.
func (s *Service) GetHold(ctx context.Context, id string) (*Hold, error) {
	if err := validID("hold id", id); err != nil {
		return nil, err
	}
	h, err := scanHold(s.pool.QueryRow(ctx, `SELECT `+holdColumns+` FROM holds WHERE id = $1::uuid`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errorf(CodeNotFound, "hold %s not found", id)
	}
	return h, err
}

// ExpireHolds releases up to limit active holds whose expiry has passed and
// returns how many it expired. Holds locked by a concurrent capture or void are
// skipped (SKIP LOCKED) and picked up on the next run.
func (s *Service) ExpireHolds(ctx context.Context, limit int) (int, error) {
	var expired int
	err := s.InTx(ctx, func(tx pgx.Tx) error {
		expired = 0
		rows, err := tx.Query(ctx,
			`SELECT id::text, account_id::text, amount FROM holds
			 WHERE status = 'active' AND expires_at <= $1
			 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED`, s.now(), limit)
		if err != nil {
			return err
		}
		var ids, accountIDs []string
		var amounts []int64
		for rows.Next() {
			var id, accountID string
			var amount int64
			if err := rows.Scan(&id, &accountID, &amount); err != nil {
				rows.Close()
				return err
			}
			ids, accountIDs, amounts = append(ids, id), append(accountIDs, accountID), append(amounts, amount)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		accounts, err := lockAccounts(ctx, tx, accountIDs)
		if err != nil {
			return err
		}
		for i := range ids {
			if err := releaseHeld(accounts[accountIDs[i]], amounts[i]); err != nil {
				return err
			}
		}
		if err := writeAccounts(ctx, tx, accounts); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE holds SET status = 'expired', updated_at = now() WHERE id = ANY($1::uuid[])`, ids); err != nil {
			return err
		}
		expired = len(ids)
		return nil
	})
	return expired, err
}
