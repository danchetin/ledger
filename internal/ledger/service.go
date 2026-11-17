// Package ledger implements a double-entry ledger on PostgreSQL.
//
// Every money movement is a transaction made of entries that sum to zero per
// currency. Concurrency is handled with row locks taken in a fixed order (by
// account id), so concurrent transactions serialize per account and never deadlock
// on each other.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Limits on request size.
const (
	MaxLegs           = 50
	MaxDescriptionLen = 500
	MaxMetadataBytes  = 8 << 10
	DefaultPageSize   = 50
	MaxPageSize       = 500
)

var (
	uuidRe     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)
)

// Service is the ledger. It is safe for concurrent use.
type Service struct {
	pool       *pgxpool.Pool
	now        func() time.Time
	maxRetries int
}

// Option configures a Service.
type Option func(*Service)

// WithClock overrides the clock used for hold expiry (tests).
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// New creates a Service on top of a migrated database.
func New(pool *pgxpool.Pool, opts ...Option) *Service {
	s := &Service{pool: pool, now: time.Now, maxRetries: 5}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Pool returns the underlying connection pool.
func (s *Service) Pool() *pgxpool.Pool { return s.pool }

// Ping checks that the database is reachable.
func (s *Service) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// InTx runs fn in a READ COMMITTED transaction. Deadlocks and serialization
// failures are retried with jittered backoff; any other error, including business
// errors, rolls back and is returned as is.
func (s *Service) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = pgx.BeginFunc(ctx, s.pool, fn)
		if err == nil || !retryable(err) || attempt >= s.maxRetries {
			return err
		}
		delay := time.Duration(1<<attempt)*5*time.Millisecond + time.Duration(rand.IntN(5))*time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func retryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}

func validID(field, id string) error {
	if !uuidRe.MatchString(id) {
		return errorf(CodeValidation, "%s must be a UUID", field)
	}
	return nil
}

// normalizeMetadata returns a JSON object as text, "{}" when empty.
func normalizeMetadata(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "{}", nil
	}
	if len(raw) > MaxMetadataBytes {
		return "", errorf(CodeValidation, "metadata must be at most %d bytes", MaxMetadataBytes)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", errorf(CodeValidation, "metadata must be a JSON object")
	}
	return string(raw), nil
}

func validDescription(d string) error {
	if len(d) > MaxDescriptionLen {
		return errorf(CodeValidation, "description must be at most %d characters", MaxDescriptionLen)
	}
	return nil
}

func pageSize(limit int) int {
	switch {
	case limit <= 0:
		return DefaultPageSize
	case limit > MaxPageSize:
		return MaxPageSize
	default:
		return limit
	}
}

func addInt64(a, b int64) (int64, bool) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, false
	}
	return c, true
}

// ---- accounts ----

// CreateAccountRequest is the input for CreateAccount.
type CreateAccountRequest struct {
	Name           string          `json:"name"`
	Currency       string          `json:"currency"`
	AllowOverdraft bool            `json:"allow_overdraft"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

const accountColumns = `id::text, name, currency, allow_overdraft, balance, held, metadata, created_at, updated_at`

func scanAccount(row pgx.Row) (*Account, error) {
	var a Account
	if err := row.Scan(&a.ID, &a.Name, &a.Currency, &a.AllowOverdraft, &a.Balance, &a.Held,
		&a.Metadata, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	a.Available = a.Balance - a.Held
	return &a, nil
}

// CreateAccountTx creates an account inside tx.
func (s *Service) CreateAccountTx(ctx context.Context, tx pgx.Tx, req CreateAccountRequest) (*Account, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" || len(name) > 200 {
		return nil, errorf(CodeValidation, "name must be 1-200 characters")
	}
	if !currencyRe.MatchString(req.Currency) {
		return nil, errorf(CodeValidation, "currency must be a 3-letter ISO 4217 code in upper case, e.g. USD")
	}
	meta, err := normalizeMetadata(req.Metadata)
	if err != nil {
		return nil, err
	}
	a, err := scanAccount(tx.QueryRow(ctx,
		`INSERT INTO accounts (name, currency, allow_overdraft, metadata)
		 VALUES ($1, $2, $3, $4::jsonb) RETURNING `+accountColumns,
		name, req.Currency, req.AllowOverdraft, meta))
	if err != nil {
		return nil, err
	}
	return a, emit(ctx, tx, EventAccountCreated, a.ID, a)
}

// CreateAccount creates an account in its own transaction.
func (s *Service) CreateAccount(ctx context.Context, req CreateAccountRequest) (*Account, error) {
	var out *Account
	err := s.InTx(ctx, func(tx pgx.Tx) (err error) {
		out, err = s.CreateAccountTx(ctx, tx, req)
		return err
	})
	return out, err
}

// GetAccount returns one account.
func (s *Service) GetAccount(ctx context.Context, id string) (*Account, error) {
	if err := validID("account id", id); err != nil {
		return nil, err
	}
	a, err := scanAccount(s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = $1::uuid`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errorf(CodeNotFound, "account %s not found", id)
	}
	return a, err
}

// ListAccounts returns accounts ordered by id after the given cursor, and the
// cursor for the next page ("" when there is none).
func (s *Service) ListAccounts(ctx context.Context, limit int, after string) ([]Account, string, error) {
	if after != "" {
		if err := validID("cursor", after); err != nil {
			return nil, "", err
		}
	}
	n := pageSize(limit)
	rows, err := s.pool.Query(ctx, `SELECT `+accountColumns+` FROM accounts
		WHERE id > COALESCE(NULLIF($1, '')::uuid, '00000000-0000-0000-0000-000000000000')
		ORDER BY id LIMIT $2`, after, n+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	accounts := []Account{}
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, "", err
		}
		accounts = append(accounts, *a)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(accounts) > n {
		accounts = accounts[:n]
		next = accounts[n-1].ID
	}
	return accounts, next, nil
}
