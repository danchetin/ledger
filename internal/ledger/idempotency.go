package ledger

import (
	"bytes"
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Response is what an idempotent operation produced: an HTTP status and body.
type Response struct {
	Status int
	Body   []byte
}

// WithIdempotency runs op at most once per key.
//
// The key row is inserted in the same database transaction as the operation:
//
//   - First request: the INSERT succeeds, op runs, and its response is stored
//     before COMMIT. Effects and the stored response commit together or not at all.
//   - Concurrent duplicate: its INSERT waits on the first request's uncommitted
//     row, then hits the conflict and replays the stored response.
//   - Later duplicate: replays the stored response; nothing runs again.
//   - Same key, different request: CodeIdempotencyKeyReused.
//
// Business errors (op returns a 4xx Response) are stored too, after rolling back
// to a savepoint so no partial effects remain. Internal errors (op returns err)
// roll back everything, key included, so the client can retry.
func (s *Service) WithIdempotency(ctx context.Context, key string, requestHash []byte,
	op func(ctx context.Context, tx pgx.Tx) (Response, error)) (resp Response, replayed bool, err error) {
	if key == "" || len(key) > 255 {
		return Response{}, false, errorf(CodeValidation, "Idempotency-Key must be 1-255 characters")
	}
	err = s.InTx(ctx, func(tx pgx.Tx) error {
		replayed = false
		tag, err := tx.Exec(ctx,
			`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
			key, requestHash)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var storedHash, body []byte
			var status *int32
			if err := tx.QueryRow(ctx,
				`SELECT request_hash, status_code, response_body FROM idempotency_keys WHERE key = $1`, key,
			).Scan(&storedHash, &status, &body); err != nil {
				return err
			}
			if !bytes.Equal(storedHash, requestHash) {
				return errorf(CodeIdempotencyKeyReused,
					"Idempotency-Key %q was already used with a different request", key)
			}
			if status == nil {
				return errorf(CodeIdempotencyInFlight, "a request with Idempotency-Key %q is still in progress", key)
			}
			resp, replayed = Response{Status: int(*status), Body: body}, true
			return nil
		}

		if _, err := tx.Exec(ctx, "SAVEPOINT idempotent_op"); err != nil {
			return err
		}
		r, err := op(ctx, tx)
		if err != nil {
			return err
		}
		if r.Status >= 400 {
			if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT idempotent_op"); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE idempotency_keys SET status_code = $2, response_body = $3 WHERE key = $1`,
			key, r.Status, r.Body); err != nil {
			return err
		}
		resp = r
		return nil
	})
	return resp, replayed, err
}

// PurgeIdempotencyKeys deletes stored responses older than ttl.
func (s *Service) PurgeIdempotencyKeys(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < $1`, s.now().Add(-ttl))
	return tag.RowsAffected(), err
}
