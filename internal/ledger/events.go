package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event types.
const (
	EventAccountCreated    = "account.created"
	EventTransactionPosted = "transaction.posted"
	EventHoldAuthorized    = "hold.authorized"
	EventHoldCaptured      = "hold.captured"
	EventHoldVoided        = "hold.voided"
	EventHoldExpired       = "hold.expired"
)

// Event is one entry of the outbox feed. Consumers process events in the order
// returned and persist Cursor after processing, which gives at-least-once delivery.
type Event struct {
	ID         int64           `json:"id"`
	Type       string          `json:"type"`
	ResourceID string          `json:"resource_id"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
	Cursor     string          `json:"cursor"`
}

// emit writes an event inside tx. It commits or rolls back with the change it describes.
func emit(ctx context.Context, tx pgx.Tx, eventType, resourceID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO events (type, resource_id, payload) VALUES ($1, $2::uuid, $3::jsonb)`,
		eventType, resourceID, string(body))
	return err
}

// parseCursor reads "<tx_id>-<id>"; the empty cursor starts from the beginning.
func parseCursor(cursor string) (txID uint64, id int64, err error) {
	if cursor == "" {
		return 0, 0, nil
	}
	a, b, ok := strings.Cut(cursor, "-")
	if ok {
		txID, err = strconv.ParseUint(a, 10, 64)
		if err == nil {
			id, err = strconv.ParseInt(b, 10, 64)
		}
	}
	if !ok || err != nil || id < 0 {
		return 0, 0, errorf(CodeValidation, "cursor must be a value returned as next_cursor")
	}
	return txID, id, nil
}

// ListEvents returns committed events after cursor, oldest first, and the cursor
// to continue from. When nothing new is available the input cursor is returned,
// so a consumer can keep polling with it.
//
// Only events written by transactions older than every transaction still in
// progress are returned. Anything that commits later is guaranteed to sort after
// the returned cursor, so a consumer that follows next_cursor never skips events.
func (s *Service) ListEvents(ctx context.Context, cursor string, limit int) ([]Event, string, error) {
	txID, id, err := parseCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, tx_id::text, type, resource_id::text, payload, created_at
		 FROM events
		 WHERE (tx_id, id) > ($1::text::xid8, $2::bigint)
		   AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
		 ORDER BY tx_id, id
		 LIMIT $3`, strconv.FormatUint(txID, 10), id, pageSize(limit))
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	events := []Event{}
	next := cursor
	for rows.Next() {
		var e Event
		var tx string
		if err := rows.Scan(&e.ID, &tx, &e.Type, &e.ResourceID, &e.Payload, &e.CreatedAt); err != nil {
			return nil, "", err
		}
		e.Cursor = fmt.Sprintf("%s-%d", tx, e.ID)
		next = e.Cursor
		events = append(events, e)
	}
	return events, next, rows.Err()
}
