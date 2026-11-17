package ledger

import (
	"encoding/json"
	"time"
)

// Amounts are integers in the currency's minor unit (cents for USD), never floats.

// Account holds money in a single currency.
//
// Balance is the sum of all posted entries. Held is the part of the balance
// reserved by active holds. Available = Balance - Held is what can be spent.
type Account struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Currency       string          `json:"currency"`
	AllowOverdraft bool            `json:"allow_overdraft"`
	Balance        int64           `json:"balance"`
	Held           int64           `json:"held"`
	Available      int64           `json:"available"`
	Metadata       json.RawMessage `json:"metadata"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// Leg is one side of a transaction: a positive amount credits the account
// (its balance goes up), a negative amount debits it.
type Leg struct {
	AccountID string `json:"account_id"`
	Amount    int64  `json:"amount"`
}

// Transaction is an atomic, balanced set of entries. Once written it never changes;
// mistakes are corrected with a reversal.
type Transaction struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Description string          `json:"description"`
	Metadata    json.RawMessage `json:"metadata"`
	ReversesID  *string         `json:"reverses_id"`
	CreatedAt   time.Time       `json:"created_at"`
	Entries     []Entry         `json:"entries"`
}

// Entry is a single posting to one account. BalanceAfter makes account
// statements cheap to read and easy to audit.
type Entry struct {
	ID            int64     `json:"id"`
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Currency      string    `json:"currency"`
	Amount        int64     `json:"amount"`
	BalanceAfter  int64     `json:"balance_after"`
	CreatedAt     time.Time `json:"created_at"`
}

// Hold statuses.
const (
	HoldActive   = "active"
	HoldCaptured = "captured"
	HoldVoided   = "voided"
	HoldExpired  = "expired"
)

// Hold reserves money on an account (authorization) until it is captured,
// voided or expires, like a card authorization.
type Hold struct {
	ID                   string          `json:"id"`
	AccountID            string          `json:"account_id"`
	DestinationAccountID string          `json:"destination_account_id"`
	Currency             string          `json:"currency"`
	Amount               int64           `json:"amount"`
	CapturedAmount       int64           `json:"captured_amount"`
	Status               string          `json:"status"`
	Description          string          `json:"description"`
	Metadata             json.RawMessage `json:"metadata"`
	ExpiresAt            time.Time       `json:"expires_at"`
	TransactionID        *string         `json:"transaction_id"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}
