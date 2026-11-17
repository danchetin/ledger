package ledger

import (
	"errors"
	"fmt"
)

// Code is a stable, machine-readable error identifier returned by the API.
type Code string

const (
	CodeValidation           Code = "validation_error"
	CodeNotFound             Code = "not_found"
	CodeInsufficientFunds    Code = "insufficient_funds"
	CodeCurrencyMismatch     Code = "currency_mismatch"
	CodeUnbalanced           Code = "unbalanced_transaction"
	CodeHoldNotActive        Code = "hold_not_active"
	CodeHoldExpired          Code = "hold_expired"
	CodeAlreadyReversed      Code = "already_reversed"
	CodeIdempotencyKeyReused Code = "idempotency_key_reused"
	CodeIdempotencyInFlight  Code = "idempotency_key_in_flight"
)

// Error is a business-rule error: the request was understood but cannot be applied.
// Anything that is not an *Error is an internal failure.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

func errorf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// AsError returns the business error inside err, if there is one.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// IsCode reports whether err is a business error with the given code.
func IsCode(err error, code Code) bool {
	e, ok := AsError(err)
	return ok && e.Code == code
}
