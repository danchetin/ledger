package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/danchetin/ledger/internal/ledger"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func errorJSON(code, message string) []byte {
	var b errorBody
	b.Error.Code, b.Error.Message = code, message
	out, _ := json.Marshal(b)
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(errorJSON(code, message)) //nolint:errcheck
}

// statusFor maps business error codes to HTTP statuses.
func statusFor(code ledger.Code) int {
	switch code {
	case ledger.CodeValidation:
		return http.StatusBadRequest
	case ledger.CodeNotFound:
		return http.StatusNotFound
	case ledger.CodeHoldNotActive, ledger.CodeHoldExpired, ledger.CodeAlreadyReversed, ledger.CodeIdempotencyInFlight:
		return http.StatusConflict
	default: // insufficient funds, currency mismatch, unbalanced, key reuse
		return http.StatusUnprocessableEntity
	}
}

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	if e, ok := ledger.AsError(err); ok {
		writeError(w, statusFor(e.Code), string(e.Code), e.Message)
		return
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
		return
	}
	if errors.Is(err, context.Canceled) {
		return // client went away
	}
	s.log.Error("internal error", "request_id", requestIDFrom(r.Context()), "error", err)
	writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
}

// decode parses a JSON body strictly. An empty body decodes to the zero value.
func decode[T any](body []byte) (T, error) {
	var v T
	if len(bytes.TrimSpace(body)) == 0 {
		return v, nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return v, &ledger.Error{Code: ledger.CodeValidation, Message: "invalid JSON body: " + err.Error()}
	}
	if dec.More() {
		return v, &ledger.Error{Code: ledger.CodeValidation, Message: "invalid JSON body: trailing data"}
	}
	return v, nil
}

type operation func(ctx context.Context, tx pgx.Tx, body []byte) (status int, v any, err error)

// mutate runs a write operation. With an Idempotency-Key header the operation
// runs at most once and its response is replayed for retries; endpoints that
// move money require the header.
func (s *Server) mutate(w http.ResponseWriter, r *http.Request, requireKey bool, op operation) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	ctx := r.Context()
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		if requireKey {
			writeError(w, http.StatusBadRequest, "idempotency_key_required",
				"this endpoint moves money and requires an Idempotency-Key header")
			return
		}
		var status int
		var v any
		err := s.svc.InTx(ctx, func(tx pgx.Tx) (err error) {
			status, v, err = op(ctx, tx, body)
			return err
		})
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		writeJSON(w, status, v)
		return
	}

	// The hash binds the key to this exact request, so reusing a key for a
	// different request is detected instead of silently replaying.
	h := sha256.New()
	io.WriteString(h, r.Method+" "+r.URL.Path+"\n") //nolint:errcheck
	h.Write(body)
	resp, replayed, err := s.svc.WithIdempotency(ctx, key, h.Sum(nil),
		func(ctx context.Context, tx pgx.Tx) (ledger.Response, error) {
			status, v, err := op(ctx, tx, body)
			if e, ok := ledger.AsError(err); ok {
				return ledger.Response{Status: statusFor(e.Code), Body: errorJSON(string(e.Code), e.Message)}, nil
			}
			if err != nil {
				return ledger.Response{}, err
			}
			b, err := json.Marshal(v)
			return ledger.Response{Status: status, Body: append(b, '\n')}, err
		})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if replayed {
		s.replays.Inc()
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.Status)
	w.Write(resp.Body) //nolint:errcheck
}

// ---- accounts ----

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, false, func(ctx context.Context, tx pgx.Tx, body []byte) (int, any, error) {
		req, err := decode[ledger.CreateAccountRequest](body)
		if err != nil {
			return 0, nil, err
		}
		a, err := s.svc.CreateAccountTx(ctx, tx, req)
		return http.StatusCreated, a, err
	})
}

type page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor"`
}

func queryLimit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, &ledger.Error{Code: ledger.CodeValidation, Message: "limit must be a positive integer"}
	}
	return n, nil
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	accounts, next, err := s.svc.ListAccounts(r.Context(), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page[ledger.Account]{Data: accounts, NextCursor: next})
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	a, err := s.svc.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	var before int64
	if c := r.URL.Query().Get("cursor"); c != "" {
		before, err = strconv.ParseInt(c, 10, 64)
		if err != nil || before < 1 {
			s.writeErr(w, r, &ledger.Error{Code: ledger.CodeValidation, Message: "cursor must come from next_cursor"})
			return
		}
	}
	entries, next, err := s.svc.ListEntries(r.Context(), r.PathValue("id"), limit, before)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	cursor := ""
	if next != 0 {
		cursor = strconv.FormatInt(next, 10)
	}
	writeJSON(w, http.StatusOK, page[ledger.Entry]{Data: entries, NextCursor: cursor})
}

// ---- transfers and transactions ----

func (s *Server) createTransfer(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, true, func(ctx context.Context, tx pgx.Tx, body []byte) (int, any, error) {
		req, err := decode[ledger.TransferRequest](body)
		if err != nil {
			return 0, nil, err
		}
		t, err := s.svc.TransferTx(ctx, tx, req)
		return http.StatusCreated, t, err
	})
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	t, err := s.svc.GetTransaction(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) reverseTransaction(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, true, func(ctx context.Context, tx pgx.Tx, body []byte) (int, any, error) {
		req, err := decode[ledger.ReverseRequest](body)
		if err != nil {
			return 0, nil, err
		}
		t, err := s.svc.ReverseTx(ctx, tx, r.PathValue("id"), req)
		return http.StatusCreated, t, err
	})
}

// ---- holds ----

func (s *Server) authorizeHold(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, true, func(ctx context.Context, tx pgx.Tx, body []byte) (int, any, error) {
		req, err := decode[ledger.AuthorizeHoldRequest](body)
		if err != nil {
			return 0, nil, err
		}
		h, err := s.svc.AuthorizeHoldTx(ctx, tx, req)
		return http.StatusCreated, h, err
	})
}

func (s *Server) getHold(w http.ResponseWriter, r *http.Request) {
	h, err := s.svc.GetHold(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) captureHold(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, true, func(ctx context.Context, tx pgx.Tx, body []byte) (int, any, error) {
		req, err := decode[ledger.CaptureHoldRequest](body)
		if err != nil {
			return 0, nil, err
		}
		h, err := s.svc.CaptureHoldTx(ctx, tx, r.PathValue("id"), req)
		return http.StatusOK, h, err
	})
}

func (s *Server) voidHold(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, true, func(ctx context.Context, tx pgx.Tx, body []byte) (int, any, error) {
		if _, err := decode[struct{}](body); err != nil {
			return 0, nil, err
		}
		h, err := s.svc.VoidHoldTx(ctx, tx, r.PathValue("id"))
		return http.StatusOK, h, err
	})
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	events, next, err := s.svc.ListEvents(r.Context(), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page[ledger.Event]{Data: events, NextCursor: next})
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.Reconcile(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}
