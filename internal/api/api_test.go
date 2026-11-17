package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/danchetin/ledger/internal/api"
	"github.com/danchetin/ledger/internal/ledger"
	"github.com/danchetin/ledger/internal/testdb"
)

type client struct {
	t      *testing.T
	base   string
	apiKey string
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

func (r response) errorCode(t *testing.T) string {
	t.Helper()
	var e struct {
		Error struct{ Code string } `json:"error"`
	}
	r.decode(t, &e)
	return e.Error.Code
}

func (c *client) do(method, path, key string, body any) response {
	c.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, c.base+path, rd)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: out}
}

func (c *client) expect(status int, r response) response {
	c.t.Helper()
	if r.status != status {
		c.t.Fatalf("status = %d, want %d, body %s", r.status, status, r.body)
	}
	return r
}

func newServer(t *testing.T, cfg api.Config) *client {
	t.Helper()
	svc := ledger.New(testdb.New(t))
	srv := httptest.NewServer(api.New(svc, cfg).Handler())
	t.Cleanup(srv.Close)
	return &client{t: t, base: srv.URL, apiKey: cfg.APIKey}
}

func (c *client) createAccount(name string, overdraft bool) ledger.Account {
	c.t.Helper()
	var a ledger.Account
	c.expect(201, c.do("POST", "/v1/accounts", "", map[string]any{
		"name": name, "currency": "USD", "allow_overdraft": overdraft,
	})).decode(c.t, &a)
	return a
}

func TestHTTPFlow(t *testing.T) {
	c := newServer(t, api.Config{})
	world := c.createAccount("world", true)
	alice := c.createAccount("alice", false)
	bob := c.createAccount("bob", false)

	deposit := map[string]any{"from_account_id": world.ID, "to_account_id": alice.ID, "amount": 1000}
	if code := c.expect(400, c.do("POST", "/v1/transfers", "", deposit)).errorCode(t); code != "idempotency_key_required" {
		t.Fatalf("code = %s", code)
	}
	c.expect(201, c.do("POST", "/v1/transfers", "dep-1", deposit))

	pay := map[string]any{"from_account_id": alice.ID, "to_account_id": bob.ID, "amount": 300, "description": "rent"}
	first := c.expect(201, c.do("POST", "/v1/transfers", "pay-1", pay))
	if first.header.Get("Idempotent-Replayed") != "" {
		t.Fatal("first request marked as replayed")
	}
	retry := c.expect(201, c.do("POST", "/v1/transfers", "pay-1", pay))
	if retry.header.Get("Idempotent-Replayed") != "true" || !bytes.Equal(retry.body, first.body) {
		t.Fatalf("retry not replayed byte-for-byte: %s vs %s", retry.body, first.body)
	}
	pay["amount"] = 301
	if code := c.expect(422, c.do("POST", "/v1/transfers", "pay-1", pay)).errorCode(t); code != "idempotency_key_reused" {
		t.Fatalf("code = %s", code)
	}

	var a ledger.Account
	c.expect(200, c.do("GET", "/v1/accounts/"+alice.ID, "", nil)).decode(t, &a)
	if a.Balance != 700 || a.Available != 700 {
		t.Fatalf("alice = %+v", a)
	}

	tooMuch := map[string]any{"from_account_id": alice.ID, "to_account_id": bob.ID, "amount": 701}
	if code := c.expect(422, c.do("POST", "/v1/transfers", "pay-2", tooMuch)).errorCode(t); code != "insufficient_funds" {
		t.Fatalf("code = %s", code)
	}
	// The stored failure is replayed too.
	c.expect(422, c.do("POST", "/v1/transfers", "pay-2", tooMuch))

	var tr ledger.Transaction
	first.decode(t, &tr)
	c.expect(201, c.do("POST", "/v1/transactions/"+tr.ID+"/reverse", "rev-1", nil))
	if code := c.expect(409, c.do("POST", "/v1/transactions/"+tr.ID+"/reverse", "rev-2", nil)).errorCode(t); code != "already_reversed" {
		t.Fatalf("code = %s", code)
	}

	var h ledger.Hold
	c.expect(201, c.do("POST", "/v1/holds", "hold-1", map[string]any{
		"account_id": alice.ID, "destination_account_id": bob.ID, "amount": 400,
	})).decode(t, &h)
	c.expect(200, c.do("POST", "/v1/holds/"+h.ID+"/capture", "cap-1", map[string]any{"amount": 150})).decode(t, &h)
	if h.Status != "captured" || h.CapturedAmount != 150 {
		t.Fatalf("hold = %+v", h)
	}
	if code := c.expect(409, c.do("POST", "/v1/holds/"+h.ID+"/void", "void-1", nil)).errorCode(t); code != "hold_not_active" {
		t.Fatalf("code = %s", code)
	}

	var page struct {
		Data       []ledger.Entry `json:"data"`
		NextCursor string         `json:"next_cursor"`
	}
	c.expect(200, c.do("GET", "/v1/accounts/"+alice.ID+"/entries?limit=2", "", nil)).decode(t, &page)
	if len(page.Data) != 2 || page.NextCursor == "" {
		t.Fatalf("page = %+v", page)
	}
	c.expect(200, c.do("GET", "/v1/accounts/"+alice.ID+"/entries?limit=2&cursor="+page.NextCursor, "", nil)).decode(t, &page)
	if len(page.Data) != 2 || page.NextCursor != "" {
		t.Fatalf("second page = %+v", page)
	}

	var report ledger.ReconcileReport
	c.expect(200, c.do("GET", "/v1/reconcile", "", nil)).decode(t, &report)
	if !report.OK {
		t.Fatalf("reconcile: %+v", report)
	}
}

func TestHTTPErrors(t *testing.T) {
	c := newServer(t, api.Config{})
	cases := []struct {
		method, path, key string
		body              any
		status            int
		code              string
	}{
		{"GET", "/v1/accounts/not-a-uuid", "", nil, 400, "validation_error"},
		{"GET", "/v1/accounts/00000000-0000-4000-8000-000000000000", "", nil, 404, "not_found"},
		{"POST", "/v1/accounts", "", `{"name":"x","currency":"USD","color":"red"}`, 400, "validation_error"},
		{"POST", "/v1/accounts", "", `{"name":`, 400, "validation_error"},
		{"POST", "/v1/transfers", "k", `{"amount": 5}`, 400, "validation_error"},
		{"GET", "/v1/accounts?limit=-1", "", nil, 400, "validation_error"},
		{"POST", "/v1/accounts", "", strings.Repeat(" ", api.MaxBodyBytes+1), 413, "request_too_large"},
	}
	for _, tc := range cases {
		r := c.do(tc.method, tc.path, tc.key, tc.body)
		if r.status != tc.status || r.errorCode(t) != tc.code {
			t.Errorf("%s %s: got %d %s, want %d %s", tc.method, tc.path, r.status, r.body, tc.status, tc.code)
		}
	}
	c.expect(404, c.do("GET", "/nope", "", nil))
	c.expect(405, c.do("DELETE", "/v1/accounts", "", nil))
}

func TestHTTPAuthAndOperationalEndpoints(t *testing.T) {
	c := newServer(t, api.Config{APIKey: "s3cret"})
	c.expect(200, c.do("GET", "/v1/accounts", "", nil))

	anon := &client{t: t, base: c.base}
	if code := anon.expect(401, anon.do("GET", "/v1/accounts", "", nil)).errorCode(t); code != "unauthorized" {
		t.Fatalf("code = %s", code)
	}
	wrong := &client{t: t, base: c.base, apiKey: "nope"}
	wrong.expect(401, wrong.do("GET", "/v1/accounts", "", nil))

	anon.expect(200, anon.do("GET", "/healthz", "", nil))
	anon.expect(200, anon.do("GET", "/readyz", "", nil))
	spec := anon.expect(200, anon.do("GET", "/openapi.yaml", "", nil))
	if !bytes.HasPrefix(spec.body, []byte("openapi: 3.1.0")) {
		t.Fatal("unexpected spec")
	}
	metrics := anon.expect(200, anon.do("GET", "/metrics", "", nil))
	for _, want := range []string{
		`ledger_http_requests_total{method="GET",route="/v1/accounts",status="ok"} 1`,
		`ledger_http_requests_total{method="GET",route="/v1/accounts",status="unauthorized"} 2`,
		"ledger_http_request_duration_seconds_bucket",
	} {
		if !bytes.Contains(metrics.body, []byte(want)) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestHTTPConcurrentRetriesPostOnce(t *testing.T) {
	c := newServer(t, api.Config{})
	world := c.createAccount("world", true)
	bob := c.createAccount("bob", false)
	body := map[string]any{"from_account_id": world.ID, "to_account_id": bob.ID, "amount": 250}

	const n = 30
	replayed := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := c.do("POST", "/v1/transfers", "same-key", body)
			if r.status != 201 {
				t.Errorf("status %d: %s", r.status, r.body)
			}
			replayed[i] = r.header.Get("Idempotent-Replayed") == "true"
		}(i)
	}
	wg.Wait()
	fresh := 0
	for _, r := range replayed {
		if !r {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d requests executed, want 1", fresh)
	}
	var a ledger.Account
	c.expect(200, c.do("GET", "/v1/accounts/"+bob.ID, "", nil)).decode(t, &a)
	if a.Balance != 250 {
		t.Fatalf("bob balance = %d, want 250", a.Balance)
	}
}
