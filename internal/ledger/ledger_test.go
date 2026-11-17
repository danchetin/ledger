package ledger_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/danchetin/ledger/internal/ledger"
)

func TestTransferMovesMoneyAndWritesEntries(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 1000)
	bob := f.account("bob", "USD")

	tr := f.transfer(alice.ID, bob.ID, 300)

	f.requireBalance(alice.ID, 700)
	f.requireBalance(bob.ID, 300)
	if tr.Kind != "transfer" || len(tr.Entries) != 2 {
		t.Fatalf("unexpected transaction: %+v", tr)
	}
	var sum int64
	for _, e := range tr.Entries {
		sum += e.Amount
		switch e.AccountID {
		case alice.ID:
			if e.Amount != -300 || e.BalanceAfter != 700 {
				t.Fatalf("alice entry = %+v", e)
			}
		case bob.ID:
			if e.Amount != 300 || e.BalanceAfter != 300 {
				t.Fatalf("bob entry = %+v", e)
			}
		}
	}
	if sum != 0 {
		t.Fatalf("entries sum to %d", sum)
	}
	got, err := f.svc.GetTransaction(f.ctx, tr.ID)
	if err != nil || len(got.Entries) != 2 {
		t.Fatalf("get transaction: %v %+v", err, got)
	}
	r := f.requireReconciled()
	if r.TotalsByCurrency["USD"] != 0 || r.Transactions != 2 || r.Entries != 4 {
		t.Fatalf("report = %+v", r)
	}
}

func TestInsufficientFundsChangesNothing(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 100)
	bob := f.account("bob", "USD")

	_, err := f.svc.Transfer(f.ctx, ledger.TransferRequest{FromAccountID: alice.ID, ToAccountID: bob.ID, Amount: 101})
	requireCode(t, err, ledger.CodeInsufficientFunds)

	f.requireBalance(alice.ID, 100)
	f.requireBalance(bob.ID, 0)
	if r := f.requireReconciled(); r.Transactions != 1 {
		t.Fatalf("failed transfer left a transaction behind: %d transactions", r.Transactions)
	}
}

func TestMultiLegTransfer(t *testing.T) {
	f := newFixture(t)
	buyer := f.funded("buyer", "USD", 1000)
	seller := f.account("seller", "USD")
	fees := f.account("platform fees", "USD")

	_, err := f.svc.Transfer(f.ctx, ledger.TransferRequest{Legs: []ledger.Leg{
		{AccountID: buyer.ID, Amount: -1000},
		{AccountID: seller.ID, Amount: 970},
		{AccountID: fees.ID, Amount: 30},
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.requireBalance(buyer.ID, 0)
	f.requireBalance(seller.ID, 970)
	f.requireBalance(fees.ID, 30)
	f.requireReconciled()
}

func TestLegsMustBalancePerCurrency(t *testing.T) {
	f := newFixture(t)
	usd := f.funded("usd", "USD", 500)
	eur := f.account("eur", "EUR")
	_, err := f.svc.Transfer(f.ctx, ledger.TransferRequest{Legs: []ledger.Leg{
		{AccountID: usd.ID, Amount: -100},
		{AccountID: eur.ID, Amount: 100},
	}})
	requireCode(t, err, ledger.CodeUnbalanced)

	// A currency exchange is four legs: each currency balances on its own.
	usdFX := f.account("fx desk usd", "USD")
	eurFX := f.funded("fx desk eur", "EUR", 1000)
	_, err = f.svc.Transfer(f.ctx, ledger.TransferRequest{Legs: []ledger.Leg{
		{AccountID: usd.ID, Amount: -100},
		{AccountID: usdFX.ID, Amount: 100},
		{AccountID: eurFX.ID, Amount: -92},
		{AccountID: eur.ID, Amount: 92},
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.requireBalance(eur.ID, 92)
	f.requireReconciled()
}

func TestTransferValidation(t *testing.T) {
	f := newFixture(t)
	a := f.funded("a", "USD", 100)
	b := f.account("b", "USD")
	missing := "00000000-0000-4000-8000-000000000000"

	cases := []struct {
		name string
		req  ledger.TransferRequest
		code ledger.Code
	}{
		{"zero amount", ledger.TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID}, ledger.CodeValidation},
		{"negative amount", ledger.TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, Amount: -5}, ledger.CodeValidation},
		{"same account", ledger.TransferRequest{FromAccountID: a.ID, ToAccountID: a.ID, Amount: 5}, ledger.CodeValidation},
		{"bad id", ledger.TransferRequest{FromAccountID: "nope", ToAccountID: b.ID, Amount: 5}, ledger.CodeValidation},
		{"unknown account", ledger.TransferRequest{FromAccountID: a.ID, ToAccountID: missing, Amount: 5}, ledger.CodeNotFound},
		{"one leg", ledger.TransferRequest{Legs: []ledger.Leg{{AccountID: a.ID, Amount: 5}}}, ledger.CodeValidation},
		{"zero leg", ledger.TransferRequest{Legs: []ledger.Leg{{AccountID: a.ID, Amount: 0}, {AccountID: b.ID, Amount: 0}}}, ledger.CodeValidation},
		{"duplicate leg", ledger.TransferRequest{Legs: []ledger.Leg{{AccountID: a.ID, Amount: -5}, {AccountID: a.ID, Amount: 5}}}, ledger.CodeValidation},
		{"both forms", ledger.TransferRequest{Amount: 5, Legs: []ledger.Leg{{AccountID: a.ID, Amount: -5}, {AccountID: b.ID, Amount: 5}}}, ledger.CodeValidation},
		{"bad metadata", ledger.TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, Amount: 5, Metadata: json.RawMessage(`[1]`)}, ledger.CodeValidation},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := f.svc.Transfer(f.ctx, c.req)
			requireCode(t, err, c.code)
		})
	}
	f.requireBalance(a.ID, 100)
}

func TestAccountValidationAndOverdraft(t *testing.T) {
	f := newFixture(t)
	for _, req := range []ledger.CreateAccountRequest{
		{Name: "", Currency: "USD"},
		{Name: "x", Currency: "usd"},
		{Name: "x", Currency: "DOLLAR"},
		{Name: "x", Currency: "USD", Metadata: json.RawMessage(`"str"`)},
	} {
		_, err := f.svc.CreateAccount(f.ctx, req)
		requireCode(t, err, ledger.CodeValidation)
	}

	credit, err := f.svc.CreateAccount(f.ctx, ledger.CreateAccountRequest{
		Name: "credit line", Currency: "USD", AllowOverdraft: true, Metadata: json.RawMessage(`{"limit":"none"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	other := f.account("other", "USD")
	f.transfer(credit.ID, other.ID, 250)
	if a := f.get(credit.ID); a.Balance != -250 || a.Available != -250 {
		t.Fatalf("overdraft account = %+v", a)
	}
	f.requireReconciled()
}

func TestReversal(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 1000)
	bob := f.account("bob", "USD")
	carol := f.account("carol", "USD")
	tr := f.transfer(alice.ID, bob.ID, 400)

	rev, err := f.svc.Reverse(f.ctx, tr.ID, ledger.ReverseRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if rev.Kind != "reversal" || rev.ReversesID == nil || *rev.ReversesID != tr.ID {
		t.Fatalf("reversal = %+v", rev)
	}
	f.requireBalance(alice.ID, 1000)
	f.requireBalance(bob.ID, 0)

	_, err = f.svc.Reverse(f.ctx, tr.ID, ledger.ReverseRequest{})
	requireCode(t, err, ledger.CodeAlreadyReversed)
	_, err = f.svc.Reverse(f.ctx, rev.ID, ledger.ReverseRequest{})
	requireCode(t, err, ledger.CodeValidation)

	// Money already moved on cannot be pulled back past zero.
	spent := f.transfer(alice.ID, bob.ID, 300)
	f.transfer(bob.ID, carol.ID, 300)
	_, err = f.svc.Reverse(f.ctx, spent.ID, ledger.ReverseRequest{})
	requireCode(t, err, ledger.CodeInsufficientFunds)
	f.requireReconciled()
}

func TestHoldCaptureReleasesRemainder(t *testing.T) {
	f := newFixture(t)
	customer := f.funded("customer", "USD", 1000)
	merchant := f.account("merchant", "USD")

	h, err := f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{
		AccountID: customer.ID, DestinationAccountID: merchant.ID, Amount: 400, Description: "taxi ride",
	})
	if err != nil {
		t.Fatal(err)
	}
	if a := f.get(customer.ID); a.Balance != 1000 || a.Held != 400 || a.Available != 600 {
		t.Fatalf("after authorize: %+v", a)
	}
	// Held money cannot be spent elsewhere.
	_, err = f.svc.Transfer(f.ctx, ledger.TransferRequest{FromAccountID: customer.ID, ToAccountID: merchant.ID, Amount: 700})
	requireCode(t, err, ledger.CodeInsufficientFunds)

	captured, err := f.svc.CaptureHold(f.ctx, h.ID, ledger.CaptureHoldRequest{Amount: 250})
	if err != nil {
		t.Fatal(err)
	}
	if captured.Status != ledger.HoldCaptured || captured.CapturedAmount != 250 || captured.TransactionID == nil {
		t.Fatalf("captured hold = %+v", captured)
	}
	if a := f.get(customer.ID); a.Balance != 750 || a.Held != 0 || a.Available != 750 {
		t.Fatalf("after capture: %+v", a)
	}
	f.requireBalance(merchant.ID, 250)

	tr, err := f.svc.GetTransaction(f.ctx, *captured.TransactionID)
	if err != nil || tr.Kind != "capture" || tr.Description != "taxi ride" {
		t.Fatalf("capture transaction = %+v, %v", tr, err)
	}
	_, err = f.svc.CaptureHold(f.ctx, h.ID, ledger.CaptureHoldRequest{})
	requireCode(t, err, ledger.CodeHoldNotActive)
	_, err = f.svc.VoidHold(f.ctx, h.ID)
	requireCode(t, err, ledger.CodeHoldNotActive)
	f.requireReconciled()
}

func TestHoldVoidAndValidation(t *testing.T) {
	f := newFixture(t)
	customer := f.funded("customer", "USD", 500)
	merchant := f.account("merchant", "USD")
	euro := f.account("euro shop", "EUR")

	_, err := f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{AccountID: customer.ID, DestinationAccountID: merchant.ID, Amount: 501})
	requireCode(t, err, ledger.CodeInsufficientFunds)
	_, err = f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{AccountID: customer.ID, DestinationAccountID: euro.ID, Amount: 10})
	requireCode(t, err, ledger.CodeCurrencyMismatch)
	_, err = f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{AccountID: customer.ID, DestinationAccountID: merchant.ID, Amount: 10, ExpiresInSeconds: -1})
	requireCode(t, err, ledger.CodeValidation)

	h, err := f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{AccountID: customer.ID, DestinationAccountID: merchant.ID, Amount: 500})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.CaptureHold(f.ctx, h.ID, ledger.CaptureHoldRequest{Amount: 501})
	requireCode(t, err, ledger.CodeValidation)

	voided, err := f.svc.VoidHold(f.ctx, h.ID)
	if err != nil || voided.Status != ledger.HoldVoided {
		t.Fatalf("void: %+v %v", voided, err)
	}
	if a := f.get(customer.ID); a.Held != 0 || a.Available != 500 {
		t.Fatalf("after void: %+v", a)
	}
	f.requireReconciled()
}

func TestHoldExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	f := newFixture(t, ledger.WithClock(func() time.Time { return now }))
	customer := f.funded("customer", "USD", 300)
	merchant := f.account("merchant", "USD")

	h, err := f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{
		AccountID: customer.ID, DestinationAccountID: merchant.ID, Amount: 300, ExpiresInSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.svc.ExpireHolds(f.ctx, 100); err != nil || n != 0 {
		t.Fatalf("expired %d holds before their time (err %v)", n, err)
	}

	now = now.Add(61 * time.Second)
	_, err = f.svc.CaptureHold(f.ctx, h.ID, ledger.CaptureHoldRequest{})
	requireCode(t, err, ledger.CodeHoldExpired)

	n, err := f.svc.ExpireHolds(f.ctx, 100)
	if err != nil || n != 1 {
		t.Fatalf("ExpireHolds = %d, %v", n, err)
	}
	got, err := f.svc.GetHold(f.ctx, h.ID)
	if err != nil || got.Status != ledger.HoldExpired {
		t.Fatalf("hold after expiry = %+v, %v", got, err)
	}
	if a := f.get(customer.ID); a.Held != 0 || a.Available != 300 {
		t.Fatalf("after expiry: %+v", a)
	}
	f.requireReconciled()
}

// The database enforces the core invariants even if the application code is wrong.
func TestDatabaseRejectsBrokenWrites(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 100)
	bob := f.account("bob", "USD")
	pool := f.svc.Pool()

	unbalanced := func(tx pgx.Tx) error {
		var id string
		if err := tx.QueryRow(f.ctx, `INSERT INTO transactions (kind) VALUES ('transfer') RETURNING id::text`).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(f.ctx, `INSERT INTO entries (transaction_id, account_id, currency, amount, balance_after)
			VALUES ($1::uuid, $2::uuid, 'USD', -50, 50), ($1::uuid, $3::uuid, 'USD', 40, 40)`, id, alice.ID, bob.ID)
		return err
	}
	requireSQLState(t, pgx.BeginFunc(f.ctx, pool, unbalanced), "23514")

	wrongCurrency := func(tx pgx.Tx) error {
		var id string
		if err := tx.QueryRow(f.ctx, `INSERT INTO transactions (kind) VALUES ('transfer') RETURNING id::text`).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(f.ctx, `INSERT INTO entries (transaction_id, account_id, currency, amount, balance_after)
			VALUES ($1::uuid, $2::uuid, 'EUR', -50, 50), ($1::uuid, $3::uuid, 'EUR', 50, 50)`, id, alice.ID, bob.ID)
		return err
	}
	requireSQLState(t, pgx.BeginFunc(f.ctx, pool, wrongCurrency), "23503")

	_, err := pool.Exec(f.ctx, `UPDATE entries SET amount = amount * 2`)
	requireSQLState(t, err, "23001")
	_, err = pool.Exec(f.ctx, `DELETE FROM transactions`)
	requireSQLState(t, err, "23001")
	_, err = pool.Exec(f.ctx, `UPDATE accounts SET balance = -1 WHERE id = $1::uuid`, bob.ID)
	requireSQLState(t, err, "23514")

	f.requireReconciled()
}

func requireSQLState(t *testing.T, err error, state string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != state {
		t.Fatalf("error = %v, want SQLSTATE %s", err, state)
	}
}

func TestPagination(t *testing.T) {
	f := newFixture(t)
	a := f.funded("a", "USD", 1000)
	b := f.account("b", "USD")
	for i := 0; i < 7; i++ {
		f.transfer(a.ID, b.ID, 10)
	}

	var all []ledger.Entry
	var cursor int64
	for {
		page, next, err := f.svc.ListEntries(f.ctx, b.ID, 3, cursor)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page...)
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(all) != 7 || all[0].BalanceAfter != 70 || all[6].BalanceAfter != 10 {
		t.Fatalf("statement = %+v", all)
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID >= all[i-1].ID {
			t.Fatalf("statement not newest-first at %d", i)
		}
	}

	seen := map[string]bool{}
	after := ""
	for {
		page, next, err := f.svc.ListAccounts(f.ctx, 1, after)
		if err != nil {
			t.Fatal(err)
		}
		for _, acc := range page {
			seen[acc.ID] = true
		}
		if next == "" {
			break
		}
		after = next
	}
	if len(seen) != 3 { // a, b, world
		t.Fatalf("listed %d accounts, want 3", len(seen))
	}
	_, _, err := f.svc.ListEntries(f.ctx, "00000000-0000-4000-8000-000000000000", 10, 0)
	requireCode(t, err, ledger.CodeNotFound)
}
