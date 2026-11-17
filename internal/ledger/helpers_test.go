package ledger_test

import (
	"context"
	"testing"

	"github.com/danchetin/ledger/internal/ledger"
	"github.com/danchetin/ledger/internal/testdb"
)

type fixture struct {
	t     *testing.T
	ctx   context.Context
	svc   *ledger.Service
	world map[string]*ledger.Account // one overdraft "outside world" account per currency
}

func newFixture(t *testing.T, opts ...ledger.Option) *fixture {
	t.Helper()
	pool := testdb.New(t)
	return &fixture{t: t, ctx: context.Background(), svc: ledger.New(pool, opts...), world: map[string]*ledger.Account{}}
}

func (f *fixture) account(name, currency string) *ledger.Account {
	f.t.Helper()
	a, err := f.svc.CreateAccount(f.ctx, ledger.CreateAccountRequest{Name: name, Currency: currency})
	if err != nil {
		f.t.Fatalf("create account %s: %v", name, err)
	}
	return a
}

func (f *fixture) worldAccount(currency string) *ledger.Account {
	f.t.Helper()
	if w, ok := f.world[currency]; ok {
		return w
	}
	w, err := f.svc.CreateAccount(f.ctx, ledger.CreateAccountRequest{
		Name: "world " + currency, Currency: currency, AllowOverdraft: true,
	})
	if err != nil {
		f.t.Fatalf("create world account: %v", err)
	}
	f.world[currency] = w
	return w
}

// funded creates an account and deposits amount into it from the world account.
func (f *fixture) funded(name, currency string, amount int64) *ledger.Account {
	f.t.Helper()
	a := f.account(name, currency)
	if amount > 0 {
		f.transfer(f.worldAccount(currency).ID, a.ID, amount)
	}
	return a
}

func (f *fixture) transfer(from, to string, amount int64) *ledger.Transaction {
	f.t.Helper()
	tr, err := f.svc.Transfer(f.ctx, ledger.TransferRequest{FromAccountID: from, ToAccountID: to, Amount: amount})
	if err != nil {
		f.t.Fatalf("transfer %d from %s to %s: %v", amount, from, to, err)
	}
	return tr
}

func (f *fixture) get(id string) *ledger.Account {
	f.t.Helper()
	a, err := f.svc.GetAccount(f.ctx, id)
	if err != nil {
		f.t.Fatalf("get account %s: %v", id, err)
	}
	return a
}

func (f *fixture) balance(id string) int64 { f.t.Helper(); return f.get(id).Balance }

func (f *fixture) requireBalance(id string, want int64) {
	f.t.Helper()
	if got := f.balance(id); got != want {
		f.t.Fatalf("account %s balance = %d, want %d", id, got, want)
	}
}

func (f *fixture) requireReconciled() *ledger.ReconcileReport {
	f.t.Helper()
	r, err := f.svc.Reconcile(f.ctx)
	if err != nil {
		f.t.Fatalf("reconcile: %v", err)
	}
	if !r.OK {
		f.t.Fatalf("reconcile found problems: %v", r.Problems)
	}
	return r
}

func requireCode(t *testing.T, err error, code ledger.Code) {
	t.Helper()
	if !ledger.IsCode(err, code) {
		t.Fatalf("error = %v, want code %s", err, code)
	}
}
