package ledger_test

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/danchetin/ledger/internal/ledger"
)

// TestRandomOperationsMatchModel runs a long random sequence of transfers,
// multi-leg transfers, holds, captures, voids and reversals, and after every
// step compares the database with a trivial in-memory model. Any divergence,
// any unexpected error and any reconciliation problem fails the test.
//
// Reproduce a failure with LEDGER_PROPERTY_SEED=<seed from the log>.
func TestRandomOperationsMatchModel(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("LEDGER_PROPERTY_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad LEDGER_PROPERTY_SEED: %v", err)
		}
		seed = v
	}
	steps := 300
	if testing.Short() {
		steps = 60
	}
	t.Logf("seed %d, %d steps", seed, steps)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	f := newFixture(t)
	type acct struct {
		id        string
		balance   int64
		held      int64
		overdraft bool
	}
	type hold struct {
		id, src, dst string
		amount       int64
		active       bool
	}
	type txn struct {
		id       string
		legs     []ledger.Leg
		reversed bool
	}
	var accts []*acct
	byID := map[string]*acct{}
	world := f.worldAccount("USD")
	accts = append(accts, &acct{id: world.ID, overdraft: true})
	for i := 0; i < 5; i++ {
		a := f.account("p", "USD")
		accts = append(accts, &acct{id: a.ID})
	}
	for _, a := range accts {
		byID[a.id] = a
	}
	var holds []*hold
	var txns []*txn

	// canApply reports whether legs pass the balance rules in the model.
	canApply := func(legs []ledger.Leg) bool {
		for _, l := range legs {
			a := byID[l.AccountID]
			if l.Amount < 0 && !a.overdraft && a.balance+l.Amount-a.held < 0 {
				return false
			}
		}
		return true
	}
	apply := func(legs []ledger.Leg) {
		for _, l := range legs {
			byID[l.AccountID].balance += l.Amount
		}
	}
	pick := func(n int) []*acct { // n distinct accounts
		perm := rng.Perm(len(accts))
		out := make([]*acct, n)
		for i := range out {
			out[i] = accts[perm[i]]
		}
		return out
	}
	expect := func(step int, op string, wantOK bool, err error, code ledger.Code) bool {
		t.Helper()
		if wantOK && err != nil {
			t.Fatalf("step %d %s: unexpected error %v", step, op, err)
		}
		if !wantOK && !ledger.IsCode(err, code) {
			t.Fatalf("step %d %s: error = %v, want %s", step, op, err, code)
		}
		return wantOK
	}

	for step := 0; step < steps; step++ {
		switch op := rng.IntN(7); op {
		case 0, 1: // simple transfer
			p := pick(2)
			legs := []ledger.Leg{{AccountID: p[0].id, Amount: -(1 + rng.Int64N(1500))}, {AccountID: p[1].id}}
			legs[1].Amount = -legs[0].Amount
			ok := canApply(legs)
			tr, err := f.svc.Transfer(f.ctx, ledger.TransferRequest{FromAccountID: p[0].id, ToAccountID: p[1].id, Amount: legs[1].Amount})
			if expect(step, "transfer", ok, err, ledger.CodeInsufficientFunds) {
				apply(legs)
				txns = append(txns, &txn{id: tr.ID, legs: legs})
			}
		case 2: // three-leg transfer
			p := pick(3)
			total := 2 + rng.Int64N(1500)
			part := 1 + rng.Int64N(total-1)
			legs := []ledger.Leg{
				{AccountID: p[0].id, Amount: -total},
				{AccountID: p[1].id, Amount: part},
				{AccountID: p[2].id, Amount: total - part},
			}
			ok := canApply(legs)
			tr, err := f.svc.Transfer(f.ctx, ledger.TransferRequest{Legs: legs})
			if expect(step, "multi-leg", ok, err, ledger.CodeInsufficientFunds) {
				apply(legs)
				txns = append(txns, &txn{id: tr.ID, legs: legs})
			}
		case 3: // authorize
			p := pick(2)
			amount := 1 + rng.Int64N(800)
			ok := p[0].overdraft || p[0].balance-p[0].held-amount >= 0
			h, err := f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{AccountID: p[0].id, DestinationAccountID: p[1].id, Amount: amount})
			if expect(step, "authorize", ok, err, ledger.CodeInsufficientFunds) {
				p[0].held += amount
				holds = append(holds, &hold{id: h.ID, src: p[0].id, dst: p[1].id, amount: amount, active: true})
			}
		case 4: // capture or void a random hold (inactive ones must be rejected)
			if len(holds) == 0 {
				continue
			}
			h := holds[rng.IntN(len(holds))]
			if rng.IntN(2) == 0 {
				amount := 1 + rng.Int64N(h.amount)
				got, err := f.svc.CaptureHold(f.ctx, h.id, ledger.CaptureHoldRequest{Amount: amount})
				if expect(step, "capture", h.active, err, ledger.CodeHoldNotActive) {
					byID[h.src].held -= h.amount
					legs := []ledger.Leg{{AccountID: h.src, Amount: -amount}, {AccountID: h.dst, Amount: amount}}
					apply(legs)
					h.active = false
					txns = append(txns, &txn{id: *got.TransactionID, legs: legs})
				}
			} else {
				_, err := f.svc.VoidHold(f.ctx, h.id)
				if expect(step, "void", h.active, err, ledger.CodeHoldNotActive) {
					byID[h.src].held -= h.amount
					h.active = false
				}
			}
		case 5, 6: // reverse a random transaction
			if len(txns) == 0 {
				continue
			}
			tx := txns[rng.IntN(len(txns))]
			legs := make([]ledger.Leg, len(tx.legs))
			for i, l := range tx.legs {
				legs[i] = ledger.Leg{AccountID: l.AccountID, Amount: -l.Amount}
			}
			_, err := f.svc.Reverse(f.ctx, tx.id, ledger.ReverseRequest{})
			switch {
			case tx.reversed:
				expect(step, "reverse", false, err, ledger.CodeAlreadyReversed)
			case expect(step, "reverse", canApply(legs), err, ledger.CodeInsufficientFunds):
				apply(legs)
				tx.reversed = true
			}
		}

		for _, a := range accts {
			got := f.get(a.id)
			if got.Balance != a.balance || got.Held != a.held {
				t.Fatalf("step %d: account %s is balance=%d held=%d, model says balance=%d held=%d",
					step, a.id, got.Balance, got.Held, a.balance, a.held)
			}
		}
	}
	f.requireReconciled()
}
