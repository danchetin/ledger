package ledger_test

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/danchetin/ledger/internal/ledger"
)

// Many workers move random amounts between a small set of accounts at once.
// Money must be conserved, no account may go negative, and the journal must
// reconcile exactly with the balances.
func TestConcurrentTransfersConserveMoney(t *testing.T) {
	f := newFixture(t)
	const (
		nAccounts = 10
		workers   = 32
		perWorker = 100
		initial   = int64(10_000)
	)
	accounts := make([]*ledger.Account, nAccounts)
	for i := range accounts {
		accounts[i] = f.funded("acc", "USD", initial)
	}

	var ok, insufficient atomic.Int64
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 42))
			for i := 0; i < perWorker; i++ {
				a := rng.IntN(nAccounts)
				b := (a + 1 + rng.IntN(nAccounts-1)) % nAccounts
				_, err := f.svc.Transfer(context.Background(), ledger.TransferRequest{
					FromAccountID: accounts[a].ID, ToAccountID: accounts[b].ID, Amount: 1 + rng.Int64N(3000),
				})
				switch {
				case err == nil:
					ok.Add(1)
				case ledger.IsCode(err, ledger.CodeInsufficientFunds):
					insufficient.Add(1)
				default:
					errs <- err
					return
				}
			}
		}(uint64(w))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	var total int64
	for _, a := range accounts {
		got := f.get(a.ID)
		if got.Balance < 0 {
			t.Fatalf("account %s went negative: %d", a.ID, got.Balance)
		}
		total += got.Balance
	}
	if total != nAccounts*initial {
		t.Fatalf("money not conserved: total %d, want %d", total, nAccounts*initial)
	}
	r := f.requireReconciled()
	if r.Transactions != nAccounts+ok.Load() || r.Entries != 2*(nAccounts+ok.Load()) {
		t.Fatalf("report %+v does not match %d successful transfers", r, ok.Load())
	}
	if ok.Load() == 0 || insufficient.Load() == 0 {
		t.Fatalf("test did not exercise both paths: ok=%d insufficient=%d", ok.Load(), insufficient.Load())
	}
	t.Logf("%d transfers posted, %d rejected for insufficient funds", ok.Load(), insufficient.Load())
}

// 50 holds of 100 race for 1000: exactly 10 may succeed. Then holds are
// captured, voided and spent around concurrently, and everything must still add up.
func TestConcurrentHoldsNeverOverspend(t *testing.T) {
	f := newFixture(t)
	customer := f.funded("customer", "USD", 1000)
	merchant := f.account("merchant", "USD")
	shop := f.account("shop", "USD")

	var mu sync.Mutex
	var holds []string
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := f.svc.AuthorizeHold(context.Background(), ledger.AuthorizeHoldRequest{
				AccountID: customer.ID, DestinationAccountID: merchant.ID, Amount: 100,
			})
			if err != nil {
				if !ledger.IsCode(err, ledger.CodeInsufficientFunds) {
					t.Errorf("authorize: %v", err)
				}
				return
			}
			mu.Lock()
			holds = append(holds, h.ID)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(holds) != 10 {
		t.Fatalf("%d holds authorized, want exactly 10", len(holds))
	}

	var captured, spent atomic.Int64
	for i, id := range holds {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			if i%2 == 0 {
				amount := int64(10 * (i + 1))
				if _, err := f.svc.CaptureHold(context.Background(), id, ledger.CaptureHoldRequest{Amount: amount}); err != nil {
					t.Errorf("capture: %v", err)
					return
				}
				captured.Add(amount)
			} else if _, err := f.svc.VoidHold(context.Background(), id); err != nil {
				t.Errorf("void: %v", err)
			}
		}(i, id)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.svc.Transfer(context.Background(), ledger.TransferRequest{
				FromAccountID: customer.ID, ToAccountID: shop.ID, Amount: 50,
			})
			if err == nil {
				spent.Add(50)
			} else if !ledger.IsCode(err, ledger.CodeInsufficientFunds) {
				t.Errorf("transfer: %v", err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.svc.ExpireHolds(context.Background(), 10); err != nil {
				t.Errorf("expire: %v", err)
			}
		}()
	}
	wg.Wait()

	c := f.get(customer.ID)
	if c.Held != 0 || c.Balance != 1000-captured.Load()-spent.Load() {
		t.Fatalf("customer = %+v, captured %d, spent %d", c, captured.Load(), spent.Load())
	}
	f.requireBalance(merchant.ID, captured.Load())
	f.requireBalance(shop.ID, spent.Load())
	f.requireReconciled()
}

func TestConcurrentReversalsOnlyOneWins(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 1000)
	bob := f.account("bob", "USD")
	tr := f.transfer(alice.ID, bob.ID, 600)

	var wins, already atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.svc.Reverse(context.Background(), tr.ID, ledger.ReverseRequest{})
			switch {
			case err == nil:
				wins.Add(1)
			case ledger.IsCode(err, ledger.CodeAlreadyReversed):
				already.Add(1)
			default:
				t.Errorf("reverse: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || already.Load() != 19 {
		t.Fatalf("wins=%d already=%d, want 1 and 19", wins.Load(), already.Load())
	}
	f.requireBalance(alice.ID, 1000)
	f.requireReconciled()
}

// transferOp adapts TransferTx to the shape WithIdempotency expects, the same way
// the HTTP layer does: business errors become a stored 422 response.
func transferOp(svc *ledger.Service, req ledger.TransferRequest) func(context.Context, pgx.Tx) (ledger.Response, error) {
	return func(ctx context.Context, tx pgx.Tx) (ledger.Response, error) {
		tr, err := svc.TransferTx(ctx, tx, req)
		if e, ok := ledger.AsError(err); ok {
			body, _ := json.Marshal(map[string]string{"code": string(e.Code)})
			return ledger.Response{Status: 422, Body: body}, nil
		}
		if err != nil {
			return ledger.Response{}, err
		}
		body, _ := json.Marshal(tr)
		return ledger.Response{Status: 201, Body: body}, nil
	}
}

func TestIdempotentRequestRunsOnceUnderConcurrency(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 1000)
	bob := f.account("bob", "USD")
	op := transferOp(f.svc, ledger.TransferRequest{FromAccountID: alice.ID, ToAccountID: bob.ID, Amount: 100})

	const n = 50
	type result struct {
		resp     ledger.Response
		replayed bool
		err      error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, replayed, err := f.svc.WithIdempotency(context.Background(), "pay-42", []byte("hash-A"), op)
			results[i] = result{r, replayed, err}
		}(i)
	}
	wg.Wait()

	fresh := 0
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("error: %v", r.err)
		}
		if r.resp.Status != 201 || string(r.resp.Body) != string(results[0].resp.Body) {
			t.Fatalf("responses differ: %d %s", r.resp.Status, r.resp.Body)
		}
		if !r.replayed {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d requests executed, want exactly 1", fresh)
	}
	f.requireBalance(bob.ID, 100)

	_, _, err := f.svc.WithIdempotency(f.ctx, "pay-42", []byte("hash-B"), op)
	requireCode(t, err, ledger.CodeIdempotencyKeyReused)
	f.requireReconciled()
}

func TestIdempotencyStoresBusinessErrorsWithoutPartialEffects(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 500)
	bob := f.account("bob", "USD")

	// Two transfers in one request: the first succeeds, the second fails. The
	// savepoint must undo the first one before the 422 is stored.
	twoStep := func(ctx context.Context, tx pgx.Tx) (ledger.Response, error) {
		if _, err := f.svc.TransferTx(ctx, tx, ledger.TransferRequest{FromAccountID: alice.ID, ToAccountID: bob.ID, Amount: 300}); err != nil {
			return ledger.Response{}, err
		}
		return transferOp(f.svc, ledger.TransferRequest{FromAccountID: alice.ID, ToAccountID: bob.ID, Amount: 300})(ctx, tx)
	}
	resp, replayed, err := f.svc.WithIdempotency(f.ctx, "k1", []byte("h"), twoStep)
	if err != nil || replayed || resp.Status != 422 {
		t.Fatalf("first call: %+v replayed=%v err=%v", resp, replayed, err)
	}
	f.requireBalance(alice.ID, 500)
	f.requireBalance(bob.ID, 0)

	// The stored failure is replayed even though the request would now succeed.
	f.transfer(f.worldAccount("USD").ID, alice.ID, 1000)
	resp, replayed, err = f.svc.WithIdempotency(f.ctx, "k1", []byte("h"), twoStep)
	if err != nil || !replayed || resp.Status != 422 {
		t.Fatalf("replay: %+v replayed=%v err=%v", resp, replayed, err)
	}
	f.requireBalance(bob.ID, 0)
	f.requireReconciled()
}

func TestIdempotencyDoesNotStoreInternalErrors(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 500)
	bob := f.account("bob", "USD")
	req := ledger.TransferRequest{FromAccountID: alice.ID, ToAccountID: bob.ID, Amount: 100}

	boom := errors.New("connection lost")
	_, _, err := f.svc.WithIdempotency(f.ctx, "k2", []byte("h"), func(ctx context.Context, tx pgx.Tx) (ledger.Response, error) {
		if _, err := f.svc.TransferTx(ctx, tx, req); err != nil {
			return ledger.Response{}, err
		}
		return ledger.Response{}, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	f.requireBalance(bob.ID, 0)

	// The key was rolled back with everything else, so a retry runs for real.
	resp, replayed, err := f.svc.WithIdempotency(f.ctx, "k2", []byte("h"), transferOp(f.svc, req))
	if err != nil || replayed || resp.Status != 201 {
		t.Fatalf("retry: %+v replayed=%v err=%v", resp, replayed, err)
	}
	f.requireBalance(bob.ID, 100)

	if n, err := f.svc.PurgeIdempotencyKeys(f.ctx, -1); err != nil || n != 1 {
		t.Fatalf("purge = %d, %v", n, err)
	}
}
