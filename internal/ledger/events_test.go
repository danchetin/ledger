package ledger_test

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/danchetin/ledger/internal/ledger"
)

func (f *fixture) allEvents(cursor string) ([]ledger.Event, string) {
	f.t.Helper()
	var out []ledger.Event
	for {
		page, next, err := f.svc.ListEvents(f.ctx, cursor, 2)
		if err != nil {
			f.t.Fatalf("list events: %v", err)
		}
		out = append(out, page...)
		if len(page) == 0 {
			return out, next
		}
		cursor = next
	}
}

func TestEventsDescribeCommittedChangesInOrder(t *testing.T) {
	f := newFixture(t)
	alice := f.funded("alice", "USD", 1000) // account.created ×2 (world, alice), transaction.posted
	bob := f.account("bob", "USD")
	tr := f.transfer(alice.ID, bob.ID, 100)
	h, err := f.svc.AuthorizeHold(f.ctx, ledger.AuthorizeHoldRequest{AccountID: alice.ID, DestinationAccountID: bob.ID, Amount: 50})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CaptureHold(f.ctx, h.ID, ledger.CaptureHoldRequest{}); err != nil {
		t.Fatal(err)
	}
	// Failed operations leave no events behind.
	_, err = f.svc.Transfer(f.ctx, ledger.TransferRequest{FromAccountID: bob.ID, ToAccountID: alice.ID, Amount: 10_000})
	requireCode(t, err, ledger.CodeInsufficientFunds)

	events, cursor := f.allEvents("")
	var types []string
	for _, e := range events {
		types = append(types, e.Type)
	}
	want := []string{
		"account.created", "account.created", "transaction.posted", "account.created",
		"transaction.posted", "hold.authorized", "transaction.posted", "hold.captured",
	}
	if len(types) != len(want) {
		t.Fatalf("events = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("events = %v, want %v", types, want)
		}
	}
	var posted ledger.Transaction
	if err := json.Unmarshal(events[4].Payload, &posted); err != nil || posted.ID != tr.ID || len(posted.Entries) != 2 {
		t.Fatalf("payload = %s (%v)", events[4].Payload, err)
	}

	// Polling from the last cursor returns nothing new, and the cursor stays put.
	page, next, err := f.svc.ListEvents(f.ctx, cursor, 10)
	if err != nil || len(page) != 0 || next != cursor {
		t.Fatalf("poll at head: %d events, next %q (want %q), err %v", len(page), next, cursor, err)
	}
	f.transfer(alice.ID, bob.ID, 1)
	page, _, _ = f.svc.ListEvents(f.ctx, cursor, 10)
	if len(page) != 1 || page[0].Type != "transaction.posted" {
		t.Fatalf("new event after cursor = %+v", page)
	}

	_, _, err = f.svc.ListEvents(f.ctx, "garbage", 10)
	requireCode(t, err, ledger.CodeValidation)
}

// Transactions commit in a different order than they insert their events. A
// consumer following the cursor while writers run must still see every event
// exactly once. Some writers deliberately hold their transaction open after
// writing, so later ids commit first.
func TestEventFeedNeverSkipsUnderConcurrency(t *testing.T) {
	f := newFixture(t)
	const nAccounts = 8
	accounts := make([]*ledger.Account, nAccounts)
	for i := range accounts {
		accounts[i] = f.funded("acc", "USD", 1_000_000)
	}
	_, startCursor := f.allEvents("")

	var done atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for i := 0; i < 40; i++ {
				a := rng.IntN(nAccounts)
				b := (a + 1 + rng.IntN(nAccounts-1)) % nAccounts
				slow := rng.IntN(4) == 0
				err := f.svc.InTx(context.Background(), func(tx pgx.Tx) error {
					_, err := f.svc.TransferTx(context.Background(), tx, ledger.TransferRequest{
						FromAccountID: accounts[a].ID, ToAccountID: accounts[b].ID, Amount: 1 + rng.Int64N(100),
					})
					if slow {
						time.Sleep(time.Duration(5+rng.IntN(20)) * time.Millisecond)
					}
					return err
				})
				if err != nil {
					t.Errorf("transfer: %v", err)
					return
				}
			}
		}(w)
	}

	seen := map[string]int{}
	consume := func() {
		cursor := startCursor
		for {
			finished := done.Load()
			page, next, err := f.svc.ListEvents(context.Background(), cursor, 37)
			if err != nil {
				t.Errorf("list events: %v", err)
				return
			}
			for _, e := range page {
				seen[e.ResourceID]++
			}
			cursor = next
			if finished && len(page) == 0 {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	// For comparison, a naive consumer that pages by id alone, run side by side.
	naiveSeen := map[string]bool{}
	naive := func() {
		var lastID int64
		f.svc.Pool().QueryRow(f.ctx, `SELECT COALESCE(max(id), 0) FROM events`).Scan(&lastID) //nolint:errcheck
		for {
			finished := done.Load()
			rows, err := f.svc.Pool().Query(context.Background(),
				`SELECT id, resource_id::text FROM events WHERE id > $1 AND type = 'transaction.posted' ORDER BY id LIMIT 37`, lastID)
			if err != nil {
				t.Errorf("naive poll: %v", err)
				return
			}
			n := 0
			for rows.Next() {
				var id int64
				var res string
				rows.Scan(&id, &res) //nolint:errcheck
				naiveSeen[res] = true
				lastID = id
				n++
			}
			rows.Close()
			if finished && n == 0 {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	consumerDone, naiveDone := make(chan struct{}), make(chan struct{})
	go func() { consume(); close(consumerDone) }()
	go func() { naive(); close(naiveDone) }()
	wg.Wait()
	done.Store(true)
	<-consumerDone
	<-naiveDone

	rows, err := f.svc.Pool().Query(f.ctx, `SELECT id::text FROM transactions`)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	missing := 0
	for _, id := range ids {
		if seen[id] == 0 {
			missing++
		}
	}
	// The funding transfers happened before startCursor and are not expected.
	if got, want := len(seen), len(ids)-nAccounts; got != want || missing != nAccounts {
		t.Fatalf("consumer saw %d transactions, want %d (missing %d)", got, want, missing-nAccounts)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("transaction %s delivered %d times", id, n)
		}
	}
	t.Logf("%d events delivered exactly once; the naive id-cursor consumer lost %d of them",
		len(seen), len(seen)-len(naiveSeen))
}
