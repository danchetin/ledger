// Command loadtest drives random transfers against a running ledger over HTTP,
// reports throughput and latency, and then verifies that money was conserved
// and the journal reconciles.
//
//	go run ./cmd/loadtest -url http://localhost:8080 -concurrency 32 -duration 30s
package main

import (
	"bytes"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	url         string
	apiKey      string
	accounts    int
	concurrency int
	duration    time.Duration
	initial     int64
	hot         float64
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", "http://localhost:8080", "ledger base URL")
	flag.StringVar(&c.apiKey, "api-key", os.Getenv("LEDGER_API_KEY"), "API key, if the server requires one")
	flag.IntVar(&c.accounts, "accounts", 100, "number of accounts to move money between")
	flag.IntVar(&c.concurrency, "concurrency", 32, "concurrent clients")
	flag.DurationVar(&c.duration, "duration", 30*time.Second, "how long to run")
	flag.Int64Var(&c.initial, "initial", 1_000_000, "starting balance of each account, minor units")
	flag.Float64Var(&c.hot, "hot", 0, "fraction of transfers that hit account #0 (contention test), 0..1")
	flag.Parse()
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

type client struct {
	http   *http.Client
	base   string
	apiKey string
}

func randomKey() string {
	b := make([]byte, 16)
	crand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

func (c *client) do(method, path string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, rd)
	if err != nil {
		return 0, err
	}
	if method == http.MethodPost {
		req.Header.Set("Idempotency-Key", randomKey())
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, err
		}
	}
	if resp.StatusCode >= 500 {
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, data)
	}
	return resp.StatusCode, nil
}

type account struct {
	ID      string `json:"id"`
	Balance int64  `json:"balance"`
}

func run(cfg config) error {
	c := &client{
		http: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: cfg.concurrency * 2, MaxConnsPerHost: cfg.concurrency * 2},
		},
		base:   cfg.url,
		apiKey: cfg.apiKey,
	}

	var world account
	if _, err := c.do("POST", "/v1/accounts", map[string]any{"name": "loadtest funding", "currency": "USD", "allow_overdraft": true}, &world); err != nil {
		return fmt.Errorf("create funding account: %w", err)
	}
	ids := make([]string, cfg.accounts)
	for i := range ids {
		var a account
		if _, err := c.do("POST", "/v1/accounts", map[string]any{"name": fmt.Sprintf("loadtest %d", i), "currency": "USD"}, &a); err != nil {
			return fmt.Errorf("create account: %w", err)
		}
		if _, err := c.do("POST", "/v1/transfers", map[string]any{"from_account_id": world.ID, "to_account_id": a.ID, "amount": cfg.initial}, nil); err != nil {
			return fmt.Errorf("fund account: %w", err)
		}
		ids[i] = a.ID
	}
	fmt.Printf("setup: %d accounts with %d each; running %d clients for %s (hot=%.2f)\n",
		cfg.accounts, cfg.initial, cfg.concurrency, cfg.duration, cfg.hot)

	var ok, rejected, failed atomic.Int64
	latencies := make([][]time.Duration, cfg.concurrency)
	deadline := time.Now().Add(cfg.duration)
	var wg sync.WaitGroup
	var firstErr atomic.Value
	start := time.Now()
	for w := 0; w < cfg.concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), uint64(time.Now().UnixNano())))
			for time.Now().Before(deadline) {
				a := rng.IntN(len(ids))
				if cfg.hot > 0 && rng.Float64() < cfg.hot {
					a = 0
				}
				b := (a + 1 + rng.IntN(len(ids)-1)) % len(ids)
				t0 := time.Now()
				status, err := c.do("POST", "/v1/transfers", map[string]any{
					"from_account_id": ids[a], "to_account_id": ids[b], "amount": 1 + rng.Int64N(1000),
				}, nil)
				latencies[w] = append(latencies[w], time.Since(t0))
				switch {
				case err != nil:
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				case status == http.StatusCreated:
					ok.Add(1)
				case status == http.StatusUnprocessableEntity:
					rejected.Add(1)
				default:
					failed.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("unexpected status %d", status))
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	slices.Sort(all)
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[min(len(all)-1, int(float64(len(all))*p))]
	}
	total := ok.Load() + rejected.Load() + failed.Load()
	fmt.Printf("requests: %d in %s (%.0f req/s)\n", total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	fmt.Printf("posted:   %d (%.0f transfers/s), rejected for funds: %d, failed: %d\n",
		ok.Load(), float64(ok.Load())/elapsed.Seconds(), rejected.Load(), failed.Load())
	fmt.Printf("latency:  p50 %s  p95 %s  p99 %s  max %s\n",
		pct(.50).Round(10*time.Microsecond), pct(.95).Round(10*time.Microsecond),
		pct(.99).Round(10*time.Microsecond), pct(1).Round(10*time.Microsecond))
	if e := firstErr.Load(); e != nil {
		fmt.Printf("first error: %s\n", e)
	}

	var sum int64
	for _, id := range ids {
		var a account
		if _, err := c.do("GET", "/v1/accounts/"+id, nil, &a); err != nil {
			return err
		}
		sum += a.Balance
	}
	want := int64(cfg.accounts) * cfg.initial
	fmt.Printf("conservation: accounts hold %d, expected %d -> %v\n", sum, want, sum == want)

	var report struct {
		OK       bool     `json:"ok"`
		Entries  int64    `json:"entries"`
		Problems []string `json:"problems"`
	}
	if _, err := c.do("GET", "/v1/reconcile", nil, &report); err != nil {
		return err
	}
	fmt.Printf("reconcile: ok=%v entries=%d problems=%d\n", report.OK, report.Entries, len(report.Problems))
	if sum != want || !report.OK || failed.Load() > 0 {
		return fmt.Errorf("verification failed")
	}
	return nil
}
