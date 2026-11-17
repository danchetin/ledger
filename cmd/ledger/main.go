// Command ledger runs the ledger HTTP service and its maintenance commands.
//
//	ledger serve      run the API (default)
//	ledger migrate    apply database migrations and exit
//	ledger reconcile  check the journal against balances; exit 1 on any problem
//
// Configuration comes from the environment; see README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/danchetin/ledger/internal/api"
	"github.com/danchetin/ledger/internal/db"
	"github.com/danchetin/ledger/internal/ledger"
)

var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	var err error
	switch cmd {
	case "serve":
		err = serve(ctx, logger)
	case "migrate":
		err = migrate(ctx, logger)
	case "reconcile":
		err = reconcile(ctx)
	case "version":
		fmt.Println(version)
	default:
		fmt.Fprintf(os.Stderr, "usage: ledger [serve|migrate|reconcile|version]\n")
		os.Exit(2)
	}
	if err != nil {
		logger.Error("fatal", "command", cmd, "error", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return slog.LevelInfo
	}
	return l
}

func open(ctx context.Context) (*ledger.Service, func(), error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, nil, errors.New("DATABASE_URL is required")
	}
	maxConns, _ := strconv.Atoi(env("DB_MAX_CONNS", "20"))
	pool, err := db.Open(ctx, url, int32(maxConns))
	if err != nil {
		return nil, nil, err
	}
	return ledger.New(pool), pool.Close, nil
}

func migrate(ctx context.Context, logger *slog.Logger) error {
	svc, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()
	applied, err := db.Migrate(ctx, svc.Pool())
	logger.Info("migrations applied", "versions", applied)
	return err
}

func reconcile(ctx context.Context) error {
	svc, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()
	report, err := svc.Reconcile(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(report) //nolint:errcheck
	if !report.OK {
		os.Exit(1)
	}
	return nil
}

func serve(ctx context.Context, logger *slog.Logger) error {
	svc, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()
	if env("AUTO_MIGRATE", "true") == "true" {
		applied, err := db.Migrate(ctx, svc.Pool())
		if err != nil {
			return err
		}
		if len(applied) > 0 {
			logger.Info("migrations applied", "versions", applied)
		}
	}

	go sweep(ctx, logger, svc, envDuration("SWEEP_INTERVAL", 30*time.Second), envDuration("IDEMPOTENCY_TTL", 24*time.Hour))

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           api.New(svc, api.Config{APIKey: os.Getenv("LEDGER_API_KEY"), Logger: logger}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errs := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", srv.Addr, "version", version, "auth", os.Getenv("LEDGER_API_KEY") != "")
		errs <- srv.ListenAndServe()
	}()
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// sweep expires overdue holds and purges old idempotency keys in the background.
func sweep(ctx context.Context, logger *slog.Logger, svc *ledger.Service, every, keyTTL time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for {
			n, err := svc.ExpireHolds(ctx, 500)
			if err != nil {
				logger.Error("expire holds", "error", err)
				break
			}
			if n > 0 {
				logger.Info("holds expired", "count", n)
			}
			if n < 500 {
				break
			}
		}
		if n, err := svc.PurgeIdempotencyKeys(ctx, keyTTL); err != nil {
			logger.Error("purge idempotency keys", "error", err)
		} else if n > 0 {
			logger.Info("idempotency keys purged", "count", n)
		}
	}
}
