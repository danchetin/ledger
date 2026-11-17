// Package testdb gives each test its own freshly migrated PostgreSQL database.
//
// Set LEDGER_TEST_DATABASE_URL to a server where the user may CREATE DATABASE,
// for example postgres://ledger:ledger@localhost:55432/ledger?sslmode=disable.
// Tests that need a database are skipped when it is not set.
package testdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/danchetin/ledger/internal/db"
)

// EnvVar is the variable holding the admin connection string.
const EnvVar = "LEDGER_TEST_DATABASE_URL"

// New creates a database, migrates it and drops it when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	base := os.Getenv(EnvVar)
	if base == "" {
		t.Skipf("%s is not set; skipping database test", EnvVar)
	}
	ctx := context.Background()
	name := fmt.Sprintf("ledger_test_%d_%d", time.Now().UnixNano(), rand.Uint32())

	exec(t, base, "CREATE DATABASE "+name)
	t.Cleanup(func() { exec(t, base, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })

	cfg, err := pgxpool.ParseConfig(base)
	if err != nil {
		t.Fatalf("parse %s: %v", EnvVar, err)
	}
	cfg.ConnConfig.Database = name
	cfg.MaxConns = 64
	db.Configure(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate test database: %v", err)
	}
	return pool
}

// exec runs one admin statement, retrying briefly because CREATE DATABASE fails
// while another session is copying the same template.
func exec(t testing.TB, url, sql string) {
	t.Helper()
	ctx := context.Background()
	var err error
	for attempt := 0; attempt < 20; attempt++ {
		var conn *pgx.Conn
		conn, err = pgx.Connect(ctx, url)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		_, err = conn.Exec(ctx, sql)
		conn.Close(ctx)
		if err == nil || !strings.Contains(err.Error(), "is being accessed by other users") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
