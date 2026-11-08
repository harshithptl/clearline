// Package pgtest gives each test its own freshly migrated Postgres database.
//
// It connects to CLEARLINE_TEST_DATABASE_URL (default: the local compose
// database), creates a uniquely named database, applies all migrations, and
// drops it when the test ends. Requires the compose Postgres to be running.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/harshithptl/clearline/migrations"
)

const defaultURL = "postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable"

// NewDB returns a single connection to a new, fully migrated database that is
// dropped on test cleanup. Not safe for concurrent use; see NewPool.
func NewDB(t *testing.T) *pgx.Conn {
	t.Helper()
	url, name := create(t)
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatalf("pgtest: parse url: %v", err)
	}
	cfg.Database = name
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgtest: connect to test database: %v", err)
	}
	// Cleanups run last-in first-out, so this closes before the drop.
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// NewPool returns a connection pool to a new, fully migrated database that is
// dropped on test cleanup. Safe for concurrent use.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url, name := create(t)
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("pgtest: parse url: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgtest: open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// create makes a new database, applies migrations, and registers its drop.
// It returns the admin URL and the new database name.
func create(t *testing.T) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminURL := os.Getenv("CLEARLINE_TEST_DATABASE_URL")
	if adminURL == "" {
		adminURL = defaultURL
	}

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgtest: connect to %s (is compose Postgres up?): %v", adminURL, err)
	}
	name := "clearline_test_" + randomSuffix(t)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close(ctx)
		t.Fatalf("pgtest: create database: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if _, err := admin.Exec(cctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
		admin.Close(cctx)
	})

	cfg, err := pgx.ParseConfig(adminURL)
	if err != nil {
		t.Fatalf("pgtest: parse url: %v", err)
	}
	cfg.Database = name
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pgtest: connect to test database: %v", err)
	}
	defer conn.Close(ctx)
	if err := migrations.Apply(ctx, conn); err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	return adminURL, name
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("pgtest: random: %v", err)
	}
	return hex.EncodeToString(b)
}
