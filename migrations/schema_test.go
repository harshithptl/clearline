package migrations_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/harshithptl/clearline/internal/pgtest"
)

// Postgres SQLSTATE codes we expect the schema to raise.
const (
	codeRestrict   = "23001" // restrict_violation: our append-only trigger
	codeForeignKey = "23503"
	codeUnique     = "23505"
	codeCheck      = "23514"
)

func requireCode(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("want SQLSTATE %s, got non-Postgres error: %v", want, err)
	}
	if pgErr.Code != want {
		t.Fatalf("want SQLSTATE %s, got %s: %s", want, pgErr.Code, pgErr.Message)
	}
}

func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// seedEntry creates account alice and one ledger row, returning the row id.
func seedEntry(t *testing.T, conn *pgx.Conn) int64 {
	t.Helper()
	mustExec(t, conn, `INSERT INTO accounts (account_id) VALUES ('alice')`)
	var id int64
	err := conn.QueryRow(context.Background(),
		`INSERT INTO ledger_entries (trade_id, account_id, asset, amount)
		 VALUES ('T1', 'alice', 'USD', -1500) RETURNING entry_id`).Scan(&id)
	if err != nil {
		t.Fatalf("seed ledger entry: %v", err)
	}
	return id
}

func TestLedgerRejectsUpdate(t *testing.T) {
	conn := pgtest.NewDB(t)
	id := seedEntry(t, conn)
	_, err := conn.Exec(context.Background(), `UPDATE ledger_entries SET amount = 1 WHERE entry_id = $1`, id)
	requireCode(t, err, codeRestrict)
}

func TestLedgerRejectsDelete(t *testing.T) {
	conn := pgtest.NewDB(t)
	id := seedEntry(t, conn)
	_, err := conn.Exec(context.Background(), `DELETE FROM ledger_entries WHERE entry_id = $1`, id)
	requireCode(t, err, codeRestrict)
}

func TestLedgerRejectsTruncate(t *testing.T) {
	conn := pgtest.NewDB(t)
	seedEntry(t, conn)
	_, err := conn.Exec(context.Background(), `TRUNCATE ledger_entries`)
	requireCode(t, err, codeRestrict)
}

func TestLedgerRequiresKnownAccount(t *testing.T) {
	conn := pgtest.NewDB(t)
	_, err := conn.Exec(context.Background(),
		`INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ('T1', 'ghost', 'USD', 10)`)
	requireCode(t, err, codeForeignKey)
}

func TestLedgerRejectsZeroAmount(t *testing.T) {
	conn := pgtest.NewDB(t)
	mustExec(t, conn, `INSERT INTO accounts (account_id) VALUES ('alice')`)
	_, err := conn.Exec(context.Background(),
		`INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ('T1', 'alice', 'USD', 0)`)
	requireCode(t, err, codeCheck)
}

// NUMERIC(20,8) must store the smallest unit exactly and sum without float drift.
func TestLedgerAmountIsExact(t *testing.T) {
	conn := pgtest.NewDB(t)
	mustExec(t, conn, `INSERT INTO accounts (account_id) VALUES ('alice')`)
	// 0.1 + 0.2 is the classic float failure: in float64 it is 0.30000000000000004.
	mustExec(t, conn, `INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ('T1', 'alice', 'USD', 0.1)`)
	mustExec(t, conn, `INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ('T2', 'alice', 'USD', 0.2)`)
	mustExec(t, conn, `INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ('T3', 'alice', 'BTC', 0.00000001)`)

	var usd, btc string
	err := conn.QueryRow(context.Background(), `
		SELECT
		  (SELECT SUM(amount) FROM ledger_entries WHERE asset = 'USD')::text,
		  (SELECT SUM(amount) FROM ledger_entries WHERE asset = 'BTC')::text`).Scan(&usd, &btc)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if usd != "0.30000000" {
		t.Errorf("USD sum = %s, want 0.30000000", usd)
	}
	if btc != "0.00000001" {
		t.Errorf("BTC sum = %s, want 0.00000001", btc)
	}
}

// The processed_events primary key is the dedupe mechanism settlement relies on.
func TestProcessedEventsDedupe(t *testing.T) {
	conn := pgtest.NewDB(t)
	mustExec(t, conn, `INSERT INTO processed_events (event_id) VALUES ('T42')`)

	_, err := conn.Exec(context.Background(), `INSERT INTO processed_events (event_id) VALUES ('T42')`)
	requireCode(t, err, codeUnique)

	// The pattern settlement will use: 0 rows affected means "already seen".
	tag, err := conn.Exec(context.Background(),
		`INSERT INTO processed_events (event_id) VALUES ('T42') ON CONFLICT DO NOTHING`)
	if err != nil {
		t.Fatalf("on conflict insert: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("duplicate insert affected %d rows, want 0", tag.RowsAffected())
	}
}

func TestOutboxEventIDUnique(t *testing.T) {
	conn := pgtest.NewDB(t)
	insert := `INSERT INTO outbox (event_id, topic, key, payload) VALUES ('T42', 'ledger-events', 'alice', '{}')`
	mustExec(t, conn, insert)
	_, err := conn.Exec(context.Background(), insert)
	requireCode(t, err, codeUnique)
}

// The relay must be able to mark rows published; outbox is not append-only.
func TestOutboxAllowsMarkPublished(t *testing.T) {
	conn := pgtest.NewDB(t)
	mustExec(t, conn, `INSERT INTO outbox (event_id, topic, key, payload) VALUES ('T42', 'ledger-events', 'alice', '{}')`)
	mustExec(t, conn, `UPDATE outbox SET published_at = now() WHERE event_id = 'T42'`)
}

// Durability rule from CLAUDE.md: never run with synchronous_commit off.
func TestSynchronousCommitOn(t *testing.T) {
	conn := pgtest.NewDB(t)
	var v string
	if err := conn.QueryRow(context.Background(), `SHOW synchronous_commit`).Scan(&v); err != nil {
		t.Fatalf("show: %v", err)
	}
	if v != "on" {
		t.Fatalf("synchronous_commit = %s, want on", v)
	}
}
