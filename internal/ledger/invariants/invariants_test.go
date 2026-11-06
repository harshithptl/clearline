package invariants_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/harshithptl/clearline/internal/ledger/invariants"
	"github.com/harshithptl/clearline/internal/pgtest"
)

// row is one ledger entry in a test fixture. Amount is a decimal string so
// fixtures stay exact.
type row struct {
	account, asset, amount string
}

// t42 is the SPEC 4.1 dry run: Alice buys 10 AAPL at $150 from Bob.
var t42 = []row{
	{"alice", "USD", "-1500"},
	{"bob", "USD", "1500"},
	{"bob", "AAPL", "-10"},
	{"alice", "AAPL", "10"},
}

func seedAccounts(t *testing.T, conn *pgx.Conn, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := conn.Exec(context.Background(), `INSERT INTO accounts (account_id) VALUES ($1)`, id); err != nil {
			t.Fatalf("seed account %s: %v", id, err)
		}
	}
}

// postLedger writes ledger rows for a trade without touching processed_events
// or outbox, so tests can build broken states on purpose.
func postLedger(t *testing.T, conn *pgx.Conn, tradeID string, rows []row) {
	t.Helper()
	for _, r := range rows {
		_, err := conn.Exec(context.Background(),
			`INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ($1, $2, $3, $4::numeric)`,
			tradeID, r.account, r.asset, r.amount)
		if err != nil {
			t.Fatalf("post %s %+v: %v", tradeID, r, err)
		}
	}
}

func markProcessed(t *testing.T, conn *pgx.Conn, eventID string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO processed_events (event_id) VALUES ($1)`, eventID); err != nil {
		t.Fatalf("mark processed %s: %v", eventID, err)
	}
}

func addOutbox(t *testing.T, conn *pgx.Conn, eventID string) {
	t.Helper()
	_, err := conn.Exec(context.Background(),
		`INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, 'ledger-events', 'k', '{}')`, eventID)
	if err != nil {
		t.Fatalf("add outbox %s: %v", eventID, err)
	}
}

// settleCorrectly writes everything a correct settlement would write.
func settleCorrectly(t *testing.T, conn *pgx.Conn, tradeID string, rows []row) {
	t.Helper()
	postLedger(t, conn, tradeID, rows)
	markProcessed(t, conn, tradeID)
	addOutbox(t, conn, tradeID)
}

func check(t *testing.T, conn *pgx.Conn) invariants.Report {
	t.Helper()
	rep, err := invariants.CheckAll(context.Background(), conn)
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	return rep
}

func requireEqual[T any](t *testing.T, name string, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n got  %+v\n want %+v", name, got, want)
	}
}

func TestEmptyLedgerIsValid(t *testing.T) {
	conn := pgtest.NewDB(t)
	rep := check(t, conn)
	if !rep.OK() {
		t.Fatalf("empty ledger should pass, got %+v", rep)
	}
}

func TestBalancedTradePasses(t *testing.T) {
	conn := pgtest.NewDB(t)
	seedAccounts(t, conn, "alice", "bob")
	settleCorrectly(t, conn, "T42", t42)

	rep := check(t, conn)
	if !rep.OK() {
		t.Fatalf("balanced T42 should pass, got %+v", rep)
	}
}

// Drop Alice's AAPL credit: AAPL no longer nets to 0, globally or within T42.
func TestMissingLegBreaksAssetAndTrade(t *testing.T) {
	conn := pgtest.NewDB(t)
	seedAccounts(t, conn, "alice", "bob")
	settleCorrectly(t, conn, "T42", t42[:3])

	rep := check(t, conn)
	requireEqual(t, "asset imbalances", rep.AssetImbalances, []invariants.AssetImbalance{
		{Asset: "AAPL", Sum: "-10.00000000"},
	})
	requireEqual(t, "trade imbalances", rep.TradeImbalances, []invariants.TradeImbalance{
		{TradeID: "T42", Asset: "AAPL", Sum: "-10.00000000"},
	})
	if rep.OK() {
		t.Fatal("report should not be OK")
	}
}

// Two wrong trades whose errors cancel out. Invariant 1 (global per asset)
// cannot see this; invariant 2 (per trade) must catch both.
func TestCompensatingErrorsCaughtPerTrade(t *testing.T) {
	conn := pgtest.NewDB(t)
	seedAccounts(t, conn, "alice", "bob")
	settleCorrectly(t, conn, "T1", []row{
		{"alice", "USD", "-100"},
		{"bob", "USD", "50"}, // short by 50
	})
	settleCorrectly(t, conn, "T2", []row{
		{"alice", "USD", "-50"},
		{"bob", "USD", "100"}, // over by 50
	})

	rep := check(t, conn)
	requireEqual(t, "asset imbalances", len(rep.AssetImbalances), 0)
	requireEqual(t, "trade imbalances", rep.TradeImbalances, []invariants.TradeImbalance{
		{TradeID: "T1", Asset: "USD", Sum: "-50.00000000"},
		{TradeID: "T2", Asset: "USD", Sum: "50.00000000"},
	})
}

// Sub-cent errors must be visible: NUMERIC keeps them exact.
func TestTinyImbalanceDetected(t *testing.T) {
	conn := pgtest.NewDB(t)
	seedAccounts(t, conn, "alice", "bob")
	settleCorrectly(t, conn, "T1", []row{
		{"alice", "BTC", "-0.5"},
		{"bob", "BTC", "0.49999999"},
	})

	rep := check(t, conn)
	requireEqual(t, "asset imbalances", rep.AssetImbalances, []invariants.AssetImbalance{
		{Asset: "BTC", Sum: "-0.00000001"},
	})
}

// A trade marked processed but with no outbox row means downstream services
// would never hear about it.
func TestMissingOutboxRowDetected(t *testing.T) {
	conn := pgtest.NewDB(t)
	seedAccounts(t, conn, "alice", "bob")
	settleCorrectly(t, conn, "T1", t42)
	postLedger(t, conn, "T42", t42)
	markProcessed(t, conn, "T42") // no addOutbox

	rep := check(t, conn)
	requireEqual(t, "missing outbox", rep.MissingOutbox, []string{"T42"})
	requireEqual(t, "asset imbalances", len(rep.AssetImbalances), 0)
}

// The primary key makes duplicates impossible today. The check exists as a
// second line of defense, so prove it works by removing the key.
func TestDuplicateProcessedEventDetectedIfKeyMissing(t *testing.T) {
	conn := pgtest.NewDB(t)
	if _, err := conn.Exec(context.Background(),
		`ALTER TABLE processed_events DROP CONSTRAINT processed_events_pkey`); err != nil {
		t.Fatalf("drop pkey: %v", err)
	}
	markProcessed(t, conn, "T42")
	markProcessed(t, conn, "T42")
	addOutbox(t, conn, "T42")

	rep := check(t, conn)
	requireEqual(t, "duplicate events", rep.DuplicateEvents, []invariants.DuplicateEvent{
		{EventID: "T42", Count: 2},
	})
}
