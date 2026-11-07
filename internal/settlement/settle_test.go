package settlement_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/ledger/invariants"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/settlement"
)

// t42 is the SPEC 4.1 dry run: Alice buys 10 AAPL at $150 from Bob.
func t42() ledger.Trade {
	return ledger.Trade{
		TradeID:    "T42",
		Symbol:     "AAPL",
		QuoteAsset: "USD",
		BuyerID:    "alice",
		SellerID:   "bob",
		Quantity:   decimal.RequireFromString("10"),
		Price:      decimal.RequireFromString("150"),
	}
}

func seedAccounts(t *testing.T, pool *pgxpool.Pool, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := pool.Exec(context.Background(), `INSERT INTO accounts (account_id) VALUES ($1)`, id); err != nil {
			t.Fatalf("seed account %s: %v", id, err)
		}
	}
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// requireCounts asserts row counts in processed_events, ledger_entries, outbox.
func requireCounts(t *testing.T, pool *pgxpool.Pool, processed, ledgerRows, outbox int) {
	t.Helper()
	got := [3]int{count(t, pool, "processed_events"), count(t, pool, "ledger_entries"), count(t, pool, "outbox")}
	want := [3]int{processed, ledgerRows, outbox}
	if got != want {
		t.Fatalf("rows (processed, ledger, outbox) = %v, want %v", got, want)
	}
}

func requireInvariants(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	rep, err := invariants.CheckAll(context.Background(), pool)
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("invariants violated: %+v", rep)
	}
}

func settle(t *testing.T, pool *pgxpool.Pool, tr ledger.Trade) settlement.Outcome {
	t.Helper()
	out, err := settlement.Settle(context.Background(), pool, tr)
	if err != nil {
		t.Fatalf("Settle(%s): %v", tr.TradeID, err)
	}
	return out
}

func TestSettlePostsTrade(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")

	if out := settle(t, pool, t42()); out != settlement.Posted {
		t.Fatalf("outcome = %v, want Posted", out)
	}
	requireCounts(t, pool, 1, 4, 2)
	requireInvariants(t, pool)

	// One ledger-events row per account, keyed by account, linked to T42.
	rows, err := pool.Query(context.Background(),
		`SELECT event_id, source_event_id, topic, key, payload FROM outbox ORDER BY event_id`)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer rows.Close()
	type outboxRow struct {
		eventID, source, topic, key string
		payload                     settlement.LedgerEvent
	}
	var got []outboxRow
	for rows.Next() {
		var r outboxRow
		var raw []byte
		if err := rows.Scan(&r.eventID, &r.source, &r.topic, &r.key, &raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if err := json.Unmarshal(raw, &r.payload); err != nil {
			t.Fatalf("payload %s: %v", raw, err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("got %d outbox rows, want 2", len(got))
	}
	alice, bob := got[0], got[1]
	if alice.eventID != "T42:alice" || alice.source != "T42" || alice.topic != "ledger-events" || alice.key != "alice" {
		t.Errorf("alice row = %+v", alice)
	}
	if bob.eventID != "T42:bob" || bob.key != "bob" {
		t.Errorf("bob row = %+v", bob)
	}
	wantAlice := []settlement.Change{{Asset: "USD", Amount: "-1500"}, {Asset: "AAPL", Amount: "10"}}
	if alice.payload.EventID != "T42:alice" || alice.payload.TradeID != "T42" ||
		alice.payload.AccountID != "alice" || !equalChanges(alice.payload.Changes, wantAlice) {
		t.Errorf("alice payload = %+v", alice.payload)
	}
	wantBob := []settlement.Change{{Asset: "USD", Amount: "1500"}, {Asset: "AAPL", Amount: "-10"}}
	if !equalChanges(bob.payload.Changes, wantBob) {
		t.Errorf("bob payload = %+v", bob.payload)
	}
}

func equalChanges(a, b []settlement.Change) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SPEC 5: "crashes after commit, before ack" means Kafka redelivers. The
// second delivery must be a no-op.
func TestSettleRedeliveryIsDuplicate(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")

	settle(t, pool, t42())
	if out := settle(t, pool, t42()); out != settlement.Duplicate {
		t.Fatalf("second delivery outcome = %v, want Duplicate", out)
	}
	requireCounts(t, pool, 1, 4, 2)
	requireInvariants(t, pool)
}

// Many consumers receive the same trade at the same moment. Exactly one posts.
func TestSettleConcurrentSameTrade(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")

	const workers = 20
	outcomes := make([]settlement.Outcome, workers)
	errs := make([]error, workers)
	var start, done sync.WaitGroup
	start.Add(1)
	for i := range workers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait() // release all goroutines together to maximize overlap
			outcomes[i], errs[i] = settlement.Settle(context.Background(), pool, t42())
		}()
	}
	start.Done()
	done.Wait()

	posted := 0
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if outcomes[i] == settlement.Posted {
			posted++
		}
	}
	if posted != 1 {
		t.Fatalf("%d workers posted, want exactly 1", posted)
	}
	requireCounts(t, pool, 1, 4, 2)
	requireInvariants(t, pool)
}

func TestSettleInvalidTradeWritesNothing(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")
	tr := t42()
	tr.Quantity = decimal.Zero

	_, err := settlement.Settle(context.Background(), pool, tr)
	if !errors.Is(err, ledger.ErrInvalidTrade) {
		t.Fatalf("err = %v, want ErrInvalidTrade", err)
	}
	requireCounts(t, pool, 0, 0, 0)
}

// SPEC 5: "crashes before commit". Here the failure is an unknown account,
// hit after the dedupe row was inserted. The whole transaction must roll
// back, including the dedupe row; otherwise the retry would be wrongly
// skipped as a duplicate and the trade lost forever.
func TestSettleFailureRollsBackDedupeRow(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice") // bob missing

	_, err := settlement.Settle(context.Background(), pool, t42())
	if !errors.Is(err, settlement.ErrUnknownAccount) {
		t.Fatalf("err = %v, want ErrUnknownAccount", err)
	}
	requireCounts(t, pool, 0, 0, 0)

	// Bob's account shows up; the retry must post, not be skipped.
	seedAccounts(t, pool, "bob")
	if out := settle(t, pool, t42()); out != settlement.Posted {
		t.Fatalf("retry outcome = %v, want Posted", out)
	}
	requireCounts(t, pool, 1, 4, 2)
	requireInvariants(t, pool)
}

func TestLedgerEventID(t *testing.T) {
	if got := settlement.LedgerEventID("T42", "alice"); got != "T42:alice" {
		t.Fatalf("LedgerEventID = %q", got)
	}
}
