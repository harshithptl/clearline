package settlement_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/settlement"
)

func TestSettleBatchOutcomes(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")
	ctx := context.Background()
	if _, err := settlement.Settle(ctx, pool, trade("T1", "AAPL", "alice", "bob", 1, "150")); err != nil {
		t.Fatal(err)
	}
	out, err := settlement.SettleBatch(ctx, pool, []ledger.Trade{
		trade("T1", "AAPL", "alice", "bob", 1, "150"), // settled before
		trade("T2", "AAPL", "alice", "bob", 2, "151"),
		trade("T2", "AAPL", "alice", "bob", 2, "151"), // repeated in the batch
		trade("T3", "AAPL", "bob", "alice", 3, "149.5"),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []settlement.Outcome{settlement.Duplicate, settlement.Posted, settlement.Duplicate, settlement.Posted}
	if !slices.Equal(out, want) {
		t.Fatalf("outcomes %v, want %v", out, want)
	}
	requireCounts(t, pool, 3, 12, 6)
	requireInvariants(t, pool)
}

// One unknown account fails the whole batch and writes nothing, not even the
// other trades' dedupe rows, so a per-trade retry is not mistaken for a
// duplicate.
func TestSettleBatchAllOrNothing(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")
	_, err := settlement.SettleBatch(context.Background(), pool, []ledger.Trade{
		trade("T1", "AAPL", "alice", "bob", 1, "150"),
		trade("T2", "AAPL", "alice", "ghost", 1, "150"),
	})
	if !errors.Is(err, settlement.ErrUnknownAccount) {
		t.Fatalf("err = %v, want ErrUnknownAccount", err)
	}
	requireCounts(t, pool, 0, 0, 0)
}

// Within a batch, outbox rows follow trade order, so the relay publishes an
// account's events in the order its trades happened.
func TestSettleBatchKeepsOutboxOrder(t *testing.T) {
	pool := pgtest.NewPool(t)
	seedAccounts(t, pool, "alice", "bob")
	var batch []ledger.Trade
	ids := []string{"T9", "T3", "T7", "T1", "T5"} // not sorted: input order must win
	for _, id := range ids {
		batch = append(batch, trade(id, "AAPL", "alice", "bob", 1, "150"))
	}
	if _, err := settlement.SettleBatch(context.Background(), pool, batch); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(context.Background(),
		`SELECT source_event_id FROM outbox WHERE key = 'alice' ORDER BY outbox_id`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if !slices.Equal(got, ids) {
		t.Fatalf("alice's outbox order %v, want trade order %v", got, ids)
	}
}
