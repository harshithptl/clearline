package reconcile_test

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/outbox"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/reconcile"
	"github.com/harshithptl/clearline/internal/redistest"
	"github.com/harshithptl/clearline/internal/settlement"
)

var accounts = []string{"alice", "bob", "carol"}

func setup(t *testing.T, trades int) (*pgxpool.Pool, *balance.Store, func(key string, kv ...any)) {
	t.Helper()
	pool := pgtest.NewPool(t)
	ctx := context.Background()
	for _, a := range accounts {
		if _, err := pool.Exec(ctx, `INSERT INTO accounts (account_id) VALUES ($1)`, a); err != nil {
			t.Fatal(err)
		}
	}
	for i := range trades {
		tr := ledger.Trade{
			TradeID: "T" + strconv.Itoa(i), Symbol: "AAPL", QuoteAsset: "USD",
			BuyerID: accounts[i%3], SellerID: accounts[(i+1)%3],
			Quantity: decimal.NewFromInt(int64(i + 1)), Price: decimal.RequireFromString("0.1"),
		}
		if _, err := settlement.Settle(ctx, pool, tr); err != nil {
			t.Fatal(err)
		}
	}
	rdb, prefix := redistest.New(t)
	hset := func(key string, kv ...any) {
		if err := rdb.HSet(ctx, prefix+key, kv...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	return pool, balance.NewStore(rdb, prefix), hset
}

// outboxEvents returns every ledger event in the outbox, in outbox order.
func outboxEvents(t *testing.T, pool *pgxpool.Pool) []messages.LedgerEvent {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT payload FROM outbox ORDER BY outbox_id`)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		t.Fatal(err)
	}
	out := make([]messages.LedgerEvent, len(raw))
	for i, b := range raw {
		if out[i], err = messages.DecodeLedgerEvent(b); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func apply(t *testing.T, s *balance.Store, evs []messages.LedgerEvent) {
	t.Helper()
	for _, ev := range evs {
		if _, err := s.Apply(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
}

func check(t *testing.T, pool *pgxpool.Pool, s *balance.Store) reconcile.Report {
	t.Helper()
	rep, err := reconcile.Check(context.Background(), pool, s)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestReconcileCaughtUp(t *testing.T) {
	pool, s, _ := setup(t, 12)
	apply(t, s, outboxEvents(t, pool))
	rep := check(t, pool, s)
	if !rep.OK() || len(rep.Lagging) != 0 || rep.Accounts != 3 {
		t.Fatalf("report = %+v", rep)
	}
}

// Cache behind the ledger is lag, not a mismatch.
func TestReconcileLaggingIsNotAnError(t *testing.T) {
	pool, s, _ := setup(t, 12)
	evs := outboxEvents(t, pool)
	apply(t, s, evs[:len(evs)-1]) // the last event is "in flight"
	rep := check(t, pool, s)
	if !rep.OK() {
		t.Fatalf("lag reported as mismatch: %+v", rep.CacheMismatches)
	}
	last := evs[len(evs)-1].AccountID
	if len(rep.Lagging) != 1 || rep.Lagging[0].Account != last ||
		rep.Lagging[0].LedgerEvents-rep.Lagging[0].CacheEvents != 1 {
		t.Fatalf("lagging = %+v, want %s one event behind", rep.Lagging, last)
	}
}

// Someone edits a cached balance: invariant 3 catches the exact asset.
func TestReconcileDetectsTamperedBalance(t *testing.T) {
	pool, s, hset := setup(t, 12)
	apply(t, s, outboxEvents(t, pool))
	hset("{bob}:bal", "USD", "1000000")
	rep := check(t, pool, s)
	if rep.OK() || len(rep.CacheMismatches) != 1 {
		t.Fatalf("report = %+v", rep)
	}
	m := rep.CacheMismatches[0]
	if m.Account != "bob" || m.Asset != "USD" || m.Cache != "1000000" {
		t.Fatalf("mismatch = %+v", m)
	}
}

// The cache applied an event the ledger never produced.
func TestReconcileDetectsCacheAhead(t *testing.T) {
	pool, s, _ := setup(t, 3)
	apply(t, s, outboxEvents(t, pool))
	apply(t, s, []messages.LedgerEvent{{
		Version: 1, EventID: "GHOST:carol", TradeID: "GHOST", AccountID: "carol",
		Changes: []messages.Change{{Asset: "USD", Amount: "0"}},
	}})
	rep := check(t, pool, s)
	if rep.OK() || len(rep.CacheMismatches) != 1 || rep.CacheMismatches[0].Asset != reconcile.EventsField {
		t.Fatalf("report = %+v", rep)
	}
}

// The whole downstream pipeline with real Kafka: settle, relay to a topic,
// balance consumer applies, then every record is delivered a second time.
// The reconciler must end clean with nothing lagging.
func TestReconcileFullPipeline(t *testing.T) {
	pool, s, _ := setup(t, 0)
	topic := kafkatest.NewTopic(t, "ledger-events", 3)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()

	relay, err := outbox.New(outbox.Config{
		DB: pool, Client: kafkatest.NewClient(t), KafkaTopic: topic, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	cons := balance.NewConsumer(
		kafkatest.NewClient(t, balance.ConsumerClientOpts(kafkatest.GroupID(t), topic)...),
		s, slog.New(slog.DiscardHandler))
	for _, run := range []func(context.Context) error{relay.Run, cons.Run} {
		wg.Add(1)
		go func() { defer wg.Done(); _ = run(ctx) }()
	}

	// Settle while the pipeline runs.
	for i := range 40 {
		tr := ledger.Trade{
			TradeID: "T" + strconv.Itoa(i), Symbol: []string{"AAPL", "MSFT"}[i%2], QuoteAsset: "USD",
			BuyerID: accounts[i%3], SellerID: accounts[(i+2)%3],
			Quantity: decimal.NewFromInt(int64(1 + i%7)), Price: decimal.RequireFromString("12.34"),
		}
		if _, err := settlement.Settle(ctx, pool, tr); err != nil {
			t.Fatal(err)
		}
	}
	waitClean(t, pool, s)

	// Redeliver everything (relay crash, consumer replay): still clean.
	prod := kafkatest.NewClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	for _, r := range kafkatest.ReadN(t, topic, 80, 30*time.Second) {
		dup := &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value, Headers: r.Headers}
		if err := prod.ProduceSync(ctx, dup).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	kafkatest.Eventually(t, 30*time.Second, "duplicates consumed", func() bool {
		return cons.Stats().Duplicates.Load() == 80
	})
	waitClean(t, pool, s)
	if cons.Stats().Applied.Load() != 80 {
		t.Fatalf("applied %d, want 80", cons.Stats().Applied.Load())
	}
}

func waitClean(t *testing.T, pool *pgxpool.Pool, s *balance.Store) {
	t.Helper()
	var rep reconcile.Report
	kafkatest.Eventually(t, 30*time.Second, "reconciler clean and caught up", func() bool {
		rep = check(t, pool, s)
		if !rep.OK() {
			t.Fatalf("reconciler mismatch: %+v", rep)
		}
		return len(rep.Lagging) == 0
	})
}
