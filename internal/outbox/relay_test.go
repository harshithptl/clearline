package outbox

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/dlq"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/ledger/invariants"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/settlement"
)

const waitLong = 30 * time.Second

var accounts = []string{"alice", "bob", "carol"}

func setup(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool := pgtest.NewPool(t)
	for _, a := range accounts {
		if _, err := pool.Exec(context.Background(), `INSERT INTO accounts (account_id) VALUES ($1)`, a); err != nil {
			t.Fatal(err)
		}
	}
	return pool, kafkatest.NewTopic(t, "ledger-events", 3)
}

// settleTrades settles trades T<from>..T<to-1>, rotating buyer and seller.
func settleTrades(t *testing.T, pool *pgxpool.Pool, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		tr := ledger.Trade{
			TradeID: "T" + strconv.Itoa(i), Symbol: "AAPL", QuoteAsset: "USD",
			BuyerID: accounts[i%3], SellerID: accounts[(i+1)%3],
			Quantity: decimal.NewFromInt(int64(i + 1)), Price: decimal.RequireFromString("2.5"),
		}
		if out, err := settlement.Settle(context.Background(), pool, tr); err != nil || out != settlement.Posted {
			t.Fatalf("settle %s: %v %v", tr.TradeID, out, err)
		}
	}
}

type running struct {
	*Relay
	stop func()
}

func start(t *testing.T, pool *pgxpool.Pool, topic string, hook func() error) running {
	t.Helper()
	r, err := New(Config{
		DB:              pool,
		Client:          kafkatest.NewClient(t),
		KafkaTopic:      topic,
		BatchSize:       7, // small, so tests cross batch boundaries
		StandbyInterval: 50 * time.Millisecond,
		Logger:          slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.afterProduce = hook
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Run(ctx); err != nil {
			t.Errorf("relay: %v", err)
		}
	}()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); wg.Wait() }) }
	t.Cleanup(stop)
	return running{Relay: r, stop: stop}
}

func unpublished(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// eventCounts maps event_id to how many times it was published, and checks
// each record's key and payload agree with its header.
func eventCounts(t *testing.T, recs []*kgo.Record) map[string]int {
	t.Helper()
	seen := map[string]int{}
	for _, r := range recs {
		id, ok := dlq.Header(r, HeaderEventID)
		if !ok {
			t.Fatalf("record without %s header", HeaderEventID)
		}
		ev, err := messages.DecodeLedgerEvent(r.Value)
		if err != nil {
			t.Fatalf("payload %s: %v", r.Value, err)
		}
		if ev.EventID != id || ev.AccountID != string(r.Key) {
			t.Fatalf("header %s key %s, payload %+v", id, r.Key, ev)
		}
		seen[id]++
	}
	return seen
}

// Every outbox row is published once, with key = account, and each
// account's events arrive in outbox order.
func TestRelayPublishesEveryRowInOrder(t *testing.T) {
	pool, topic := setup(t)
	settleTrades(t, pool, 0, 30) // 60 outbox rows
	r := start(t, pool, topic, nil)
	kafkatest.Eventually(t, waitLong, "60 published", func() bool { return r.Stats().Published.Load() == 60 })

	recs := kafkatest.ReadN(t, topic, 60, waitLong)
	for id, n := range eventCounts(t, recs) {
		if n != 1 {
			t.Fatalf("%s published %d times", id, n)
		}
	}
	if n := unpublished(t, pool); n != 0 {
		t.Fatalf("%d rows still unpublished", n)
	}

	// Per account (one key, so one partition), events follow trade order.
	last := map[string]int{}
	for _, rec := range recs {
		ev, _ := messages.DecodeLedgerEvent(rec.Value)
		n, _ := strconv.Atoi(ev.TradeID[1:])
		if prev, ok := last[ev.AccountID]; ok && n <= prev {
			t.Fatalf("%s: trade %d after %d", ev.AccountID, n, prev)
		}
		last[ev.AccountID] = n
	}

	report, err := invariants.CheckAll(context.Background(), pool)
	if err != nil || !report.OK() {
		t.Fatalf("invariants: %+v %v", report, err)
	}
}

// SPEC 5, last row: the relay dies after Kafka acked a batch but before it
// marked the rows published. The rows are sent again; nothing is lost and the
// duplicates carry the same event_id for the consumer to drop.
func TestRelayCrashAfterProduceRepublishes(t *testing.T) {
	pool, topic := setup(t)
	settleTrades(t, pool, 0, 3) // 6 rows, one batch of 7
	var crashed atomic.Bool
	hook := func() error {
		if crashed.CompareAndSwap(false, true) {
			return errors.New("simulated crash before marking published")
		}
		return nil
	}
	r := start(t, pool, topic, hook)
	kafkatest.Eventually(t, waitLong, "6 published", func() bool { return r.Stats().Published.Load() == 6 })

	counts := eventCounts(t, kafkatest.ReadN(t, topic, 12, waitLong))
	if len(counts) != 6 {
		t.Fatalf("%d distinct events, want 6", len(counts))
	}
	for id, n := range counts {
		if n != 2 {
			t.Fatalf("%s published %d times, want 2 (once before the crash, once after)", id, n)
		}
	}
	if n := unpublished(t, pool); n != 0 {
		t.Fatalf("%d rows still unpublished", n)
	}
}

// Two relays: one leads, the other stands by and publishes nothing. When the
// leader stops, the standby takes over and nothing is published twice.
func TestRelayStandbyTakesOver(t *testing.T) {
	pool, topic := setup(t)
	a := start(t, pool, topic, nil)
	kafkatest.Eventually(t, waitLong, "a leads", func() bool { return a.Stats().Leader.Load() })
	b := start(t, pool, topic, nil)

	settleTrades(t, pool, 0, 10)
	kafkatest.Eventually(t, waitLong, "20 published", func() bool { return a.Stats().Published.Load() == 20 })
	if b.Stats().Leader.Load() || b.Stats().Published.Load() != 0 {
		t.Fatalf("standby published %d (leader=%v)", b.Stats().Published.Load(), b.Stats().Leader.Load())
	}

	a.stop()
	kafkatest.Eventually(t, waitLong, "b leads", func() bool { return b.Stats().Leader.Load() })
	settleTrades(t, pool, 10, 20)
	kafkatest.Eventually(t, waitLong, "b published 20", func() bool { return b.Stats().Published.Load() == 20 })

	for id, n := range eventCounts(t, kafkatest.ReadN(t, topic, 40, waitLong)) {
		if n != 1 {
			t.Fatalf("%s published %d times", id, n)
		}
	}
}
