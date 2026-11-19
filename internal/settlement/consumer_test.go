package settlement_test

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/backoff"
	"github.com/harshithptl/clearline/internal/dlq"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/settlement"
)

const waitLong = 30 * time.Second

type pipeline struct {
	pool   *pgxpool.Pool
	trades string
	dlq    string
	prod   *kgo.Client
}

func newPipeline(t *testing.T) pipeline {
	t.Helper()
	return pipeline{
		pool:   pgtest.NewPool(t),
		trades: kafkatest.NewTopic(t, "trades", 3),
		dlq:    kafkatest.NewTopic(t, "trades-dlq", 1),
		prod:   kafkatest.NewClient(t, kgo.RequiredAcks(kgo.AllISRAcks())),
	}
}

func (p pipeline) produce(t *testing.T, key string, value []byte) {
	t.Helper()
	r := &kgo.Record{Topic: p.trades, Key: []byte(key), Value: value}
	if err := p.prod.ProduceSync(context.Background(), r).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func (p pipeline) produceTrade(t *testing.T, tr ledger.Trade) {
	t.Helper()
	b, err := messages.EncodeTrade(tr)
	if err != nil {
		t.Fatal(err)
	}
	p.produce(t, tr.Symbol, b)
}

// runningConsumer is a Consumer running in a goroutine.
type runningConsumer struct {
	*settlement.Consumer
	stop func()
}

func (p pipeline) start(t *testing.T, group string, maxAttempts int, reg ...prometheus.Registerer) runningConsumer {
	t.Helper()
	cl := kafkatest.NewClient(t, settlement.ConsumerClientOpts(group, p.trades)...)
	c, err := settlement.NewConsumer(settlement.ConsumerConfig{
		Client:      cl,
		DB:          p.pool,
		DLQTopic:    p.dlq,
		MaxAttempts: maxAttempts,
		Backoff:     backoff.Policy{Base: time.Millisecond, Max: 5 * time.Millisecond},
		Logger:      slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range reg {
		c.Instrument(r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(ctx); err != nil {
			t.Errorf("consumer: %v", err)
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			cl.Close()
		})
	}
	t.Cleanup(stop)
	return runningConsumer{Consumer: c, stop: stop}
}

func trade(id, symbol, buyer, seller string, qty int64, price string) ledger.Trade {
	return ledger.Trade{
		TradeID: id, Symbol: symbol, QuoteAsset: "USD", BuyerID: buyer, SellerID: seller,
		Quantity: decimal.NewFromInt(qty), Price: decimal.RequireFromString(price),
	}
}

func waitCommitted(t *testing.T, c runningConsumer, n int64) {
	t.Helper()
	kafkatest.Eventually(t, waitLong, fmt.Sprintf("%d committed records", n), func() bool {
		return c.Stats().Committed.Load() >= n
	})
	if got := c.Stats().Committed.Load(); got != n {
		t.Fatalf("committed %d records, want %d", got, n)
	}
}

// Every trade published 1 to 3 times, interleaved across symbols: the ledger
// posts each exactly once.
func TestConsumerRedeliveredTradesPostOnce(t *testing.T) {
	p := newPipeline(t)
	seedAccounts(t, p.pool, "a", "b", "c", "d")
	accounts := []string{"a", "b", "c", "d"}
	symbols := []string{"AAPL", "MSFT", "TSLA", "NVDA"}
	rng := rand.New(rand.NewPCG(7, 7))

	const trades = 60
	total := 0
	for i := range trades {
		buyer := rng.IntN(len(accounts))
		seller := (buyer + 1 + rng.IntN(len(accounts)-1)) % len(accounts)
		tr := trade("T"+strconv.Itoa(i), symbols[rng.IntN(len(symbols))], accounts[buyer], accounts[seller],
			int64(1+rng.IntN(100)), "1.25")
		for range 1 + rng.IntN(3) {
			p.produceTrade(t, tr)
			total++
		}
	}

	c := p.start(t, kafkatest.GroupID(t), 3)
	waitCommitted(t, c, int64(total))

	s := c.Stats()
	if s.Posted.Load() != trades || s.Duplicates.Load() != int64(total-trades) || s.DeadLettered.Load() != 0 {
		t.Fatalf("posted=%d duplicates=%d dlq=%d, want %d %d 0",
			s.Posted.Load(), s.Duplicates.Load(), s.DeadLettered.Load(), trades, total-trades)
	}
	requireCounts(t, p.pool, trades, 4*trades, 2*trades)
	requireInvariants(t, p.pool)
}

// Bad messages go to the DLQ with their reason, and good messages around
// them still settle: one poison message does not stop the pipeline.
func TestConsumerDeadLettersPoisonAndKeepsFlowing(t *testing.T) {
	p := newPipeline(t)
	seedAccounts(t, p.pool, "alice", "bob")

	p.produceTrade(t, trade("T1", "AAPL", "alice", "bob", 1, "10"))
	p.produce(t, "AAPL", []byte(`{not json`))                         // malformed: DLQ on first try
	p.produceTrade(t, trade("T2", "AAPL", "alice", "alice", 1, "10")) // self-trade: invalid, DLQ on first try
	p.produceTrade(t, trade("T3", "AAPL", "alice", "ghost", 1, "10")) // unknown account: retried, then DLQ
	p.produceTrade(t, trade("T4", "AAPL", "bob", "alice", 2, "10"))

	const maxAttempts = 3
	c := p.start(t, kafkatest.GroupID(t), maxAttempts)
	waitCommitted(t, c, 5)

	s := c.Stats()
	if s.Posted.Load() != 2 || s.DeadLettered.Load() != 3 || s.Retries.Load() != maxAttempts-1 {
		t.Fatalf("posted=%d dlq=%d retries=%d, want 2 3 %d",
			s.Posted.Load(), s.DeadLettered.Load(), s.Retries.Load(), maxAttempts-1)
	}
	requireCounts(t, p.pool, 2, 8, 4)
	requireInvariants(t, p.pool)

	// Same partition (key AAPL), so the DLQ keeps their relative order.
	recs := kafkatest.ReadN(t, p.dlq, 3, waitLong)
	wantAttempts := []string{"1", "1", strconv.Itoa(maxAttempts)}
	for i, r := range recs {
		if string(r.Key) != "AAPL" {
			t.Errorf("dlq[%d] key = %q, want AAPL", i, r.Key)
		}
		if topic, _ := dlq.Header(r, dlq.HeaderTopic); topic != p.trades {
			t.Errorf("dlq[%d] origin topic = %q", i, topic)
		}
		if msg, _ := dlq.Header(r, dlq.HeaderError); msg == "" {
			t.Errorf("dlq[%d] has no error header", i)
		}
		if a, _ := dlq.Header(r, dlq.HeaderAttempts); a != wantAttempts[i] {
			t.Errorf("dlq[%d] attempts = %s, want %s", i, a, wantAttempts[i])
		}
	}
}

// SPEC 5 rows 1 and 2. Trades T0..T4 were committed to Postgres by a consumer
// that crashed before committing their offsets. The next consumer gets them
// again, skips them, and settles the rest. A third consumer in the same group
// resumes after the committed offsets instead of starting over.
func TestConsumerRestartAfterCrash(t *testing.T) {
	p := newPipeline(t)
	seedAccounts(t, p.pool, "alice", "bob")
	group := kafkatest.GroupID(t)

	for i := range 10 {
		tr := trade("T"+strconv.Itoa(i), "AAPL", "alice", "bob", 1, "100")
		if i < 5 {
			settle(t, p.pool, tr)
		}
		p.produceTrade(t, tr)
	}

	first := p.start(t, group, 3)
	waitCommitted(t, first, 10)
	if first.Stats().Posted.Load() != 5 || first.Stats().Duplicates.Load() != 5 {
		t.Fatalf("first: posted=%d duplicates=%d, want 5 5",
			first.Stats().Posted.Load(), first.Stats().Duplicates.Load())
	}
	first.stop()

	for i := 10; i < 15; i++ {
		p.produceTrade(t, trade("T"+strconv.Itoa(i), "AAPL", "bob", "alice", 1, "100"))
	}
	second := p.start(t, group, 3)
	waitCommitted(t, second, 5)
	if second.Stats().Posted.Load() != 5 || second.Stats().Duplicates.Load() != 0 {
		t.Fatalf("second: posted=%d duplicates=%d, want 5 0",
			second.Stats().Posted.Load(), second.Stats().Duplicates.Load())
	}
	requireCounts(t, p.pool, 15, 60, 30)
	requireInvariants(t, p.pool)
}

// SPEC F7: an unknown-account trade is dead-lettered, the account gets
// created, and replaying the DLQ settles it. Replaying again does nothing.
func TestDLQReplaySettlesAfterFix(t *testing.T) {
	p := newPipeline(t)
	seedAccounts(t, p.pool, "alice")
	c := p.start(t, kafkatest.GroupID(t), 2)

	p.produceTrade(t, trade("T1", "AAPL", "alice", "late", 3, "7.5"))
	kafkatest.Eventually(t, waitLong, "trade dead-lettered", func() bool {
		return c.Stats().DeadLettered.Load() == 1
	})
	requireCounts(t, p.pool, 0, 0, 0)

	seedAccounts(t, p.pool, "late")
	group := kafkatest.GroupID(t)
	n := replay(t, p.dlq, group)
	if n != 1 {
		t.Fatalf("replayed %d, want 1", n)
	}
	kafkatest.Eventually(t, waitLong, "replayed trade posted", func() bool {
		return c.Stats().Posted.Load() == 1
	})
	requireCounts(t, p.pool, 1, 4, 2)
	requireInvariants(t, p.pool)

	if n := replay(t, p.dlq, group); n != 0 {
		t.Fatalf("second replay republished %d, want 0", n)
	}
}

func replay(t *testing.T, topic, group string) int {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafkatest.Brokers()...)},
		dlq.ReplayClientOpts(topic, group)...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), waitLong)
	defer cancel()
	n, err := dlq.Replay(ctx, cl, topic, group)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return n
}

// A trade carrying its order's timestamp (as the matcher adapter sets it)
// feeds the order-to-ledger histogram; duplicates do not count twice.
func TestConsumerRecordsOrderToLedger(t *testing.T) {
	p := newPipeline(t)
	seedAccounts(t, p.pool, "alice", "bob")
	b, err := messages.EncodeTrade(trade("T1", "AAPL", "alice", "bob", 1, "150"))
	if err != nil {
		t.Fatal(err)
	}
	ordered := time.Now().Add(-250 * time.Millisecond)
	for range 2 { // the second copy is a duplicate
		r := &kgo.Record{Topic: p.trades, Key: []byte("AAPL"), Value: b,
			Headers: []kgo.RecordHeader{messages.TimeHeader(messages.HeaderOrderTime, ordered)}}
		if err := p.prod.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	reg := prometheus.NewRegistry()
	c := p.start(t, kafkatest.GroupID(t), 5, reg)
	waitCommitted(t, c, 2)

	got := histogram(t, reg, "clearline_settlement_order_to_ledger_seconds")
	if got.GetSampleCount() != 1 || got.GetSampleSum() < 0.25 {
		t.Fatalf("order_to_ledger count=%d sum=%v; want 1 sample of at least 0.25s",
			got.GetSampleCount(), got.GetSampleSum())
	}
	if n := histogram(t, reg, "clearline_settlement_settle_duration_seconds").GetSampleCount(); n != 1 {
		t.Fatalf("settle duration samples = %d, want 1", n)
	}
	if problems, err := testutil.GatherAndLint(reg); err != nil || len(problems) > 0 {
		t.Fatalf("promlint: %v %v", problems, err)
	}
}

func histogram(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetHistogram()
		}
	}
	t.Fatalf("%s not registered", name)
	return nil
}
