package matcher_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	matcherv1 "github.com/harshithptl/clearline/gen/matcher/v1"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/matcher"
	"github.com/harshithptl/clearline/internal/messages"
)

const waitLong = 30 * time.Second

func engineAddr() string {
	if a := os.Getenv("CLEARLINE_TEST_MATCHER_ADDR"); a != "" {
		return a
	}
	return "localhost:50051"
}

func newEngine(t *testing.T) matcherv1.MatcherClient {
	t.Helper()
	conn, err := grpc.NewClient(engineAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return matcherv1.NewMatcherClient(conn)
}

// Small books so test sessions stay light on the shared engine.
func symbols() []matcher.SymbolSpec {
	return []matcher.SymbolSpec{
		{Symbol: "AAPL", QuoteAsset: "USD", TickSize: decimal.RequireFromString("0.01"),
			ReferencePrice: decimal.RequireFromString("150"), TicksEachSide: 20000, MaxOrderID: 10000, MaxResting: 1000},
		{Symbol: "MSFT", QuoteAsset: "USD", TickSize: decimal.RequireFromString("0.01"),
			ReferencePrice: decimal.RequireFromString("400"), TicksEachSide: 20000, MaxOrderID: 10000, MaxResting: 1000},
	}
}

type fixture struct {
	engine matcherv1.MatcherClient
	orders string
	trades string
	prod   *kgo.Client
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	return fixture{
		engine: newEngine(t),
		orders: kafkatest.NewTopic(t, "orders", 3),
		trades: kafkatest.NewTopic(t, "trades", 3),
		prod:   kafkatest.NewClient(t, kgo.RequiredAcks(kgo.AllISRAcks())),
	}
}

func (f fixture) produceRaw(t *testing.T, key string, value []byte) {
	t.Helper()
	if err := f.prod.ProduceSync(context.Background(), &kgo.Record{Topic: f.orders, Key: []byte(key), Value: value}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func (f fixture) order(t *testing.T, account, symbol, side, qty, price string) {
	t.Helper()
	b, err := messages.EncodeOrder(messages.ParsedOrder{
		OrderID: account + "-" + side, AccountID: account, Symbol: symbol, Side: side,
		Quantity: decimal.RequireFromString(qty), Price: decimal.RequireFromString(price),
	})
	if err != nil {
		t.Fatal(err)
	}
	f.produceRaw(t, symbol, b)
}

type running struct {
	*matcher.Adapter
	done chan error
	stop func()
}

func (f fixture) start(t *testing.T, group string) running {
	t.Helper()
	a, err := matcher.New(matcher.Config{
		Engine: f.engine, Brokers: kafkatest.Brokers(),
		OrdersTopic: f.orders, TradesTopic: f.trades, Group: group,
		Symbols: symbols(), Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var once sync.Once
	var result error
	stop := func() {
		once.Do(func() {
			cancel()
			result = <-done
		})
		if result != nil {
			t.Errorf("adapter: %v", result)
		}
	}
	t.Cleanup(stop)
	return running{Adapter: a, done: done, stop: stop}
}

func decodeTrades(t *testing.T, recs []*kgo.Record) []ledger.Trade {
	t.Helper()
	out := make([]ledger.Trade, len(recs))
	for i, r := range recs {
		tr, err := messages.DecodeTrade(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if string(r.Key) != tr.Symbol {
			t.Fatalf("trade key %q, symbol %q", r.Key, tr.Symbol)
		}
		out[i] = tr
	}
	return out
}

func requireTrade(t *testing.T, got ledger.Trade, buyer, seller, qty, price string) {
	t.Helper()
	if got.BuyerID != buyer || got.SellerID != seller || got.QuoteAsset != "USD" ||
		!got.Quantity.Equal(decimal.RequireFromString(qty)) || !got.Price.Equal(decimal.RequireFromString(price)) {
		t.Fatalf("trade %s = %s buys %s from %s at %s; want %s buys %s from %s at %s",
			got.TradeID, got.BuyerID, got.Quantity, got.SellerID, got.Price, buyer, qty, seller, price)
	}
}

// crossingFlow sends the four orders of the dry run below and returns the
// three trades it produces.
//
//	o0 bob   sell 10 @ 150.00  rests 10
//	o1 alice buy   4 @ 150.50  fills 4 vs bob @ 150.00
//	o2 alice buy  10 @ 150.00  fills 6 vs bob @ 150.00, rests 4
//	o3 carol sell  5 @ 149.00  fills 4 vs alice @ 150.00, rests 1
func crossingFlow(t *testing.T, f fixture) {
	t.Helper()
	f.order(t, "bob", "AAPL", messages.SideSell, "10", "150.00")
	f.order(t, "alice", "AAPL", messages.SideBuy, "4", "150.50")
	f.order(t, "alice", "AAPL", messages.SideBuy, "10", "150")
	f.order(t, "carol", "AAPL", messages.SideSell, "5", "149")
}

func TestCrossingOrdersProduceTrades(t *testing.T) {
	f := newFixture(t)
	a := f.start(t, kafkatest.GroupID(t))
	crossingFlow(t, f)

	trades := decodeTrades(t, kafkatest.ReadN(t, f.trades, 3, waitLong))
	requireTrade(t, trades[0], "alice", "bob", "4", "150")
	requireTrade(t, trades[1], "alice", "bob", "6", "150")
	requireTrade(t, trades[2], "alice", "carol", "4", "150")
	for i, tr := range trades {
		parts := strings.Split(tr.TradeID, "-")
		if len(parts) != 4 || parts[0] != "AAPL" || parts[2] != []string{"1", "2", "3"}[i] || parts[3] != "1" {
			t.Fatalf("trade id %q, want AAPL-<partition>-<offset>-1 with offsets 1,2,3", tr.TradeID)
		}
	}
	kafkatest.Eventually(t, waitLong, "4 orders", func() bool { return a.Stats().Orders.Load() == 4 })
}

// The adapter restarts: the new engine session is empty, so it replays the
// four orders without publishing, then matches a new order against the
// rebuilt book.
func TestRestartReplaysWithoutRepublishing(t *testing.T) {
	f := newFixture(t)
	group := kafkatest.GroupID(t)
	first := f.start(t, group)
	crossingFlow(t, f)
	kafkatest.ReadN(t, f.trades, 3, waitLong)
	kafkatest.Eventually(t, waitLong, "first adapter done", func() bool { return first.Stats().Orders.Load() == 4 })
	first.stop()

	second := f.start(t, group)
	kafkatest.Eventually(t, waitLong, "replay", func() bool {
		return second.Stats().CaughtUp.Load() && second.Stats().Replayed.Load() == 4
	})
	f.order(t, "dave", "AAPL", messages.SideBuy, "1", "149")

	trades := decodeTrades(t, kafkatest.ReadN(t, f.trades, 4, waitLong))
	requireTrade(t, trades[3], "dave", "carol", "1", "149") // carol's rebuilt resting 1 @ 149
	if second.Stats().Trades.Load() != 1 {
		t.Fatalf("second adapter published %d trades, want 1", second.Stats().Trades.Load())
	}
}

// Lost progress (crash after publishing, before committing): everything is
// processed live again and republished with identical ids and contents,
// which settlement dedupes.
func TestLostProgressRepublishesIdenticalTrades(t *testing.T) {
	f := newFixture(t)
	first := f.start(t, kafkatest.GroupID(t))
	crossingFlow(t, f)
	kafkatest.ReadN(t, f.trades, 3, waitLong)
	first.stop()

	f.start(t, kafkatest.GroupID(t)) // no progress recorded
	recs := kafkatest.ReadN(t, f.trades, 6, waitLong)
	for i := range 3 {
		if string(recs[i].Value) != string(recs[i+3].Value) {
			t.Fatalf("republished trade differs:\n%s\n%s", recs[i].Value, recs[i+3].Value)
		}
	}
}

// If the engine loses the session (restart, eviction), Run returns an error
// so the process restarts and replays.
func TestEngineSessionLostStopsAdapter(t *testing.T) {
	f := newFixture(t)
	a, err := matcher.New(matcher.Config{
		Engine: f.engine, Brokers: kafkatest.Brokers(), OrdersTopic: f.orders, TradesTopic: f.trades,
		Group: kafkatest.GroupID(t), Symbols: symbols(), Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), waitLong)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	kafkatest.Eventually(t, waitLong, "session", func() bool { return a.Session() != "" })
	if _, err := f.engine.CloseSession(ctx, &matcherv1.CloseSessionRequest{Session: a.Session()}); err != nil {
		t.Fatal(err)
	}
	f.order(t, "bob", "AAPL", messages.SideSell, "1", "150")

	err = <-done
	if status.Code(errors.Unwrap(err)) != codes.NotFound {
		t.Fatalf("Run = %v, want engine NotFound", err)
	}
}

// Bad orders are skipped without consuming engine ids; a self-match publishes
// nothing; normal trading continues.
func TestRejectsAndSelfTrades(t *testing.T) {
	f := newFixture(t)
	a := f.start(t, kafkatest.GroupID(t))

	f.order(t, "bob", "GOOG", messages.SideSell, "1", "150")                                                                       // unknown symbol
	f.order(t, "bob", "AAPL", messages.SideSell, "1", "150.001")                                                                   // off tick
	f.order(t, "bob", "AAPL", messages.SideSell, "1.5", "150")                                                                     // fractional quantity
	f.order(t, "b:ob", "AAPL", messages.SideSell, "1", "150")                                                                      // bad account id
	f.order(t, "bob", "AAPL", messages.SideSell, "1", "1000")                                                                      // outside the book: engine rejects
	f.produceRaw(t, "AAPL", []byte(`{nope`))                                                                                       // malformed
	f.produceRaw(t, "MSFT", []byte(`{"version":1,"account_id":"bob","symbol":"AAPL","side":"sell","quantity":"1","price":"150"}`)) // key mismatch
	f.order(t, "alice", "AAPL", messages.SideSell, "1", "150")
	f.order(t, "alice", "AAPL", messages.SideBuy, "1", "150") // self-match
	f.order(t, "bob", "AAPL", messages.SideSell, "2", "150")
	f.order(t, "carol", "AAPL", messages.SideBuy, "2", "150")

	trades := decodeTrades(t, kafkatest.ReadN(t, f.trades, 1, waitLong))
	requireTrade(t, trades[0], "carol", "bob", "2", "150")
	kafkatest.Eventually(t, waitLong, "all orders seen", func() bool {
		return a.Stats().Rejected.Load() == 7 && a.Stats().Orders.Load() == 5
	})
	if a.Stats().SelfTrades.Load() != 1 {
		t.Fatalf("self trades = %d, want 1", a.Stats().SelfTrades.Load())
	}
}

func TestParseSymbols(t *testing.T) {
	specs, err := matcher.ParseSymbols(matcher.DefaultSymbols)
	if err != nil || len(specs) != 4 || specs[0].Symbol != "AAPL" || !specs[0].TickSize.Equal(decimal.RequireFromString("0.01")) {
		t.Fatalf("default symbols: %+v %v", specs, err)
	}
	for _, bad := range []string{"", "AAPL", "AAPL:USD:0:150", "AAPL:USD:0.01:150.001", "A-B:USD:0.01:1", "AAPL:AAPL:0.01:1", "AAPL:USD:0.01:150,AAPL:USD:0.01:150"} {
		if _, err := matcher.ParseSymbols(bad); err == nil {
			t.Errorf("ParseSymbols(%q) succeeded", bad)
		}
	}
}
