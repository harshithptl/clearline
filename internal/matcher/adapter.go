// Package matcher is the matcher adapter (SPEC 3.2): it feeds the orders
// topic through the C++ engine and publishes the resulting trades.
//
// The engine keeps its books in memory, so the adapter owns recovery
// (docs/decisions/0002-matcher-replay.md):
//
//	start:  open a new engine session (empty books)
//	        read progress: the committed offset per orders partition
//	        consume every orders partition from offset 0
//	record below committed offset  -> replay: submit, discard the trades
//	record at or after it          -> live: submit, publish trades (acks=all),
//	                                  then commit progress
//	engine or Kafka error          -> return; the process restarts and replays
//
// Trade ids are derived from the order's position in the log:
// <symbol>-<partition>-<offset>-<fill number>. The engine is deterministic, so
// replaying the same log always yields the same fills with the same ids. A
// crash after publishing but before committing progress republishes those
// trades with identical ids, and settlement drops them as duplicates.
//
// Run one adapter per engine session. Two adapters on the same log are wasteful
// but safe: they produce identical trades, which settlement dedupes.
package matcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	matcherv1 "github.com/harshithptl/clearline/gen/matcher/v1"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
)

// Config configures an Adapter.
type Config struct {
	Engine      matcherv1.MatcherClient
	Brokers     []string
	OrdersTopic string
	TradesTopic string
	// Group stores the adapter's progress (committed offsets). The adapter
	// does not join it; it only reads and writes its offsets.
	Group   string
	Symbols []SymbolSpec
	Logger  *slog.Logger
}

// Stats are running counters, safe to read while the adapter runs.
type Stats struct {
	Replayed   atomic.Int64 // orders resubmitted to rebuild the books
	Orders     atomic.Int64 // live orders submitted
	Rejected   atomic.Int64 // orders invalid or rejected by the engine (replay included)
	Trades     atomic.Int64 // trades published
	SelfTrades atomic.Int64 // matches between one account's own orders, not published
	CaughtUp   atomic.Bool  // replay finished on every partition
}

// Adapter runs the orders -> engine -> trades loop. Create with New.
type Adapter struct {
	cfg     Config
	symbols map[string]SymbolSpec
	stats   Stats
	session atomic.Value // string
}

// New validates cfg.
func New(cfg Config) (*Adapter, error) {
	if cfg.Engine == nil || len(cfg.Brokers) == 0 || cfg.OrdersTopic == "" || cfg.TradesTopic == "" || cfg.Group == "" {
		return nil, errors.New("matcher adapter: Engine, Brokers, OrdersTopic, TradesTopic, Group are required")
	}
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("matcher adapter: no symbols")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	a := &Adapter{cfg: cfg, symbols: map[string]SymbolSpec{}}
	for _, s := range cfg.Symbols {
		if err := s.validate(); err != nil {
			return nil, err
		}
		a.symbols[s.Symbol] = s
	}
	a.session.Store("")
	return a, nil
}

// Stats returns the live counters.
func (a *Adapter) Stats() *Stats { return &a.stats }

// Session returns the current engine session id ("" before Run opens one).
func (a *Adapter) Session() string { return a.session.Load().(string) }

// resting is the adapter's view of one resting order. The engine knows only
// integer ids; accounts live here.
type resting struct {
	account   string
	remaining uint32
}

// book is per-symbol adapter state, rebuilt on every start by replay.
type book struct {
	spec    SymbolSpec
	nextID  uint64
	resting map[uint64]*resting
}

// pending is one order sent to the engine in the current batch.
type pending struct {
	rec     *kgo.Record
	order   messages.ParsedOrder
	book    *book
	id      uint64
	qty     uint32
	replay  bool
	request *matcherv1.Order
}

const (
	maxBatch      = 5000 // below the engine's 10,000 orders per Submit
	engineTimeout = 30 * time.Second
)

// Run processes orders until ctx is canceled (returns nil) or a fatal error
// (returns it; restart to recover).
func (a *Adapter) Run(ctx context.Context) error {
	admCl, err := kgo.NewClient(kgo.SeedBrokers(a.cfg.Brokers...))
	if err != nil {
		return err
	}
	defer admCl.Close()
	adm := kadm.NewClient(admCl)

	committed, err := a.progress(ctx, adm)
	if err != nil {
		return err
	}
	books, err := a.openSession(ctx)
	if err != nil {
		return err
	}
	defer a.closeSession()

	// replayEnd is fixed at start; committed advances as live records commit.
	starts := map[int32]kgo.Offset{}
	replayEnd := map[int32]int64{}
	for p, off := range committed {
		starts[p] = kgo.NewOffset().At(0)
		if off > 0 {
			replayEnd[p] = off
		}
	}
	pendingReplay := len(replayEnd)
	a.stats.CaughtUp.Store(pendingReplay == 0)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(a.cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{a.cfg.OrdersTopic: starts}),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	a.cfg.Logger.Info("matcher adapter started", "session", a.Session(), "committed", committed)

	for {
		fetches := cl.PollRecords(ctx, maxBatch)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("fetch orders: %w", err)
		}
		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		if err := a.process(ctx, cl, adm, books, committed, recs); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for _, r := range recs {
			if end, ok := replayEnd[r.Partition]; ok && r.Offset+1 >= end {
				delete(replayEnd, r.Partition)
				pendingReplay--
			}
		}
		if pendingReplay == 0 && !a.stats.CaughtUp.Load() {
			a.stats.CaughtUp.Store(true)
			a.cfg.Logger.Info("matcher adapter caught up", "replayed", a.stats.Replayed.Load())
		}
	}
}

// progress returns the committed offset for every orders partition (0 if
// none), and refuses to start if the log no longer begins at offset 0.
func (a *Adapter) progress(ctx context.Context, adm *kadm.Client) (map[int32]int64, error) {
	starts, err := adm.ListStartOffsets(ctx, a.cfg.OrdersTopic)
	if err != nil {
		return nil, fmt.Errorf("orders start offsets: %w", err)
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("orders start offsets: %w", err)
	}
	offs, err := adm.FetchOffsetsForTopics(ctx, a.cfg.Group, a.cfg.OrdersTopic)
	if err != nil {
		return nil, fmt.Errorf("adapter progress: %w", err)
	}
	out := map[int32]int64{}
	var truncated error
	starts.Each(func(s kadm.ListedOffset) {
		if s.Offset != 0 {
			truncated = fmt.Errorf("orders partition %d starts at offset %d, not 0: the books cannot be rebuilt", s.Partition, s.Offset)
		}
		out[s.Partition] = 0
		if c, ok := offs.Lookup(a.cfg.OrdersTopic, s.Partition); ok && c.Err == nil && c.At > 0 {
			out[s.Partition] = c.At
		}
	})
	if truncated != nil {
		return nil, truncated
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("orders topic %s has no partitions", a.cfg.OrdersTopic)
	}
	return out, nil
}

func (a *Adapter) openSession(ctx context.Context) (map[string]*book, error) {
	req := &matcherv1.OpenSessionRequest{}
	books := map[string]*book{}
	for _, s := range a.cfg.Symbols {
		spec, err := s.bookSpec()
		if err != nil {
			return nil, err
		}
		req.Books = append(req.Books, spec)
		books[s.Symbol] = &book{spec: s, resting: map[uint64]*resting{}}
	}
	cctx, cancel := context.WithTimeout(ctx, engineTimeout)
	defer cancel()
	resp, err := a.cfg.Engine.OpenSession(cctx, req)
	if err != nil {
		return nil, fmt.Errorf("engine open session: %w", err)
	}
	a.session.Store(resp.GetSession())
	return books, nil
}

func (a *Adapter) closeSession() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.cfg.Engine.CloseSession(ctx, &matcherv1.CloseSessionRequest{Session: a.Session()}); err != nil {
		a.cfg.Logger.Warn("engine close session", "err", err)
	}
}

// process submits one batch, publishes live trades, then commits progress.
func (a *Adapter) process(ctx context.Context, cl *kgo.Client, adm *kadm.Client,
	books map[string]*book, committed map[int32]int64, recs []*kgo.Record) error {

	batch := make([]*pending, 0, len(recs))
	for _, r := range recs {
		replay := r.Offset < committed[r.Partition]
		p, err := a.prepare(r, books, replay)
		if err != nil {
			a.stats.Rejected.Add(1)
			if !replay {
				a.cfg.Logger.Warn("order rejected", "partition", r.Partition, "offset", r.Offset, "err", err)
			}
			continue
		}
		batch = append(batch, p)
	}

	var trades []*kgo.Record
	if len(batch) > 0 {
		req := &matcherv1.SubmitRequest{Session: a.Session(), Orders: make([]*matcherv1.Order, len(batch))}
		for i, p := range batch {
			req.Orders[i] = p.request
		}
		cctx, cancel := context.WithTimeout(ctx, engineTimeout)
		resp, err := a.cfg.Engine.Submit(cctx, req)
		cancel()
		if err != nil {
			return fmt.Errorf("engine submit: %w", err)
		}
		if len(resp.GetResults()) != len(batch) {
			return fmt.Errorf("engine returned %d results for %d orders", len(resp.GetResults()), len(batch))
		}
		for i, p := range batch {
			recs, err := a.apply(p, resp.GetResults()[i])
			if err != nil {
				return err
			}
			trades = append(trades, recs...)
		}
	}

	if len(trades) > 0 {
		if err := cl.ProduceSync(ctx, trades...).FirstErr(); err != nil {
			return fmt.Errorf("produce trades: %w", err)
		}
		a.stats.Trades.Add(int64(len(trades)))
	}
	return a.commit(ctx, adm, committed, recs)
}

// prepare validates an order and assigns its engine id. Invalid orders
// consume no id. Validation depends only on the record and the symbol
// config, so replay makes the same decisions.
func (a *Adapter) prepare(r *kgo.Record, books map[string]*book, replay bool) (*pending, error) {
	o, err := messages.DecodeOrder(r.Value)
	if err != nil {
		return nil, err
	}
	b, ok := books[o.Symbol]
	if !ok {
		return nil, fmt.Errorf("unknown symbol %q", o.Symbol)
	}
	if string(r.Key) != o.Symbol {
		return nil, fmt.Errorf("record key %q does not match symbol %q", r.Key, o.Symbol)
	}
	if err := ledger.ValidAccountID(o.AccountID); err != nil {
		return nil, err
	}
	if !o.Quantity.IsInteger() || !o.Quantity.IsPositive() || o.Quantity.GreaterThan(decimal.NewFromInt(math.MaxUint32)) {
		return nil, fmt.Errorf("quantity %s must be a whole number from 1 to %d", o.Quantity, uint32(math.MaxUint32))
	}
	ticks, err := b.spec.ticks(o.Price)
	if err != nil {
		return nil, fmt.Errorf("price %s (tick %s): %w", o.Price, b.spec.TickSize, err)
	}
	side := matcherv1.Side_SIDE_BUY
	if o.Side == messages.SideSell {
		side = matcherv1.Side_SIDE_SELL
	}
	id := b.nextID
	b.nextID++
	qty := uint32(o.Quantity.IntPart())
	return &pending{
		rec: r, order: o, book: b, id: id, qty: qty, replay: replay,
		request: &matcherv1.Order{Symbol: o.Symbol, OrderId: id, Side: side, PriceTicks: ticks, Quantity: qty},
	}, nil
}

// apply updates adapter state from one engine result and returns the trade
// records to publish (none during replay).
func (a *Adapter) apply(p *pending, res *matcherv1.OrderResult) ([]*kgo.Record, error) {
	if p.replay {
		a.stats.Replayed.Add(1)
	} else {
		a.stats.Orders.Add(1)
	}
	if res.GetStatus() != matcherv1.Status_STATUS_ACCEPTED {
		a.stats.Rejected.Add(1)
		if !p.replay {
			a.cfg.Logger.Warn("engine rejected order", "partition", p.rec.Partition, "offset", p.rec.Offset, "status", res.GetStatus())
		}
		return nil, nil
	}

	var out []*kgo.Record
	for n, f := range res.GetFills() {
		maker, ok := p.book.resting[f.GetMakerOrderId()]
		if !ok || maker.remaining < f.GetQuantity() {
			return nil, fmt.Errorf("engine filled unknown maker %d on %s: adapter and engine state diverged",
				f.GetMakerOrderId(), p.book.spec.Symbol)
		}
		maker.remaining -= f.GetQuantity()
		if maker.remaining == 0 {
			delete(p.book.resting, f.GetMakerOrderId())
		}
		if maker.account == p.order.AccountID {
			a.stats.SelfTrades.Add(1) // nets to zero: nothing to settle
			continue
		}
		if p.replay {
			continue
		}
		buyer, seller := p.order.AccountID, maker.account
		if p.order.Side == messages.SideSell {
			buyer, seller = seller, buyer
		}
		trade := ledger.Trade{
			TradeID:    fmt.Sprintf("%s-%d-%d-%d", p.book.spec.Symbol, p.rec.Partition, p.rec.Offset, n+1),
			Symbol:     p.book.spec.Symbol,
			QuoteAsset: p.book.spec.QuoteAsset,
			BuyerID:    buyer,
			SellerID:   seller,
			Quantity:   decimal.NewFromInt(int64(f.GetQuantity())),
			Price:      p.book.spec.price(f.GetPriceTicks()),
		}
		b, err := messages.EncodeTrade(trade)
		if err != nil {
			return nil, err
		}
		out = append(out, &kgo.Record{
			Topic:   a.cfg.TradesTopic,
			Key:     []byte(trade.Symbol),
			Value:   b,
			Headers: p.rec.Headers, // carries trace context once Phase 3 adds it
		})
	}
	if rest := res.GetRestingQuantity(); rest > 0 {
		p.book.resting[p.id] = &resting{account: p.order.AccountID, remaining: rest}
	}
	return out, nil
}

// commit records progress for live records, after their trades are durable.
func (a *Adapter) commit(ctx context.Context, adm *kadm.Client, committed map[int32]int64, recs []*kgo.Record) error {
	offs := kadm.Offsets{}
	for _, r := range recs {
		if next := r.Offset + 1; next > committed[r.Partition] {
			committed[r.Partition] = next
			offs.AddOffset(r.Topic, r.Partition, next, -1)
		}
	}
	if len(offs) == 0 {
		return nil
	}
	if err := adm.CommitAllOffsets(ctx, a.cfg.Group, offs); err != nil {
		return fmt.Errorf("commit progress: %w", err)
	}
	return nil
}
