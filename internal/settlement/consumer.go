package settlement

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/backoff"
	"github.com/harshithptl/clearline/internal/dlq"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/metrics"
)

// Consumer reads the trades topic and settles each trade (SPEC 3.4 #4):
//
//	poll a batch of records
//	for each record, in partition order:
//	    decode, Settle         -> Posted or Duplicate
//	    permanent failure      -> DLQ now
//	    message-level failure  -> retry with backoff, DLQ after MaxAttempts
//	    infrastructure failure -> retry with backoff until it works
//	commit offsets for the batch
//
// An offset is committed only after its trade is in Postgres or its record is
// in the DLQ, so a crash anywhere leads to redelivery, never to loss.
// Redelivery is safe because Settle dedupes on trade_id.
//
// Infrastructure failures (Postgres down) retry forever instead of going to
// the DLQ: dead-lettering every trade during an outage would just move the
// whole stream into the DLQ. The pipeline stalls instead, and consumer lag
// alerts fire.
type Consumer struct {
	cfg     ConsumerConfig
	stats   Stats
	metrics *consumerMetrics // set by Instrument; nil records nothing
}

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	// Client must be created with ConsumerClientOpts.
	Client *kgo.Client
	DB     TxBeginner
	// DLQTopic receives records that cannot be settled.
	DLQTopic string
	// MaxAttempts bounds retries of message-level failures. Default 5.
	MaxAttempts int
	// Backoff between retries. Default 100ms doubling up to 5s.
	Backoff backoff.Policy
	Logger  *slog.Logger
}

// Stats are running counters, safe to read while the consumer runs.
type Stats struct {
	Posted       atomic.Int64
	Duplicates   atomic.Int64
	DeadLettered atomic.Int64
	Retries      atomic.Int64
	Committed    atomic.Int64 // records whose offsets were committed
}

// maxPollRecords bounds a batch so offsets are committed regularly.
const maxPollRecords = 2000

// ConsumerClientOpts are the client options a Consumer requires: manual
// commits, no rebalance mid-batch, durable DLQ produces.
func ConsumerClientOpts(group, tradesTopic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(tradesTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// Rebalances wait until we commit the batch we polled, so partitions
		// never move to another consumer with work half done.
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
}

// NewConsumer validates cfg and applies defaults.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Client == nil || cfg.DB == nil || cfg.DLQTopic == "" {
		return nil, errors.New("settlement consumer: Client, DB, and DLQTopic are required")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff.Base <= 0 {
		cfg.Backoff.Base = 100 * time.Millisecond
	}
	if cfg.Backoff.Max <= 0 {
		cfg.Backoff.Max = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Consumer{cfg: cfg}, nil
}

// Stats returns the live counters.
func (c *Consumer) Stats() *Stats { return &c.stats }

// Run consumes until ctx is canceled, then commits what it finished and
// returns nil. Any other return is a bug or a closed client.
//
// Each fetch is split by partition. Partitions settle concurrently, one
// goroutine each, and each partition's records settle as one batch
// (SettleBatch: one transaction, one commit). Offsets are committed after
// every partition has finished, still only after the database commit.
func (c *Consumer) Run(ctx context.Context) error {
	cl := c.cfg.Client
	for {
		fetches := cl.PollRecords(ctx, maxPollRecords)
		if fetches.IsClientClosed() {
			return errors.New("settlement consumer: client closed")
		}
		if ctx.Err() != nil {
			cl.AllowRebalance()
			return nil
		}
		fetches.EachError(c.logFetchError)

		var parts [][]*kgo.Record
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) > 0 {
				parts = append(parts, p.Records)
			}
		})
		done := make([][]*kgo.Record, len(parts))
		var wg sync.WaitGroup
		for i, recs := range parts {
			wg.Add(1)
			go func(i int, recs []*kgo.Record) {
				defer wg.Done()
				done[i] = recs[:c.handlePartition(ctx, recs)]
			}(i, recs)
		}
		wg.Wait()
		var ok []*kgo.Record
		for _, d := range done {
			ok = append(ok, d...)
		}
		c.commit(ok)
		cl.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
}

// handlePartition settles one partition's records and returns how many, from
// the start, are finished (safe to commit). Fewer than len(recs) only on
// shutdown.
//
// The fast path settles them all in one SettleBatch. A batch-level failure
// caused by a message (invalid trade, unknown account) falls back to settling
// one record at a time, which retries and dead-letters only the bad record.
// Infrastructure failures (Postgres down) retry the whole batch.
func (c *Consumer) handlePartition(ctx context.Context, recs []*kgo.Record) int {
	trades := make([]ledger.Trade, len(recs))
	for i, r := range recs {
		t, err := messages.DecodeTrade(r.Value)
		if err != nil {
			return c.handleEach(ctx, recs) // a malformed record: isolate it
		}
		trades[i] = t
	}
	for attempt := 1; ; attempt++ {
		start := time.Now()
		out, err := SettleBatch(ctx, c.cfg.DB, trades)
		if err == nil {
			c.observeBatch(recs, out, start)
			return len(recs)
		}
		if ctx.Err() != nil {
			return 0
		}
		if classify(err) != infrastructure {
			c.cfg.Logger.Warn("batch failed on a message, settling one by one", "records", len(recs), "err", err)
			return c.handleEach(ctx, recs)
		}
		c.stats.Retries.Add(1)
		c.cfg.Logger.Warn("settle batch failed, retrying", "records", len(recs), "attempt", attempt, "err", err)
		if backoff.Sleep(ctx, c.cfg.Backoff.Delay(attempt)) != nil {
			return 0
		}
	}
}

// handleEach settles records one at a time (the per-record path with
// retries and DLQ) and returns how many finished.
func (c *Consumer) handleEach(ctx context.Context, recs []*kgo.Record) int {
	for i, r := range recs {
		if err := c.handle(ctx, r); err != nil {
			return i // only on shutdown
		}
	}
	return len(recs)
}

func (c *Consumer) logFetchError(topic string, partition int32, err error) {
	c.cfg.Logger.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
}

// commit commits offsets for processed records. It uses a fresh context so a
// shutdown still commits finished work. A failed commit is only logged: the
// records will be redelivered and deduped.
func (c *Consumer) commit(recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.cfg.Client.CommitRecords(ctx, recs...); err != nil {
		c.cfg.Logger.Warn("commit failed; records will be redelivered", "records", len(recs), "err", err)
		return
	}
	c.stats.Committed.Add(int64(len(recs)))
}

// failure classes, see Consumer.
type failureClass int

const (
	permanent failureClass = iota
	messageLevel
	infrastructure
)

// classify decides how to handle a Settle error.
func classify(err error) failureClass {
	if errors.Is(err, ledger.ErrInvalidTrade) || errors.Is(err, messages.ErrMalformed) {
		return permanent
	}
	if errors.Is(err, ErrUnknownAccount) {
		return messageLevel
	}
	// Postgres data exceptions (22) and integrity violations (23) are caused
	// by this message's content, so retrying forever would block the
	// partition. Everything else (connection refused, timeouts, shutdown) is
	// the environment.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		switch pgErr.Code[:2] {
		case "22", "23":
			return messageLevel
		}
	}
	return infrastructure
}

// handle settles one record or dead-letters it. It returns an error only
// when ctx is canceled, in which case the record must not be committed.
func (c *Consumer) handle(ctx context.Context, r *kgo.Record) error {
	trade, err := messages.DecodeTrade(r.Value)
	if err != nil {
		return c.deadLetter(ctx, r, err, 1)
	}
	for attempt := 1; ; attempt++ {
		start := time.Now()
		out, err := Settle(ctx, c.cfg.DB, trade)
		if err == nil {
			if out == Posted {
				c.stats.Posted.Add(1)
				c.observe(r, start)
			} else {
				c.stats.Duplicates.Add(1)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		class := classify(err)
		if class == permanent || (class == messageLevel && attempt >= c.cfg.MaxAttempts) {
			return c.deadLetter(ctx, r, err, attempt)
		}
		c.stats.Retries.Add(1)
		c.cfg.Logger.Warn("settle failed, retrying",
			"trade_id", trade.TradeID, "attempt", attempt, "infrastructure", class == infrastructure, "err", err)
		if err := backoff.Sleep(ctx, c.cfg.Backoff.Delay(attempt)); err != nil {
			return err
		}
	}
}

// observeBatch counts a settled batch and records its latencies.
func (c *Consumer) observeBatch(recs []*kgo.Record, out []Outcome, start time.Time) {
	now := time.Now()
	for i, o := range out {
		if o == Posted {
			c.stats.Posted.Add(1)
		} else {
			c.stats.Duplicates.Add(1)
		}
		if o == Posted && c.metrics != nil {
			if d, ok := metrics.Since(messages.HeaderTime(recs[i].Headers, messages.HeaderOrderTime), now); ok {
				c.metrics.orderToLedger.Observe(d)
			}
		}
	}
	if c.metrics != nil {
		c.metrics.settle.Observe(now.Sub(start).Seconds())
		c.metrics.batchSize.Observe(float64(len(recs)))
	}
}

// observe records the latencies of a newly posted trade.
func (c *Consumer) observe(r *kgo.Record, start time.Time) {
	if c.metrics == nil {
		return
	}
	now := time.Now()
	c.metrics.settle.Observe(now.Sub(start).Seconds())
	if d, ok := metrics.Since(messages.HeaderTime(r.Headers, messages.HeaderOrderTime), now); ok {
		c.metrics.orderToLedger.Observe(d)
	}
}

// deadLetter produces r to the DLQ, retrying until it succeeds: the offset is
// committed only after the DLQ has the record.
func (c *Consumer) deadLetter(ctx context.Context, r *kgo.Record, cause error, attempts int) error {
	rec := dlq.NewRecord(c.cfg.DLQTopic, r, cause, attempts)
	for try := 1; ; try++ {
		err := c.cfg.Client.ProduceSync(ctx, rec).FirstErr()
		if err == nil {
			c.stats.DeadLettered.Add(1)
			c.cfg.Logger.Error("trade dead-lettered",
				"partition", r.Partition, "offset", r.Offset, "attempts", attempts, "err", cause)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.cfg.Logger.Warn("dlq produce failed, retrying", "try", try, "err", err)
		if err := backoff.Sleep(ctx, c.cfg.Backoff.Delay(try)); err != nil {
			return err
		}
	}
}
