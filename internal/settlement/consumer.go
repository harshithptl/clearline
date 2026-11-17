package settlement

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/backoff"
	"github.com/harshithptl/clearline/internal/dlq"
	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
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
	cfg   ConsumerConfig
	stats Stats
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
const maxPollRecords = 500

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

		recs := fetches.Records()
		done := 0
		for _, r := range recs {
			if err := c.handle(ctx, r); err != nil {
				break // only on shutdown
			}
			done++
		}
		c.commit(recs[:done])
		cl.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
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
		out, err := Settle(ctx, c.cfg.DB, trade)
		if err == nil {
			if out == Posted {
				c.stats.Posted.Add(1)
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
