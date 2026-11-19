package balance

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/backoff"
	"github.com/harshithptl/clearline/internal/messages"
)

// Consumer applies ledger-events to the Store and commits offsets after each
// batch is applied. Redelivered events are dropped by Store.Apply.
//
// A malformed event is logged and skipped: it can only come from a bug in our
// own outbox, and the reconciler then reports that account as lagging. Valkey
// errors are retried with backoff until they clear.
type Consumer struct {
	cl      *kgo.Client
	store   *Store
	backoff backoff.Policy
	log     *slog.Logger
	stats   ConsumerStats
	// onApplied, if set, is called with each newly applied event (not
	// duplicates), after Valkey has it. Must not block.
	onApplied func(messages.LedgerEvent)
}

// OnApplied registers f to be called with each newly applied event, for
// example Hub.Publish. Call before Run. f must not block.
func (c *Consumer) OnApplied(f func(messages.LedgerEvent)) { c.onApplied = f }

// ConsumerStats are running counters, safe to read while the consumer runs.
type ConsumerStats struct {
	Applied    atomic.Int64
	Duplicates atomic.Int64
	Malformed  atomic.Int64
	Retries    atomic.Int64
	Committed  atomic.Int64
}

// ConsumerClientOpts are the client options a Consumer requires.
func ConsumerClientOpts(group, topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
}

// NewConsumer returns a Consumer. cl must be created with ConsumerClientOpts.
func NewConsumer(cl *kgo.Client, store *Store, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		cl:      cl,
		store:   store,
		backoff: backoff.Policy{Base: 50 * time.Millisecond, Max: 5 * time.Second},
		log:     log,
	}
}

// Stats returns the live counters.
func (c *Consumer) Stats() *ConsumerStats { return &c.stats }

// Run consumes until ctx is canceled, then returns nil.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollRecords(ctx, 500)
		if fetches.IsClientClosed() {
			return errors.New("balance consumer: client closed")
		}
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
			return nil
		}
		fetches.EachError(c.logFetchError)
		recs := fetches.Records()
		done := 0
		for _, r := range recs {
			if err := c.handle(ctx, r); err != nil {
				break // shutdown
			}
			done++
		}
		c.commit(recs[:done])
		c.cl.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) logFetchError(topic string, partition int32, err error) {
	c.log.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
}

func (c *Consumer) commit(recs []*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(ctx, recs...); err != nil {
		c.log.Warn("commit failed; records will be redelivered", "records", len(recs), "err", err)
		return
	}
	c.stats.Committed.Add(int64(len(recs)))
}

// handle applies one record. It returns an error only on shutdown.
func (c *Consumer) handle(ctx context.Context, r *kgo.Record) error {
	ev, err := messages.DecodeLedgerEvent(r.Value)
	if err == nil && ev.AccountID != string(r.Key) {
		err = errors.New("account_id does not match record key")
	}
	if err != nil {
		c.stats.Malformed.Add(1)
		c.log.Error("skipping malformed ledger event", "partition", r.Partition, "offset", r.Offset, "err", err)
		return nil
	}
	for attempt := 1; ; attempt++ {
		applied, err := c.store.Apply(ctx, ev)
		if err == nil {
			if applied {
				c.stats.Applied.Add(1)
				if c.onApplied != nil {
					c.onApplied(ev)
				}
			} else {
				c.stats.Duplicates.Add(1)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, messages.ErrMalformed) {
			c.stats.Malformed.Add(1)
			c.log.Error("skipping malformed ledger event", "event_id", ev.EventID, "err", err)
			return nil
		}
		c.stats.Retries.Add(1)
		c.log.Warn("apply failed, retrying", "event_id", ev.EventID, "attempt", attempt, "err", err)
		if err := backoff.Sleep(ctx, c.backoff.Delay(attempt)); err != nil {
			return err
		}
	}
}
