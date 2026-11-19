package balance

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/backoff"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/metrics"
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
	// Set by Instrument; nil records nothing.
	applyDuration prometheus.Histogram
	batchSize     prometheus.Histogram
	freshness     prometheus.Histogram

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
//
// Each fetch is split by partition and the partitions are applied
// concurrently, one goroutine each. That keeps every account's events in
// order (ledger-events is keyed by account, so an account lives on one
// partition) while partitions, which share no accounts, proceed in
// parallel. Within a partition the records go to Valkey as one batch.
// Offsets are committed after every partition's batch has been applied.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollRecords(ctx, 2000)
		if fetches.IsClientClosed() {
			return errors.New("balance consumer: client closed")
		}
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
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
				if c.handleBatch(ctx, recs) == nil {
					done[i] = recs
				}
			}(i, recs)
		}
		wg.Wait()
		var ok []*kgo.Record
		for _, d := range done {
			ok = append(ok, d...)
		}
		c.commit(ok)
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

// observe records the latency of one newly applied event's record.
func (c *Consumer) observe(r *kgo.Record, now time.Time) {
	if c.freshness == nil {
		return
	}
	if d, ok := metrics.Since(messages.HeaderTime(r.Headers, messages.HeaderCommitTime), now); ok {
		c.freshness.Observe(d)
	}
}

// handleBatch applies one partition's records, in order, as one batch. It
// returns an error only on shutdown, in which case nothing may be committed.
func (c *Consumer) handleBatch(ctx context.Context, recs []*kgo.Record) error {
	events := make([]messages.LedgerEvent, 0, len(recs))
	kept := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		ev, err := messages.DecodeLedgerEvent(r.Value)
		if err == nil && ev.AccountID != string(r.Key) {
			err = errors.New("account_id does not match record key")
		}
		if err != nil {
			c.stats.Malformed.Add(1)
			c.log.Error("skipping malformed ledger event", "partition", r.Partition, "offset", r.Offset, "err", err)
			continue
		}
		events = append(events, ev)
		kept = append(kept, r)
	}
	if len(events) == 0 {
		return nil
	}
	for attempt := 1; ; attempt++ {
		start := time.Now()
		results, err := c.store.ApplyBatch(ctx, events)
		if err == nil {
			now := time.Now()
			if c.applyDuration != nil {
				c.applyDuration.Observe(now.Sub(start).Seconds())
				c.batchSize.Observe(float64(len(events)))
			}
			for i, res := range results {
				switch res {
				case Applied:
					c.stats.Applied.Add(1)
					c.observe(kept[i], now)
					if c.onApplied != nil {
						c.onApplied(events[i])
					}
				case Duplicate:
					c.stats.Duplicates.Add(1)
				case Malformed:
					c.stats.Malformed.Add(1)
					c.log.Error("skipping malformed ledger event", "event_id", events[i].EventID)
				}
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.stats.Retries.Add(1)
		c.log.Warn("apply batch failed, retrying", "events", len(events), "attempt", attempt, "err", err)
		if err := backoff.Sleep(ctx, c.backoff.Delay(attempt)); err != nil {
			return err
		}
	}
}
