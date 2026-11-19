// Package outbox publishes rows from the transactional outbox to Kafka
// (SPEC 3.4 #3).
//
// One relay loop, per batch:
//
//	BEGIN
//	  SELECT unpublished rows ORDER BY outbox_id LIMIT n FOR UPDATE
//	  produce them to Kafka (acks=all, idempotent producer), wait for acks
//	  UPDATE outbox SET published_at = now() for those rows
//	COMMIT
//
// A crash after the produce but before the COMMIT leaves the rows
// unpublished, so the next batch sends them again. That is at-least-once
// delivery; the balance service dedupes on event_id (SPEC 5, last row).
//
// Only one relay is active at a time: it holds a Postgres advisory lock (a
// named lock not tied to any table) on a dedicated connection. Other
// instances wait as standbys and take over when that connection closes.
// Two active relays could each publish half of an account's events and
// reorder them on the partition.
//
// Ordering caveat: rows are published in outbox_id order, but ids are
// assigned at INSERT, not at COMMIT. If two settlement transactions touching
// the same account commit in the opposite order of their ids, the relay can
// see and publish the later id first. Balance updates are additions and the
// balance service dedupes them, so the final balances are the same either
// way.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/backoff"
	"github.com/harshithptl/clearline/internal/topics"
)

// HeaderEventID carries the outbox event_id on every published record, so
// consumers can dedupe without parsing the payload.
const HeaderEventID = "clearline-event-id"

// leaderLockKey is the advisory lock id for "the active outbox relay".
const leaderLockKey int64 = 0x636c5f6f7574626f // "cl_outbo"

// Config configures a Relay.
type Config struct {
	DB *pgxpool.Pool
	// Client produces the records. Default producer settings are acks=all
	// and idempotent; do not override them.
	Client *kgo.Client
	// OutboxTopic selects which outbox rows to publish. Default ledger-events.
	OutboxTopic string
	// KafkaTopic is where they go. Default OutboxTopic. Tests override it.
	KafkaTopic string
	// BatchSize is the most rows per transaction. Default 500.
	BatchSize int
	// PollInterval is the wait after an empty batch. Default 10ms.
	PollInterval time.Duration
	// StandbyInterval is how often a standby retries the leader lock.
	// Default 1s.
	StandbyInterval time.Duration
	Logger          *slog.Logger
}

// Stats are running counters, safe to read while the relay runs.
type Stats struct {
	Published atomic.Int64 // rows marked published
	Batches   atomic.Int64 // committed batches with at least one row
	Leader    atomic.Bool  // currently holds the leader lock
}

// Relay publishes outbox rows. Create with New.
type Relay struct {
	cfg   Config
	stats Stats

	// afterProduce, if set, runs after Kafka acked a batch and before the
	// rows are marked published. Tests use it to simulate a crash there.
	afterProduce func() error
}

// New validates cfg and applies defaults.
func New(cfg Config) (*Relay, error) {
	if cfg.DB == nil || cfg.Client == nil {
		return nil, errors.New("outbox relay: DB and Client are required")
	}
	if cfg.OutboxTopic == "" {
		cfg.OutboxTopic = topics.LedgerEvents
	}
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = cfg.OutboxTopic
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	if cfg.StandbyInterval <= 0 {
		cfg.StandbyInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Relay{cfg: cfg}, nil
}

// Stats returns the live counters.
func (r *Relay) Stats() *Stats { return &r.stats }

// Run publishes until ctx is canceled, then returns nil. Errors (Postgres or
// Kafka unavailable) are logged and retried with backoff.
func (r *Relay) Run(ctx context.Context) error {
	retry := backoff.Policy{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for attempt := 1; ; attempt++ {
		err := r.lead(ctx)
		if ctx.Err() != nil {
			return nil
		}
		r.cfg.Logger.Warn("outbox relay error, restarting", "attempt", attempt, "err", err)
		if backoff.Sleep(ctx, retry.Delay(attempt)) != nil {
			return nil
		}
	}
}

// lead waits for the leader lock, then publishes until an error or ctx ends.
func (r *Relay) lead(ctx context.Context) error {
	pc, err := r.cfg.DB.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	// The lock belongs to this connection's session. Never hand the
	// connection back to the pool, or the lock would outlive this relay
	// inside an idle pooled connection; close it, which releases the lock.
	conn := pc.Hijack()
	defer conn.Close(context.Background())
	defer r.stats.Leader.Store(false)

	for {
		var locked bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&locked); err != nil {
			return fmt.Errorf("leader lock: %w", err)
		}
		if locked {
			break
		}
		if err := backoff.Sleep(ctx, r.cfg.StandbyInterval); err != nil {
			return err
		}
	}
	r.stats.Leader.Store(true)
	r.cfg.Logger.Info("outbox relay is leader")

	for {
		n, err := r.publishBatch(ctx, conn)
		if err != nil {
			return err
		}
		if n == 0 {
			if err := backoff.Sleep(ctx, r.cfg.PollInterval); err != nil {
				return err
			}
		}
	}
}

const (
	sqlClaim = `SELECT outbox_id, event_id, key, payload FROM outbox
		WHERE published_at IS NULL AND topic = $1
		ORDER BY outbox_id LIMIT $2 FOR UPDATE`
	sqlMarkPublished = `UPDATE outbox SET published_at = now() WHERE outbox_id = ANY($1)`
)

// publishBatch publishes one batch and returns how many rows it published.
func (r *Relay) publishBatch(ctx context.Context, conn *pgx.Conn) (int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer rollback(ctx, tx)

	rows, err := tx.Query(ctx, sqlClaim, r.cfg.OutboxTopic, r.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}
	var (
		ids  []int64
		recs []*kgo.Record
	)
	for rows.Next() {
		var (
			id      int64
			eventID string
			key     string
			payload []byte
		)
		if err := rows.Scan(&id, &eventID, &key, &payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		ids = append(ids, id)
		recs = append(recs, &kgo.Record{
			Topic:   r.cfg.KafkaTopic,
			Key:     []byte(key),
			Value:   payload,
			Headers: []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte(eventID)}},
		})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}

	// The idempotent producer keeps records with one key in order.
	if err := r.cfg.Client.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce: %w", err)
	}
	if r.afterProduce != nil {
		if err := r.afterProduce(); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(ctx, sqlMarkPublished, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	r.stats.Published.Add(int64(len(ids)))
	r.stats.Batches.Add(1)
	return len(ids), nil
}
