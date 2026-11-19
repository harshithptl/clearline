package outbox

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/harshithptl/clearline/internal/metrics"
)

// Instrument registers the relay's metrics on reg. Call before Run.
//
//	clearline_outbox_batch_duration_seconds   claim + produce (acks=all) + mark published + commit
//	clearline_outbox_published_total          rows published
//	clearline_outbox_leader                   1 while this instance holds the leader lock
//	clearline_outbox_unpublished_rows         backlog, queried at scrape time
func (r *Relay) Instrument(reg prometheus.Registerer) {
	r.batchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "outbox", Name: "batch_duration_seconds",
		Help:    "Time to publish one non-empty batch: claim, produce with acks=all, mark, commit.",
		Buckets: metrics.LatencyBuckets,
	})
	reg.MustRegister(r.batchDuration)
	metrics.CounterFunc(reg, "outbox", "published_total", "Outbox rows published to Kafka.", nil,
		func() float64 { return float64(r.stats.Published.Load()) })
	metrics.GaugeFunc(reg, "outbox", "leader", "1 while this relay holds the leader lock.", nil,
		func() float64 {
			if r.stats.Leader.Load() {
				return 1
			}
			return 0
		})
	metrics.GaugeFunc(reg, "outbox", "unpublished_rows",
		"Outbox rows committed but not yet published (-1 if the query failed).", nil, r.backlog)
}

func (r *Relay) backlog() float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var n int64
	err := r.cfg.DB.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL AND topic = $1`, r.cfg.OutboxTopic).Scan(&n)
	if err != nil {
		return -1
	}
	return float64(n)
}
