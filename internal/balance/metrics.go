package balance

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/harshithptl/clearline/internal/metrics"
)

// Instrument registers the consumer's metrics on reg. Call before Run.
//
//	clearline_balance_freshness_seconds      SPEC 7.1: ledger commit -> balance visible in Valkey
//	clearline_balance_apply_duration_seconds one partition's batch applied to Valkey
//	clearline_balance_apply_batch_events     events per batch
//	clearline_balance_events_total{result}   applied | duplicate | malformed
//	clearline_balance_retries_total
func (c *Consumer) Instrument(reg prometheus.Registerer) {
	c.freshness = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "balance", Name: "freshness_seconds",
		Help:    "From settlement's ledger transaction to the balance being visible in Valkey.",
		Buckets: metrics.LatencyBuckets,
	})
	c.applyDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "balance", Name: "apply_duration_seconds",
		Help:    "Time to apply one partition's batch of ledger events to Valkey (optimistic transactions, conflict retries included).",
		Buckets: metrics.LatencyBuckets,
	})
	c.batchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "balance", Name: "apply_batch_events",
		Help:    "Ledger events per applied batch.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1 to 2048
	})
	reg.MustRegister(c.freshness, c.applyDuration, c.batchSize)
	s := &c.stats
	for result, v := range map[string]func() int64{
		"applied":   s.Applied.Load,
		"duplicate": s.Duplicates.Load,
		"malformed": s.Malformed.Load,
	} {
		metrics.CounterFunc(reg, "balance", "events_total", "Ledger events handled, by result.",
			prometheus.Labels{"result": result}, func() float64 { return float64(v()) })
	}
	metrics.CounterFunc(reg, "balance", "retries_total", "Valkey applies retried after an error.", nil,
		func() float64 { return float64(s.Retries.Load()) })
}

// Instrument registers clearline_balance_stream_subscribers on reg.
func (h *Hub) Instrument(reg prometheus.Registerer) {
	metrics.GaugeFunc(reg, "balance", "stream_subscribers", "Open /events streams.", nil,
		func() float64 { return float64(h.Subscribers()) })
}
