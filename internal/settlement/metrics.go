package settlement

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/harshithptl/clearline/internal/metrics"
)

// consumerMetrics are the histograms Instrument adds; nil means not instrumented.
type consumerMetrics struct {
	settle        prometheus.Histogram // one settle transaction (a batch or a single trade), commit included
	batchSize     prometheus.Histogram // records per batch
	orderToLedger prometheus.Histogram // SPEC 7.1: gateway produce to ledger commit
}

// Instrument registers the consumer's metrics on reg. Call before Run.
//
//	clearline_settlement_settle_duration_seconds        one DB transaction incl. commit (fsync): a batch, or one trade on the fallback path
//	clearline_settlement_batch_trades                   trades per batch
//	clearline_settlement_order_to_ledger_seconds        order produced at the gateway -> trade committed
//	clearline_settlement_trades_total{result}           posted | duplicate | dead_lettered
//	clearline_settlement_retries_total
func (c *Consumer) Instrument(reg prometheus.Registerer) {
	c.metrics = &consumerMetrics{
		settle: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace, Subsystem: "settlement", Name: "settle_duration_seconds",
			Help:    "Settle transaction time (dedupe insert, ledger rows, outbox rows, commit), per batch.",
			Buckets: metrics.LatencyBuckets,
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace, Subsystem: "settlement", Name: "batch_trades",
			Help:    "Trades per settle batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1 to 2048
		}),
		orderToLedger: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metrics.Namespace, Subsystem: "settlement", Name: "order_to_ledger_seconds",
			Help:    "From the order's Kafka timestamp (gateway produce) to its trade committing in Postgres.",
			Buckets: metrics.LatencyBuckets,
		}),
	}
	reg.MustRegister(c.metrics.settle, c.metrics.orderToLedger, c.metrics.batchSize)
	s := &c.stats
	for result, v := range map[string]func() int64{
		"posted":        s.Posted.Load,
		"duplicate":     s.Duplicates.Load,
		"dead_lettered": s.DeadLettered.Load,
	} {
		metrics.CounterFunc(reg, "settlement", "trades_total", "Trades handled, by result.",
			prometheus.Labels{"result": result}, loadFloat(v))
	}
	metrics.CounterFunc(reg, "settlement", "retries_total", "Settle attempts retried after an error.",
		nil, loadFloat(s.Retries.Load))
}

func loadFloat(f func() int64) func() float64 {
	return func() float64 { return float64(f()) }
}
