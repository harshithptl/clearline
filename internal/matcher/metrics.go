package matcher

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/harshithptl/clearline/internal/metrics"
)

// Instrument registers the adapter's metrics on reg. Call before Run.
//
//	clearline_matcher_submit_duration_seconds   one gRPC Submit round trip (batch of orders)
//	clearline_matcher_submit_batch_orders       orders per Submit
//	clearline_matcher_orders_total{result}      live | replayed | rejected | duplicate
//	clearline_matcher_trades_total              trades published
//	clearline_matcher_self_trades_total         matches within one account, not published
//	clearline_matcher_caught_up                 1 once replay has finished
func (a *Adapter) Instrument(reg prometheus.Registerer) {
	a.submitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "matcher", Name: "submit_duration_seconds",
		Help:    "gRPC Submit round trip to the C++ engine, per batch.",
		Buckets: metrics.LatencyBuckets,
	})
	a.batchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "matcher", Name: "submit_batch_orders",
		Help:    "Orders per Submit call.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 13), // 1 to 4096
	})
	reg.MustRegister(a.submitDuration, a.batchSize)
	s := &a.stats
	for result, v := range map[string]func() int64{
		"live":      s.Orders.Load,
		"replayed":  s.Replayed.Load,
		"rejected":  s.Rejected.Load,
		"duplicate": s.Duplicates.Load,
	} {
		metrics.CounterFunc(reg, "matcher", "orders_total", "Orders handled, by result.",
			prometheus.Labels{"result": result}, func() float64 { return float64(v()) })
	}
	metrics.CounterFunc(reg, "matcher", "trades_total", "Trades published.", nil,
		func() float64 { return float64(s.Trades.Load()) })
	metrics.CounterFunc(reg, "matcher", "self_trades_total", "Matches between one account's own orders (not published).", nil,
		func() float64 { return float64(s.SelfTrades.Load()) })
	metrics.GaugeFunc(reg, "matcher", "caught_up", "1 once replay of the orders log has finished.", nil,
		func() float64 {
			if s.CaughtUp.Load() {
				return 1
			}
			return 0
		})
}
