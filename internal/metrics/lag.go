package metrics

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

// LagCollector reports a consumer group's lag per partition at scrape time:
// end offset minus committed offset, i.e. records the group has not yet
// finished. Asking Kafka on each scrape keeps it exact, with no background
// loop; a scrape every few seconds is a few cheap admin requests.
//
// Lag that keeps growing is the SPEC 9 alert: the consumer is falling behind
// the producer.
type LagCollector struct {
	adm     *kadm.Client
	group   string
	timeout time.Duration
	lag     *prometheus.Desc
	errs    *prometheus.Desc
}

// NewLagCollector returns a collector for group. Register it on a registry.
func NewLagCollector(adm *kadm.Client, group string) *LagCollector {
	return &LagCollector{
		adm:     adm,
		group:   group,
		timeout: 3 * time.Second,
		lag: prometheus.NewDesc(
			prometheus.BuildFQName(Namespace, "kafka", "consumer_lag_records"),
			"Records produced but not yet committed by the consumer group.",
			[]string{"group", "topic", "partition"}, nil),
		errs: prometheus.NewDesc(
			prometheus.BuildFQName(Namespace, "kafka", "consumer_lag_scrape_errors"),
			"1 if the last lag lookup failed, else 0.",
			[]string{"group"}, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *LagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lag
	ch <- c.errs
}

// Collect implements prometheus.Collector.
func (c *LagCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	lags, err := c.adm.Lag(ctx, c.group)
	if err == nil {
		err = lags.Error()
	}
	failed := 0.0
	if err != nil {
		failed = 1
	} else {
		for _, l := range lags[c.group].Lag.Sorted() {
			if l.Err != nil || l.Lag < 0 {
				failed = 1
				continue
			}
			ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, float64(l.Lag),
				c.group, l.Topic, strconv.Itoa(int(l.Partition)))
		}
	}
	ch <- prometheus.MustNewConstMetric(c.errs, prometheus.GaugeValue, failed, c.group)
}
