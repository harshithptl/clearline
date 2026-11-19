package loadgen

import (
	"math"
	"slices"
	"time"
)

// Latencies summarizes successful samples. Percentiles are exact (computed
// from every sample, nearest-rank), not estimated from buckets.
type Latencies struct {
	Count  int           `json:"count"`
	Errors int           `json:"errors"`
	P50    time.Duration `json:"p50_ns"`
	P90    time.Duration `json:"p90_ns"`
	P99    time.Duration `json:"p99_ns"`
	P999   time.Duration `json:"p999_ns"`
	Max    time.Duration `json:"max_ns"`
}

// Summarize computes Latencies over samples.
func Summarize(samples []Sample) Latencies {
	var l Latencies
	ok := make([]time.Duration, 0, len(samples))
	for _, s := range samples {
		if s.OK {
			ok = append(ok, s.Latency)
		} else {
			l.Errors++
		}
	}
	l.Count = len(ok)
	if l.Count == 0 {
		return l
	}
	slices.Sort(ok)
	l.P50 = Percentile(ok, 50)
	l.P90 = Percentile(ok, 90)
	l.P99 = Percentile(ok, 99)
	l.P999 = Percentile(ok, 99.9)
	l.Max = ok[len(ok)-1]
	return l
}

// Percentile returns the nearest-rank p-th percentile of sorted, which must
// be non-empty and ascending.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	// The epsilon absorbs float error (99.9/100*1000 is 999.0000000000001).
	rank := int(math.Ceil(p*float64(len(sorted))/100 - 1e-9))
	return sorted[min(max(rank-1, 0), len(sorted)-1)]
}
