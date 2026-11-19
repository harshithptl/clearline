// Package metrics holds what every Clearline service shares for Prometheus
// (SPEC 9): a registry with runtime metrics, the /metrics handler, HTTP
// request instrumentation, latency buckets, and a Kafka consumer lag
// collector.
//
// Naming: clearline_<component>_<what>_<unit>, counters end in _total,
// durations are seconds (Prometheus conventions, checked by promlint in tests).
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every Clearline metric.
const Namespace = "clearline"

// LatencyBuckets cover 0.5 ms to about 10 s in 20 exponential steps (each
// about 1.68x the previous). Settlement is durability bound, so most
// observations land in the 1 to 50 ms range, where buckets are densest in
// absolute terms. Quantiles computed from buckets are estimates within one
// bucket's width; the benchmark report states that.
var LatencyBuckets = prometheus.ExponentialBucketsRange(0.0005, 10, 20)

// NewRegistry returns a registry with Go runtime and process metrics.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Handler serves reg in the Prometheus exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, EnableOpenMetrics: true})
}

// Serve runs a /metrics server on addr until ctx is done. For services with
// no HTTP API of their own (settlement, outbox relay, matcher adapter).
func Serve(ctx context.Context, addr string, reg *prometheus.Registry) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler(reg))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		// ctx is already done here; keep its values but give shutdown its own deadline.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	slog.Info("metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server", "err", err)
	}
}

// HTTP instruments every request mux serves: a duration histogram and a
// counter by route pattern and status code. Routes listed in skip (long-lived
// streams such as "GET /events", or /metrics itself) are served but not
// timed; a one-hour stream would swamp the histogram.
type HTTP struct {
	duration *prometheus.HistogramVec
	skip     map[string]bool
}

// NewHTTP registers clearline_http_request_duration_seconds for service.
func NewHTTP(reg prometheus.Registerer, service string, skip ...string) *HTTP {
	h := &HTTP{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   Namespace,
			Subsystem:   "http",
			Name:        "request_duration_seconds",
			Help:        "HTTP request latency by route and status code.",
			Buckets:     LatencyBuckets,
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"route", "code"}),
		skip: map[string]bool{},
	}
	for _, s := range skip {
		h.skip[s] = true
	}
	reg.MustRegister(h.duration)
	return h
}

// Wrap returns mux with instrumentation. The route label is the mux pattern
// ("POST /orders"), never the raw path, so label cardinality stays bounded.
func (h *HTTP) Wrap(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, route := mux.Handler(r)
		if route == "" {
			route = "unmatched"
		}
		if h.skip[route] {
			mux.ServeHTTP(w, r)
			return
		}
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		mux.ServeHTTP(sw, r)
		h.duration.WithLabelValues(route, strconv.Itoa(sw.code)).Observe(time.Since(start).Seconds())
	})
}

// statusWriter records the status code. It keeps Flush working (Server-Sent
// Events need it) and exposes the original writer to http.ResponseController.
type statusWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// CounterFunc registers a counter read from f at scrape time, for counts the
// services already keep in atomic Stats fields.
func CounterFunc(reg prometheus.Registerer, subsystem, name, help string, labels prometheus.Labels, f func() float64) {
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help, ConstLabels: labels,
	}, f))
}

// GaugeFunc registers a gauge read from f at scrape time.
func GaugeFunc(reg prometheus.Registerer, subsystem, name, help string, labels prometheus.Labels, f func() float64) {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: subsystem, Name: name, Help: help, ConstLabels: labels,
	}, f))
}

// Since returns the seconds elapsed since t, or false if t is zero or in
// the future (clock skew between hosts): a negative latency is not recorded.
func Since(t time.Time, now time.Time) (float64, bool) {
	if t.IsZero() || now.Before(t) {
		return 0, false
	}
	return now.Sub(t).Seconds(), true
}
