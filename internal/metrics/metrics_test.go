package metrics_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/metrics"
)

func TestHTTPLabelsByRoutePattern(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /balances/{account}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, _ *http.Request) {})
	srv := httptest.NewServer(metrics.NewHTTP(reg, "test", "GET /events").Wrap(mux))
	defer srv.Close()

	for _, path := range []string{"/balances/alice", "/balances/bob", "/events", "/nope"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	// Two accounts, one series: the label is the pattern, not the path.
	// The skipped stream is not timed; unknown paths share one label.
	got := scrape(t, reg)
	for _, line := range []string{
		`clearline_http_request_duration_seconds_count{code="418",route="GET /balances/{account}",service="test"} 2`,
		`clearline_http_request_duration_seconds_count{code="404",route="unmatched",service="test"} 1`,
	} {
		if !strings.Contains(got, line) {
			t.Fatalf("missing %s in:\n%s", line, got)
		}
	}
	if strings.Contains(got, `route="GET /events"`) {
		t.Fatal("skipped route was timed")
	}
}

// Server-Sent Events need Flush through the wrapper.
func TestHTTPKeepsFlusher(t *testing.T) {
	reg := prometheus.NewRegistry()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, _ *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "data: 1\n\n")
		f.Flush()
	})
	srv := httptest.NewServer(metrics.NewHTTP(reg, "test").Wrap(mux))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: the wrapper hid http.Flusher", resp.StatusCode)
	}
}

func TestSinceDropsSkew(t *testing.T) {
	now := time.Now()
	if d, ok := metrics.Since(now.Add(-150*time.Millisecond), now); !ok || d < 0.149 || d > 0.151 {
		t.Fatalf("Since = %v %v", d, ok)
	}
	if _, ok := metrics.Since(now.Add(time.Second), now); ok {
		t.Fatal("future start (clock skew) accepted")
	}
	if _, ok := metrics.Since(time.Time{}, now); ok {
		t.Fatal("zero time accepted")
	}
}

// Lag = end offset - committed offset, per partition, read at scrape time.
func TestLagCollector(t *testing.T) {
	ctx := context.Background()
	topic := kafkatest.NewTopic(t, "lag", 1)
	group := kafkatest.GroupID(t)
	prod := kafkatest.NewClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	for i := range 5 {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "%d", i)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adm := kadm.NewClient(kafkatest.NewClient(t))
	offs := kadm.Offsets{}
	offs.Add(kadm.Offset{Topic: topic, Partition: 0, At: 2, LeaderEpoch: -1})
	if err := adm.CommitAllOffsets(ctx, group, offs); err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewLagCollector(adm, group))
	got := scrape(t, reg)
	want := fmt.Sprintf(`clearline_kafka_consumer_lag_records{group=%q,partition="0",topic=%q} 3`, group, topic)
	if !strings.Contains(got, want) || !strings.Contains(got, `clearline_kafka_consumer_lag_scrape_errors{group=`+fmt.Sprintf("%q", group)+`} 0`) {
		t.Fatalf("want %s in:\n%s", want, got)
	}
}

// promlint enforces Prometheus naming conventions (units, _total, ...).
func TestNamesFollowConventions(t *testing.T) {
	reg := metrics.NewRegistry()
	metrics.NewHTTP(reg, "test")
	metrics.CounterFunc(reg, "test", "things_total", "Things.", nil, func() float64 { return 1 })
	metrics.GaugeFunc(reg, "test", "queue_rows", "Rows.", nil, func() float64 { return 1 })
	problems, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		if strings.HasPrefix(p.Metric, metrics.Namespace) {
			t.Errorf("%s: %s", p.Metric, p.Text)
		}
	}
}

// scrape renders reg in the text format.
func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	srv := httptest.NewServer(metrics.Handler(reg))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("Accept", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}
