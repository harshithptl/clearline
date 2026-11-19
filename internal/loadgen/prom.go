package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Prom queries a Prometheus server's HTTP API.
type Prom struct {
	URL    string
	Client *http.Client
}

// ErrNoData means the query returned no series (for example, no samples in
// the window).
var ErrNoData = errors.New("no data")

// Query evaluates q at time at and returns its single value. NaN results
// (histogram_quantile over an empty window) return ErrNoData.
func (p Prom) Query(ctx context.Context, q string, at time.Time) (float64, error) {
	u := p.URL + "/api/v1/query?" + url.Values{
		"query": {q},
		"time":  {strconv.FormatFloat(float64(at.UnixMilli())/1000, 'f', 3, 64)},
	}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	cl := p.Client
	if cl == nil {
		cl = http.DefaultClient
	}
	resp, err := cl.Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus: %w", err)
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("prometheus: decode: %w", err)
	}
	if body.Status != "success" {
		return 0, fmt.Errorf("prometheus: %s: %s", q, body.Error)
	}
	if len(body.Data.Result) == 0 {
		return 0, ErrNoData
	}
	s, _ := body.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("prometheus: value %q: %w", s, err)
	}
	if math.IsNaN(v) {
		return 0, ErrNoData
	}
	return v, nil
}

// Pipeline is what the services measured during a run window. Throughput is
// not here: it is counted exactly from the ledger (Result.Settled), since a
// Prometheus increase() over a short window can miss up to one scrape.
type Pipeline struct {
	OrderToLedger  Quantiles     `json:"order_to_ledger"`
	Freshness      Quantiles     `json:"balance_freshness"`
	GatewayServer  Quantiles     `json:"gateway_post_orders"`
	SettleTx       Quantiles     `json:"settle_transaction"`
	SettlementLag  LagSummary    `json:"settlement_lag"`
	BalanceLag     LagSummary    `json:"balance_lag"`
	Window         time.Duration `json:"window_ns"`
	QueryErrors    []string      `json:"query_errors,omitempty"`
	BucketCaveat   string        `json:"bucket_caveat"`
	PrometheusTime time.Time     `json:"prometheus_eval_time"`
}

// Quantiles from a Prometheus histogram; each is accurate to one bucket.
type Quantiles struct {
	P50 time.Duration `json:"p50_ns"`
	P99 time.Duration `json:"p99_ns"`
}

// LagSummary is a consumer group's lag during and at the end of the window.
type LagSummary struct {
	Max float64 `json:"max_records"`
	End float64 `json:"end_records"`
}

// Measure reads the pipeline's metrics over [start, end]. Call it at least
// one scrape interval after end so the final samples are in.
func (p Prom) Measure(ctx context.Context, start, end time.Time) Pipeline {
	w := end.Sub(start).Round(time.Second)
	rng := fmt.Sprintf("%ds", int(w.Seconds()))
	out := Pipeline{
		Window:         w,
		PrometheusTime: end,
		BucketCaveat:   "quantiles from histogram buckets (0.5ms to 10s, 20 exponential steps): accurate to within one bucket",
	}
	get := func(q string) float64 {
		v, err := p.Query(ctx, q, end)
		if err != nil {
			out.QueryErrors = append(out.QueryErrors, q+": "+err.Error())
			return 0
		}
		return v
	}
	quant := func(metric, selector string) Quantiles {
		f := func(q float64) time.Duration {
			s := get(fmt.Sprintf(`histogram_quantile(%g, sum by (le) (increase(%s_bucket%s[%s])))`, q, metric, selector, rng))
			return time.Duration(s * float64(time.Second))
		}
		return Quantiles{P50: f(0.5), P99: f(0.99)}
	}
	lag := func(group string) LagSummary {
		sel := fmt.Sprintf(`sum(clearline_kafka_consumer_lag_records{group=%q})`, group)
		return LagSummary{
			Max: get(fmt.Sprintf(`max_over_time(%s[%s:5s])`, sel, rng)),
			End: get(sel),
		}
	}

	out.OrderToLedger = quant("clearline_settlement_order_to_ledger_seconds", "")
	out.Freshness = quant("clearline_balance_freshness_seconds", "")
	out.GatewayServer = quant("clearline_http_request_duration_seconds", `{service="gateway",route="POST /orders",code="202"}`)
	out.SettleTx = quant("clearline_settlement_settle_duration_seconds", "")
	out.SettlementLag = lag("settlement")
	out.BalanceLag = lag("balance")
	return out
}

// WaitDrained polls until the settlement and balance groups have no lag, or
// timeout. It returns how long draining took.
func (p Prom) WaitDrained(ctx context.Context, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		v, err := p.Query(ctx, `sum(clearline_kafka_consumer_lag_records{group=~"settlement|balance|matcher-adapter"})`, time.Now())
		if err == nil && v == 0 {
			return time.Since(start), nil
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return time.Since(start), fmt.Errorf("lag not drained after %s", timeout)
}
