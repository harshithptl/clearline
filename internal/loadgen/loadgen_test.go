package loadgen_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/harshithptl/clearline/internal/loadgen"
)

func TestRunHoldsTheRate(t *testing.T) {
	var calls atomic.Int64
	start := time.Now()
	samples := loadgen.Run(context.Background(),
		loadgen.Schedule{Rate: 200, Duration: time.Second, MaxInFlight: 50},
		func(context.Context, int) bool { calls.Add(1); return true })
	took := time.Since(start)
	if len(samples) != 200 || calls.Load() != 200 {
		t.Fatalf("%d samples, %d calls; want 200", len(samples), calls.Load())
	}
	if took < 990*time.Millisecond || took > 1300*time.Millisecond {
		t.Fatalf("200 requests at 200/s took %s, want about 1s", took)
	}
	for i, s := range samples {
		if want := time.Duration(i) * 5 * time.Millisecond; s.Due != want {
			t.Fatalf("sample %d due %s, want %s", i, s.Due, want)
		}
	}
}

// The heart of open-loop testing: if the system stalls, requests that were
// due during the stall are charged the wait. A closed-loop tester would
// report every request as fast and simply send fewer of them.
//
//	rate 100/s, 1 slot, first request takes 200 ms
//	requests due at 10, 20, ... ms wait for the slot, so their latency
//	includes the queue; the run's p99 reflects the stall.
func TestRunChargesQueueingToLatency(t *testing.T) {
	samples := loadgen.Run(context.Background(),
		loadgen.Schedule{Rate: 100, Duration: 500 * time.Millisecond, MaxInFlight: 1},
		func(_ context.Context, i int) bool {
			if i == 0 {
				time.Sleep(200 * time.Millisecond)
			}
			return true
		})
	if len(samples) != 50 {
		t.Fatalf("%d samples", len(samples))
	}
	// Request 1 was due at 10 ms but could only start at about 200 ms.
	if samples[1].Latency < 180*time.Millisecond {
		t.Fatalf("request queued behind the stall reported %s; coordinated omission", samples[1].Latency)
	}
	if p99 := loadgen.Summarize(samples).P99; p99 < 180*time.Millisecond {
		t.Fatalf("p99 %s hides the stall", p99)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	samples := loadgen.Run(ctx, loadgen.Schedule{Rate: 100, Duration: 10 * time.Second, MaxInFlight: 4},
		func(context.Context, int) bool { return true })
	if len(samples) > 20 {
		t.Fatalf("%d samples after cancel at 100ms", len(samples))
	}
}

func TestSummarize(t *testing.T) {
	var samples []loadgen.Sample
	for i := 1; i <= 1000; i++ {
		samples = append(samples, loadgen.Sample{Latency: time.Duration(i) * time.Millisecond, OK: true})
	}
	samples = append(samples, loadgen.Sample{Latency: time.Hour, OK: false})
	l := loadgen.Summarize(samples)
	if l.Count != 1000 || l.Errors != 1 {
		t.Fatalf("count %d errors %d", l.Count, l.Errors)
	}
	if l.P50 != 500*time.Millisecond || l.P99 != 990*time.Millisecond || l.P999 != 999*time.Millisecond || l.Max != time.Second {
		t.Fatalf("percentiles %+v", l)
	}
}

func TestJudge(t *testing.T) {
	base := func() loadgen.Result {
		return loadgen.Result{
			Settings:      loadgen.Settings{OrdersPerSec: 1000, TradesPerSec: 500},
			Ack:           loadgen.Latencies{Count: 1000},
			SettledPerSec: 498,
		}
	}
	ok := base()
	loadgen.Judge(&ok)
	if !ok.Sustained {
		t.Fatalf("held rate judged not sustained: %s", ok.Explanation)
	}
	slow := base()
	slow.SettledPerSec = 400
	loadgen.Judge(&slow)
	behind := base()
	behind.Pipeline.SettlementLag.End = 5000
	loadgen.Judge(&behind)
	errs := base()
	errs.Ack.Errors = 3
	loadgen.Judge(&errs)
	balance := base()
	balance.Pipeline.BalanceLag.End = 20000 // settlement keeps up, the cache does not
	loadgen.Judge(&balance)
	for name, r := range map[string]loadgen.Result{"slow": slow, "behind": behind, "errors": errs, "balance": balance} {
		if r.Sustained {
			t.Errorf("%s judged sustained", name)
		}
	}
}

func TestPlotSVG(t *testing.T) {
	r := loadgen.Report{Runs: []loadgen.Result{
		{SettledPerSec: 100, Sustained: true, Pipeline: loadgen.Pipeline{OrderToLedger: loadgen.Quantiles{P50: 2 * time.Millisecond, P99: 9 * time.Millisecond}}, Ack: loadgen.Latencies{P99: 12 * time.Millisecond}},
		{SettledPerSec: 400, Sustained: false, Pipeline: loadgen.Pipeline{OrderToLedger: loadgen.Quantiles{P50: 30 * time.Millisecond, P99: 900 * time.Millisecond}}, Ack: loadgen.Latencies{P99: 80 * time.Millisecond}},
	}}
	var b strings.Builder
	if err := r.PlotSVG(&b, "test <chart>", "sub"); err != nil {
		t.Fatal(err)
	}
	svg := b.String()
	for _, want := range []string{"<svg", "test &lt;chart&gt;", "order to ledger p99", "not sustained", "<polyline"} {
		if !strings.Contains(svg, want) {
			t.Fatalf("svg lacks %q", want)
		}
	}
	if strings.Count(svg, "<circle") != 3*2+1 {
		t.Fatalf("want 6 points + 1 legend marker, got %d circles", strings.Count(svg, "<circle"))
	}
	if err := (loadgen.Report{}).PlotSVG(&b, "empty", ""); err == nil {
		t.Fatal("empty report plotted")
	}
}

func TestPlotCompare(t *testing.T) {
	run := func(tps float64, p99 time.Duration, ok bool) loadgen.Result {
		return loadgen.Result{SettledPerSec: tps, Sustained: ok,
			Pipeline: loadgen.Pipeline{OrderToLedger: loadgen.Quantiles{P50: p99 / 10, P99: p99}}}
	}
	configs := []loadgen.Configuration{
		{Name: "before", Report: loadgen.Report{Runs: []loadgen.Result{run(100, 80*time.Millisecond, true), run(250, 4*time.Second, false)}}},
		{Name: "after", Report: loadgen.Report{Runs: []loadgen.Result{run(100, 20*time.Millisecond, true), run(1000, 150*time.Millisecond, true)}}},
	}
	var b strings.Builder
	if err := loadgen.PlotCompare(&b, "cmp", "hw", "p99", configs); err != nil {
		t.Fatal(err)
	}
	svg := b.String()
	if strings.Count(svg, "<polyline") != 2 || !strings.Contains(svg, ">before<") || !strings.Contains(svg, ">after<") {
		t.Fatalf("want two labeled lines:\n%s", svg)
	}
	if err := loadgen.PlotCompare(&b, "x", "", "p99", nil); err == nil {
		t.Fatal("empty comparison plotted")
	}
}
