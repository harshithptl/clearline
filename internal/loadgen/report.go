package loadgen

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// Settings describe one run. Every reported number travels with them.
type Settings struct {
	OrdersPerSec float64       `json:"orders_per_sec"`
	TradesPerSec float64       `json:"target_trades_per_sec"` // two crossing orders make one trade
	Duration     time.Duration `json:"duration_ns"`
	MaxInFlight  int           `json:"max_in_flight"`
	Accounts     int           `json:"accounts"`
	Symbol       string        `json:"symbol"`
}

// Environment is the hardware and configuration the run used.
type Environment struct {
	Hardware   string    `json:"hardware"` // operator supplied (-hardware)
	Host       string    `json:"host"`     // GOOS/GOARCH and CPU count of the load generator host
	Docker     string    `json:"docker"`   // docker info summary, if available
	Commit     string    `json:"commit"`
	Durability string    `json:"durability"`
	StartedAt  time.Time `json:"started_at"`
}

// Result is one run.
type Result struct {
	Settings   Settings  `json:"settings"`
	Ack        Latencies `json:"gateway_ack_client"` // client view: due time to 202
	SentPerSec float64   `json:"sent_per_sec"`
	// Settled counts trades committed to the ledger inside the run window,
	// exactly, from Postgres.
	Settled       int64         `json:"settled_trades"`
	SettledPerSec float64       `json:"settled_trades_per_sec"`
	Pipeline      Pipeline      `json:"pipeline"`
	DrainTime     time.Duration `json:"drain_ns"`
	Sustained     bool          `json:"sustained"`
	Explanation   string        `json:"sustained_reason"`
}

// Report is a set of runs (a sweep) in one environment.
type Report struct {
	Environment Environment `json:"environment"`
	Runs        []Result    `json:"runs"`
}

// Judge decides whether the system sustained the run's rate, by SPEC 2.2's
// definition: throughput held without consumer lag growing, for every
// consumer on the path. Concretely:
//   - no gateway errors
//   - settled at least 95% of the target trades per second
//   - settlement ended the window less than one second of trades behind
//   - the balance service ended it less than one second of ledger events
//     behind (two events per trade)
func Judge(r *Result) {
	target := r.Settings.TradesPerSec
	got := r.SettledPerSec
	switch {
	case r.Ack.Errors > 0:
		r.Sustained, r.Explanation = false, fmt.Sprintf("%d gateway errors", r.Ack.Errors)
	case got < 0.95*target:
		r.Sustained, r.Explanation = false, fmt.Sprintf("settled %.0f/s of %.0f/s target", got, target)
	case r.Pipeline.SettlementLag.End > target:
		r.Sustained, r.Explanation = false, fmt.Sprintf("settlement %.0f trades behind", r.Pipeline.SettlementLag.End)
	case r.Pipeline.BalanceLag.End > 2*target:
		r.Sustained, r.Explanation = false, fmt.Sprintf("balance %.0f events behind", r.Pipeline.BalanceLag.End)
	default:
		r.Sustained, r.Explanation = true, "throughput held, lag bounded"
	}
}

// PrintTable writes a human-readable summary of the report.
func (r Report) PrintTable(w io.Writer) {
	e := r.Environment
	fmt.Fprintf(w, "hardware:   %s\nhost:       %s\ndocker:     %s\ncommit:     %s\ndurability: %s\n\n",
		e.Hardware, e.Host, e.Docker, e.Commit, e.Durability)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "orders/s\ttrades/s\tack p50\tack p99\to->l p50\to->l p99\tfresh p50\tfresh p99\tsettle tx p50\tsettle lag\tbalance lag\terrors\tsustained\t")
	for _, x := range r.Runs {
		p := x.Pipeline
		fmt.Fprintf(tw, "%.0f\t%.0f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.0f\t%.0f\t%d\t%s\t\n",
			x.Settings.OrdersPerSec, x.SettledPerSec,
			ms(x.Ack.P50), ms(x.Ack.P99),
			ms(p.OrderToLedger.P50), ms(p.OrderToLedger.P99), ms(p.Freshness.P50), ms(p.Freshness.P99),
			ms(p.SettleTx.P50), p.SettlementLag.End, p.BalanceLag.End, x.Ack.Errors, yesNo(x))
	}
	_ = tw.Flush()
	for _, x := range r.Runs {
		if len(x.Pipeline.QueryErrors) > 0 {
			fmt.Fprintf(w, "\n%.0f orders/s: Prometheus query errors:\n  %s\n", x.Settings.OrdersPerSec,
				strings.Join(x.Pipeline.QueryErrors, "\n  "))
		}
	}
}

// ms formats a latency; values at the top histogram bucket (10 s) are shown
// as a lower bound.
func ms(d time.Duration) string {
	if d >= 10*time.Second-time.Millisecond {
		return ">=10s"
	}
	return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
}

func yesNo(r Result) string {
	if r.Sustained {
		return "yes"
	}
	return "no (" + r.Explanation + ")"
}
