package reconcile

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/harshithptl/clearline/internal/metrics"
)

// States reported by GET /status.
const (
	StatePending  = "pending"  // no check has finished yet
	StateClean    = "clean"    // every invariant holds
	StateLagging  = "lagging"  // ledger fine, some cached balances catching up
	StateMismatch = "mismatch" // an invariant is broken
	StateError    = "error"    // the last check could not run
)

// Status is the last reconciliation result, as served by GET /status.
type Status struct {
	State     string     `json:"state"`
	CheckedAt *time.Time `json:"checked_at"`
	Accounts  int        `json:"accounts"`
	// Counts of each finding; all zero when State is clean.
	AssetImbalances  int    `json:"asset_imbalances"`
	TradeImbalances  int    `json:"trade_imbalances"`
	DuplicateEvents  int    `json:"duplicate_events"`
	OutboxMismatches int    `json:"outbox_mismatches"`
	CacheMismatches  int    `json:"cache_mismatches"`
	Lagging          int    `json:"lagging"`
	Error            string `json:"error,omitempty"`
}

// Tracker holds the latest Status. Safe for concurrent use.
type Tracker struct {
	mu   sync.RWMutex
	last Status

	checkDuration prometheus.Histogram // set by Instrument; nil records nothing
}

// NewTracker returns a tracker in the pending state.
func NewTracker() *Tracker { return &Tracker{last: Status{State: StatePending}} }

// Record stores the outcome of one Check that ran from start to end.
func (t *Tracker) Record(rep Report, err error, start, end time.Time) {
	if t.checkDuration != nil {
		t.checkDuration.Observe(end.Sub(start).Seconds())
	}
	s := Status{CheckedAt: &end}
	switch {
	case err != nil:
		s.State, s.Error = StateError, err.Error()
	default:
		l := rep.Ledger
		s.Accounts = rep.Accounts
		s.AssetImbalances, s.TradeImbalances = len(l.AssetImbalances), len(l.TradeImbalances)
		s.DuplicateEvents, s.OutboxMismatches = len(l.DuplicateEvents), len(l.OutboxMismatches)
		s.CacheMismatches, s.Lagging = len(rep.CacheMismatches), len(rep.Lagging)
		switch {
		case !rep.OK():
			s.State = StateMismatch
		case len(rep.Lagging) > 0:
			s.State = StateLagging
		default:
			s.State = StateClean
		}
	}
	t.mu.Lock()
	t.last = s
	t.mu.Unlock()
}

// Last returns the latest Status.
func (t *Tracker) Last() Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.last
}

// states lists every State, for one-hot gauges.
var states = []string{StatePending, StateClean, StateLagging, StateMismatch, StateError}

// Instrument registers the reconciler's metrics on reg.
//
//	clearline_reconciler_state{state}           1 for the current state, 0 for the others
//	clearline_reconciler_findings{kind}         count per invariant finding in the last check
//	clearline_reconciler_last_check_timestamp_seconds
//	clearline_reconciler_check_duration_seconds
//
// Alert on clearline_reconciler_state{state="mismatch"} == 1 (SPEC 9).
func (t *Tracker) Instrument(reg prometheus.Registerer) {
	t.checkDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: "reconciler", Name: "check_duration_seconds",
		Help:    "Time for one full reconciliation (ledger invariants and cache comparison).",
		Buckets: metrics.LatencyBuckets,
	})
	reg.MustRegister(t.checkDuration)
	for _, st := range states {
		metrics.GaugeFunc(reg, "reconciler", "state", "1 for the reconciler's current state.",
			prometheus.Labels{"state": st}, func() float64 {
				if t.Last().State == st {
					return 1
				}
				return 0
			})
	}
	for kind, f := range map[string]func(Status) int{
		"asset_imbalance": func(s Status) int { return s.AssetImbalances },
		"trade_imbalance": func(s Status) int { return s.TradeImbalances },
		"duplicate_event": func(s Status) int { return s.DuplicateEvents },
		"outbox_mismatch": func(s Status) int { return s.OutboxMismatches },
		"cache_mismatch":  func(s Status) int { return s.CacheMismatches },
		"lagging_account": func(s Status) int { return s.Lagging },
	} {
		metrics.GaugeFunc(reg, "reconciler", "findings", "Findings in the last check, by kind (all 0 when clean).",
			prometheus.Labels{"kind": kind}, func() float64 { return float64(f(t.Last())) })
	}
	metrics.GaugeFunc(reg, "reconciler", "last_check_timestamp_seconds", "Unix time of the last finished check (0 before the first).", nil,
		func() float64 {
			if at := t.Last().CheckedAt; at != nil {
				return float64(at.UnixMilli()) / 1000
			}
			return 0
		})
}

// StatusHandler serves GET /status (the latest Status as JSON; always 200,
// read State) and GET /healthz.
func StatusHandler(t *Tracker) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_ = json.NewEncoder(w).Encode(t.Last())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
