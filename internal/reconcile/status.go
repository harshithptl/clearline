package reconcile

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
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
}

// NewTracker returns a tracker in the pending state.
func NewTracker() *Tracker { return &Tracker{last: Status{State: StatePending}} }

// Record stores the outcome of one Check.
func (t *Tracker) Record(rep Report, err error, at time.Time) {
	s := Status{CheckedAt: &at}
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

// StatusHandler serves GET /status (the latest Status as JSON; always 200,
// read State) and GET /healthz.
func StatusHandler(t *Tracker) http.Handler {
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
