package reconcile_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/harshithptl/clearline/internal/ledger/invariants"
	"github.com/harshithptl/clearline/internal/reconcile"
)

func TestTrackerStates(t *testing.T) {
	tr := reconcile.NewTracker()
	if s := tr.Last(); s.State != reconcile.StatePending || s.CheckedAt != nil {
		t.Fatalf("initial %+v", s)
	}
	now := time.Now()
	cases := []struct {
		name string
		rep  reconcile.Report
		err  error
		want string
	}{
		{"clean", reconcile.Report{Accounts: 3}, nil, reconcile.StateClean},
		{"lagging", reconcile.Report{Lagging: []reconcile.Lag{{Account: "a"}}}, nil, reconcile.StateLagging},
		{"cache mismatch", reconcile.Report{CacheMismatches: []reconcile.CacheMismatch{{Account: "a"}}}, nil, reconcile.StateMismatch},
		{"ledger broken", reconcile.Report{Ledger: invariants.Report{DuplicateEvents: []invariants.DuplicateEvent{{}}}}, nil, reconcile.StateMismatch},
		{"error", reconcile.Report{}, errors.New("db down"), reconcile.StateError},
	}
	for _, c := range cases {
		tr.Record(c.rep, c.err, now)
		if s := tr.Last(); s.State != c.want || s.CheckedAt == nil {
			t.Errorf("%s: state %q, want %q", c.name, s.State, c.want)
		}
	}
}

func TestStatusHandler(t *testing.T) {
	tr := reconcile.NewTracker()
	tr.Record(reconcile.Report{Accounts: 7}, nil, time.Now())
	srv := httptest.NewServer(reconcile.StatusHandler(tr))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s reconcile.Status
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	if s.State != reconcile.StateClean || s.Accounts != 7 {
		t.Fatalf("status %+v", s)
	}
}
