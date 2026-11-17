package balance_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/redistest"
)

func newStore(t *testing.T) *balance.Store {
	t.Helper()
	rdb, prefix := redistest.New(t)
	return balance.NewStore(rdb, prefix)
}

func event(id, account string, changes ...string) messages.LedgerEvent {
	ev := messages.LedgerEvent{Version: messages.LedgerEventVersion, EventID: id, TradeID: id, AccountID: account}
	for i := 0; i < len(changes); i += 2 {
		ev.Changes = append(ev.Changes, messages.Change{Asset: changes[i], Amount: changes[i+1]})
	}
	return ev
}

func requireBalance(t *testing.T, s *balance.Store, account, asset, want string, events int64) {
	t.Helper()
	snap, err := s.Get(context.Background(), account)
	if err != nil {
		t.Fatal(err)
	}
	if got := snap.Balances[asset]; !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("%s %s = %s, want %s", account, asset, got, want)
	}
	if snap.Events != events {
		t.Fatalf("%s events = %d, want %d", account, snap.Events, events)
	}
}

func TestApplyOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ev := event("T42:alice", "alice", "USD", "-1500", "AAPL", "10")
	for i, want := range []bool{true, false, false} {
		got, err := s.Apply(ctx, ev)
		if err != nil || got != want {
			t.Fatalf("apply #%d = %v, %v; want %v", i+1, got, err, want)
		}
	}
	requireBalance(t, s, "alice", "USD", "-1500", 1)
	requireBalance(t, s, "alice", "AAPL", "10", 1)
}

// 0.1 added ten times is exactly 1, which a float would not give.
func TestApplyIsExact(t *testing.T) {
	s := newStore(t)
	for i := range 10 {
		if _, err := s.Apply(context.Background(), event("T"+strconv.Itoa(i)+":a", "a", "USD", "0.1")); err != nil {
			t.Fatal(err)
		}
	}
	requireBalance(t, s, "a", "USD", "1", 10)

	// Bigger than int64 at 1e-8 units (9.2e10): fine as decimal strings.
	if _, err := s.Apply(context.Background(), event("big:a", "a", "USD", "999999999999.99999999")); err != nil {
		t.Fatal(err)
	}
	requireBalance(t, s, "a", "USD", "1000000000000.99999999", 11)
}

// The same event applied by 20 goroutines at once (two consumers overlapping
// during a rebalance, at worst): exactly one applies it.
func TestApplyConcurrentSameEvent(t *testing.T) {
	s := newStore(t)
	var applied atomic.Int64
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.Apply(context.Background(), event("T1:a", "a", "USD", "5"))
			if err != nil {
				t.Error(err)
			}
			if ok {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	if applied.Load() != 1 {
		t.Fatalf("applied %d times, want 1", applied.Load())
	}
	requireBalance(t, s, "a", "USD", "5", 1)
}

// Different events racing on one account: WATCH retries, none are lost.
func TestApplyConcurrentDifferentEvents(t *testing.T) {
	s := newStore(t)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Apply(context.Background(), event("T"+strconv.Itoa(i)+":a", "a", "USD", "1.5", "AAPL", "-1")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	requireBalance(t, s, "a", "USD", "75", 50)
	requireBalance(t, s, "a", "AAPL", "-50", 50)
}

func TestGetUnknownAccountIsEmpty(t *testing.T) {
	s := newStore(t)
	snap, err := s.Get(context.Background(), "nobody")
	if err != nil || len(snap.Balances) != 0 || snap.Events != 0 {
		t.Fatalf("snapshot = %+v, %v", snap, err)
	}
}
