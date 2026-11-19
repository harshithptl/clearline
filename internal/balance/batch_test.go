package balance_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/messages"
)

// One batch: duplicates within the batch, duplicates of earlier batches, and
// a malformed event in the middle. Results line up with the input.
func TestApplyBatchResults(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Apply(ctx, event("T0:a", "a", "USD", "1")); err != nil {
		t.Fatal(err)
	}
	batch := []messages.LedgerEvent{
		event("T0:a", "a", "USD", "1"),   // applied in an earlier batch
		event("T1:a", "a", "USD", "2.5"), // new
		event("T1:a", "a", "USD", "2.5"), // repeated inside this batch
		event("T2:b", "b", "USD", "x"),   // malformed amount
		event("T3:b", "b", "USD", "-4", "AAPL", "1"),
		event("T4:a", "a", "USD", "0.5"),
	}
	got, err := s.ApplyBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	want := []balance.Result{balance.Duplicate, balance.Applied, balance.Duplicate, balance.Malformed, balance.Applied, balance.Applied}
	if !slices.Equal(got, want) {
		t.Fatalf("results %v, want %v", got, want)
	}
	requireBalance(t, s, "a", "USD", "4", 3) // 1 + 2.5 + 0.5
	requireBalance(t, s, "b", "USD", "-4", 1)
	requireBalance(t, s, "b", "AAPL", "1", 1)
}

// More accounts than one transaction holds: every chunk lands, balances exact.
func TestApplyBatchSpansChunks(t *testing.T) {
	s := newStore(t)
	var batch []messages.LedgerEvent
	for i := range 250 {
		acct := fmt.Sprintf("acct%03d", i)
		batch = append(batch, event("T1:"+acct, acct, "USD", "0.1"), event("T2:"+acct, acct, "USD", "0.2"))
	}
	got, err := s.ApplyBatch(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range got {
		if r != balance.Applied {
			t.Fatalf("event %d: %v", i, r)
		}
	}
	for _, i := range []int{0, 99, 100, 249} {
		requireBalance(t, s, fmt.Sprintf("acct%03d", i), "USD", "0.3", 2)
	}
}

// Property: for random event streams with redeliveries, batching in any
// split gives exactly the balances of applying each distinct event once.
func TestApplyBatchMatchesOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	for round := range 5 {
		s := newStore(t)
		ctx := context.Background()
		want := map[string]decimal.Decimal{}
		var stream []messages.LedgerEvent
		for i := range 400 {
			acct := fmt.Sprintf("u%d", rng.IntN(20))
			amt := decimal.New(rng.Int64N(20000)-10000, -2)
			ev := event(fmt.Sprintf("R%d-T%d:%s", round, i, acct), acct, "USD", amt.String())
			want[acct] = want[acct].Add(amt)
			stream = append(stream, ev)
			if rng.IntN(4) == 0 { // redelivered right away or later
				stream = append(stream, ev)
			}
		}
		for len(stream) > 0 {
			n := min(len(stream), 1+rng.IntN(120))
			if _, err := s.ApplyBatch(ctx, stream[:n]); err != nil {
				t.Fatal(err)
			}
			stream = stream[n:]
		}
		for acct, w := range want {
			snap, err := s.Get(ctx, acct)
			if err != nil {
				t.Fatal(err)
			}
			if !snap.Balances["USD"].Equal(w) {
				t.Fatalf("round %d %s: %s, want %s", round, acct, snap.Balances["USD"], w)
			}
		}
	}
}

// Consumers racing on the same accounts (a rebalance overlap): the
// optimistic transaction makes every event land exactly once. Workers start
// together behind a barrier and apply small chunks, so their read-compute-
// write windows genuinely interleave; without WATCH this loses updates.
func TestApplyBatchConcurrentOverlap(t *testing.T) {
	s := newStore(t)
	var batch []messages.LedgerEvent
	for i := range 400 {
		acct := fmt.Sprintf("c%d", i%5)
		batch = append(batch, event(fmt.Sprintf("T%d:%s", i, acct), acct, "USD", "1"))
	}
	const workers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	applied := make([]int, workers)
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < len(batch); i += 4 {
				res, err := s.ApplyBatch(context.Background(), batch[i:min(i+4, len(batch))])
				if err != nil {
					t.Error(err)
					return
				}
				for _, r := range res {
					if r == balance.Applied {
						applied[w]++
					}
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	total := 0
	for _, n := range applied {
		total += n
	}
	if total != 400 {
		t.Fatalf("%d events applied across %d racing workers, want exactly 400", total, workers)
	}
	for i := range 5 {
		requireBalance(t, s, fmt.Sprintf("c%d", i), "USD", "80", 80)
	}
}
