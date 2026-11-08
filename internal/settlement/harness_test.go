package settlement_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/settlement"
)

// Phase 1 exit criterion: invariants hold and duplicate trades never
// double-post, under random trades, random duplicate deliveries, and
// concurrent workers.
//
// The seed is fixed so a failure reproduces exactly. Override with
// CLEARLINE_HARNESS_SEED=<n> to explore other sequences.
func TestHarnessDuplicatesNeverDoublePost(t *testing.T) {
	const (
		numAccounts = 20
		numTrades   = 500
		maxCopies   = 3 // each trade delivered 1..3 times
		workers     = 8
	)
	seed := uint64(1)
	if s := os.Getenv("CLEARLINE_HARNESS_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("CLEARLINE_HARNESS_SEED: %v", err)
		}
		seed = v
	}
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	pool := pgtest.NewPool(t)
	accounts := make([]string, numAccounts)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("acct%02d", i)
	}
	seedAccounts(t, pool, accounts...)

	// Generate unique trades and the expected balances, computed in Go,
	// independently of the database. This is the oracle.
	symbols := []string{"AAPL", "MSFT", "BTC"}
	trades := make([]ledger.Trade, numTrades)
	expected := map[string]decimal.Decimal{} // "account/asset" -> balance
	for i := range trades {
		buyer := rng.IntN(numAccounts)
		seller := (buyer + 1 + rng.IntN(numAccounts-1)) % numAccounts // never equals buyer
		trades[i] = ledger.Trade{
			TradeID:    fmt.Sprintf("T%05d", i),
			Symbol:     symbols[rng.IntN(len(symbols))],
			QuoteAsset: "USD",
			BuyerID:    accounts[buyer],
			SellerID:   accounts[seller],
			Quantity:   decimal.New(rng.Int64N(1_000_000)+1, -4),  // 0.0001 .. 100
			Price:      decimal.New(rng.Int64N(10_000_000)+1, -4), // 0.0001 .. 1000
		}
		entries, err := ledger.BuildEntries(trades[i])
		if err != nil {
			t.Fatalf("generated invalid trade %+v: %v", trades[i], err)
		}
		for _, e := range entries {
			k := e.AccountID + "/" + e.Asset
			expected[k] = expected[k].Add(e.Amount)
		}
	}

	// Build the delivery stream: every trade 1..maxCopies times, shuffled so
	// duplicates arrive at random points, sometimes concurrently.
	var deliveries []ledger.Trade
	for _, tr := range trades {
		for range 1 + rng.IntN(maxCopies) {
			deliveries = append(deliveries, tr)
		}
	}
	rng.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })
	t.Logf("%d trades, %d deliveries, %d workers", numTrades, len(deliveries), workers)

	// Fan deliveries out to concurrent workers, like consumers on a topic.
	// Workers never exit early on error: they record it and keep draining,
	// so a broken Settle fails the test instead of deadlocking the feed.
	var posted, duplicate, failed atomic.Int64
	var firstErr atomic.Value
	feed := make(chan ledger.Trade)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tr := range feed {
				out, err := settlement.Settle(context.Background(), pool, tr)
				switch {
				case err != nil:
					failed.Add(1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("%s: %v", tr.TradeID, err))
				case out == settlement.Posted:
					posted.Add(1)
				default:
					duplicate.Add(1)
				}
			}
		}()
	}
	for _, tr := range deliveries {
		feed <- tr
	}
	close(feed)
	wg.Wait()
	if n := failed.Load(); n > 0 {
		t.Fatalf("%d deliveries failed, first: %v", n, firstErr.Load())
	}

	// Every unique trade posted exactly once; every extra copy was skipped.
	if got := posted.Load(); got != numTrades {
		t.Fatalf("posted %d, want %d", got, numTrades)
	}
	if got := duplicate.Load(); got != int64(len(deliveries)-numTrades) {
		t.Fatalf("duplicates %d, want %d", got, len(deliveries)-numTrades)
	}
	requireCounts(t, pool, numTrades, 4*numTrades, 2*numTrades)
	requireInvariants(t, pool)

	// Every account's balance in Postgres matches the oracle exactly.
	rows, err := pool.Query(context.Background(),
		`SELECT account_id, asset, SUM(amount)::text FROM ledger_entries GROUP BY account_id, asset`)
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var acct, asset, sum string
		if err := rows.Scan(&acct, &asset, &sum); err != nil {
			t.Fatalf("scan: %v", err)
		}
		k := acct + "/" + asset
		want, ok := expected[k]
		if !ok {
			t.Fatalf("unexpected balance %s = %s", k, sum)
		}
		if got := decimal.RequireFromString(sum); !got.Equal(want) {
			t.Fatalf("balance %s = %s, want %s", k, got, want)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if seen != len(expected) {
		t.Fatalf("saw %d balances, want %d", seen, len(expected))
	}
}
