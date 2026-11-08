package ledger_test

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/ledger"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// t42 is the SPEC 4.1 dry run: Alice buys 10 AAPL at $150 from Bob.
func t42() ledger.Trade {
	return ledger.Trade{
		TradeID:    "T42",
		Symbol:     "AAPL",
		QuoteAsset: "USD",
		BuyerID:    "alice",
		SellerID:   "bob",
		Quantity:   d("10"),
		Price:      d("150"),
	}
}

func TestBuildEntriesDryRun(t *testing.T) {
	got, err := ledger.BuildEntries(t42())
	if err != nil {
		t.Fatalf("BuildEntries: %v", err)
	}
	want := []struct{ account, asset, amount string }{
		{"alice", "USD", "-1500"},
		{"bob", "USD", "1500"},
		{"bob", "AAPL", "-10"},
		{"alice", "AAPL", "10"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i, w := range want {
		e := got[i]
		if e.TradeID != "T42" || e.AccountID != w.account || e.Asset != w.asset || !e.Amount.Equal(d(w.amount)) {
			t.Errorf("entry %d = %+v, want %s %s %s", i, e, w.account, w.asset, w.amount)
		}
	}
}

// Fractional prices and quantities stay exact: 0.1 * 3 is exactly 0.3.
func TestBuildEntriesFractional(t *testing.T) {
	tr := t42()
	tr.Quantity, tr.Price = d("3"), d("0.1")
	got, err := ledger.BuildEntries(tr)
	if err != nil {
		t.Fatalf("BuildEntries: %v", err)
	}
	if !got[0].Amount.Equal(d("-0.3")) {
		t.Fatalf("notional = %s, want -0.3", got[0].Amount)
	}
}

func TestBuildEntriesRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ledger.Trade)
	}{
		{"empty trade id", func(tr *ledger.Trade) { tr.TradeID = "" }},
		{"empty symbol", func(tr *ledger.Trade) { tr.Symbol = "" }},
		{"empty quote asset", func(tr *ledger.Trade) { tr.QuoteAsset = "" }},
		{"symbol equals quote", func(tr *ledger.Trade) { tr.Symbol = "USD" }},
		{"empty buyer", func(tr *ledger.Trade) { tr.BuyerID = "" }},
		{"empty seller", func(tr *ledger.Trade) { tr.SellerID = "" }},
		{"self trade", func(tr *ledger.Trade) { tr.SellerID = tr.BuyerID }},
		{"zero quantity", func(tr *ledger.Trade) { tr.Quantity = d("0") }},
		{"negative quantity", func(tr *ledger.Trade) { tr.Quantity = d("-1") }},
		{"zero price", func(tr *ledger.Trade) { tr.Price = d("0") }},
		{"negative price", func(tr *ledger.Trade) { tr.Price = d("-150") }},
		{"quantity beyond 8 dp", func(tr *ledger.Trade) { tr.Quantity = d("0.000000001") }},
		{"price beyond 8 dp", func(tr *ledger.Trade) { tr.Price = d("1.000000001") }},
		// Each input fits 8 dp, but the product needs 16: refuse rather than round.
		{"notional beyond 8 dp", func(tr *ledger.Trade) { tr.Quantity, tr.Price = d("0.12345678"), d("1.23456789") }},
		// NUMERIC(20,8) allows 12 integer digits.
		{"notional too large", func(tr *ledger.Trade) { tr.Quantity, tr.Price = d("1000000"), d("1000000") }},
		{"quantity too large", func(tr *ledger.Trade) { tr.Quantity = d("1000000000000") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr := t42()
			c.mutate(&tr)
			_, err := ledger.BuildEntries(tr)
			if !errors.Is(err, ledger.ErrInvalidTrade) {
				t.Fatalf("err = %v, want ErrInvalidTrade", err)
			}
		})
	}
}

// Largest values that still fit NUMERIC(20,8) must be accepted.
func TestBuildEntriesBoundary(t *testing.T) {
	tr := t42()
	tr.Quantity, tr.Price = d("1"), d("999999999999.99999999")
	if _, err := ledger.BuildEntries(tr); err != nil {
		t.Fatalf("max notional rejected: %v", err)
	}
	tr.Quantity, tr.Price = d("0.00000001"), d("1")
	if _, err := ledger.BuildEntries(tr); err != nil {
		t.Fatalf("smallest unit rejected: %v", err)
	}
}

// Property test: for many random valid trades, entries always net to zero per
// asset, there are exactly 4, none is zero, and every amount fits the column.
func TestBuildEntriesAlwaysBalanced(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 7)) // fixed seed: failures are reproducible
	for i := range 10000 {
		tr := t42()
		// qty up to 1e6 with 4 dp, price up to 1e5 with 4 dp: product has <= 8 dp.
		tr.Quantity = decimal.New(rng.Int64N(10_000_000_000)+1, -4)
		tr.Price = decimal.New(rng.Int64N(1_000_000_000)+1, -4)

		entries, err := ledger.BuildEntries(tr)
		if err != nil {
			t.Fatalf("trade %d (%s @ %s): %v", i, tr.Quantity, tr.Price, err)
		}
		if len(entries) != 4 {
			t.Fatalf("trade %d: %d entries", i, len(entries))
		}
		sums := map[string]decimal.Decimal{}
		for _, e := range entries {
			if e.Amount.IsZero() {
				t.Fatalf("trade %d: zero amount %+v", i, e)
			}
			sums[e.Asset] = sums[e.Asset].Add(e.Amount)
		}
		for asset, s := range sums {
			if !s.IsZero() {
				t.Fatalf("trade %d: %s nets to %s", i, asset, s)
			}
		}
	}
}
