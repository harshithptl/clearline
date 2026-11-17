package messages_test

import (
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
)

func TestTradeRoundTrip(t *testing.T) {
	in := ledger.Trade{
		TradeID: "T42", Symbol: "AAPL", QuoteAsset: "USD", BuyerID: "alice", SellerID: "bob",
		Quantity: decimal.RequireFromString("10"), Price: decimal.RequireFromString("150.12345678"),
	}
	b, err := messages.EncodeTrade(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := messages.DecodeTrade(b)
	if err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	if out.TradeID != in.TradeID || out.Symbol != in.Symbol || out.QuoteAsset != in.QuoteAsset ||
		out.BuyerID != in.BuyerID || out.SellerID != in.SellerID ||
		!out.Quantity.Equal(in.Quantity) || !out.Price.Equal(in.Price) {
		t.Fatalf("round trip: got %+v want %+v", out, in)
	}
}

func TestDecodeTradeRejects(t *testing.T) {
	cases := map[string]string{
		"not json":       `{`,
		"empty":          ``,
		"wrong version":  `{"version":2,"trade_id":"T1","quantity":"1","price":"1"}`,
		"no version":     `{"trade_id":"T1","quantity":"1","price":"1"}`,
		"number amount":  `{"version":1,"trade_id":"T1","quantity":1,"price":"1"}`,
		"bad decimal":    `{"version":1,"trade_id":"T1","quantity":"1x","price":"1"}`,
		"missing price":  `{"version":1,"trade_id":"T1","quantity":"1"}`,
		"trailing data":  `{"version":1,"trade_id":"T1","quantity":"1","price":"1"} {}`,
		"array":          `[]`,
		"float notation": `{"version":1,"trade_id":"T1","quantity":"1","price":"NaN"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := messages.DecodeTrade([]byte(raw)); !errors.Is(err, messages.ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestDecodeTradeAllowsUnknownFields(t *testing.T) {
	raw := `{"version":1,"trade_id":"T1","symbol":"AAPL","quote_asset":"USD","buyer_id":"a","seller_id":"b","quantity":"1","price":"2","future":"x"}`
	if _, err := messages.DecodeTrade([]byte(raw)); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeLedgerEvent(t *testing.T) {
	ok := `{"version":1,"event_id":"T1:a","trade_id":"T1","account_id":"a","changes":[{"asset":"USD","amount":"-1.5"}]}`
	ev, err := messages.DecodeLedgerEvent([]byte(ok))
	if err != nil || ev.EventID != "T1:a" || len(ev.Changes) != 1 {
		t.Fatalf("decode: %+v %v", ev, err)
	}
	bad := map[string]string{
		"no changes":   `{"version":1,"event_id":"T1:a","account_id":"a","changes":[]}`,
		"no event id":  `{"version":1,"account_id":"a","changes":[{"asset":"USD","amount":"1"}]}`,
		"bad amount":   `{"version":1,"event_id":"e","account_id":"a","changes":[{"asset":"USD","amount":"x"}]}`,
		"no asset":     `{"version":1,"event_id":"e","account_id":"a","changes":[{"amount":"1"}]}`,
		"version zero": `{"event_id":"e","account_id":"a","changes":[{"asset":"USD","amount":"1"}]}`,
	}
	for name, raw := range bad {
		if _, err := messages.DecodeLedgerEvent([]byte(raw)); !errors.Is(err, messages.ErrMalformed) {
			t.Errorf("%s: err = %v, want ErrMalformed", name, err)
		}
	}
}

func TestLedgerEventID(t *testing.T) {
	if got := messages.LedgerEventID("T42", "alice"); got != "T42:alice" {
		t.Fatalf("LedgerEventID = %q", got)
	}
}

func TestOrderRoundTrip(t *testing.T) {
	in := messages.ParsedOrder{
		OrderID: "o-1", AccountID: "alice", Symbol: "AAPL", Side: messages.SideBuy,
		Quantity: decimal.RequireFromString("10"), Price: decimal.RequireFromString("150.25"),
	}
	b, err := messages.EncodeOrder(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := messages.DecodeOrder(b)
	if err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	if out.OrderID != in.OrderID || out.AccountID != in.AccountID || out.Symbol != in.Symbol ||
		out.Side != in.Side || !out.Quantity.Equal(in.Quantity) || !out.Price.Equal(in.Price) {
		t.Fatalf("round trip: %+v", out)
	}
}

func TestDecodeOrderRejects(t *testing.T) {
	cases := map[string]string{
		"bad side":      `{"version":1,"account_id":"a","symbol":"AAPL","side":"hold","quantity":"1","price":"1"}`,
		"no account":    `{"version":1,"symbol":"AAPL","side":"buy","quantity":"1","price":"1"}`,
		"no symbol":     `{"version":1,"account_id":"a","side":"buy","quantity":"1","price":"1"}`,
		"number qty":    `{"version":1,"account_id":"a","symbol":"AAPL","side":"buy","quantity":1,"price":"1"}`,
		"wrong version": `{"version":9,"account_id":"a","symbol":"AAPL","side":"buy","quantity":"1","price":"1"}`,
	}
	for name, raw := range cases {
		if _, err := messages.DecodeOrder([]byte(raw)); !errors.Is(err, messages.ErrMalformed) {
			t.Errorf("%s: err = %v, want ErrMalformed", name, err)
		}
	}
}
