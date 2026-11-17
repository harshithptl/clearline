// Package messages defines the JSON wire formats on Clearline's Kafka topics.
//
// Every message carries a version field. Money and quantities are decimal
// strings, never JSON numbers, so no consumer can parse them as floats.
package messages

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/ledger"
)

// ErrMalformed marks a message that can never be decoded. Retrying it is
// pointless, so consumers dead-letter it immediately.
var ErrMalformed = errors.New("malformed message")

// Current versions. A decoder rejects versions it does not know.
const (
	TradeVersion       = 1
	LedgerEventVersion = 1
)

// Trade is a message on the trades topic (key = symbol).
type Trade struct {
	Version    int    `json:"version"`
	TradeID    string `json:"trade_id"`
	Symbol     string `json:"symbol"`
	QuoteAsset string `json:"quote_asset"`
	BuyerID    string `json:"buyer_id"`
	SellerID   string `json:"seller_id"`
	Quantity   string `json:"quantity"`
	Price      string `json:"price"`
}

// EncodeTrade returns the trades-topic payload for t.
func EncodeTrade(t ledger.Trade) ([]byte, error) {
	return json.Marshal(Trade{
		Version:    TradeVersion,
		TradeID:    t.TradeID,
		Symbol:     t.Symbol,
		QuoteAsset: t.QuoteAsset,
		BuyerID:    t.BuyerID,
		SellerID:   t.SellerID,
		Quantity:   t.Quantity.String(),
		Price:      t.Price.String(),
	})
}

// DecodeTrade parses a trades-topic payload. Errors wrap ErrMalformed.
// Business validation (positive quantity, distinct accounts, ...) is left to
// ledger.BuildEntries.
func DecodeTrade(b []byte) (ledger.Trade, error) {
	var m Trade
	if err := strictUnmarshal(b, &m); err != nil {
		return ledger.Trade{}, err
	}
	if m.Version != TradeVersion {
		return ledger.Trade{}, fmt.Errorf("%w: trade version %d", ErrMalformed, m.Version)
	}
	qty, err := parseDecimal("quantity", m.Quantity)
	if err != nil {
		return ledger.Trade{}, err
	}
	price, err := parseDecimal("price", m.Price)
	if err != nil {
		return ledger.Trade{}, err
	}
	return ledger.Trade{
		TradeID:    m.TradeID,
		Symbol:     m.Symbol,
		QuoteAsset: m.QuoteAsset,
		BuyerID:    m.BuyerID,
		SellerID:   m.SellerID,
		Quantity:   qty,
		Price:      price,
	}, nil
}

// LedgerEvent is a message on the ledger-events topic (key = account): one
// account's balance changes from one trade.
type LedgerEvent struct {
	Version   int      `json:"version"`
	EventID   string   `json:"event_id"`
	TradeID   string   `json:"trade_id"`
	AccountID string   `json:"account_id"`
	Changes   []Change `json:"changes"`
}

// Change is a signed balance change for one asset, as a decimal string.
type Change struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

// LedgerEventID is the event ID for one account's ledger event from a trade.
// Unambiguous because account IDs cannot contain ':' (migration 0003).
func LedgerEventID(tradeID, accountID string) string {
	return tradeID + ":" + accountID
}

// DecodeLedgerEvent parses and checks a ledger-events payload. Errors wrap
// ErrMalformed.
func DecodeLedgerEvent(b []byte) (LedgerEvent, error) {
	var ev LedgerEvent
	if err := strictUnmarshal(b, &ev); err != nil {
		return LedgerEvent{}, err
	}
	if ev.Version != LedgerEventVersion {
		return LedgerEvent{}, fmt.Errorf("%w: ledger event version %d", ErrMalformed, ev.Version)
	}
	if ev.EventID == "" || ev.AccountID == "" || len(ev.Changes) == 0 {
		return LedgerEvent{}, fmt.Errorf("%w: ledger event missing fields", ErrMalformed)
	}
	for _, c := range ev.Changes {
		if c.Asset == "" {
			return LedgerEvent{}, fmt.Errorf("%w: change without asset", ErrMalformed)
		}
		if _, err := parseDecimal("amount", c.Amount); err != nil {
			return LedgerEvent{}, err
		}
	}
	return ev, nil
}

// strictUnmarshal decodes exactly one JSON object. Unknown fields are allowed
// (forward compatibility); trailing data and type mismatches are not.
func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data", ErrMalformed)
	}
	return nil
}

func parseDecimal(field, s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Decimal{}, fmt.Errorf("%w: %s missing", ErrMalformed, field)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("%w: %s %q: %v", ErrMalformed, field, s, err)
	}
	return d, nil
}
