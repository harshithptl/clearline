// Package ledger turns trades into double-entry ledger rows.
package ledger

import (
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// ErrInvalidTrade marks a trade that can never be settled, no matter how many
// times it is retried. Settlement sends these to the DLQ instead of retrying.
var ErrInvalidTrade = errors.New("invalid trade")

// Column limits for ledger_entries.amount NUMERIC(20,8).
const amountScale = 8

var amountLimit = decimal.New(1, 20-amountScale) // 10^12: max 12 integer digits

// Trade is a match between a buyer and a seller: BuyerID receives Quantity
// of Symbol and pays Quantity*Price of QuoteAsset to SellerID.
type Trade struct {
	TradeID    string
	Symbol     string // asset being bought, e.g. "AAPL"
	QuoteAsset string // asset paid with, e.g. "USD"
	BuyerID    string
	SellerID   string
	Quantity   decimal.Decimal
	Price      decimal.Decimal
}

// Entry is one ledger row. Amount is signed: negative = debit, positive = credit.
type Entry struct {
	TradeID   string
	AccountID string
	Asset     string
	Amount    decimal.Decimal
}

// BuildEntries returns the four ledger rows for a trade, in a fixed order:
//
//	buyer  quote  -notional
//	seller quote  +notional
//	seller symbol -quantity
//	buyer  symbol +quantity
//
// It never rounds. If the notional (quantity * price) is not exactly
// representable in NUMERIC(20,8), the trade is rejected with ErrInvalidTrade;
// a rounding policy is a business decision, not something to do silently.
func BuildEntries(t Trade) ([]Entry, error) {
	if err := validate(t); err != nil {
		return nil, err
	}
	notional := t.Quantity.Mul(t.Price)
	if err := checkAmount("notional", notional); err != nil {
		return nil, err
	}
	return []Entry{
		{TradeID: t.TradeID, AccountID: t.BuyerID, Asset: t.QuoteAsset, Amount: notional.Neg()},
		{TradeID: t.TradeID, AccountID: t.SellerID, Asset: t.QuoteAsset, Amount: notional},
		{TradeID: t.TradeID, AccountID: t.SellerID, Asset: t.Symbol, Amount: t.Quantity.Neg()},
		{TradeID: t.TradeID, AccountID: t.BuyerID, Asset: t.Symbol, Amount: t.Quantity},
	}, nil
}

func validate(t Trade) error {
	switch {
	case t.TradeID == "":
		return invalid(t, "empty trade id")
	case t.Symbol == "":
		return invalid(t, "empty symbol")
	case t.QuoteAsset == "":
		return invalid(t, "empty quote asset")
	case t.Symbol == t.QuoteAsset:
		return invalid(t, "symbol equals quote asset")
	case t.BuyerID == "":
		return invalid(t, "empty buyer")
	case t.SellerID == "":
		return invalid(t, "empty seller")
	case t.BuyerID == t.SellerID:
		return invalid(t, "buyer and seller are the same account")
	case !t.Quantity.IsPositive():
		return invalid(t, "quantity must be positive, got "+t.Quantity.String())
	case !t.Price.IsPositive():
		return invalid(t, "price must be positive, got "+t.Price.String())
	}
	if err := checkAmount("quantity", t.Quantity); err != nil {
		return err
	}
	return checkAmount("price", t.Price)
}

// checkAmount rejects values that NUMERIC(20,8) cannot store exactly.
func checkAmount(name string, v decimal.Decimal) error {
	if !v.Truncate(amountScale).Equal(v) {
		return fmt.Errorf("%w: %s %s has more than %d decimal places", ErrInvalidTrade, name, v, amountScale)
	}
	if v.Abs().GreaterThanOrEqual(amountLimit) {
		return fmt.Errorf("%w: %s %s exceeds NUMERIC(20,8)", ErrInvalidTrade, name, v)
	}
	return nil
}

func invalid(t Trade, reason string) error {
	return fmt.Errorf("%w: trade %q: %s", ErrInvalidTrade, t.TradeID, reason)
}
