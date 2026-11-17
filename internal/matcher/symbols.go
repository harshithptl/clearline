package matcher

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/shopspring/decimal"

	matcherv1 "github.com/harshithptl/clearline/gen/matcher/v1"
)

// SymbolSpec is one tradable symbol and its book.
type SymbolSpec struct {
	Symbol     string
	QuoteAsset string
	// TickSize is the price increment, e.g. 0.01. Prices must be whole
	// multiples of it.
	TickSize decimal.Decimal
	// ReferencePrice centers the book; it accepts prices within
	// TicksEachSide ticks of it.
	ReferencePrice decimal.Decimal
	TicksEachSide  int32
	MaxOrderID     uint64 // lifetime orders this book can take
	MaxResting     uint32 // orders resting at once
}

// Defaults for book sizes (memory per book is roughly
// 48 bytes * 2 * TicksEachSide + 8 bytes * MaxOrderID + 128 bytes * MaxResting,
// about 26 MB with these).
const (
	DefaultTicksEachSide = 100_000
	DefaultMaxOrderID    = 1 << 20
	DefaultMaxResting    = 1 << 16
)

// DefaultSymbols is the CLEARLINE_SYMBOLS default.
const DefaultSymbols = "AAPL:USD:0.01:150,MSFT:USD:0.01:400,TSLA:USD:0.01:250,NVDA:USD:0.01:500"

// ParseSymbols parses "SYMBOL:QUOTE:TICK:REFERENCE,..." with default book
// sizes, e.g. "AAPL:USD:0.01:150".
func ParseSymbols(s string) ([]SymbolSpec, error) {
	var out []SymbolSpec
	seen := map[string]bool{}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("symbol %q: want SYMBOL:QUOTE:TICK:REFERENCE", item)
		}
		tick, err := decimal.NewFromString(parts[2])
		if err != nil {
			return nil, fmt.Errorf("symbol %q: tick: %w", item, err)
		}
		ref, err := decimal.NewFromString(parts[3])
		if err != nil {
			return nil, fmt.Errorf("symbol %q: reference: %w", item, err)
		}
		spec := SymbolSpec{
			Symbol: parts[0], QuoteAsset: parts[1], TickSize: tick, ReferencePrice: ref,
			TicksEachSide: DefaultTicksEachSide, MaxOrderID: DefaultMaxOrderID, MaxResting: DefaultMaxResting,
		}
		if err := spec.validate(); err != nil {
			return nil, err
		}
		if seen[spec.Symbol] {
			return nil, fmt.Errorf("symbol %s listed twice", spec.Symbol)
		}
		seen[spec.Symbol] = true
		out = append(out, spec)
	}
	if len(out) == 0 {
		return nil, errors.New("no symbols configured")
	}
	return out, nil
}

func (s SymbolSpec) validate() error {
	switch {
	case s.Symbol == "" || s.QuoteAsset == "" || s.Symbol == s.QuoteAsset:
		return fmt.Errorf("symbol %q / quote %q invalid", s.Symbol, s.QuoteAsset)
	case strings.ContainsAny(s.Symbol, ":-"):
		// '-' separates trade id parts; ':' separates event id parts.
		return fmt.Errorf("symbol %q must not contain ':' or '-'", s.Symbol)
	case !s.TickSize.IsPositive():
		return fmt.Errorf("symbol %s: tick size must be positive", s.Symbol)
	}
	if _, err := s.ticks(s.ReferencePrice); err != nil {
		return fmt.Errorf("symbol %s: reference price: %w", s.Symbol, err)
	}
	return nil
}

// errPrice: a price that is not a positive whole number of ticks.
var errPrice = errors.New("price is not a positive multiple of the tick size")

// ticks converts a decimal price to whole ticks, exactly.
func (s SymbolSpec) ticks(price decimal.Decimal) (int64, error) {
	if !price.IsPositive() || !price.Mod(s.TickSize).IsZero() {
		return 0, errPrice
	}
	t := price.Div(s.TickSize)
	if !t.IsInteger() || t.GreaterThan(decimal.NewFromInt(math.MaxInt64)) {
		return 0, errPrice
	}
	return t.IntPart(), nil
}

// price converts whole ticks back to a decimal price, exactly.
func (s SymbolSpec) price(ticks int64) decimal.Decimal {
	return decimal.NewFromInt(ticks).Mul(s.TickSize)
}

func (s SymbolSpec) bookSpec() (*matcherv1.BookSpec, error) {
	ref, err := s.ticks(s.ReferencePrice)
	if err != nil {
		return nil, err
	}
	return &matcherv1.BookSpec{
		Symbol:         s.Symbol,
		ReferenceTicks: ref,
		TicksEachSide:  s.TicksEachSide,
		MaxOrderId:     s.MaxOrderID,
		MaxResting:     s.MaxResting,
	}, nil
}
