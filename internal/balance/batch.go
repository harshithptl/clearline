package balance

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/messages"
)

// Result is what ApplyBatch did with one event.
type Result int

// ApplyBatch results.
const (
	Applied   Result = iota // balances changed and the event marked applied
	Duplicate               // already applied earlier (or earlier in the same batch)
	Malformed               // an amount is not a decimal; skipped, nothing written
)

// batchAccounts bounds the accounts per transaction, so one EXEC stays
// small and a conflict retries a bounded amount of work.
const batchAccounts = 100

// ApplyBatch applies events, each at most once, and returns one Result per
// event in input order. Events for one account must be in their ledger order
// (the consumer passes a partition's records in offset order, and an
// account's events all live on one partition).
//
// Atomicity is per chunk of accounts: each chunk's dedupe marks and balances
// commit together or not at all. If a later chunk fails, earlier chunks stay
// applied; redelivery then reports their events as Duplicate, so the result
// is still exactly once.
func (s *Store) ApplyBatch(ctx context.Context, events []messages.LedgerEvent) ([]Result, error) {
	results := make([]Result, len(events))
	parsed := make([][]decimal.Decimal, len(events))
	byAccount := map[string][]int{} // account -> indexes into events, in order
	var order []string
	for i, ev := range events {
		deltas, ok := parseDeltas(ev)
		if !ok {
			results[i] = Malformed
			continue
		}
		parsed[i] = deltas
		if _, seen := byAccount[ev.AccountID]; !seen {
			order = append(order, ev.AccountID)
		}
		byAccount[ev.AccountID] = append(byAccount[ev.AccountID], i)
	}

	for start := 0; start < len(order); start += batchAccounts {
		chunk := order[start:min(start+batchAccounts, len(order))]
		if err := s.applyChunk(ctx, events, parsed, byAccount, chunk, results); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func parseDeltas(ev messages.LedgerEvent) ([]decimal.Decimal, bool) {
	out := make([]decimal.Decimal, len(ev.Changes))
	for i, c := range ev.Changes {
		d, err := decimal.NewFromString(c.Amount)
		if err != nil {
			return nil, false
		}
		out[i] = d
	}
	return out, true
}

// applyChunk runs the optimistic transaction for one chunk of accounts,
// retrying from a fresh read when another client changed a watched key.
func (s *Store) applyChunk(ctx context.Context, events []messages.LedgerEvent, parsed [][]decimal.Decimal,
	byAccount map[string][]int, chunk []string, results []Result) error {

	keys := make([]string, 0, 2*len(chunk))
	for _, a := range chunk {
		keys = append(keys, s.balKey(a), s.eventsKey(a))
	}
	for range maxTxAttempts {
		var pending map[int]Result
		txf := func(tx *redis.Tx) error {
			// Read everything for the chunk in one round trip.
			seen := make([]*redis.BoolSliceCmd, len(chunk))
			bals := make([]*redis.MapStringStringCmd, len(chunk))
			if _, err := tx.Pipelined(ctx, func(p redis.Pipeliner) error {
				for j, a := range chunk {
					ids := make([]any, len(byAccount[a]))
					for k, i := range byAccount[a] {
						ids[k] = events[i].EventID
					}
					seen[j] = p.SMIsMember(ctx, s.eventsKey(a), ids...)
					bals[j] = p.HGetAll(ctx, s.balKey(a))
				}
				return nil
			}); err != nil {
				return err
			}

			// Compute in event order, deduping against Valkey and within the batch.
			pending = map[int]Result{}
			type write struct {
				ids  []any
				bals map[string]decimal.Decimal
			}
			writes := make([]write, len(chunk))
			for j, a := range chunk {
				already := seen[j].Val()
				cur, err := parseBalances(a, bals[j].Val())
				if err != nil {
					return err
				}
				inBatch := map[string]bool{}
				changed := map[string]decimal.Decimal{}
				var ids []any
				for k, i := range byAccount[a] {
					ev := events[i]
					if already[k] || inBatch[ev.EventID] {
						pending[i] = Duplicate
						continue
					}
					inBatch[ev.EventID] = true
					for c, ch := range ev.Changes {
						cur[ch.Asset] = cur[ch.Asset].Add(parsed[i][c])
						changed[ch.Asset] = cur[ch.Asset]
					}
					ids = append(ids, ev.EventID)
					pending[i] = Applied
				}
				writes[j] = write{ids: ids, bals: changed}
			}

			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				for j, a := range chunk {
					w := writes[j]
					if len(w.ids) == 0 {
						continue
					}
					p.SAdd(ctx, s.eventsKey(a), w.ids...)
					fields := make([]any, 0, 2*len(w.bals))
					for asset, v := range w.bals {
						fields = append(fields, asset, v.String())
					}
					p.HSet(ctx, s.balKey(a), fields...)
				}
				return nil
			})
			return err
		}
		err := s.rdb.Watch(ctx, txf, keys...)
		if errors.Is(err, redis.TxFailedErr) {
			continue // a watched key changed: read again
		}
		if err != nil {
			return err
		}
		var touched []any
		for i, r := range pending {
			results[i] = r
			if r == Applied {
				touched = append(touched, events[i].AccountID)
			}
		}
		if len(touched) > 0 {
			// The listing index, kept outside the transaction: it is only
			// for listing and IndexAccounts repairs it. One SADD per chunk.
			return s.rdb.SAdd(ctx, s.accountsKey(), touched...).Err()
		}
		return nil
	}
	return ErrContention
}

func parseBalances(account string, stored map[string]string) (map[string]decimal.Decimal, error) {
	out := make(map[string]decimal.Decimal, len(stored))
	for asset, v := range stored {
		d, err := decimal.NewFromString(v)
		if err != nil {
			return nil, fmt.Errorf("balance %s %s: stored %q: %w", account, asset, v, err)
		}
		out[asset] = d
	}
	return out, nil
}
