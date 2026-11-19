// Package balance keeps per-account balances in Valkey, built from
// ledger-events (SPEC F6), and serves them over HTTP.
//
// Per account there are two keys, sharing a {account} hash tag so they live
// on one node if this ever runs on Redis Cluster:
//
//	<prefix>{alice}:bal     hash   asset -> exact decimal string
//	<prefix>{alice}:events  set    event_ids already applied
//
// Apply is an optimistic transaction (WATCH/MULTI/EXEC):
//
//	WATCH bal events
//	SISMEMBER events <event_id>   already applied -> done, duplicate
//	HMGET bal <assets>            current balances
//	MULTI
//	  SADD events <event_id>
//	  HSET bal <asset> <new balance> ...
//	EXEC                          aborts if anyone touched bal or events since WATCH
//
// The dedupe mark and the balance change commit together or not at all, so a
// crash or a second consumer during a rebalance can never apply an event
// twice. Balances are decimal strings, not HINCRBY integers: NUMERIC(20,8)
// values do not fit in int64 once scaled to 1e-8 units.
//
// The events set grows with every event an account receives. That is fine at
// demo scale; trimming it needs a per-account sequence number (future work).
package balance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/messages"
)

// ErrContention: Apply lost the optimistic race too many times in a row.
var ErrContention = errors.New("balance: too much contention")

const maxTxAttempts = 100

// Store reads and writes balances.
type Store struct {
	rdb    *redis.Client
	prefix string
}

// NewStore returns a Store whose keys start with prefix (e.g. "clearline:").
func NewStore(rdb *redis.Client, prefix string) *Store {
	return &Store{rdb: rdb, prefix: prefix}
}

func (s *Store) balKey(account string) string    { return s.prefix + "{" + account + "}:bal" }
func (s *Store) eventsKey(account string) string { return s.prefix + "{" + account + "}:events" }

// Apply applies ev once. It returns false if ev was already applied.
func (s *Store) Apply(ctx context.Context, ev messages.LedgerEvent) (bool, error) {
	assets := make([]string, len(ev.Changes))
	deltas := make([]decimal.Decimal, len(ev.Changes))
	for i, c := range ev.Changes {
		d, err := decimal.NewFromString(c.Amount)
		if err != nil {
			return false, fmt.Errorf("%w: %s amount %q", messages.ErrMalformed, c.Asset, c.Amount)
		}
		assets[i], deltas[i] = c.Asset, d
	}
	bk, ek := s.balKey(ev.AccountID), s.eventsKey(ev.AccountID)

	for range maxTxAttempts {
		applied := false
		txf := func(tx *redis.Tx) error {
			seen, err := tx.SIsMember(ctx, ek, ev.EventID).Result()
			if err != nil || seen {
				return err
			}
			cur, err := tx.HMGet(ctx, bk, assets...).Result()
			if err != nil {
				return err
			}
			next := map[string]decimal.Decimal{}
			for i, a := range assets {
				base, ok := next[a]
				if !ok {
					if base, err = parseStored(cur[i]); err != nil {
						return fmt.Errorf("balance %s %s: %w", ev.AccountID, a, err)
					}
				}
				next[a] = base.Add(deltas[i])
			}
			fields := make([]any, 0, 2*len(next))
			for a, v := range next {
				fields = append(fields, a, v.String())
			}
			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.SAdd(ctx, ek, ev.EventID)
				p.HSet(ctx, bk, fields...)
				return nil
			})
			if err == nil {
				applied = true
			}
			return err
		}
		err := s.rdb.Watch(ctx, txf, bk, ek)
		if errors.Is(err, redis.TxFailedErr) {
			continue // someone changed the keys; read again
		}
		return applied, err
	}
	return false, ErrContention
}

func parseStored(v any) (decimal.Decimal, error) {
	switch x := v.(type) {
	case nil:
		return decimal.Zero, nil
	case string:
		return decimal.NewFromString(x)
	}
	return decimal.Decimal{}, fmt.Errorf("unexpected stored value %T", v)
}

// Accounts returns up to limit account ids that have cached balances,
// sorted. It SCANs (incrementally, never blocking Valkey like KEYS), so it is
// meant for dashboards and tools, not the hot path.
func (s *Store) Accounts(ctx context.Context, limit int) ([]string, error) {
	var out []string
	pattern := s.prefix + "{*}:bal"
	iter := s.rdb.Scan(ctx, 0, pattern, 500).Iterator()
	for iter.Next(ctx) && len(out) < limit {
		k := strings.TrimPrefix(iter.Val(), s.prefix+"{")
		if acct, ok := strings.CutSuffix(k, "}:bal"); ok && acct != "" {
			out = append(out, acct)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	sort.Strings(out)
	return out, nil
}

// Snapshot is one account's cached state, read atomically.
type Snapshot struct {
	Balances map[string]decimal.Decimal
	Events   int64 // number of distinct events applied
}

// Get returns one account's snapshot.
func (s *Store) Get(ctx context.Context, account string) (Snapshot, error) {
	m, err := s.GetMany(ctx, []string{account})
	if err != nil {
		return Snapshot{}, err
	}
	return m[account], nil
}

// snapshotChunk bounds how many accounts one MULTI reads, so a big
// reconciliation does not block Valkey for long.
const snapshotChunk = 100

// GetMany returns snapshots for accounts. Each account's balances and event
// count come from the same MULTI, so they are consistent with each other.
func (s *Store) GetMany(ctx context.Context, accounts []string) (map[string]Snapshot, error) {
	out := make(map[string]Snapshot, len(accounts))
	for start := 0; start < len(accounts); start += snapshotChunk {
		chunk := accounts[start:min(start+snapshotChunk, len(accounts))]
		bals := make([]*redis.MapStringStringCmd, len(chunk))
		counts := make([]*redis.IntCmd, len(chunk))
		_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
			for i, a := range chunk {
				bals[i] = p.HGetAll(ctx, s.balKey(a))
				counts[i] = p.SCard(ctx, s.eventsKey(a))
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("balance snapshot: %w", err)
		}
		for i, a := range chunk {
			snap := Snapshot{Balances: map[string]decimal.Decimal{}, Events: counts[i].Val()}
			for asset, v := range bals[i].Val() {
				d, err := decimal.NewFromString(v)
				if err != nil {
					return nil, fmt.Errorf("balance %s %s: stored %q: %w", a, asset, v, err)
				}
				snap.Balances[asset] = d
			}
			out[a] = snap
		}
	}
	return out, nil
}
