// Package balance keeps per-account balances in Valkey, built from
// ledger-events (SPEC F6), and serves them over HTTP.
//
// Per account there are two keys, sharing a {account} hash tag so they live
// on one node if this ever runs on Redis Cluster:
//
//	<prefix>{alice}:bal     hash   asset -> exact decimal string
//	<prefix>{alice}:events  set    event_ids already applied
//
// ApplyBatch is one optimistic transaction (WATCH/MULTI/EXEC) per chunk of
// up to batchAccounts accounts, however many events the batch holds:
//
//	WATCH bal/events of every account in the chunk
//	pipeline: SMISMEMBER events <ids>, HGETALL bal   per account (one round trip)
//	compute new balances in event order, skipping ids already applied
//	MULTI
//	  SADD events <new ids>, HSET bal <changed assets>  per account
//	EXEC   aborts if anyone touched any watched key; then read and retry
//
// Apply is ApplyBatch of one event.
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
	res, err := s.ApplyBatch(ctx, []messages.LedgerEvent{ev})
	if err != nil {
		return false, err
	}
	if res[0] == Malformed {
		return false, fmt.Errorf("%w: event %s has a malformed amount", messages.ErrMalformed, ev.EventID)
	}
	return res[0] == Applied, nil
}

// accountsKey is the index of accounts that have balances: a set, so
// listing accounts costs O(accounts) instead of scanning the whole keyspace
// (which also holds every idempotency key and event set).
func (s *Store) accountsKey() string { return s.prefix + "accounts" }

// Accounts returns up to limit account ids that have cached balances, sorted.
func (s *Store) Accounts(ctx context.Context, limit int) ([]string, error) {
	out, err := s.rdb.SMembers(ctx, s.accountsKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// IndexAccounts adds every account that already has a balance key to the
// account index, scanning the keyspace once. Run at startup: it backfills
// data written before the index existed and repairs any SADD lost in a crash.
func (s *Store) IndexAccounts(ctx context.Context) (int, error) {
	iter := s.rdb.Scan(ctx, 0, s.prefix+"{*}:bal", 1000).Iterator()
	var batch []any
	n := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := s.rdb.SAdd(ctx, s.accountsKey(), batch...).Err()
		n += len(batch)
		batch = batch[:0]
		return err
	}
	for iter.Next(ctx) {
		k := strings.TrimPrefix(iter.Val(), s.prefix+"{")
		if acct, ok := strings.CutSuffix(k, "}:bal"); ok && acct != "" {
			batch = append(batch, acct)
			if len(batch) == 500 {
				if err := flush(); err != nil {
					return n, err
				}
			}
		}
	}
	if err := iter.Err(); err != nil {
		return n, fmt.Errorf("index accounts: %w", err)
	}
	return n, flush()
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
