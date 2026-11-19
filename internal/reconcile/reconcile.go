// Package reconcile proves the books are right (SPEC F8): the ledger
// invariants (SPEC 4.2 #1, #2, #4, #5) and that every account's cached
// balances match the ledger (#3).
//
// The cache lags the ledger by design (outbox relay, Kafka, balance
// consumer), so comparing balances directly would flag every in-flight
// trade. Instead each account's cache records how many distinct events it
// applied, and the ledger knows how many events exist for that account (its
// outbox rows):
//
//	cache events < ledger events  -> lagging, not an error (yet)
//	cache events = ledger events  -> balances must match exactly
//	cache events > ledger events  -> mismatch: the cache applied something
//	                                 the ledger never produced
//
// Read order matters. The cache is read first, then the ledger in one
// REPEATABLE READ snapshot. Anything the cache applied was committed to the
// ledger before, so the later snapshot always includes it, and "cache ahead
// of ledger" is a real error, never a race.
package reconcile

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/ledger/invariants"
)

// EventsField is the CacheMismatch.Asset used when the event counts, not a
// balance, disagree.
const EventsField = "#events"

// CacheMismatch is invariant 3 failing for one account and asset.
type CacheMismatch struct {
	Account string
	Asset   string
	Cache   string
	Ledger  string
}

// Lag is an account whose cache has not caught up with the ledger yet.
type Lag struct {
	Account      string
	CacheEvents  int64
	LedgerEvents int64
}

// Report is the result of one reconciliation.
type Report struct {
	Ledger          invariants.Report
	CacheMismatches []CacheMismatch
	Lagging         []Lag
	Accounts        int // accounts compared
}

// OK reports whether every invariant holds. Lagging accounts do not count
// against it; alert on lag that persists instead.
func (r Report) OK() bool { return r.Ledger.OK() && len(r.CacheMismatches) == 0 }

// Check runs one full reconciliation.
func Check(ctx context.Context, pool *pgxpool.Pool, store *balance.Store) (Report, error) {
	accounts, err := listAccounts(ctx, pool)
	if err != nil {
		return Report{}, err
	}
	// Cache first, then the ledger snapshot (see package doc).
	cache, err := store.GetMany(ctx, accounts)
	if err != nil {
		return Report{}, err
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Report{}, fmt.Errorf("reconcile: begin: %w", err)
	}
	defer rollback(ctx, tx)

	rep := Report{Accounts: len(accounts)}
	if rep.Ledger, err = invariants.CheckAll(ctx, tx); err != nil {
		return Report{}, err
	}
	sums, err := ledgerSums(ctx, tx)
	if err != nil {
		return Report{}, err
	}
	counts, err := ledgerEventCounts(ctx, tx)
	if err != nil {
		return Report{}, err
	}

	for _, a := range accounts {
		snap := cache[a]
		want := counts[a]
		switch {
		case snap.Events < want:
			rep.Lagging = append(rep.Lagging, Lag{Account: a, CacheEvents: snap.Events, LedgerEvents: want})
		case snap.Events > want:
			rep.CacheMismatches = append(rep.CacheMismatches, CacheMismatch{
				Account: a, Asset: EventsField,
				Cache: fmt.Sprint(snap.Events), Ledger: fmt.Sprint(want),
			})
		default:
			rep.CacheMismatches = append(rep.CacheMismatches, compareBalances(a, snap.Balances, sums[a])...)
		}
	}
	return rep, nil
}

// compareBalances returns one mismatch per asset whose cached and ledger
// balances differ. A missing asset counts as zero on either side.
func compareBalances(account string, cache, ledger map[string]decimal.Decimal) []CacheMismatch {
	assets := map[string]bool{}
	for a := range cache {
		assets[a] = true
	}
	for a := range ledger {
		assets[a] = true
	}
	names := make([]string, 0, len(assets))
	for a := range assets {
		names = append(names, a)
	}
	sort.Strings(names)

	var out []CacheMismatch
	for _, a := range names {
		c, l := cache[a], ledger[a] // zero value is 0
		if !c.Equal(l) {
			out = append(out, CacheMismatch{Account: account, Asset: a, Cache: c.String(), Ledger: l.String()})
		}
	}
	return out
}

func listAccounts(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT account_id FROM accounts ORDER BY account_id`)
	if err != nil {
		return nil, fmt.Errorf("reconcile: accounts: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("reconcile: accounts: %w", err)
	}
	return ids, nil
}

func ledgerSums(ctx context.Context, tx pgx.Tx) (map[string]map[string]decimal.Decimal, error) {
	rows, err := tx.Query(ctx, `SELECT account_id, asset, SUM(amount)::text FROM ledger_entries GROUP BY 1, 2`)
	if err != nil {
		return nil, fmt.Errorf("reconcile: ledger sums: %w", err)
	}
	defer rows.Close()
	out := map[string]map[string]decimal.Decimal{}
	for rows.Next() {
		var account, asset, sum string
		if err := rows.Scan(&account, &asset, &sum); err != nil {
			return nil, fmt.Errorf("reconcile: ledger sums: %w", err)
		}
		d, err := decimal.NewFromString(sum)
		if err != nil {
			return nil, fmt.Errorf("reconcile: ledger sum %q: %w", sum, err)
		}
		if out[account] == nil {
			out[account] = map[string]decimal.Decimal{}
		}
		out[account][asset] = d
	}
	return out, rows.Err()
}

func ledgerEventCounts(ctx context.Context, tx pgx.Tx) (map[string]int64, error) {
	rows, err := tx.Query(ctx, `SELECT key, count(*) FROM outbox WHERE topic = $1 GROUP BY key`, invariants.LedgerEventsTopic)
	if err != nil {
		return nil, fmt.Errorf("reconcile: event counts: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var key string
		var n int64
		if err := rows.Scan(&key, &n); err != nil {
			return nil, fmt.Errorf("reconcile: event counts: %w", err)
		}
		out[key] = n
	}
	return out, rows.Err()
}
