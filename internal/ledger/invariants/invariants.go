// Package invariants checks the ledger invariants from SPEC 4.2 against
// Postgres. Each check returns the rows that violate it, not just a boolean,
// so a failure says exactly what is wrong. An empty result means the
// invariant holds.
//
// Used by tests today and by the reconciler later. Invariant 3 (Redis
// balances match the ledger) lives with the balance service, since it needs
// Redis.
package invariants

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier is satisfied by *pgx.Conn, pgx.Tx and *pgxpool.Pool, so checks can
// run inside a transaction or against a pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Amounts are returned as Postgres' exact decimal text (e.g. "-10.00000000")
// so nothing passes through a float.

// AssetImbalance: invariant 1 violation. All ledger rows for Asset sum to Sum, not 0.
type AssetImbalance struct {
	Asset string
	Sum   string
}

// TradeImbalance: invariant 2 violation. Trade's rows for Asset sum to Sum, not 0.
type TradeImbalance struct {
	TradeID string
	Asset   string
	Sum     string
}

// DuplicateEvent: invariant 4 violation. EventID appears Count times.
type DuplicateEvent struct {
	EventID string
	Count   int64
}

// LedgerEventsTopic is the topic for per-account balance updates. Its outbox
// rows use the account ID as the Kafka key.
const LedgerEventsTopic = "ledger-events"

// OutboxProblem says which way an outbox mismatch goes.
type OutboxProblem string

const (
	// OutboxMissing: the trade touched the account but no event was queued,
	// so that balance would never update.
	OutboxMissing OutboxProblem = "missing"
	// OutboxUnexpected: an event exists for an account the trade did not
	// touch, or for a trade that was never settled.
	OutboxUnexpected OutboxProblem = "unexpected"
)

// OutboxMismatch: invariant 5 violation for one (event, account) pair.
type OutboxMismatch struct {
	EventID string
	Account string
	Problem OutboxProblem
}

// Report collects every violation found by CheckAll.
type Report struct {
	AssetImbalances  []AssetImbalance // invariant 1
	TradeImbalances  []TradeImbalance // invariant 2
	DuplicateEvents  []DuplicateEvent // invariant 4
	OutboxMismatches []OutboxMismatch // invariant 5
}

// OK reports whether every invariant holds.
func (r Report) OK() bool {
	return len(r.AssetImbalances) == 0 &&
		len(r.TradeImbalances) == 0 &&
		len(r.DuplicateEvents) == 0 &&
		len(r.OutboxMismatches) == 0
}

// CheckAll runs every check. For a consistent snapshot across checks, pass a
// transaction opened with REPEATABLE READ isolation.
func CheckAll(ctx context.Context, q Querier) (Report, error) {
	var rep Report
	var err error
	if rep.AssetImbalances, err = AssetsBalance(ctx, q); err != nil {
		return Report{}, err
	}
	if rep.TradeImbalances, err = TradesBalance(ctx, q); err != nil {
		return Report{}, err
	}
	if rep.DuplicateEvents, err = EventsProcessedOnce(ctx, q); err != nil {
		return Report{}, err
	}
	if rep.OutboxMismatches, err = OutboxMatchesLedger(ctx, q); err != nil {
		return Report{}, err
	}
	return rep, nil
}

// AssetsBalance checks invariant 1: for every asset, all ledger amounts sum to 0.
func AssetsBalance(ctx context.Context, q Querier) ([]AssetImbalance, error) {
	const sql = `
		SELECT asset, SUM(amount)::text
		FROM ledger_entries
		GROUP BY asset
		HAVING SUM(amount) <> 0
		ORDER BY asset`
	return collect(ctx, q, "invariant 1 (assets balance)", sql, func(rows pgx.Rows) (AssetImbalance, error) {
		var v AssetImbalance
		err := rows.Scan(&v.Asset, &v.Sum)
		return v, err
	})
}

// TradesBalance checks invariant 2: every trade's rows net to 0 per asset.
// This catches errors that cancel out across trades, which invariant 1 misses.
func TradesBalance(ctx context.Context, q Querier) ([]TradeImbalance, error) {
	const sql = `
		SELECT trade_id, asset, SUM(amount)::text
		FROM ledger_entries
		GROUP BY trade_id, asset
		HAVING SUM(amount) <> 0
		ORDER BY trade_id, asset`
	return collect(ctx, q, "invariant 2 (trades balance)", sql, func(rows pgx.Rows) (TradeImbalance, error) {
		var v TradeImbalance
		err := rows.Scan(&v.TradeID, &v.Asset, &v.Sum)
		return v, err
	})
}

// EventsProcessedOnce checks invariant 4: every event_id appears at most once.
// The primary key already guarantees this; the check is a second line of
// defense in case the constraint is ever dropped or bypassed.
func EventsProcessedOnce(ctx context.Context, q Querier) ([]DuplicateEvent, error) {
	const sql = `
		SELECT event_id, COUNT(*)
		FROM processed_events
		GROUP BY event_id
		HAVING COUNT(*) > 1
		ORDER BY event_id`
	return collect(ctx, q, "invariant 4 (events processed once)", sql, func(rows pgx.Rows) (DuplicateEvent, error) {
		var v DuplicateEvent
		err := rows.Scan(&v.EventID, &v.Count)
		return v, err
	})
}

// OutboxMatchesLedger checks invariant 5: every settled trade has exactly one
// ledger-events outbox row per account in its ledger rows, and no others.
//
// expected = (trade, account) pairs from ledger rows of processed trades
// actual   = (source_event_id, key) pairs from ledger-events outbox rows
//
// A FULL OUTER JOIN keeps pairs found on either side only: expected-only is
// "missing", actual-only is "unexpected". "More than one per pair" cannot
// happen because of the UNIQUE (source_event_id, topic, key) constraint.
//
// Assumes a trade's event_id in processed_events equals its trade_id in
// ledger_entries.
func OutboxMatchesLedger(ctx context.Context, q Querier) ([]OutboxMismatch, error) {
	const sql = `
		WITH expected AS (
			SELECT DISTINCT l.trade_id AS event_id, l.account_id
			FROM ledger_entries l
			JOIN processed_events p ON p.event_id = l.trade_id
		),
		actual AS (
			SELECT source_event_id AS event_id, key AS account_id
			FROM outbox
			WHERE topic = $1
		)
		SELECT
			COALESCE(e.event_id, a.event_id),
			COALESCE(e.account_id, a.account_id),
			CASE WHEN a.event_id IS NULL THEN 'missing' ELSE 'unexpected' END
		FROM expected e
		FULL OUTER JOIN actual a
			ON a.event_id = e.event_id AND a.account_id = e.account_id
		WHERE e.event_id IS NULL OR a.event_id IS NULL
		ORDER BY 1, 2`
	return collect(ctx, q, "invariant 5 (outbox matches ledger)", sql, func(rows pgx.Rows) (OutboxMismatch, error) {
		var v OutboxMismatch
		err := rows.Scan(&v.EventID, &v.Account, &v.Problem)
		return v, err
	}, LedgerEventsTopic)
}

// collect runs sql and scans every row with scan. It returns nil (not an
// empty slice) when there are no violations.
func collect[T any](ctx context.Context, q Querier, name, sql string, scan func(pgx.Rows) (T, error), args ...any) ([]T, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: query: %w", name, err)
	}
	defer rows.Close()

	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", name, err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: rows: %w", name, err)
	}
	return out, nil
}
