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

// Report collects every violation found by CheckAll.
type Report struct {
	AssetImbalances []AssetImbalance // invariant 1
	TradeImbalances []TradeImbalance // invariant 2
	DuplicateEvents []DuplicateEvent // invariant 4
	MissingOutbox   []string         // invariant 5: processed event IDs with no outbox row
}

// OK reports whether every invariant holds.
func (r Report) OK() bool {
	return len(r.AssetImbalances) == 0 &&
		len(r.TradeImbalances) == 0 &&
		len(r.DuplicateEvents) == 0 &&
		len(r.MissingOutbox) == 0
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
	if rep.MissingOutbox, err = OutboxComplete(ctx, q); err != nil {
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

// OutboxComplete checks invariant 5: every processed event has an outbox row.
// "At most one" is already guaranteed by the UNIQUE constraint on
// outbox.event_id, so this only looks for missing rows.
func OutboxComplete(ctx context.Context, q Querier) ([]string, error) {
	const sql = `
		SELECT DISTINCT p.event_id
		FROM processed_events p
		LEFT JOIN outbox o ON o.event_id = p.event_id
		WHERE o.event_id IS NULL
		ORDER BY p.event_id`
	return collect(ctx, q, "invariant 5 (outbox complete)", sql, func(rows pgx.Rows) (string, error) {
		var id string
		err := rows.Scan(&id)
		return id, err
	})
}

// collect runs sql and scans every row with scan. It returns nil (not an
// empty slice) when there are no violations.
func collect[T any](ctx context.Context, q Querier, name, sql string, scan func(pgx.Rows) (T, error)) ([]T, error) {
	rows, err := q.Query(ctx, sql)
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
