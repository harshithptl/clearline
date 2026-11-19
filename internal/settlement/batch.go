package settlement

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/ledger/invariants"
)

// SettleBatch posts many trades in ONE transaction, each exactly once, and
// returns one Outcome per trade in input order. It is Settle with the
// per-trade costs (a round trip per statement, a commit and its fsync per
// trade) paid once per batch:
//
//	BEGIN
//	  INSERT processed_events SELECT unnest(<trade ids>) ON CONFLICT DO NOTHING
//	    RETURNING event_id                 -> which trades are new
//	  INSERT ledger_entries SELECT unnest(...) WITH ORDINALITY ORDER BY ordinality
//	  INSERT outbox         SELECT unnest(...) WITH ORDINALITY ORDER BY ordinality
//	COMMIT
//
// The batch is all or nothing: if any trade fails (invalid, unknown account,
// any insert error), nothing commits, and the caller settles the trades one
// at a time with Settle to isolate the bad one. Repeated trade ids within the
// batch settle once; the repeats return Duplicate.
func SettleBatch(ctx context.Context, db TxBeginner, trades []ledger.Trade) ([]Outcome, error) {
	type built struct {
		entries []ledger.Entry
		events  []encodedEvent
	}
	out := make([]Outcome, len(trades))
	prepared := make([]built, len(trades))
	first := map[string]int{} // trade id -> index of its first occurrence
	var ids []string
	for i, t := range trades {
		if _, dup := first[t.TradeID]; dup {
			out[i] = Duplicate
			continue
		}
		entries, err := ledger.BuildEntries(t)
		if err != nil {
			return nil, err
		}
		events, err := buildEvents(t.TradeID, entries)
		if err != nil {
			return nil, err
		}
		prepared[i] = built{entries: entries, events: events}
		first[t.TradeID] = i
		ids = append(ids, t.TradeID)
	}
	if len(ids) == 0 {
		return out, nil
	}

	// Lock order: inserting the dedupe keys sorted means two batches that
	// share trade ids take the unique-index locks in the same order, so one
	// simply waits for the other instead of deadlocking (40P01), which the
	// batched harness reproduced with unsorted ids.
	slices.Sort(ids)

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("settle batch: begin: %w", err)
	}
	defer rollback(ctx, tx)

	rows, err := tx.Query(ctx, sqlDedupeBatch, ids)
	if err != nil {
		return nil, fmt.Errorf("settle batch: dedupe: %w", err)
	}
	fresh := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("settle batch: dedupe: %w", err)
		}
		fresh[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("settle batch: dedupe: %w", err)
	}

	// Column arrays for the new trades, in input order.
	var lt, la, ls, lm []string     // ledger: trade, account, asset, amount
	var oe, os, ot, ok, op []string // outbox: event, source, topic, key, payload
	for i, t := range trades {
		if out[i] == Duplicate {
			continue
		}
		if !fresh[t.TradeID] {
			out[i] = Duplicate
			continue
		}
		out[i] = Posted
		for _, e := range prepared[i].entries {
			lt, la, ls, lm = append(lt, e.TradeID), append(la, e.AccountID), append(ls, e.Asset), append(lm, e.Amount.String())
		}
		for _, ev := range prepared[i].events {
			oe = append(oe, ev.event.EventID)
			os = append(os, t.TradeID)
			ot = append(ot, invariants.LedgerEventsTopic)
			ok = append(ok, ev.event.AccountID)
			op = append(op, string(ev.payload))
		}
	}
	if len(lt) > 0 {
		if _, err := tx.Exec(ctx, sqlLedgerBatch, lt, la, ls, lm); err != nil {
			return nil, fmt.Errorf("settle batch: ledger: %w", wrapInsert(err))
		}
		if _, err := tx.Exec(ctx, sqlOutboxBatch, oe, os, ot, ok, op); err != nil {
			return nil, fmt.Errorf("settle batch: outbox: %w", wrapInsert(err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("settle batch: commit: %w", err)
	}
	return out, nil
}

// Set-based statements: one round trip each, however many trades. WITH
// ORDINALITY ... ORDER BY keeps rows in input order, so entry_id and
// outbox_id follow trade order within the batch.
const (
	sqlDedupeBatch = `INSERT INTO processed_events (event_id)
		SELECT unnest($1::text[]) ON CONFLICT DO NOTHING RETURNING event_id`
	sqlLedgerBatch = `INSERT INTO ledger_entries (trade_id, account_id, asset, amount)
		SELECT t, a, s, m::numeric FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
		WITH ORDINALITY AS u(t, a, s, m, n) ORDER BY n`
	sqlOutboxBatch = `INSERT INTO outbox (event_id, source_event_id, topic, key, payload)
		SELECT e, src, tp, k, p::jsonb FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
		WITH ORDINALITY AS u(e, src, tp, k, p, n) ORDER BY n`
)

// wrapInsert maps a foreign key violation to ErrUnknownAccount, like Settle.
func wrapInsert(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == fkViolation {
		return fmt.Errorf("%w: %s", ErrUnknownAccount, pgErr.Detail)
	}
	return err
}
