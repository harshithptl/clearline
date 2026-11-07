// Package settlement posts trades to the ledger exactly once.
//
// Settle runs one database transaction per trade (SPEC 4):
//
//	BEGIN
//	  INSERT processed_events (trade_id) ON CONFLICT DO NOTHING
//	    0 rows -> already settled: roll back, return Duplicate
//	  INSERT 4 ledger_entries rows           (ledger.BuildEntries)
//	  INSERT 1 outbox row per account touched (ledger-events, key = account)
//	COMMIT
//
// Everything commits together or not at all. In particular, if any insert
// fails, the dedupe row rolls back too, so a retry is not mistaken for a
// duplicate. The caller acks the Kafka message only after Settle returns
// Posted or Duplicate.
package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/ledger/invariants"
)

// Outcome of a successful Settle call. Both outcomes mean "safe to ack".
type Outcome int

const (
	// Posted: this call wrote the trade to the ledger.
	Posted Outcome = iota + 1
	// Duplicate: the trade was already settled; nothing was written.
	Duplicate
)

func (o Outcome) String() string {
	switch o {
	case Posted:
		return "Posted"
	case Duplicate:
		return "Duplicate"
	}
	return fmt.Sprintf("Outcome(%d)", int(o))
}

// ErrUnknownAccount: a ledger row referenced an account that does not exist.
// Possibly transient (account not created yet), so the caller retries with
// backoff and sends it to the DLQ if it keeps failing (SPEC 5).
var ErrUnknownAccount = errors.New("unknown account")

// TxBeginner is satisfied by *pgxpool.Pool and *pgx.Conn.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// LedgerEvent is the payload of a ledger-events message: one account's
// balance changes from one trade. Amounts are decimal strings, never JSON
// numbers, so no consumer parses money as a float.
type LedgerEvent struct {
	EventID   string   `json:"event_id"`
	TradeID   string   `json:"trade_id"`
	AccountID string   `json:"account_id"`
	Changes   []Change `json:"changes"`
}

// Change is a signed balance change for one asset.
type Change struct {
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

// LedgerEventID is the event ID for one account's ledger event from a trade.
// Unambiguous because account IDs cannot contain ':' (migration 0003).
func LedgerEventID(tradeID, accountID string) string {
	return tradeID + ":" + accountID
}

const (
	sqlDedupe = `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`
	sqlLedger = `INSERT INTO ledger_entries (trade_id, account_id, asset, amount) VALUES ($1, $2, $3, $4::numeric)`
	sqlOutbox = `INSERT INTO outbox (event_id, source_event_id, topic, key, payload) VALUES ($1, $2, $3, $4, $5)`

	fkViolation = "23503"
)

// Settle posts a trade exactly once. Invalid trades return an error wrapping
// ledger.ErrInvalidTrade and write nothing.
func Settle(ctx context.Context, db TxBeginner, t ledger.Trade) (Outcome, error) {
	entries, err := ledger.BuildEntries(t)
	if err != nil {
		return 0, err
	}
	events, err := buildEvents(t.TradeID, entries)
	if err != nil {
		return 0, err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("settle %s: begin: %w", t.TradeID, err)
	}
	// Safe after Commit: it then returns ErrTxClosed, which we ignore.
	defer tx.Rollback(ctx)

	// If another transaction is inserting the same event_id right now, this
	// blocks until that one commits (then 0 rows) or rolls back (then 1 row).
	tag, err := tx.Exec(ctx, sqlDedupe, t.TradeID)
	if err != nil {
		return 0, fmt.Errorf("settle %s: dedupe: %w", t.TradeID, err)
	}
	if tag.RowsAffected() == 0 {
		return Duplicate, nil
	}

	// Send all inserts in one round trip.
	batch := &pgx.Batch{}
	for _, e := range entries {
		batch.Queue(sqlLedger, e.TradeID, e.AccountID, e.Asset, e.Amount.String())
	}
	for _, ev := range events {
		batch.Queue(sqlOutbox, ev.event.EventID, t.TradeID, invariants.LedgerEventsTopic, ev.event.AccountID, ev.payload)
	}
	if err := execBatch(ctx, tx, batch); err != nil {
		return 0, fmt.Errorf("settle %s: %w", t.TradeID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("settle %s: commit: %w", t.TradeID, err)
	}
	return Posted, nil
}

func execBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) error {
	results := tx.SendBatch(ctx, batch)
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			results.Close()
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == fkViolation {
				return fmt.Errorf("%w: %s", ErrUnknownAccount, pgErr.Detail)
			}
			return fmt.Errorf("insert: %w", err)
		}
	}
	return results.Close()
}

type encodedEvent struct {
	event   LedgerEvent
	payload []byte
}

// buildEvents groups entries into one LedgerEvent per account, in order of
// first appearance (buyer, then seller).
func buildEvents(tradeID string, entries []ledger.Entry) ([]encodedEvent, error) {
	var order []string
	byAccount := map[string]*LedgerEvent{}
	for _, e := range entries {
		ev, ok := byAccount[e.AccountID]
		if !ok {
			ev = &LedgerEvent{
				EventID:   LedgerEventID(tradeID, e.AccountID),
				TradeID:   tradeID,
				AccountID: e.AccountID,
			}
			byAccount[e.AccountID] = ev
			order = append(order, e.AccountID)
		}
		ev.Changes = append(ev.Changes, Change{Asset: e.Asset, Amount: e.Amount.String()})
	}

	out := make([]encodedEvent, 0, len(order))
	for _, acct := range order {
		ev := byAccount[acct]
		payload, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("settle %s: encode event: %w", tradeID, err)
		}
		out = append(out, encodedEvent{event: *ev, payload: payload})
	}
	return out, nil
}
