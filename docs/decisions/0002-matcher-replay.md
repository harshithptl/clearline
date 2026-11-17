# 0002: Rebuilding matcher state by replaying the orders log

Status: accepted.

## Context

The C++ engine keeps its order books in memory. Matching is not idempotent: sending an
order twice matches it twice. If the engine or the adapter restarts, the books are gone,
and every resting order with them.

## Decision

The `orders` topic is the source of truth for book state, kept forever
(`retention.ms=-1`). The adapter rebuilds the books on every start:

```
open a new engine session (empty books)
read committed progress per orders partition
consume orders from offset 0
  offset < committed  -> submit, discard resulting trades   (replay)
  offset >= committed -> submit, publish trades, commit     (live)
```

Trade ids come from the order's log position: `<symbol>-<partition>-<offset>-<fill n>`.
The engine is deterministic and receives the same orders in the same order, so a replay
produces the same fills with the same ids.

| Crash point | Effect |
|---|---|
| Before publishing a batch's trades | Orders reprocessed live after restart, trades published once |
| After publishing, before committing progress | Trades republished with the same ids; settlement dedupes on trade_id |
| Engine restarts or evicts the session | Submit returns `NOT_FOUND`; adapter exits, restarts, replays |

Engine ids (dense integers the engine needs for O(1) lookup) are assigned by the adapter
per book, in log order, and the account behind each resting order is tracked in Go.
Self-matches are not published: buyer and seller are the same account, so the trade
nets to zero.

## Tradeoffs

- **Startup time grows with history.** Every order since the beginning is resubmitted.
  Matching itself is cheap; replay time is dominated by reading the log and the gRPC
  batches, and gets measured in Phase 3. Snapshots (book state + offsets, written
  periodically) would bound it; future work.
- **Book capacity is lifetime, not live.** `max_order_id` caps orders ever submitted to a
  book, because ids are never reused. Snapshots with id compaction would lift this.
- **Symbol config is part of the state.** Changing a tick size or reference price changes
  which orders are accepted and how they match, so a replay would produce different
  trades under the same ids. Changing it needs a new epoch (new orders topic or a reset
  procedure), not a restart.
- **Partitions are fixed at start.** Adding orders partitions needs an adapter restart.

## Alternatives considered

- **Persist the book in the engine** (write-ahead log or snapshots in C++): more code in
  the hot path, and the log already exists in Kafka.
- **Kafka transactions (exactly-once consume-produce)**: would make publish + progress
  atomic, but replay is still needed to rebuild memory, and dedupe already covers the
  republish window.
