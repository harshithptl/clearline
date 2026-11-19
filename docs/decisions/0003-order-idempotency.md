# 0003: Order idempotency in two layers

Status: accepted.

## Context

SPEC F1: a retried `POST /orders` must never create two orders. Clients retry whenever they are unsure, for example after a timeout, a dropped connection or a 503. The gateway answers 202 only after the order is durably in Kafka (`acks=all`).

The textbook answer is a Redis `SET NX` on the idempotency key. On its own that has gaps:

| Failure | Plain SET NX |
|---|---|
| Gateway crashes after the Kafka write, before marking the key done | Pending entry expires, the retry writes the order again |
| Kafka write times out but actually landed | Key released, the retry writes the order again |
| Valkey loses the key (failover, eviction, flush) | The retry writes the order again |

## Decision

Two layers with different jobs.

**1. Valkey: response cache (fast path).** The first request claims `idem:{account}:<key>` as `pending` with `SET NX` and a short TTL. After the Kafka write it swaps the entry to `done`, storing the response for 24h, using a compare-and-set Lua script, so a request whose entry already expired cannot overwrite a newer owner. Retries get the stored 202 back with `Idempotent-Replayed: true`, or a 409 while the first request is still in flight. Reusing a key for a different order gets a 422.

**2. The log: order_id dedupe (guarantee).** `order_id = "o-" + SHA-256(account NUL key)[:16 bytes]`. Every copy of a request carries the same id, whatever happened to Valkey. The matcher adapter keeps the order ids each book has taken and drops repeats before they reach the engine.

```
 retry after any failure ──> gateway ──> same order_id ──> orders log ──> adapter: seen? drop
```

| Failure | Result |
|---|---|
| Crash while pending | Entry expires (30s), the retry writes a copy, the adapter drops it |
| Produce timeout that landed | 503, key released, the retry writes a copy, the adapter drops it |
| Valkey loses the entry | The retry writes a copy, the adapter drops it; the client still gets a 202 |
| Concurrent identical requests | One claims the key; the others get 409 or the replayed 202 |

### Why dedupe per symbol, not globally

The adapter's decisions must replay identically (decision 0002). Orders are keyed by symbol, so one symbol's orders sit on one partition in a fixed order, and "first copy wins" is the same on every replay. A set shared across symbols would depend on how partitions interleave when they are consumed, which differs between runs. Two copies of one request always share a symbol (same body), so per-symbol dedupe catches every copy.

### Why the gateway checks the account exists

An order from an unknown account would still match. Settlement would then reject the whole trade (unknown account, then the DLQ), taking the real counterparty's fill with it. The gateway checks Postgres and returns a 422 instead. It caches only positive answers: accounts are never deleted, so "exists" cannot go stale.

## Tradeoffs

- **Adapter memory:** one map entry per accepted order per book, bounded by the book's `MaxOrderID` (about 1M by default), since each entry also takes an engine id.
- **A crashed request blocks its key** with 409s for up to the pending TTL (30s). A shorter TTL lets retries through sooner but means more copies for the adapter to drop. Correctness holds either way.
- **The key is tied to one order.** The gateway changes nothing based on balance: there is no funds or risk check, and accounts can go negative. That is out of scope for Phase 2.
- **Not public:** `POST /orders` must not be exposed on the live demo (SPEC 2.2).
