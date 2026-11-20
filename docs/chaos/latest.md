# Chaos run 2026-11-19 22:38 UTC

Commit `db5d6ca`. Load: 100 crossing pairs/s (200 orders/s) for 20s per scenario; fault at +5s, recovered after 5s. Clients retry each order with the same Idempotency-Key until 202.

| Scenario | SPEC 5 row | Pairs accepted | Trades in ledger | Client retries | Processed twice | Unpublished outbox | Reconciler | Recovery | Result |
|---|---|---|---|---|---|---|---|---|---|
| settlement killed mid-stream | settlement crashes before/after commit | 2000 | 2000 | 0 | 0 | 0 | clean | 39s | pass |
| outbox relay killed mid-publish | outbox relay crashes mid-publish | 2000 | 2000 | 0 | 0 | 0 | clean | 8s | pass |
| balance service killed | consumer crash (balance cache) | 2000 | 2000 | 0 | 0 | 0 | clean | 39s | pass |
| matcher adapter killed | consumer crash (engine rebuilt by replay) | 2000 | 2000 | 0 | 0 | 0 | clean | 9s | pass |
| gateway killed | client retries during gateway outage | 2000 | 2000 | 3746 | 0 | 0 | clean | 9s | pass |
| Kafka broker restarted | Kafka broker restart | 2000 | 2000 | 1980 | 0 | 0 | clean | 9s | pass |
| Valkey restarted | cache restart (balances and idempotency keys) | 2000 | 2000 | 20 | 0 | 0 | clean | 11s | pass |
| duplicate trades injected into Kafka | duplicate trade published upstream | 500 | 0 | 0 | 0 | 0 | clean | - | pass |

For the duplicate row, "Pairs accepted" is the number of trade records re-produced, and "Trades in ledger" is the new trades they caused (must be 0).
