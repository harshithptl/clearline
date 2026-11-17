# Clearline

An event-driven trade settlement and double-entry ledger engine. Go services connected by Kafka, a Postgres ledger, a Redis balance cache, and a C++ matching engine called over gRPC.

The matching engine is the fast part: it matches an order in microseconds. The hard part is everything after a match. Every trade has to be posted to the ledger **exactly once**, never lost, with the books provably balanced, even when messages are redelivered, processes crash mid-write, or brokers restart. Clearline is built around that problem.

> **Status:** in progress. Phase 1 (Postgres ledger, settlement, dedupe, invariant tests) is complete. Phase 2 (Kafka pipeline) is next. See [Roadmap](#roadmap). No performance numbers are published yet; every number will come with hardware, settings, and a script to reproduce it.

## How a trade flows

```mermaid
flowchart LR
    C[Client] -->|REST + idempotency key| G[Order gateway]
    G --> KO{{Kafka: orders}}
    KO --> A[Matcher adapter]
    A <-->|gRPC| M[C++ matching engine]
    A --> KT{{Kafka: trades}}
    KT --> S[Settlement]
    S --> PG[(Postgres:<br/>ledger + outbox)]
    PG --> OR[Outbox relay]
    OR --> KL{{Kafka: ledger-events}}
    KL --> B[Balance service]
    B <--> R[(Redis)]
    PG --> REC[Reconciler]
```

Example: Alice buys 10 AAPL at $150 from Bob. The matcher emits trade `T42`, and settlement writes four ledger rows in one transaction:

| account | asset | amount |
|---|---|---|
| Alice | USD | -1,500 |
| Bob | USD | +1,500 |
| Bob | AAPL | -10 |
| Alice | AAPL | +10 |

Every asset nets to zero. A background reconciler continuously checks that this holds for the entire ledger.

## How correctness is guaranteed

Kafka delivers messages **at least once**, so a trade can arrive twice. Clearline turns that into an exactly-once *effect*:

1. **Dedupe on `event_id`.** Settlement inserts the trade's ID into `processed_events` (primary key) in the same transaction as the ledger rows. A duplicate hits the key and is skipped.
2. **Ack after commit.** The Kafka offset is acknowledged only after the database commit.
3. **Transactional outbox.** Settlement never publishes to Kafka directly. It writes the downstream event to an `outbox` table in the same transaction, and a relay publishes it afterward. The ledger write and the event are all or nothing.
4. **One event per account, not per trade.** `ledger-events` is keyed by account so each account's updates stay in order with one consumer. A trade touches two accounts, so settlement writes two events (`T42:alice`, `T42:bob`), both in the same transaction.

What happens if settlement crashes:

| Crash point | Database | Kafka redelivers? | Result |
|---|---|---|---|
| Before commit | Rolled back | Yes | Posted once on retry |
| After commit, before ack | Committed | Yes | Dedupe skips it |
| After ack | Committed | No | Done |

Other guarantees, enforced by the database rather than application code:

- **Append-only ledger.** Triggers reject `UPDATE`, `DELETE`, and `TRUNCATE` on `ledger_entries`. Mistakes are corrected with reversing entries.
- **Exact money.** Amounts are `NUMERIC(20,8)`, never floating point.
- **Durability is never traded for speed.** Kafka `acks=all`, Postgres `synchronous_commit=on`. Throughput comes from batching.

### Invariants

Checked by tests now and by the reconciler later:

1. For every asset, the sum of all ledger amounts is 0.
2. For every trade, its ledger rows net to 0 per asset.
3. Every cached Redis balance equals the sum of that account's ledger rows.
4. Every `event_id` is processed at most once.
5. Every settled trade has exactly one balance event per account it touched, and no others.

## Running locally

Requirements: Go 1.27+, Docker.

```sh
# start Postgres, Kafka (KRaft, single broker) and Valkey
docker compose -f deploy/compose/docker-compose.yml up -d --wait

# apply schema migrations and create Kafka topics (both idempotent)
go run ./cmd/migrate
go run ./cmd/topics

# run all tests (each test gets its own throwaway database and Kafka topics)
go test ./... -race

# run the settlement harness with a different random seed
CLEARLINE_HARNESS_SEED=7 go test ./internal/settlement -run Harness -v
```

Tests connect to `postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable` by default. Override with `CLEARLINE_TEST_DATABASE_URL`.

## Repository layout

```
docs/SPEC.md          full design spec: requirements, architecture, failure handling, targets
cmd/migrate/          applies pending migrations (goose)
cmd/topics/           creates Kafka topics (SPEC 3.3)
migrations/           SQL schema migrations, embedded into Go, applied by goose
internal/pgtest/      test helper: fresh migrated Postgres database per test
internal/kafkatest/   test helper: unique Kafka topics and consumer groups per test
internal/topics/      topic names, partition counts, idempotent creation
internal/ledger/      trade -> double-entry ledger rows
internal/ledger/invariants/  SQL checks for the SPEC 4.2 ledger invariants
internal/settlement/  Settle(): one transaction per trade, dedupe + ledger rows + outbox
deploy/compose/       local Docker Compose stack: Postgres, Kafka, Valkey
docs/decisions/       short design-decision notes
```

Planned: `services/` (gateway, matcher-adapter, settlement, outbox-relay, balance, reconciler), `proto/`, `engine/` (C++ matcher + gRPC server), `deploy/helm/`, `deploy/terraform/`, `loadtest/`, `chaos/`.

## Roadmap

Each phase is done when its exit criteria pass, not on a fixed schedule.

| Phase | Scope | Done when |
|---|---|---|
| 1 | Postgres ledger, settlement logic, dedupe, invariant tests (trades fed by a test harness) | Invariant tests pass; duplicate trades never double-post |
| 2 | Kafka topics, outbox relay, retries + dead-letter queue, gRPC matcher adapter, gateway with Redis idempotency | One order flows end to end into the ledger |
| 3 | OpenTelemetry tracing, Prometheus, Grafana, load tests, chaos tests | Benchmark report with latency/throughput graphs and chaos results |
| 4 | Helm, kind, Terraform + k3s on a free-tier Arm VM | Live read-only demo |
| 5 (optional) | AI agent that answers "why is this balance off?" using ledger and reconciler tools | Agent with tool calls and a small eval set |

Progress:

- [x] Ledger schema: append-only triggers, constraints, dedupe and outbox tables
- [x] Invariant checker (invariants 1, 2, 4, 5)
- [x] Ledger entry generation (exact decimals, no silent rounding)
- [x] Settlement transaction with dedupe
- [x] Randomized duplicate-delivery test harness (concurrent workers, checked against an independent oracle)
- [x] Phase 2 infra: Kafka (KRaft) + Valkey in compose, goose migrations, topic creation
- [ ] Settlement service (Kafka consumer, retries, DLQ)
- [ ] Outbox relay
- [ ] Balance service + reconciler
- [ ] C++ engine over gRPC + matcher adapter
- [ ] Gateway
- [ ] End-to-end: one order flows into the ledger

## Design

The full spec lives in [docs/SPEC.md](docs/SPEC.md): requirements, architecture, Kafka topic keys, failure handling, data model, and latency targets. Short design-decision notes will go in `docs/decisions/`.
