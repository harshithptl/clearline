# Clearline

Event-driven trade settlement and ledger engine. Go services, Kafka, Postgres, Redis, plus an existing C++ matching engine called over gRPC. Full spec is imported below.

@docs/SPEC.md

## Current status
- Phase 1 done (schema, invariants, BuildEntries, Settle, duplicate-delivery harness). Next: Phase 2 (Kafka pipeline). See SPEC section 11.
- Update this line as phases complete.

## Hard rules for this codebase
- Correctness first. Never weaken durability for speed: Kafka `acks=all`, Postgres `synchronous_commit=on`.
- Ack a Kafka message only after the database commit. Every consumer dedupes on `event_id`.
- Never publish to Kafka directly from settlement. Use the transactional outbox.
- Money is `NUMERIC`, never float. Ledger is append-only; corrections are reversing entries.
- Write invariant tests (SPEC 4.2) before settlement logic.
- Every performance number must be reproducible, with hardware and settings stated.

## How I want help
- Casual, direct, concise. Technically grounded, no hedging.
- Explain any jargon the first time it appears.
- Prefer concrete dry runs and visual traces (tables, ASCII flows) over abstract explanation.
- C++: no lambdas. Use named static functions or comparators and explicit control flow.
- Never use em dashes in generated text, docs, or comments.
- Work in small increments: propose the plan, then implement one piece with tests.
- If you are unsure about a library API or version, say so and check rather than guess.

## Commands
(Fill in as they exist.)
- Run stack locally: `docker compose -f deploy/compose/docker-compose.yml up -d --wait` (Postgres, Kafka KRaft, Valkey)
- Apply migrations: `go run ./cmd/migrate` (goose, idempotent)
- Create Kafka topics: `go run ./cmd/topics` (idempotent)
- Run Go tests: `go test ./... -race` (needs compose up; each test gets its own database via `internal/pgtest` and its own topics via `internal/kafkatest`; override with `CLEARLINE_TEST_DATABASE_URL`, `CLEARLINE_TEST_KAFKA_BROKERS`)
- Load test: TBD
- Chaos tests: TBD
- Local database URL: postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable
- Local Kafka: localhost:9092 from the host, kafka:19092 from containers. Local Valkey: localhost:6379
