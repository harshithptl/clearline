# Clearline

Event-driven trade settlement and ledger engine. Go services, Kafka, Postgres, Redis, plus an existing C++ matching engine called over gRPC. Full spec is imported below.

@docs/SPEC.md

## Current status
- Planning done. Next: Weekend 1 (settlement service, Postgres ledger, dedupe, invariant tests). See SPEC section 11.
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
- Run stack locally: `docker compose -f deploy/compose/docker-compose.yml up -d`
- Run Go tests: `go test ./...`
- Load test: TBD
- Chaos tests: TBD
- Local database URL: postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable
