# Clearline

Event-driven trade settlement and ledger engine. Go services, Kafka, Postgres, Redis, plus an existing C++ matching engine called over gRPC. Full spec is imported below.

@docs/SPEC.md

## Current status
- Phase 1 done (schema, invariants, BuildEntries, Settle, duplicate-delivery harness). Phase 2 done (Kafka pipeline, outbox relay, balance, reconciler, C++ engine + adapter, gateway, compose stack, e2e test in CI). CI + repo standards done. Dashboard (web/) done. Phase 3 in progress: Prometheus metrics + alerts done (docs/observability.md). Load generator done. Clean benchmark ladder in docs/benchmarks: baseline 100 trades/s sustained; + balance batching 731; + settlement batching 2000 (highest offered). Chaos tests done (8 SPEC 5 scenarios pass). Next: Helm + kind in CI, repeated benchmark runs, then OpenTelemetry and Grafana. See SPEC section 11.
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
- Frontend: TypeScript strict; build feature UI only from `web/src/components/ui` primitives and the tokens in `web/src/index.css`; money is decimal strings / `lib/decimal.ts`, never JS numbers.
- Never use em dashes in generated text, docs, or comments.
- Work in small increments: propose the plan, then implement one piece with tests.
- If you are unsure about a library API or version, say so and check rather than guess.
- Always warn before anything that could cost money (cloud resources, paid CI runners, paid APIs, domains, anything past a free tier): say what is charged, the expected amount, and how to avoid it, then wait for an explicit OK. Budget is $0.

## Commands
(Fill in as they exist.) `make help` lists shortcuts; `make check` runs what CI runs (.github/workflows/ci.yml).
- Run stack locally: `docker compose -f deploy/compose/docker-compose.yml up -d --wait --build` (Postgres, Kafka KRaft, Valkey, C++ engine on :50051)
- C++ engine tests: `docker build -f engine/Dockerfile --target test .`
- Regenerate Go gRPC code: `scripts/gen-proto.sh` (protoc in Docker; output in `gen/`, committed)
- Apply migrations: `go run ./cmd/migrate` (goose, idempotent)
- Create Kafka topics: `go run ./cmd/topics` (idempotent)
- Run services: `go run ./services/{gateway,matcher-adapter,settlement,outbox-relay,balance,reconciler}` (env `CLEARLINE_DATABASE_URL`, `CLEARLINE_KAFKA_BROKERS`, `CLEARLINE_REDIS_ADDR`); gateway POST /orders on :8080 (needs `Idempotency-Key` header); balance API on :8081; `reconciler -once` exits 0 clean / 1 mismatch / 2 lagging
- Replay DLQ: `go run ./cmd/dlq-replay`
- Run Go tests: `go test ./... -race` (needs compose up; each test gets its own database via `internal/pgtest` and its own topics via `internal/kafkatest`; its own Valkey key prefix via `internal/redistest`; override with `CLEARLINE_TEST_DATABASE_URL`, `CLEARLINE_TEST_KAFKA_BROKERS`, `CLEARLINE_TEST_REDIS_ADDR`, `CLEARLINE_TEST_MATCHER_ADDR`; matcher tests use the compose engine)
- Lint: `make lint` (gofmt, go vet, golangci-lint v2 in Docker, config .golangci.yml). Vulns: `make vuln`
- Full stack in containers: `make up-all` (compose profile `app`, image from deploy/docker/services.Dockerfile); end-to-end: `make e2e` (test/e2e, build tag e2e)
- Dashboard: `make web-install` once, then `make web-dev` (:5173) or `make up-all` (:3000); `make web-check` (tsc, eslint, prettier, vitest, build); `make web-e2e` (Playwright vs stack). Node 24, npm; generate lockfile changes with npm 11+ (npm 10 has a peer-resolution bug)
- Metrics: every service serves /metrics (adapter :9101, settlement :9102, relay :9103, others on their API port); Prometheus at :9090 in `make up-all`; `make prom-check` validates config and unit-tests alert rules
- Load test: `make bench HARDWARE="..."` (needs `make up-all`); `go run ./cmd/loadgen -replot <json>` re-judges and redraws a saved run
- Chaos tests: `make chaos` (resets the stack; test/chaos, build tag chaos; report in docs/chaos/latest.md); CI workflow chaos.yml on main + manual
- Local database URL: postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable
- Local Kafka: localhost:9092 from the host, kafka:19092 from containers. Local Valkey: localhost:6379
