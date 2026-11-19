# engine

The C++ matching engine, served over gRPC.

`include/llb/` and `src/` are copied from
[LowLatencyBuffer](https://github.com/harshithptl/LowLatencyBuffer) at commit
[`1c5ad00`](https://github.com/harshithptl/LowLatencyBuffer/commit/1c5ad00)
("feat: matching engine on the consumer thread"), originally on its `spsc_buffer` branch.

Its README has the benchmark: 5.2 µs P50 submit-to-book through an SPSC ring and
1.1 to 1.6 µs per book operation, on an Apple M4 under macOS with `-O3 -ffast-math`.
Those numbers do not carry over here. This build uses different flags (below), runs in
Docker on Linux, and sits behind gRPC, so Clearline measures its own in Phase 3.

Changes from the original:

| Change | Why |
|---|---|
| `submit(Order*, std::vector<Fill>*)` reports each match (maker id, price, quantity) | The original returned only a fill count; trades need counterparties |
| `accepts_price()`, `is_live()` | The original silently recycles out-of-range orders; the server needs to report rejections |
| Portable `cpu_relax()` instead of inline ARM `yield` | `yield` does not assemble on x86 |
| Built with `-O2`, no `-ffast-math`, no `-march=native` | Replay needs repeatable arithmetic; the image must run on any CPU of its architecture |
| Comments reworded where they used em dashes | Repo style |

The matching logic (price-time priority, intrusive FIFO levels, slab allocation) is unchanged.

## Server

`server/book.*` wraps one `llb::OrderBook` with its slab and recycle ring, single-threaded,
and validates what the engine trusts (id order, quantity, price range). Prices are integer
ticks: the book runs with tick size 1.0 and a reference price in ticks, so every price the
engine sees is an exact whole number.

`server/matcher_service.*` implements `proto/matcher/v1/matcher.proto`. State lives in
sessions (isolated sets of books). At the session limit, the least recently used one is
evicted; its owner gets `NOT_FOUND` and rebuilds by replay
(`docs/decisions/0002-matcher-replay.md`). One mutex guards everything.

C++ style: no lambdas; named functions and explicit loops.

## Build and test

From the repository root:

```sh
docker build -f engine/Dockerfile --target test .          # compile + run gtest suite
docker compose -f deploy/compose/docker-compose.yml up -d --wait --build engine
```

Debian trixie packages: gRPC 1.51, protobuf 3.21, GoogleTest 1.16, GCC 14, C++20.
