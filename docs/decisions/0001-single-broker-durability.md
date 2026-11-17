# 0001: Durability with a single Kafka broker

Status: accepted, revisit in Phase 3 (benchmarks) and Phase 4 (deploy).

## Context

CLAUDE.md requires Kafka `acks=all`. That setting means "every in-sync replica has the record before the producer gets an ack". Its strength depends on how many replicas exist.

Locally and on the Oracle Always Free VM (2 cores, 12 GB) we run **one** broker, so every topic has replication factor 1 and `min.insync.replicas=1`.

## What acks=all actually guarantees here

Kafka acknowledges a write once it is in the broker's log, which on Linux means the OS page cache. It does not fsync each record by default; it relies on replication for durability.

| Failure | 1 broker, acks=all | 3 brokers, RF=3, min.insync=2 |
|---|---|---|
| Broker process crash (OOM, kill -9) | Safe. Page cache belongs to the OS and is flushed later. | Safe |
| Broker restart / redeploy | Safe | Safe |
| Host power loss or kernel panic | **Recent acked records can be lost** (not yet flushed to disk) | Safe while 2 of 3 hosts survive |
| Disk loss | **Topic data lost** | Safe |

## Decision

Run a single broker with `acks=all`, replication factor 1, and document the gap instead of hiding it.

The Postgres ledger stays fully durable (`synchronous_commit=on`, `fsync=on`). Once a trade is settled, it is safe. The exposure is trades that were acked by Kafka but not yet settled when the host loses power.

## Alternatives considered

- **`flush.messages=1`** (fsync every record): closes the power-loss gap on one broker, at a large throughput cost. Worth measuring in Phase 3 as one benchmark variable, not as the default.
- **Three brokers on one VM**: protects against a broker process dying, but not against the host dying, which is the actual gap. Costs memory we do not have on the free tier.
- **Three brokers on three hosts**: the real answer in production. Out of budget for this project.

## Consequences

- Resume and README claims say "single broker" next to any durability or throughput number.
- Chaos tests (Phase 3) cover broker restart, which this setup does survive, and do not claim to survive host power loss.
- `cmd/topics` takes `CLEARLINE_KAFKA_REPLICATION`, so a multi-broker deploy only changes configuration.
