# Observability

Every service exposes Prometheus metrics on `/metrics`. Prometheus (compose, http://localhost:9090) scrapes them every 5 s and evaluates the alert rules in `deploy/prometheus/alerts.yml`.

| Service | Metrics endpoint |
|---|---|
| gateway | `:8080/metrics` |
| matcher-adapter | `:9101/metrics` (`CLEARLINE_METRICS_ADDR`) |
| settlement | `:9102/metrics` (`CLEARLINE_METRICS_ADDR`) |
| outbox-relay | `:9103/metrics` (`CLEARLINE_METRICS_ADDR`) |
| balance | `:8081/metrics` |
| reconciler | `:8082/metrics` |

## The SPEC 7.1 latencies

Each is measured inside the pipeline, from a timestamp carried in Kafka headers, so no extra probe traffic is needed:

```
order acknowledged   POST /orders received ................ 202 sent
                     clearline_http_request_duration_seconds{service="gateway",route="POST /orders"}
                     (of which Kafka acks=all: clearline_gateway_produce_duration_seconds)

order to ledger      gateway produce ──(order record timestamp)──> adapter copies it to the
                     trade's clearline-order-time header ──> settlement commit
                     clearline_settlement_order_to_ledger_seconds

balance freshness    outbox row written ──(created_at)──> relay sets clearline-commit-time
                     ──> balance service applies to Valkey
                     clearline_balance_freshness_seconds
```

Caveats, stated wherever these numbers are reported:

- **Clocks.** Both ends of each measurement read the same host's clock in compose and on the single-node deploy. Across hosts it would include clock skew; negative values are dropped.
- **Freshness starts early.** It starts at the outbox row's `created_at`, which is the start of settlement's transaction, not its commit. That overstates freshness by about one transaction.
- **Bucket precision.** Quantiles come from histogram buckets (`metrics.LatencyBuckets`: 0.5 ms to 10 s, 20 exponential steps, each about 1.7x the last). A p99 is accurate to within one bucket.

## Other metrics

| Metric | What |
|---|---|
| `clearline_kafka_consumer_lag_records{group,topic,partition}` | End offset minus committed offset, read from Kafka at scrape time |
| `clearline_settlement_trades_total{result}` | posted, duplicate, dead_lettered |
| `clearline_settlement_settle_duration_seconds` | One Settle transaction, including the commit fsync |
| `clearline_matcher_submit_duration_seconds` | gRPC round trip to the C++ engine, per batch |
| `clearline_matcher_orders_total{result}` | live, replayed, rejected, duplicate |
| `clearline_outbox_unpublished_rows` | Relay backlog |
| `clearline_outbox_batch_duration_seconds` | Claim, produce with acks=all, mark, commit |
| `clearline_balance_apply_duration_seconds` | Valkey WATCH/MULTI apply |
| `clearline_gateway_orders_total{result}` | accepted, replayed, in_progress, rejected, unavailable |
| `clearline_reconciler_state{state}`, `clearline_reconciler_findings{kind}` | The latest verdict |
| Go runtime and process metrics | GC pauses, goroutines, memory, CPU |

Names follow Prometheus conventions: base units, `_total` on counters, checked by promlint in tests.

## Alerts (SPEC 9)

| Alert | Fires when |
|---|---|
| ReconcilerMismatch | Any invariant is broken (immediately) |
| ReconcilerStale | No finished check for 60 s |
| ConsumerLagGrowing | A group is more than 1000 records behind and still falling behind, for 2 min |
| TradesDeadLettered | Any trade went to the DLQ in the last 10 min |
| OutboxBacklog | More than 1000 unpublished outbox rows for 1 min |
| ServiceDown | A scrape target has been down for 30 s |

Each rule has a unit test in `deploy/prometheus/alerts_test.yml`; run them with `make prom-check`, which CI also runs.

## Useful queries

```promql
# order-to-ledger p50 / p99 over the last minute
histogram_quantile(0.5,  sum by (le) (rate(clearline_settlement_order_to_ledger_seconds_bucket[1m])))
histogram_quantile(0.99, sum by (le) (rate(clearline_settlement_order_to_ledger_seconds_bucket[1m])))

# settled trades per second
sum(rate(clearline_settlement_trades_total{result="posted"}[1m]))

# total lag per consumer group
sum by (group) (clearline_kafka_consumer_lag_records)
```
