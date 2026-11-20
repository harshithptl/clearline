# Chaos tests (SPEC 5)

`make chaos` resets the stack, then runs `test/chaos` against it. For each scenario it streams crossing order pairs at 100 pairs/s (200 orders/s) for 20 s. At +5 s it kills or restarts one component, and brings it back 5 s later. Clients retry every order with the same `Idempotency-Key` until they get a 202, which is the contract a real client follows.

Each scenario passes only if all of these hold:

- **Nothing lost:** every accepted pair became exactly one trade.
- **Nothing doubled:** no trade has more than its 4 ledger rows.
- **Fully published:** no outbox row is left unpublished.
- **Reconciler clean, with no lag:** books balanced, and the cache equals the ledger.

Latest results: [latest.md](latest.md). Hardware: Apple M4, Docker Desktop, single Kafka broker.

## Reading the recovery column

Recovery is the time from the component coming back to every check above passing.

- **Settlement and balance take about 39 s.** A process killed with SIGKILL never leaves its Kafka consumer group, so its partitions stay assigned to the dead member until the group's session timeout expires (franz-go's default is 45 s). Only then are they reassigned and consumed. Nothing is lost in the meantime; it just waits. A shorter session timeout or static group membership would cut this, at the cost of more rebalances on brief pauses.
- **The matcher adapter recovers in about 9 s.** It doesn't use a consumer group: it rebuilds the engine by replaying the orders log and resumes from its own committed offsets.
- **The gateway and Kafka scenarios show many client retries.** Those are 503s and refused connections during the outage, retried with the same idempotency keys. No order was created twice: 2000 pairs, 2000 trades.

## Not covered

- Host power loss. With one broker, acked records can be lost if the host dies before the page cache is flushed ([decision 0001](../decisions/0001-single-broker-durability.md)).
- Postgres crash. Committed transactions are durable (`synchronous_commit=on`), but this suite doesn't kill Postgres yet.
