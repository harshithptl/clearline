# 0004: Dashboard architecture

Status: accepted.

## Context

SPEC section 1 lists TypeScript as a gap, and Phase 4 needs a public, read-only view of the live system. The dashboard needs live balances, settled trades, and the reconciler's verdict. Locally it should also place orders, to demonstrate the gateway's idempotency.

## Decisions

| Choice | Picked | Over | Why |
|---|---|---|---|
| Live updates | Server-Sent Events from the balance service | WebSockets, polling | One-way push over plain HTTP. The browser reconnects by itself, and nginx proxies it with buffering off. Nothing flows from the browser to the server, so a two-way socket buys nothing. |
| Stream semantics | Signal to refetch | Apply deltas in the browser | Valkey stays the only source of balances. A dropped event or a reconnect costs a refetch, never a wrong number. |
| Slow clients | Dropped by the hub (`internal/balance/hub.go`) | Blocking the consumer | One stuck tab must not add consumer lag for everyone. A dropped client reconnects and refetches. |
| API access | Same-origin `/api/*` proxy (nginx; Vite in dev) | CORS on each service | No cross-origin policy to maintain or get wrong, and the public demo can remove the write route in one place. |
| Money | Decimal strings to BigInt fixed point | JS numbers | Floats cannot represent most decimal amounts exactly, and the ledger never rounds. |
| UI | Radix primitives + Tailwind v4 + `cva` variants (shadcn/ui pattern), owned in `src/components/ui` | A component kit as a dependency | Accessible behavior comes from Radix, styling stays in our code, and design tokens live in one CSS file. |
| Data fetching | TanStack Query | Hand-rolled `useEffect` fetches | Caching, refetch intervals, invalidation on stream events, and error states without bespoke code. |

## Tradeoffs

- **The trade tape starts empty.** It holds only trades settled since the page opened, because it's built from the stream and no history endpoint exists yet.
- **One balance instance.** The hub fans out events consumed by this one balance service instance. With several instances, each stream would see only its own partitions. Running several replicas would need a shared fan-out, such as Valkey pub/sub. That's out of scope while one replica handles the load.
- **`GET /balances` lists at most 500 accounts**, found with SCAN. That's fine for a dashboard, but it isn't an API for large account sets.
