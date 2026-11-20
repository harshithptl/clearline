# Helm chart: `deploy/helm/clearline`

Deploys the whole system to Kubernetes:

- the six Go services and the C++ engine
- the dashboard
- Prometheus, with the alert rules from `deploy/prometheus`
- by default, in-cluster Postgres, Kafka (single KRaft broker) and Valkey, using the same durability settings as compose

CI installs it on a throwaway **kind** cluster on every PR and runs the end-to-end test against it (`kubernetes` job).

## Quick start (kind)

```sh
make kind-e2e     # build images, create a kind cluster, install the chart, run the e2e test
kubectl port-forward svc/clearline-web 3000:80     # dashboard at http://localhost:3000
make kind-down    # delete the cluster
```

This needs `kind`, `helm` (v4) and `kubectl`. Nothing in the chart costs money: kind runs inside Docker on your machine.

## How it starts

- Every app pod runs two **init containers**, `migrate` and `topics`, before its service starts. Both are idempotent, and `migrate` holds a Postgres advisory lock, so pods starting together take turns. They retry with backoff until Postgres and Kafka are reachable, so the release converges without ordering hooks.
- The **matcher adapter** runs exactly one replica with the `Recreate` strategy. It owns the engine session and rebuilds the books by replaying the orders log ([decision 0002](../../docs/decisions/0002-matcher-replay.md)). If it exits on an engine or Kafka error, the restart is the recovery path, so a restart or two during first install is expected.
- Readiness probes use each service's `/healthz`, or `/metrics` for the consumers without an API.

## Security defaults

- Go services: non-root (uid 65532), read-only root filesystem, all Linux capabilities dropped, `RuntimeDefault` seccomp profile, no service-account token mounted.
- The engine (uid 65534), nginx (uid 101) and Prometheus run the same way. nginx gets small `emptyDir` volumes for its rendered config and temp files.
- **The default database password is for kind only.** Anywhere else, set `existingSecret` to a Secret with `DATABASE_URL` (and `POSTGRES_PASSWORD` when `infra.enabled`). The install notes warn when the default is in use.

## Values that matter

| Value | Default | Notes |
|---|---|---|
| `images.*.repository/tag` | `clearline:dev` etc. | Local images loaded with `kind load` |
| `infra.enabled` | `true` | `false` uses `external.databaseURL`, `external.kafkaBrokers` and `external.redisAddr` (all required then) |
| `settlement.replicas` | 1 | Up to the `trades` partition count (3) |
| `outboxRelay.replicas` | 1 | More instances are hot standbys behind the advisory-lock leader |
| `symbols` | as compose | The gateway and the adapter must agree |
| `prometheus.enabled` | `true` | Scrapes every service; the alert rules come from `files/alerts.yml` |

`files/alerts.yml` is a copy of `deploy/prometheus/alerts.yml` (Helm can't read files outside the chart). `make helm-sync` updates it, and `make helm-lint` and CI fail if they differ.

## Not included

- Ingress or TLS. Use port-forward, or add your own Ingress.
- Highly available Kafka, Postgres or Valkey. In production those would be managed services or operators, with `infra.enabled=false`.
