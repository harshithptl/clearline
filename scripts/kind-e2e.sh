#!/usr/bin/env bash
# Deploys the Helm chart to a kind cluster and runs the end-to-end test
# against it through port-forwards.
#
#   scripts/kind-e2e.sh            build images, create cluster if needed, install, test
#   SKIP_BUILD=1 scripts/kind-e2e.sh   use images already in the local Docker daemon (CI)
#
# Needs docker, kind, helm, kubectl and go on PATH. Leaves the cluster
# running; `make kind-down` deletes it.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${KIND_CLUSTER:-clearline}
CTX="kind-$CLUSTER"
CHART=deploy/helm/clearline
IMAGES=(clearline:dev clearline-engine:dev clearline-web:dev)

if [[ -z "${SKIP_BUILD:-}" ]]; then
  docker build -q -f deploy/docker/services.Dockerfile -t clearline:dev . >/dev/null
  docker build -q -f engine/Dockerfile --target runtime -t clearline-engine:dev . >/dev/null
  docker build -q -t clearline-web:dev web >/dev/null
fi

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --wait 120s
fi
kind load docker-image --name "$CLUSTER" "${IMAGES[@]}"

dump() {
  echo "--- pods"; kubectl --context "$CTX" get pods -o wide || true
  echo "--- events"; kubectl --context "$CTX" get events --sort-by=.lastTimestamp | tail -40 || true
  for d in gateway matcher-adapter settlement outbox-relay balance reconciler; do
    echo "--- logs $d"; kubectl --context "$CTX" logs "deploy/clearline-$d" --all-containers --tail=40 || true
  done
}

existed=$(helm --kube-context "$CTX" status clearline >/dev/null 2>&1 && echo yes || echo no)
helm --kube-context "$CTX" upgrade --install clearline "$CHART" --wait --timeout 10m || { dump; exit 1; }
if [[ $existed == yes ]]; then
  # Same image tags, new image contents: only a rollout makes pods use them.
  kubectl --context "$CTX" rollout restart deploy -l app.kubernetes.io/instance=clearline >/dev/null
  kubectl --context "$CTX" rollout status deploy -l app.kubernetes.io/instance=clearline --timeout 5m || { dump; exit 1; }
fi

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
for pf in gateway:8080 balance:8081 postgres:5432 valkey:6379 prometheus:9090; do
  svc=${pf%%:*} port=${pf##*:}
  kubectl --context "$CTX" port-forward --address 127.0.0.1 "svc/clearline-$svc" "$port:$port" >/dev/null 2>&1 &
  pids+=($!)
done
for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:8080/healthz >/dev/null && curl -sf http://127.0.0.1:9090/-/ready >/dev/null && break
  sleep 1
done

CLEARLINE_E2E_GATEWAY=http://127.0.0.1:8080 \
CLEARLINE_E2E_BALANCE=http://127.0.0.1:8081 \
CLEARLINE_E2E_PROMETHEUS=http://127.0.0.1:9090 \
CLEARLINE_DATABASE_URL="postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable" \
CLEARLINE_REDIS_ADDR=127.0.0.1:6379 \
  go test -tags e2e -count=1 -v ./test/e2e || { dump; exit 1; }
