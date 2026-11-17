#!/usr/bin/env bash
# Regenerates Go code from proto/ into gen/, inside Docker so no protoc
# install is needed on the host. Generated code is committed.
#
#   scripts/gen-proto.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROTOC_GEN_GO=v1.36.12
PROTOC_GEN_GO_GRPC=v1.6.2

docker run --rm -v "$PWD":/work -w /work golang:1.27.3-trixie bash -c "
  set -euo pipefail
  apt-get update -qq >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends protobuf-compiler >/dev/null
  go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO}
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC}
  protoc -I proto \
    --go_out=gen --go_opt=paths=source_relative \
    --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
    proto/matcher/v1/matcher.proto
  chown -R $(id -u):$(id -g) gen
"
echo "generated:"
find gen -name '*.go' | sort
