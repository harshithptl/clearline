# One image with every Go binary (services and tools); compose and Kubernetes
# pick the binary with the container command. One build instead of nine.
#
# Build context: repository root.
#   docker build -f deploy/docker/services.Dockerfile -t clearline:dev .
#   docker buildx build --platform linux/amd64,linux/arm64 -f deploy/docker/services.Dockerfile .
#
# Cross-compiles on the build machine's native platform (pure Go, CGO off),
# so a multi-arch build does not run the compiler under emulation.

FROM --platform=$BUILDPLATFORM golang:1.27.4-trixie AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/ \
      ./services/gateway ./services/matcher-adapter ./services/settlement \
      ./services/outbox-relay ./services/balance ./services/reconciler \
      ./cmd/migrate ./cmd/topics ./cmd/dlq-replay ./cmd/healthcheck

# distroless static: no shell or package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
