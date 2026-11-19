COMPOSE  := docker compose -f deploy/compose/docker-compose.yml
GOLANGCI := v2.15.0

.DEFAULT_GOAL := help
.PHONY: help up up-all down reset migrate topics init test e2e lint vuln engine-test proto check web-install web-dev web-check web-e2e

help: ## list targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

up: ## start Postgres, Kafka, Valkey and the C++ engine
	$(COMPOSE) up -d --wait --build

up-all: ## infrastructure plus every Clearline service (gateway :8080, balance :8081)
	$(COMPOSE) --profile app up -d --wait --build

down: ## stop the stack, services included (data kept)
	$(COMPOSE) --profile app down

reset: ## stop the stack and delete its data volumes
	$(COMPOSE) --profile app down -v

migrate: ## apply database migrations
	go run ./cmd/migrate

topics: ## create Kafka topics
	go run ./cmd/topics

init: up migrate topics ## start the stack and prepare it

test: ## Go tests with the race detector (needs make init)
	go test ./... -race -count=1

e2e: ## end-to-end test against the running stack (needs make up-all)
	go test -tags e2e -count=1 -v ./test/e2e

lint: ## gofmt, go vet, golangci-lint
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...
	docker run --rm -v "$$PWD":/app -w /app golangci/golangci-lint:$(GOLANGCI) golangci-lint run ./...

vuln: ## known-vulnerability scan
	go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...

engine-test: ## build the C++ engine and run its gtest suite
	docker build -f engine/Dockerfile --target test .

proto: ## regenerate Go gRPC code into gen/
	scripts/gen-proto.sh

web-install: ## install dashboard dependencies (web/, Node 24)
	cd web && npm ci --no-audit --no-fund

web-dev: ## dashboard dev server on :5173 with hot reload (proxies to make up-all services)
	cd web && npm run dev

web-check: ## dashboard typecheck, lint, format, unit tests, build
	cd web && npm run typecheck && npm run lint && npm run format:check && npm test && npm run build

web-e2e: ## Playwright browser test against the running stack (needs make up-all)
	cd web && npx playwright test

check: lint vuln test engine-test web-check ## everything CI runs except e2e (make up-all && make e2e web-e2e)
