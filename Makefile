COMPOSE  := docker compose -f deploy/compose/docker-compose.yml
GOLANGCI := v2.15.0

.DEFAULT_GOAL := help
.PHONY: help up down reset migrate topics init test lint vuln engine-test proto check

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

up: ## start Postgres, Kafka, Valkey and the C++ engine
	$(COMPOSE) up -d --wait --build

down: ## stop the stack (data kept)
	$(COMPOSE) down

reset: ## stop the stack and delete its data volumes
	$(COMPOSE) down -v

migrate: ## apply database migrations
	go run ./cmd/migrate

topics: ## create Kafka topics
	go run ./cmd/topics

init: up migrate topics ## start the stack and prepare it

test: ## Go tests with the race detector (needs make init)
	go test ./... -race -count=1

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

check: lint vuln test engine-test ## everything CI runs
