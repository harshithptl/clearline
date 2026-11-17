// Command outbox-relay publishes outbox rows to the ledger-events topic
// (SPEC 3.2). Run more than one for failover: only one is active at a time.
//
// Environment:
//
//	CLEARLINE_DATABASE_URL   Postgres (default: local compose)
//	CLEARLINE_KAFKA_BROKERS  comma separated (default localhost:9092)
//	CLEARLINE_RELAY_BATCH    rows per transaction (default 500)
//	CLEARLINE_RELAY_POLL     wait after an empty batch (default 10ms)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		slog.Error("outbox-relay exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	batch, err := env.Int("CLEARLINE_RELAY_BATCH", 500)
	if err != nil {
		return err
	}
	poll, err := env.Duration("CLEARLINE_RELAY_POLL", 10*time.Millisecond)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, env.DatabaseURL())
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers()...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer cl.Close()

	r, err := outbox.New(outbox.Config{DB: pool, Client: cl, BatchSize: batch, PollInterval: poll})
	if err != nil {
		return err
	}
	slog.Info("outbox-relay running")
	err = r.Run(ctx)
	slog.Info("outbox-relay stopped", "published", r.Stats().Published.Load())
	return err
}
