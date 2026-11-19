// Command settlement consumes the trades topic and posts each trade to the
// ledger exactly once (SPEC 3.2).
//
// Environment:
//
//	CLEARLINE_DATABASE_URL       Postgres (default: local compose)
//	CLEARLINE_KAFKA_BROKERS      comma separated (default localhost:9092)
//	CLEARLINE_SETTLEMENT_GROUP   consumer group (default settlement)
//	CLEARLINE_MAX_ATTEMPTS       retries before DLQ for message errors (default 5)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/metrics"
	"github.com/harshithptl/clearline/internal/settlement"
	"github.com/harshithptl/clearline/internal/topics"
)

func main() {
	if err := run(); err != nil {
		slog.Error("settlement exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	maxAttempts, err := env.Int("CLEARLINE_MAX_ATTEMPTS", 5)
	if err != nil {
		return err
	}
	group := env.String("CLEARLINE_SETTLEMENT_GROUP", "settlement")

	pool, err := pgxpool.New(ctx, env.DatabaseURL())
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	opts := append([]kgo.Opt{kgo.SeedBrokers(env.Brokers()...)},
		settlement.ConsumerClientOpts(group, topics.Trades)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer cl.Close()

	c, err := settlement.NewConsumer(settlement.ConsumerConfig{
		Client:      cl,
		DB:          pool,
		DLQTopic:    topics.TradesDLQ,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		return err
	}
	reg := metrics.NewRegistry()
	c.Instrument(reg)
	reg.MustRegister(metrics.NewLagCollector(kadm.NewClient(cl), group))
	go metrics.Serve(ctx, env.String("CLEARLINE_METRICS_ADDR", ":9102"), reg)

	slog.Info("settlement running", "group", group, "topic", topics.Trades)
	err = c.Run(ctx)
	s := c.Stats()
	slog.Info("settlement stopped", "posted", s.Posted.Load(), "duplicates", s.Duplicates.Load(),
		"dead_lettered", s.DeadLettered.Load(), "retries", s.Retries.Load())
	return err
}
