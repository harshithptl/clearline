// Command matcher-adapter feeds the orders topic through the C++ engine and
// publishes trades (SPEC 3.2). On any engine or Kafka error it exits non-zero;
// run it under a supervisor that restarts it (compose, Kubernetes). Restart is
// the recovery path: it replays the orders log into a fresh engine session.
//
// Environment:
//
//	CLEARLINE_KAFKA_BROKERS   comma separated (default localhost:9092)
//	CLEARLINE_MATCHER_ADDR    engine gRPC address (default localhost:50051)
//	CLEARLINE_SYMBOLS         SYMBOL:QUOTE:TICK:REFERENCE,... (default AAPL, MSFT, TSLA, NVDA in USD)
//	CLEARLINE_ADAPTER_GROUP   where progress is stored (default matcher-adapter)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	matcherv1 "github.com/harshithptl/clearline/gen/matcher/v1"
	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/matcher"
	"github.com/harshithptl/clearline/internal/metrics"
	"github.com/harshithptl/clearline/internal/topics"
)

func main() {
	if err := run(); err != nil {
		slog.Error("matcher-adapter exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	symbols, err := matcher.ParseSymbols(env.String("CLEARLINE_SYMBOLS", matcher.DefaultSymbols))
	if err != nil {
		return err
	}
	addr := env.String("CLEARLINE_MATCHER_ADDR", "localhost:50051")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("engine %s: %w", addr, err)
	}
	defer conn.Close()

	group := env.String("CLEARLINE_ADAPTER_GROUP", "matcher-adapter")
	a, err := matcher.New(matcher.Config{
		Engine:      matcherv1.NewMatcherClient(conn),
		Brokers:     env.Brokers(),
		OrdersTopic: topics.Orders,
		TradesTopic: topics.Trades,
		Group:       group,
		Symbols:     symbols,
	})
	if err != nil {
		return err
	}

	// The adapter commits progress to group without joining it; lag is
	// still end offset minus committed offset.
	admCl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers()...))
	if err != nil {
		return fmt.Errorf("kafka admin: %w", err)
	}
	defer admCl.Close()
	reg := metrics.NewRegistry()
	a.Instrument(reg)
	reg.MustRegister(metrics.NewLagCollector(kadm.NewClient(admCl), group))
	go metrics.Serve(ctx, env.String("CLEARLINE_METRICS_ADDR", ":9101"), reg)
	err = a.Run(ctx)
	s := a.Stats()
	slog.Info("matcher-adapter stopped", "orders", s.Orders.Load(), "replayed", s.Replayed.Load(),
		"trades", s.Trades.Load(), "rejected", s.Rejected.Load(), "self_trades", s.SelfTrades.Load())
	return err
}
