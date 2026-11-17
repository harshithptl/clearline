// Command dlq-replay republishes dead-lettered trades to their original topic
// after the cause is fixed (SPEC F7), then exits.
//
//	go run ./cmd/dlq-replay
//
// Progress is tracked by a consumer group (CLEARLINE_DLQ_REPLAY_GROUP,
// default dlq-replay), so each dead-lettered record is replayed once per
// group. Replaying something that already settled is harmless: settlement
// dedupes on trade_id.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/dlq"
	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/topics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dlq-replay:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	group := env.String("CLEARLINE_DLQ_REPLAY_GROUP", "dlq-replay")
	opts := append([]kgo.Opt{kgo.SeedBrokers(env.Brokers()...)}, dlq.ReplayClientOpts(topics.TradesDLQ, group)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	n, err := dlq.Replay(ctx, cl, topics.TradesDLQ, group)
	fmt.Printf("replayed %d records from %s\n", n, topics.TradesDLQ)
	return err
}
