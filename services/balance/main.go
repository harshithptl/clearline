// Command balance consumes ledger-events into Valkey and serves balances
// (SPEC F6):
//
//	GET /balances/{account}
//	GET /healthz
//
// Environment:
//
//	CLEARLINE_KAFKA_BROKERS   comma separated (default localhost:9092)
//	CLEARLINE_REDIS_ADDR      Valkey (default localhost:6379)
//	CLEARLINE_REDIS_PREFIX    key prefix (default clearline:)
//	CLEARLINE_BALANCE_GROUP   consumer group (default balance)
//	CLEARLINE_BALANCE_ADDR    HTTP listen address (default :8081)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/topics"
)

func main() {
	if err := run(); err != nil {
		slog.Error("balance exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr()})
	defer rdb.Close()
	store := balance.NewStore(rdb, env.RedisPrefix())

	group := env.String("CLEARLINE_BALANCE_GROUP", "balance")
	opts := append([]kgo.Opt{kgo.SeedBrokers(env.Brokers()...)}, balance.ConsumerClientOpts(group, topics.LedgerEvents)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer cl.Close()
	cons := balance.NewConsumer(cl, store, nil)

	srv := &http.Server{
		Addr:              env.String("CLEARLINE_BALANCE_ADDR", ":8081"),
		Handler:           balance.Handler(store),
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpErr := make(chan error, 1)
	go serve(srv, httpErr)

	consErr := make(chan error, 1)
	go consume(ctx, cons, consErr)
	slog.Info("balance running", "addr", srv.Addr, "group", group)

	select {
	case err = <-httpErr:
		stop()
	case err = <-consErr:
		stop()
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	s := cons.Stats()
	slog.Info("balance stopped", "applied", s.Applied.Load(), "duplicates", s.Duplicates.Load(), "malformed", s.Malformed.Load())
	return err
}

func serve(srv *http.Server, errs chan<- error) {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errs <- err
	}
}

func consume(ctx context.Context, cons *balance.Consumer, errs chan<- error) {
	errs <- cons.Run(ctx)
}
