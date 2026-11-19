// Command gateway is the order gateway (SPEC F1): POST /orders with an
// Idempotency-Key, validated and written to the orders topic with acks=all
// before it answers 202. See internal/gateway for the API and
// docs/decisions/0003-order-idempotency.md for the retry guarantees.
//
// Environment:
//
//	CLEARLINE_DATABASE_URL               Postgres, for the account check
//	CLEARLINE_KAFKA_BROKERS              comma separated (default localhost:9092)
//	CLEARLINE_REDIS_ADDR                 Valkey (default localhost:6379)
//	CLEARLINE_REDIS_PREFIX               key prefix (default clearline:)
//	CLEARLINE_SYMBOLS                    same format and default as the matcher adapter
//	CLEARLINE_GATEWAY_ADDR               HTTP listen address (default :8080)
//	CLEARLINE_GATEWAY_PRODUCE_TIMEOUT    Kafka write limit (default 5s)
//	CLEARLINE_IDEMPOTENCY_PENDING_TTL    how long an in-flight key blocks retries (default 30s)
//	CLEARLINE_IDEMPOTENCY_TTL            how long a response is replayed (default 24h)
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/gateway"
	"github.com/harshithptl/clearline/internal/matcher"
	"github.com/harshithptl/clearline/internal/topics"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gateway exited", "err", err)
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
	produceTimeout, err := env.Duration("CLEARLINE_GATEWAY_PRODUCE_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	pendingTTL, err := env.Duration("CLEARLINE_IDEMPOTENCY_PENDING_TTL", 30*time.Second)
	if err != nil {
		return err
	}
	doneTTL, err := env.Duration("CLEARLINE_IDEMPOTENCY_TTL", 24*time.Hour)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, env.DatabaseURL())
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr()})
	defer rdb.Close()

	opts := append([]kgo.Opt{kgo.SeedBrokers(env.Brokers()...)}, gateway.ProducerClientOpts(produceTimeout)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer cl.Close()

	gw, err := gateway.New(gateway.Config{
		Producer:       cl,
		OrdersTopic:    topics.Orders,
		Idempotency:    gateway.NewIdempotency(rdb, env.RedisPrefix(), pendingTTL, doneTTL),
		Accounts:       gateway.NewAccounts(pool),
		Symbols:        symbols,
		ProduceTimeout: produceTimeout,
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              env.String("CLEARLINE_GATEWAY_ADDR", ":8080"),
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      produceTimeout + 10*time.Second,
	}
	errs := make(chan error, 1)
	go serve(srv, errs)
	slog.Info("gateway running", "addr", srv.Addr, "symbols", len(symbols))

	select {
	case err = <-errs:
	case <-ctx.Done():
	}
	// Let in-flight requests finish their Kafka write and idempotency update.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), produceTimeout+5*time.Second)
	defer cancel()
	if serr := srv.Shutdown(shutdownCtx); serr != nil && err == nil {
		err = serr
	}
	slog.Info("gateway stopped")
	return err
}

func serve(srv *http.Server, errs chan<- error) {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errs <- err
	}
}
