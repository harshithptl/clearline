// Command reconciler continuously proves the books balance (SPEC F8): ledger
// invariants plus cached balances against the ledger.
//
//	go run ./services/reconciler          run every CLEARLINE_RECONCILE_INTERVAL (default 10s)
//	go run ./services/reconciler -once    run once; exit 0 clean, 1 mismatch, 2 only lagging
//
// In continuous mode it also serves the latest result over HTTP, for the
// dashboard: GET /status and GET /healthz on CLEARLINE_RECONCILER_ADDR
// (default :8082).
//
// Environment: CLEARLINE_DATABASE_URL, CLEARLINE_REDIS_ADDR,
// CLEARLINE_REDIS_PREFIX (defaults: local compose).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/reconcile"
)

const (
	exitClean    = 0
	exitMismatch = 1
	exitLagging  = 2
)

func main() {
	once := flag.Bool("once", false, "run one reconciliation and exit")
	flag.Parse()
	code, err := run(*once)
	if err != nil {
		slog.Error("reconciler", "err", err)
		os.Exit(exitMismatch)
	}
	os.Exit(code)
}

func run(once bool) (int, error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	interval, err := env.Duration("CLEARLINE_RECONCILE_INTERVAL", 10*time.Second)
	if err != nil {
		return exitMismatch, err
	}

	pool, err := pgxpool.New(ctx, env.DatabaseURL())
	if err != nil {
		return exitMismatch, fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr()})
	defer rdb.Close()
	store := balance.NewStore(rdb, env.RedisPrefix())

	if once {
		rep, err := reconcile.Check(ctx, pool, store)
		if err != nil {
			return exitMismatch, err
		}
		logReport(rep)
		switch {
		case !rep.OK():
			return exitMismatch, nil
		case len(rep.Lagging) > 0:
			return exitLagging, nil
		}
		return exitClean, nil
	}

	tracker := reconcile.NewTracker()
	srv := &http.Server{
		Addr:              env.String("CLEARLINE_RECONCILER_ADDR", ":8082"),
		Handler:           reconcile.StatusHandler(tracker),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go serve(srv)
	defer shutdown(srv)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		rep, err := reconcile.Check(ctx, pool, store)
		if err != nil && ctx.Err() == nil {
			slog.Error("reconcile failed", "err", err)
			tracker.Record(rep, err, time.Now())
		} else if err == nil {
			logReport(rep)
			tracker.Record(rep, nil, time.Now())
		}
		select {
		case <-ctx.Done():
			return exitClean, nil
		case <-t.C:
		}
	}
}

func serve(srv *http.Server) {
	slog.Info("reconciler status", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("status server", "err", err)
	}
}

func shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func logReport(rep reconcile.Report) {
	l := rep.Ledger
	attrs := []any{
		"accounts", rep.Accounts,
		"asset_imbalances", len(l.AssetImbalances),
		"trade_imbalances", len(l.TradeImbalances),
		"duplicate_events", len(l.DuplicateEvents),
		"outbox_mismatches", len(l.OutboxMismatches),
		"cache_mismatches", len(rep.CacheMismatches),
		"lagging", len(rep.Lagging),
	}
	if rep.OK() {
		slog.Info("reconcile ok", attrs...)
		return
	}
	slog.Error("reconcile MISMATCH", attrs...)
	for _, m := range rep.CacheMismatches {
		slog.Error("cache mismatch", "account", m.Account, "asset", m.Asset, "cache", m.Cache, "ledger", m.Ledger)
	}
}
