// Command loadgen drives the running stack (make up-all) at fixed order rates
// and reports latency and throughput per rate (SPEC 7.4).
//
//	go run ./cmd/loadgen -rates 200,500,1000 -duration 30s \
//	    -hardware "Apple M4, 16 GB, Docker Desktop 10 CPU / 8 GB" \
//	    -json docs/benchmarks/results/run.json -svg docs/benchmarks/run.svg
//
// Orders are crossing pairs on the LOAD book (sell then buy, 1 unit at 100),
// so every two orders make exactly one trade and the book never grows.
// Sellers use even accounts and buyers odd ones, so no pair self-matches.
//
// Per rate it reports:
//   - gateway ack latency as the client sees it, exact, open loop (latency
//     measured from each request's due time; see internal/loadgen)
//   - settled trades per second, counted exactly from the ledger
//   - order-to-ledger and balance freshness quantiles, from the services'
//     own histograms in Prometheus
//   - consumer lag, and whether the rate was sustained
//
// Environment: CLEARLINE_DATABASE_URL, CLEARLINE_REDIS_ADDR (defaults: compose).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/loadgen"
)

const symbol = "LOAD"

type options struct {
	gateway, prometheus string
	rates               []float64
	duration            time.Duration
	inFlight, accounts  int
	hardware            string
	jsonOut, svgOut     string
	scrapeWait, drain   time.Duration
	replot              string
	compare             string
	allowObservers      bool
}

func main() {
	opts, err := parseFlags()
	if err == nil {
		err = run(opts)
	}
	if err != nil {
		slog.Error("loadgen", "err", err)
		os.Exit(1)
	}
}

func parseFlags() (options, error) {
	var o options
	rates := flag.String("rates", "200", "comma-separated order rates (orders/s); two orders make one trade")
	flag.StringVar(&o.gateway, "gateway", "http://localhost:8080", "gateway base URL")
	flag.StringVar(&o.prometheus, "prometheus", "http://localhost:9090", "Prometheus base URL")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "time at each rate")
	flag.IntVar(&o.inFlight, "in-flight", 512, "max concurrent requests (waits for a slot count as latency)")
	flag.IntVar(&o.accounts, "accounts", 100, "number of load accounts (even)")
	flag.StringVar(&o.hardware, "hardware", "", "hardware description for the report (required)")
	flag.StringVar(&o.jsonOut, "json", "", "write the full report as JSON to this file")
	flag.StringVar(&o.svgOut, "svg", "", "write a latency vs throughput chart to this file")
	flag.DurationVar(&o.scrapeWait, "scrape-wait", 12*time.Second, "wait after each run for Prometheus to scrape")
	flag.DurationVar(&o.drain, "drain-timeout", 3*time.Minute, "max wait for consumer lag to reach 0 between runs")
	flag.BoolVar(&o.allowObservers, "allow-observers", false, "run even if a dashboard or other client is reading the balance API (it skews results)")
	flag.StringVar(&o.replot, "replot", "", "re-judge an existing report, print it, and rewrite -json / -svg; no load is sent")
	flag.StringVar(&o.compare, "compare", "", "name=report.json,... : draw -svg comparing order-to-ledger p99 across configurations; no load is sent")
	flag.Parse()
	if o.replot != "" || o.compare != "" {
		return o, nil
	}
	if o.hardware == "" {
		return o, fmt.Errorf("-hardware is required: every number is reported with its hardware")
	}
	if o.accounts < 2 || o.accounts%2 != 0 {
		return o, fmt.Errorf("-accounts must be even and at least 2")
	}
	for _, s := range strings.Split(*rates, ",") {
		r, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || r <= 0 {
			return o, fmt.Errorf("bad rate %q", s)
		}
		o.rates = append(o.rates, r)
	}
	return o, nil
}

func run(o options) error {
	if o.replot != "" {
		return replot(o)
	}
	if o.compare != "" {
		return compare(o)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, env.DatabaseURL())
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := seedAccounts(ctx, pool, o.accounts); err != nil {
		return err
	}
	durability, err := durabilitySettings(ctx, pool)
	if err != nil {
		return err
	}

	prom := loadgen.Prom{URL: o.prometheus, Client: &http.Client{Timeout: 10 * time.Second}}
	observers := checkObservers(ctx, prom)
	if observers != "none" && !o.allowObservers {
		return fmt.Errorf("other clients are using the balance API (%s); close the dashboard or pass -allow-observers", observers)
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConns: o.inFlight, MaxIdleConnsPerHost: o.inFlight, IdleConnTimeout: time.Minute},
	}
	report := loadgen.Report{Environment: loadgen.Environment{
		Hardware:   o.hardware,
		Host:       fmt.Sprintf("%s/%s, %d CPUs visible to the load generator", runtime.GOOS, runtime.GOARCH, runtime.NumCPU()),
		Docker:     dockerInfo(ctx),
		Commit:     gitCommit(ctx),
		Durability: durability,
		Observers:  observers,
		StartedAt:  time.Now().UTC(),
	}}

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	for i, rate := range o.rates {
		if _, err := prom.WaitDrained(ctx, o.drain); err != nil {
			slog.Warn("starting with lag", "err", err)
		}
		slog.Info("run", "orders_per_sec", rate, "duration", o.duration)
		res := runOnce(ctx, o, rate, fmt.Sprintf("%s-%d", runID, i), client, pool, prom)
		report.Runs = append(report.Runs, res)
		if ctx.Err() != nil {
			break
		}
	}

	report.PrintTable(os.Stdout)
	if o.jsonOut != "" {
		if err := writeJSON(o.jsonOut, report); err != nil {
			return err
		}
	}
	if o.svgOut != "" {
		if err := writeSVG(o.svgOut, report); err != nil {
			return err
		}
	}
	return nil
}

func runOnce(ctx context.Context, o options, rate float64, runID string, client *http.Client,
	pool *pgxpool.Pool, prom loadgen.Prom) loadgen.Result {

	res := loadgen.Result{Settings: loadgen.Settings{
		OrdersPerSec: rate, TradesPerSec: rate / 2, Duration: o.duration,
		MaxInFlight: o.inFlight, Accounts: o.accounts, Symbol: symbol,
	}}
	send := orderSender(o, runID, client)
	start := time.Now()
	samples := loadgen.Run(ctx, loadgen.Schedule{Rate: rate, Duration: o.duration, MaxInFlight: o.inFlight}, send)
	end := start.Add(o.duration)
	if took := time.Since(start); took > 0 {
		res.SentPerSec = float64(len(samples)) / took.Seconds()
	}
	res.Ack = loadgen.Summarize(samples)

	select {
	case <-ctx.Done():
	case <-time.After(o.scrapeWait):
	}
	res.Pipeline = prom.Measure(ctx, start, end)
	if n, err := settledBetween(ctx, pool, start, end); err == nil {
		res.Settled = n
		res.SettledPerSec = float64(n) / o.duration.Seconds()
	} else {
		slog.Error("count settled trades", "err", err)
	}
	loadgen.Judge(&res)
	if d, err := prom.WaitDrained(ctx, o.drain); err == nil {
		res.DrainTime = d
	}
	return res
}

// orderSender returns a SendFunc posting crossing pairs: request 2k is a sell
// by an even account, 2k+1 the matching buy by an odd one.
func orderSender(o options, runID string, client *http.Client) loadgen.SendFunc {
	return func(ctx context.Context, i int) bool {
		pair := i / 2
		side, acct := "sell", 2*(pair%(o.accounts/2))
		if i%2 == 1 {
			side, acct = "buy", acct+1
		}
		body := fmt.Sprintf(`{"account_id":"load-%03d","symbol":%q,"side":%q,"quantity":"1","price":"100"}`, acct, symbol, side)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.gateway+"/orders", strings.NewReader(body))
		if err != nil {
			return false
		}
		req.Header.Set("Idempotency-Key", fmt.Sprintf("load-%s-%d", runID, i))
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusAccepted
	}
}

func seedAccounts(ctx context.Context, pool *pgxpool.Pool, n int) error {
	_, err := pool.Exec(ctx, `INSERT INTO accounts (account_id)
		SELECT 'load-' || lpad(g::text, 3, '0') FROM generate_series(0, $1 - 1) g
		ON CONFLICT DO NOTHING`, n)
	if err != nil {
		return fmt.Errorf("seed accounts: %w", err)
	}
	return nil
}

// settledBetween counts LOAD trades whose ledger rows were written in [start, end).
func settledBetween(ctx context.Context, pool *pgxpool.Pool, start, end time.Time) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(DISTINCT trade_id) FROM ledger_entries
		WHERE trade_id LIKE 'LOAD-%' AND created_at >= $1 AND created_at < $2`, start, end).Scan(&n)
	return n, err
}

// durabilitySettings reads the settings that bound latency from the running
// system rather than trusting config files.
func durabilitySettings(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var syncCommit, fsync string
	if err := pool.QueryRow(ctx, `SELECT current_setting('synchronous_commit'), current_setting('fsync')`).Scan(&syncCommit, &fsync); err != nil {
		return "", fmt.Errorf("read postgres settings: %w", err)
	}
	appendfsync := "unknown"
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr()})
	defer rdb.Close()
	if v, err := rdb.ConfigGet(ctx, "appendfsync").Result(); err == nil {
		appendfsync = v["appendfsync"]
	}
	return fmt.Sprintf("Kafka acks=all (1 broker, replication 1); Postgres synchronous_commit=%s fsync=%s; Valkey appendfsync=%s",
		syncCommit, fsync, appendfsync), nil
}

func dockerInfo(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "docker", "info", "--format",
		"{{.OperatingSystem}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes memory, server {{.ServerVersion}}").Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(out))
}

func gitCommit(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	c := strings.TrimSpace(string(out))
	if err := exec.CommandContext(ctx, "git", "diff", "--quiet", "HEAD").Run(); err != nil {
		c += "-dirty"
	}
	return c
}

func writeJSON(path string, r loadgen.Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

func writeSVG(path string, r loadgen.Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer f.Close()
	return r.PlotSVG(f, "Clearline: latency vs settled throughput",
		r.Environment.Hardware+"; commit "+r.Environment.Commit)
}

func replot(o options) error {
	b, err := os.ReadFile(filepath.Clean(o.replot))
	if err != nil {
		return err
	}
	var r loadgen.Report
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	for i := range r.Runs {
		loadgen.Judge(&r.Runs[i]) // re-judge with the current criteria
	}
	r.PrintTable(os.Stdout)
	if o.jsonOut != "" {
		if err := writeJSON(o.jsonOut, r); err != nil {
			return err
		}
	}
	if o.svgOut == "" {
		return nil
	}
	return writeSVG(o.svgOut, r)
}

// checkObservers reports clients that would compete with the run: open
// dashboard streams, or anything polling the balance list. An open dashboard
// once turned into a full-keyspace SCAN storm during a sweep.
func checkObservers(ctx context.Context, prom loadgen.Prom) string {
	now := time.Now()
	subs, _ := prom.Query(ctx, `sum(clearline_balance_stream_subscribers)`, now)
	lists, _ := prom.Query(ctx,
		`sum(rate(clearline_http_request_duration_seconds_count{service="balance",route="GET /balances"}[1m]))`, now)
	if subs == 0 && lists < 0.1 {
		return "none"
	}
	return fmt.Sprintf("%.0f dashboard streams, %.1f balance list requests/s", subs, lists)
}

func compare(o options) error {
	var configs []loadgen.Configuration
	for _, item := range strings.Split(o.compare, ",") {
		name, path, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("-compare item %q: want name=path", item)
		}
		b, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return err
		}
		var r loadgen.Report
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}
		for i := range r.Runs {
			loadgen.Judge(&r.Runs[i])
		}
		configs = append(configs, loadgen.Configuration{Name: name, Report: r})
	}
	if o.svgOut == "" {
		return fmt.Errorf("-compare needs -svg")
	}
	f, err := os.Create(filepath.Clean(o.svgOut))
	if err != nil {
		return err
	}
	defer f.Close()
	return loadgen.PlotCompare(f, "Clearline: order-to-ledger p99 vs settled throughput",
		configs[0].Report.Environment.Hardware, "p99", configs)
}
