//go:build chaos

// Package chaos runs SPEC 5's failure table against the live compose stack
// (`make chaos`): crash or restart a component while orders flow, then prove
// nothing was lost or posted twice.
//
// Each scenario:
//
//	start a steady stream of crossing order pairs (fresh accounts, CHAOS book)
//	clients retry every order with the SAME Idempotency-Key until it gets 202,
//	  which is the contract a real client follows
//	inject the fault a few seconds in, recover it a few seconds later
//	wait for every consumer to drain
//	assert: trades == pairs (exactly one trade per pair: nothing lost, nothing doubled)
//	        every outbox row published, no event processed twice
//	        reconciler clean with no lag (books balance, cache == ledger)
//
// A results table is written to CHAOS_REPORT (default docs/chaos/latest.md).
package chaos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/loadgen"
	"github.com/harshithptl/clearline/internal/reconcile"
	"github.com/harshithptl/clearline/internal/topics"
)

const (
	symbol     = "CHAOS"
	pairsPerS  = 100 // trades per second offered
	runFor     = 20 * time.Second
	faultAt    = 5 * time.Second
	downFor    = 5 * time.Second
	drainLimit = 4 * time.Minute
)

var (
	gatewayURL = env.String("CLEARLINE_CHAOS_GATEWAY", "http://localhost:8080")
	composeArg = []string{"compose", "-f", composeFile(), "--profile", "app"}
	results    []result
	resultsMu  sync.Mutex
)

func composeFile() string {
	if f := os.Getenv("CLEARLINE_CHAOS_COMPOSE"); f != "" {
		return f
	}
	return filepath.Join("..", "..", "deploy", "compose", "docker-compose.yml")
}

// fault is what a scenario does to the stack, and how it undoes it.
type fault struct {
	name   string
	spec   string // the SPEC 5 row it covers
	inject func(t *testing.T)
	heal   func(t *testing.T)
}

// kill sends SIGKILL (no graceful shutdown: in-flight work is cut mid-way),
// then starts the container again, as a supervisor would.
func kill(service string) (func(*testing.T), func(*testing.T)) {
	return func(t *testing.T) { compose(t, "kill", "-s", "SIGKILL", service) },
		func(t *testing.T) { compose(t, "start", service) }
}

func restart(service string) (func(*testing.T), func(*testing.T)) {
	return func(t *testing.T) { compose(t, "restart", service) }, func(*testing.T) {}
}

func scenarios() []fault {
	f := func(name, spec string, pair func() (func(*testing.T), func(*testing.T))) fault {
		inject, heal := pair()
		return fault{name: name, spec: spec, inject: inject, heal: heal}
	}
	return []fault{
		f("settlement killed mid-stream", "settlement crashes before/after commit",
			func() (func(*testing.T), func(*testing.T)) { return kill("settlement") }),
		f("outbox relay killed mid-publish", "outbox relay crashes mid-publish",
			func() (func(*testing.T), func(*testing.T)) { return kill("outbox-relay") }),
		f("balance service killed", "consumer crash (balance cache)",
			func() (func(*testing.T), func(*testing.T)) { return kill("balance") }),
		f("matcher adapter killed", "consumer crash (engine rebuilt by replay)",
			func() (func(*testing.T), func(*testing.T)) { return kill("matcher-adapter") }),
		f("gateway killed", "client retries during gateway outage",
			func() (func(*testing.T), func(*testing.T)) { return kill("gateway") }),
		f("Kafka broker restarted", "Kafka broker restart",
			func() (func(*testing.T), func(*testing.T)) { return restart("kafka") }),
		f("Valkey restarted", "cache restart (balances and idempotency keys)",
			func() (func(*testing.T), func(*testing.T)) { return restart("valkey") }),
	}
}

type result struct {
	name, spec           string
	pairs, trades        int64
	retries              int64
	doubled, unpublished int64
	recovery             time.Duration
	reconciled           string
	ok                   bool
}

type stack struct {
	pool  *pgxpool.Pool
	store *balance.Store
	http  *http.Client
}

func connect(t *testing.T) stack {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), env.DatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr()})
	t.Cleanup(func() { rdb.Close() })
	return stack{pool: pool, store: balance.NewStore(rdb, env.RedisPrefix()),
		http: &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}}
}

func TestChaos(t *testing.T) {
	s := connect(t)
	waitHealthy(t, s)
	for _, f := range scenarios() {
		t.Run(f.name, func(t *testing.T) { runScenario(t, s, f) })
	}
	t.Run("duplicate trades injected into Kafka", func(t *testing.T) { runDuplicates(t, s) })
	writeReport(t)
}

func runScenario(t *testing.T, s stack, f fault) {
	id := randomID(t)
	seller, buyer := "chaos-s-"+id, "chaos-b-"+id
	createAccounts(t, s, seller, buyer)

	// Load in the background; the fault lands mid-stream.
	var retries atomic.Int64
	loadDone := make(chan []loadgen.Sample, 1)
	go func() {
		loadDone <- loadgen.Run(context.Background(),
			loadgen.Schedule{Rate: 2 * pairsPerS, Duration: runFor, MaxInFlight: 512},
			sender(s, id, seller, buyer, &retries))
	}()
	time.Sleep(faultAt)
	t.Logf("inject: %s", f.name)
	f.inject(t)
	time.Sleep(downFor)
	f.heal(t)
	healed := time.Now()
	samples := <-loadDone
	// A trade needs one accepted sell and one accepted buy.
	var sells, buys int64
	for i, smp := range samples {
		if smp.OK && i%2 == 0 {
			sells++
		} else if smp.OK {
			buys++
		}
	}
	pairs := min(sells, buys)

	// Drain: every pair has become a trade, every event is published and applied.
	var trades int64
	deadline := time.Now().Add(drainLimit)
	for {
		trades = countTrades(t, s, buyer)
		if trades >= pairs && unpublished(t, s) == 0 {
			if rep, err := reconcile.Check(context.Background(), s.pool, s.store); err == nil && rep.OK() && len(rep.Lagging) == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	recovery := time.Since(healed)

	r := result{name: f.name, spec: f.spec, pairs: pairs, trades: trades, retries: retries.Load(),
		doubled: doubleProcessed(t, s), unpublished: unpublished(t, s), recovery: recovery}
	rep, err := reconcile.Check(context.Background(), s.pool, s.store)
	switch {
	case err != nil:
		r.reconciled = "error: " + err.Error()
	case !rep.OK():
		r.reconciled = fmt.Sprintf("MISMATCH %+v", rep.Ledger)
	case len(rep.Lagging) > 0:
		r.reconciled = fmt.Sprintf("lagging (%d accounts)", len(rep.Lagging))
	default:
		r.reconciled = "clean"
	}
	r.ok = r.trades == r.pairs && r.doubled == 0 && r.unpublished == 0 && r.reconciled == "clean"
	record(r)
	if !r.ok {
		t.Fatalf("%s: pairs %d, trades %d, doubled %d, unpublished %d, reconciler %s",
			f.name, r.pairs, r.trades, r.doubled, r.unpublished, r.reconciled)
	}
	t.Logf("%d pairs -> %d trades, %d client retries, recovered in %s", r.pairs, r.trades, r.retries, recovery.Round(time.Second))
}

// sender posts request i (2k sell, 2k+1 buy) and retries with the same key
// until the gateway returns 202, which is the client contract under faults.
func sender(s stack, runID, seller, buyer string, retries *atomic.Int64) loadgen.SendFunc {
	return func(ctx context.Context, i int) bool {
		side, acct := "sell", seller
		if i%2 == 1 {
			side, acct = "buy", buyer
		}
		body := fmt.Sprintf(`{"account_id":%q,"symbol":%q,"side":%q,"quantity":"1","price":"100"}`, acct, symbol, side)
		key := fmt.Sprintf("chaos-%s-%d", runID, i)
		for attempt := 0; ; attempt++ {
			if attempt > 0 {
				retries.Add(1)
				time.Sleep(min(time.Duration(attempt)*200*time.Millisecond, 2*time.Second))
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL+"/orders", strings.NewReader(body))
			req.Header.Set("Idempotency-Key", key)
			resp, err := s.http.Do(req)
			if err != nil {
				continue
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusAccepted {
				return true
			}
			if resp.StatusCode == http.StatusUnprocessableEntity || resp.StatusCode == http.StatusBadRequest {
				return false // a test bug, not a fault: do not retry
			}
		}
	}
}

// SPEC 5 row 3: a duplicate trade published upstream. Copy already-settled
// trade records straight back onto the trades topic; the ledger must not move.
func runDuplicates(t *testing.T, s stack) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Brokers()...),
		kgo.ConsumeTopics(topics.Trades), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var dups []*kgo.Record
	for len(dups) < 500 && ctx.Err() == nil {
		fs := cl.PollRecords(ctx, 500)
		fs.EachRecord(func(r *kgo.Record) {
			if len(dups) < 500 && strings.HasPrefix(string(r.Key), symbol) {
				dups = append(dups, &kgo.Record{Topic: topics.Trades, Key: r.Key, Value: r.Value, Headers: r.Headers})
			}
		})
	}
	before := totalTrades(t, s)
	if err := cl.ProduceSync(ctx, dups...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second) // settlement consumes them
	after := totalTrades(t, s)
	rep, err := reconcile.Check(context.Background(), s.pool, s.store)
	r := result{name: "duplicate trades injected into Kafka", spec: "duplicate trade published upstream",
		pairs: int64(len(dups)), trades: after - before, doubled: doubleProcessed(t, s), unpublished: unpublished(t, s)}
	r.reconciled = "clean"
	if err != nil || !rep.OK() {
		r.reconciled = fmt.Sprintf("MISMATCH %v %+v", err, rep.Ledger)
	}
	r.ok = r.trades == 0 && r.doubled == 0 && r.reconciled == "clean"
	record(r)
	if !r.ok {
		t.Fatalf("%d duplicates produced: %d new trades (want 0), reconciler %s", len(dups), r.trades, r.reconciled)
	}
	t.Logf("%d duplicate trade records re-produced, 0 new ledger rows", len(dups))
}

func record(r result) {
	resultsMu.Lock()
	defer resultsMu.Unlock()
	results = append(results, r)
}

func writeReport(t *testing.T) {
	path := env.String("CHAOS_REPORT", filepath.Join("..", "..", "docs", "chaos", "latest.md"))
	var b strings.Builder
	fmt.Fprintf(&b, "# Chaos run %s\n\n", time.Now().UTC().Format("2006-01-02 15:04 MST"))
	fmt.Fprintf(&b, "Commit `%s`. Load: %d crossing pairs/s (%d orders/s) for %s per scenario; fault at +%s, recovered after %s. Clients retry each order with the same Idempotency-Key until 202.\n\n",
		gitCommit(), pairsPerS, 2*pairsPerS, runFor, faultAt, downFor)
	b.WriteString("| Scenario | SPEC 5 row | Pairs accepted | Trades in ledger | Client retries | Processed twice | Unpublished outbox | Reconciler | Recovery | Result |\n|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		verdict := "pass"
		if !r.ok {
			verdict = "**FAIL**"
		}
		rec := "-"
		if r.recovery > 0 {
			rec = r.recovery.Round(time.Second).String()
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %d | %d | %s | %s | %s |\n",
			r.name, r.spec, r.pairs, r.trades, r.retries, r.doubled, r.unpublished, r.reconciled, rec, verdict)
	}
	b.WriteString("\nFor the duplicate row, \"Pairs accepted\" is the number of trade records re-produced, and \"Trades in ledger\" is the new trades they caused (must be 0).\n")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Error(err)
		return
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Error(err)
	}
	t.Logf("report written to %s", path)
}

func waitHealthy(t *testing.T, s stack) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if resp, err := s.http.Get(gatewayURL + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("gateway not healthy at %s (is `make up-all` running?)", gatewayURL)
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append(append([]string{}, composeArg...), args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func createAccounts(t *testing.T, s stack, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if _, err := s.pool.Exec(context.Background(), `INSERT INTO accounts (account_id) VALUES ($1)`, id); err != nil {
			t.Fatal(err)
		}
	}
}

// countTrades returns distinct trades the buyer took part in, retrying while
// Postgres is briefly unreachable.
func countTrades(t *testing.T, s stack, buyer string) int64 {
	return queryInt(t, s, `SELECT count(DISTINCT trade_id) FROM ledger_entries WHERE account_id = $1`, buyer)
}

func totalTrades(t *testing.T, s stack) int64 {
	return queryInt(t, s, `SELECT count(DISTINCT trade_id) FROM ledger_entries WHERE trade_id LIKE 'CHAOS-%'`)
}

func unpublished(t *testing.T, s stack) int64 {
	return queryInt(t, s, `SELECT count(*) FROM outbox WHERE published_at IS NULL`)
}

// doubleProcessed counts trades with more than one set of ledger rows: the
// double-posting SPEC 2.2 forbids. (processed_events has a primary key, so
// this is the ledger-side check that nothing went around it.)
func doubleProcessed(t *testing.T, s stack) int64 {
	return queryInt(t, s, `SELECT count(*) FROM (
		SELECT trade_id FROM ledger_entries WHERE trade_id LIKE 'CHAOS-%' GROUP BY trade_id HAVING count(*) <> 4) d`)
}

func queryInt(t *testing.T, s stack, q string, args ...any) int64 {
	t.Helper()
	var n int64
	var err error
	for range 30 {
		if err = s.pool.QueryRow(context.Background(), q, args...).Scan(&n); err == nil {
			return n
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("query %q: %v", q, err)
	return 0
}

func randomID(t *testing.T) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
