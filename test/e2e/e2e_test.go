//go:build e2e

// Package e2e drives the full compose stack from the outside: REST in at the
// gateway, then ledger, outbox, balances and reconciler checked from their
// own stores. SPEC 11 Phase 2 exit: one order flows end to end into the
// ledger. Needs `make up-all`; run with `make e2e`.
//
// Every run uses fresh account ids, and orders go to the E2E book (only this
// test trades there), so reruns against a long-lived stack are safe.
package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/env"
	"github.com/harshithptl/clearline/internal/gateway"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/reconcile"
)

const (
	symbol  = "E2E"
	price   = "100.25"
	qty     = "5"
	timeout = 60 * time.Second
)

var (
	gatewayURL = env.String("CLEARLINE_E2E_GATEWAY", "http://localhost:8080")
	balanceURL = env.String("CLEARLINE_E2E_BALANCE", "http://localhost:8081")
	notional   = decimal.RequireFromString(price).Mul(decimal.RequireFromString(qty)) // 501.25
)

type stack struct {
	t     *testing.T
	pool  *pgxpool.Pool
	rdb   *redis.Client
	store *balance.Store
}

func connect(t *testing.T) *stack {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, env.DatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr()})
	t.Cleanup(func() { rdb.Close() })
	resp, err := http.Get(gatewayURL + "/healthz")
	if err != nil {
		t.Fatalf("gateway at %s not reachable (is `make up-all` running?): %v", gatewayURL, err)
	}
	resp.Body.Close()
	return &stack{t: t, pool: pool, rdb: rdb, store: balance.NewStore(rdb, env.RedisPrefix())}
}

func runID(t *testing.T) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func (s *stack) accounts(ids ...string) {
	s.t.Helper()
	for _, id := range ids {
		if _, err := s.pool.Exec(context.Background(), `INSERT INTO accounts (account_id) VALUES ($1)`, id); err != nil {
			s.t.Fatal(err)
		}
	}
}

type response struct {
	status   int
	replayed bool
	body     string
}

func order(t *testing.T, key, account, side string) response {
	t.Helper()
	body := `{"account_id":"` + account + `","symbol":"` + symbol + `","side":"` + side +
		`","quantity":"` + qty + `","price":"` + price + `"}`
	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/orders", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(gateway.HeaderIdempotencyKey, key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, replayed: resp.Header.Get(gateway.HeaderReplayed) == "true", body: string(b)}
}

func accepted(t *testing.T, r response, wantReplayed bool) {
	t.Helper()
	if r.status != http.StatusAccepted || r.replayed != wantReplayed {
		t.Fatalf("status %d replayed %v, want 202 replayed %v: %s", r.status, r.replayed, wantReplayed, r.body)
	}
}

type row struct {
	trade, account, asset string
	amount                decimal.Decimal
}

func (s *stack) ledger(account string) []row {
	s.t.Helper()
	rows, err := s.pool.Query(context.Background(),
		`SELECT trade_id, account_id, asset, amount::text FROM ledger_entries WHERE account_id = $1 ORDER BY entry_id`, account)
	if err != nil {
		s.t.Fatal(err)
	}
	defer rows.Close()
	var out []row
	for rows.Next() {
		var r row
		var amt string
		if err := rows.Scan(&r.trade, &r.account, &r.asset, &amt); err != nil {
			s.t.Fatal(err)
		}
		r.amount = decimal.RequireFromString(amt)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		s.t.Fatal(err)
	}
	return out
}

// tradeRows waits until account has want ledger rows and returns them.
func (s *stack) tradeRows(account string, want int) []row {
	s.t.Helper()
	var got []row
	kafkatest.Eventually(s.t, timeout, account+" ledger rows", func() bool {
		got = s.ledger(account)
		return len(got) >= want
	})
	if len(got) != want {
		s.t.Fatalf("%s has %d ledger rows, want %d: %+v", account, len(got), want, got)
	}
	return got
}

func requireRow(t *testing.T, r row, account, asset string, amount decimal.Decimal) {
	t.Helper()
	if r.account != account || r.asset != asset || !r.amount.Equal(amount) {
		t.Fatalf("ledger row %+v, want %s %s %s", r, account, asset, amount)
	}
}

type balances struct {
	Balances map[string]string `json:"balances"`
	Events   int               `json:"events"`
}

func (s *stack) balanceAPI(account string, wantEvents int) balances {
	s.t.Helper()
	var b balances
	kafkatest.Eventually(s.t, timeout, account+" balance", func() bool {
		resp, err := http.Get(balanceURL + "/balances/" + account)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b = balances{}
		return resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&b) == nil && b.Events == wantEvents
	})
	return b
}

func requireBalance(t *testing.T, b balances, asset string, want decimal.Decimal) {
	t.Helper()
	got, err := decimal.NewFromString(b.Balances[asset])
	if err != nil || !got.Equal(want) {
		t.Fatalf("balance %s = %q, want %s (%+v)", asset, b.Balances[asset], want, b)
	}
}

// One order pair through every service, a retry at each idempotency layer,
// and the reconciler's verdict.
//
//	seller sells 5 E2E @ 100.25        (rests)
//	buyer  buys  5 E2E @ 100.25        -> trade T: 4 ledger rows, 2 outbox events
//	buyer  retries                     -> replayed by Valkey, nothing written
//	Valkey entry deleted, buyer retries -> written again, same order_id, adapter drops it
//	seller sells 5, marker buys 5      -> marker trades with seller; had the
//	                                      duplicate buy rested, the seller would
//	                                      have filled it instead
func TestOrderFlowsIntoLedger(t *testing.T) {
	s := connect(t)
	id := runID(t)
	seller, buyer, marker := "e2e-s-"+id, "e2e-b-"+id, "e2e-m-"+id
	s.accounts(seller, buyer, marker)

	accepted(t, order(t, "sell-1", seller, "sell"), false)
	first := order(t, "buy-1", buyer, "buy")
	accepted(t, first, false)
	if !strings.Contains(first.body, gateway.OrderID(buyer, "buy-1")) {
		t.Fatalf("response %s lacks order id %s", first.body, gateway.OrderID(buyer, "buy-1"))
	}
	retry := order(t, "buy-1", buyer, "buy")
	accepted(t, retry, true)
	if retry.body != first.body {
		t.Fatalf("replayed body differs:\n%s\n%s", first.body, retry.body)
	}

	// Ledger: 4 rows for the trade, exact amounts, netting to zero per asset.
	b := s.tradeRows(buyer, 2)
	trade := b[0].trade
	requireRow(t, b[0], buyer, "USD", notional.Neg())
	requireRow(t, b[1], buyer, symbol, decimal.RequireFromString(qty))
	sl := s.tradeRows(seller, 2)
	requireRow(t, sl[0], seller, "USD", notional)
	requireRow(t, sl[1], seller, symbol, decimal.RequireFromString(qty).Neg())
	if sl[0].trade != trade || !strings.HasPrefix(trade, symbol+"-") {
		t.Fatalf("trade ids %q and %q", trade, sl[0].trade)
	}

	// Outbox: one event per account, both published by the relay.
	kafkatest.Eventually(t, timeout, "outbox published", func() bool {
		var n int
		err := s.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM outbox WHERE source_event_id = $1 AND published_at IS NOT NULL`, trade).Scan(&n)
		return err == nil && n == 2
	})

	// Balances through the read API.
	bb := s.balanceAPI(buyer, 1)
	requireBalance(t, bb, "USD", notional.Neg())
	requireBalance(t, bb, symbol, decimal.RequireFromString(qty))
	sb := s.balanceAPI(seller, 1)
	requireBalance(t, sb, "USD", notional)
	requireBalance(t, sb, symbol, decimal.RequireFromString(qty).Neg())

	// Lose the Valkey entry: the retry is written to Kafka again with the
	// same order_id, and the matcher adapter must drop it.
	key := env.RedisPrefix() + "idem:{" + buyer + "}:buy-1"
	if n, err := s.rdb.Del(context.Background(), key).Result(); err != nil || n != 1 {
		t.Fatalf("delete %s: %d, %v", key, n, err)
	}
	accepted(t, order(t, "buy-1", buyer, "buy"), false)
	accepted(t, order(t, "sell-2", seller, "sell"), false)
	accepted(t, order(t, "buy-m", marker, "buy"), false)
	m := s.tradeRows(marker, 2)
	requireRow(t, m[0], marker, "USD", notional.Neg())
	if got := s.ledger(buyer); len(got) != 2 {
		t.Fatalf("duplicate order traded: buyer has %d ledger rows, want 2", len(got))
	}

	// The reconciler's checks over the whole ledger and cache.
	var rep reconcile.Report
	kafkatest.Eventually(t, timeout, "reconciler clean", func() bool {
		r, err := reconcile.Check(context.Background(), s.pool, s.store)
		rep = r
		return err == nil && r.OK() && len(r.Lagging) == 0
	})
	t.Logf("reconciled %d accounts, trade %s", rep.Accounts, trade)
}
