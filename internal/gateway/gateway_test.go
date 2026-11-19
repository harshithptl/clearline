package gateway_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/gateway"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/matcher"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/internal/redistest"
)

const (
	waitLong       = 30 * time.Second
	produceTimeout = 2 * time.Second
	aliceBuy       = `{"account_id":"alice","symbol":"AAPL","side":"buy","quantity":"10","price":"150.25"}`
)

type env struct {
	t      *testing.T
	topic  string
	rdb    *redis.Client
	prefix string
	accts  *gateway.Accounts
	srv    *httptest.Server
}

func symbols() []matcher.SymbolSpec {
	return []matcher.SymbolSpec{{
		Symbol: "AAPL", QuoteAsset: "USD", TickSize: decimal.RequireFromString("0.01"),
		ReferencePrice: decimal.RequireFromString("150"), TicksEachSide: 20000, MaxOrderID: 1000, MaxResting: 100,
	}}
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := pgtest.NewPool(t)
	if _, err := pool.Exec(context.Background(), `INSERT INTO accounts (account_id) VALUES ('alice'), ('bob')`); err != nil {
		t.Fatal(err)
	}
	rdb, prefix := redistest.New(t)
	e := &env{t: t, topic: kafkatest.NewTopic(t, "orders", 3), rdb: rdb, prefix: prefix, accts: gateway.NewAccounts(pool)}
	e.srv = e.serve(kafkatest.NewClient(t, gateway.ProducerClientOpts(produceTimeout)...), rdb)
	return e
}

// serve starts a gateway on e's topic and accounts with the given clients.
func (e *env) serve(producer *kgo.Client, rdb *redis.Client) *httptest.Server {
	e.t.Helper()
	gw, err := gateway.New(gateway.Config{
		Producer: producer, OrdersTopic: e.topic,
		Idempotency:    gateway.NewIdempotency(rdb, e.prefix, 10*time.Second, time.Hour),
		Accounts:       e.accts,
		Symbols:        symbols(),
		ProduceTimeout: produceTimeout,
		Logger:         slog.New(slog.DiscardHandler),
	})
	if err != nil {
		e.t.Fatal(err)
	}
	srv := httptest.NewServer(gw.Handler())
	e.t.Cleanup(srv.Close)
	return srv
}

type response struct {
	status int
	header http.Header
	body   string
}

func post(t *testing.T, srv *httptest.Server, key, body string) response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/orders", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set(gateway.HeaderIdempotencyKey, key)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response{status: resp.StatusCode, header: resp.Header, body: string(b)}
}

func requireStatus(t *testing.T, r response, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("status %d, want %d: %s", r.status, want, r.body)
	}
}

// count returns how many records the topic holds.
func (e *env) count() int64 {
	e.t.Helper()
	adm := kadm.NewClient(kafkatest.NewClient(e.t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ends, err := adm.ListEndOffsets(ctx, e.topic)
	if err != nil {
		e.t.Fatal(err)
	}
	var n int64
	ends.Each(func(o kadm.ListedOffset) { n += o.Offset })
	return n
}

// dropEntries deletes every idempotency entry under e's prefix, simulating
// Valkey losing them.
func (e *env) dropEntries() {
	e.t.Helper()
	ctx := context.Background()
	ks, err := e.rdb.Keys(ctx, e.prefix+"*").Result()
	if err != nil || len(ks) == 0 {
		e.t.Fatalf("entries to drop: %v, %v", ks, err)
	}
	if err := e.rdb.Del(ctx, ks...).Err(); err != nil {
		e.t.Fatal(err)
	}
}

// keys returns how many idempotency entries exist under e's prefix.
func (e *env) keys() int {
	e.t.Helper()
	ks, err := e.rdb.Keys(context.Background(), e.prefix+"*").Result()
	if err != nil {
		e.t.Fatal(err)
	}
	return len(ks)
}

func TestAcceptedOrderIsInKafka(t *testing.T) {
	e := newEnv(t)
	r := post(t, e.srv, "req-1", aliceBuy)
	requireStatus(t, r, http.StatusAccepted)
	if r.header.Get(gateway.HeaderReplayed) != "" {
		t.Fatal("first response marked replayed")
	}

	recs := kafkatest.ReadN(t, e.topic, 1, waitLong)
	if string(recs[0].Key) != "AAPL" {
		t.Fatalf("key %q, want AAPL (orders are keyed by symbol)", recs[0].Key)
	}
	o, err := messages.DecodeOrder(recs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if o.OrderID != gateway.OrderID("alice", "req-1") || o.AccountID != "alice" || o.Side != messages.SideBuy ||
		!o.Quantity.Equal(decimal.RequireFromString("10")) || !o.Price.Equal(decimal.RequireFromString("150.25")) {
		t.Fatalf("order on topic = %+v", o)
	}
	want := `"order_id":"` + o.OrderID + `"`
	if !strings.Contains(r.body, want) || !strings.Contains(r.body, `"status":"accepted"`) {
		t.Fatalf("response %s, want it to contain %s", r.body, want)
	}
}

// F1: a retried request never creates two orders.
func TestSameKeyTwiceIsOneOrder(t *testing.T) {
	e := newEnv(t)
	first := post(t, e.srv, "req-1", aliceBuy)
	requireStatus(t, first, http.StatusAccepted)
	// Same order, formatted differently: still the same request.
	again := post(t, e.srv, "req-1", `{"price":"150.250","quantity":"10.0","side":"buy","symbol":"AAPL","account_id":"alice"}`)
	requireStatus(t, again, http.StatusAccepted)
	if again.header.Get(gateway.HeaderReplayed) != "true" || again.body != first.body {
		t.Fatalf("retry not replayed:\nfirst %s\nretry %s", first.body, again.body)
	}
	if n := e.count(); n != 1 {
		t.Fatalf("%d records on orders, want 1", n)
	}
}

// Many concurrent copies of one request: one Kafka write; every response is
// that order's 202 or "in progress" 409.
func TestConcurrentRetriesWriteOnce(t *testing.T) {
	e := newEnv(t)
	const n = 20
	var wg sync.WaitGroup
	results := make([]response, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = post(t, e.srv, "burst", aliceBuy)
		}(i)
	}
	wg.Wait()
	var accepted string
	for _, r := range results {
		switch r.status {
		case http.StatusAccepted:
			if accepted != "" && r.body != accepted {
				t.Fatalf("two different 202 bodies:\n%s\n%s", accepted, r.body)
			}
			accepted = r.body
		case http.StatusConflict:
		default:
			t.Fatalf("status %d: %s", r.status, r.body)
		}
	}
	if accepted == "" {
		t.Fatal("no request accepted")
	}
	if c := e.count(); c != 1 {
		t.Fatalf("%d records on orders, want 1", c)
	}
}

func TestKeyReusedForDifferentOrder(t *testing.T) {
	e := newEnv(t)
	requireStatus(t, post(t, e.srv, "req-1", aliceBuy), http.StatusAccepted)
	r := post(t, e.srv, "req-1", strings.Replace(aliceBuy, `"10"`, `"11"`, 1))
	requireStatus(t, r, http.StatusUnprocessableEntity)
	if n := e.count(); n != 1 {
		t.Fatalf("%d records on orders, want 1", n)
	}
}

func TestKeysAreScopedPerAccount(t *testing.T) {
	e := newEnv(t)
	requireStatus(t, post(t, e.srv, "same", aliceBuy), http.StatusAccepted)
	r := post(t, e.srv, "same", strings.Replace(aliceBuy, "alice", "bob", 1))
	requireStatus(t, r, http.StatusAccepted)
	if r.header.Get(gateway.HeaderReplayed) != "" {
		t.Fatal("bob got alice's response")
	}
	if gateway.OrderID("alice", "same") == gateway.OrderID("bob", "same") {
		t.Fatal("order ids collide across accounts")
	}
	if n := e.count(); n != 2 {
		t.Fatalf("%d records on orders, want 2", n)
	}
}

// If the Valkey entry is lost, a retry is written again but with the same
// order_id, which the matcher adapter dedupes (TestDuplicateOrderIDMatchedOnce).
func TestLostEntryRewritesSameOrderID(t *testing.T) {
	e := newEnv(t)
	requireStatus(t, post(t, e.srv, "req-1", aliceBuy), http.StatusAccepted)
	e.dropEntries()
	requireStatus(t, post(t, e.srv, "req-1", aliceBuy), http.StatusAccepted)

	recs := kafkatest.ReadN(t, e.topic, 2, waitLong)
	a, _ := messages.DecodeOrder(recs[0].Value)
	b, _ := messages.DecodeOrder(recs[1].Value)
	if a.OrderID != b.OrderID || a.OrderID != gateway.OrderID("alice", "req-1") {
		t.Fatalf("order ids %q and %q, want both %q", a.OrderID, b.OrderID, gateway.OrderID("alice", "req-1"))
	}
}

func TestRejectedRequestsWriteNothing(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name, key, body string
		want            int
	}{
		{"no key", "", aliceBuy, 400},
		{"key with space", "a b", aliceBuy, 400},
		{"key too long", strings.Repeat("k", 256), aliceBuy, 400},
		{"not json", "k", `{nope`, 400},
		{"unknown field", "k", `{"account_id":"alice","symbol":"AAPL","side":"buy","qty":"10","price":"150"}`, 400},
		{"number quantity", "k", `{"account_id":"alice","symbol":"AAPL","side":"buy","quantity":10,"price":"150"}`, 400},
		{"two objects", "k", aliceBuy + aliceBuy, 400},
		{"bad account id", "k", strings.Replace(aliceBuy, "alice", "Alice", 1), 422},
		{"unknown account", "k", strings.Replace(aliceBuy, "alice", "ghost", 1), 422},
		{"unknown symbol", "k", strings.Replace(aliceBuy, "AAPL", "GOOG", 1), 422},
		{"bad side", "k", strings.Replace(aliceBuy, `"buy"`, `"hold"`, 1), 422},
		{"fractional quantity", "k", strings.Replace(aliceBuy, `"10"`, `"1.5"`, 1), 422},
		{"off tick", "k", strings.Replace(aliceBuy, "150.25", "150.251", 1), 422},
		{"outside book", "k", strings.Replace(aliceBuy, "150.25", "9999", 1), 422},
		{"price not decimal", "k", strings.Replace(aliceBuy, "150.25", "1e", 1), 422},
	}
	for _, c := range cases {
		r := post(t, e.srv, c.key, c.body)
		if r.status != c.want || !strings.Contains(r.body, `"error"`) {
			t.Errorf("%s: status %d, want %d: %s", c.name, r.status, c.want, r.body)
		}
	}
	if n := e.count(); n != 0 {
		t.Fatalf("%d records on orders, want 0", n)
	}
	if n := e.keys(); n != 0 {
		t.Fatalf("%d idempotency entries, want 0 (rejections must not claim keys)", n)
	}
}

// Kafka unreachable: 503, and no idempotency entry is left claiming success,
// so the same key works once Kafka is back.
func TestKafkaDownIs503AndKeyStaysUsable(t *testing.T) {
	e := newEnv(t)
	dead, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}, gateway.ProducerClientOpts(produceTimeout)...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dead.Close)
	down := e.serve(dead, e.rdb)

	start := time.Now()
	r := post(t, down, "req-1", aliceBuy)
	requireStatus(t, r, http.StatusServiceUnavailable)
	if r.header.Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}
	if took := time.Since(start); took > produceTimeout+3*time.Second {
		t.Fatalf("503 took %s, want about the %s produce timeout", took, produceTimeout)
	}
	if n := e.keys(); n != 0 {
		t.Fatalf("%d idempotency entries after a failed produce, want 0", n)
	}

	requireStatus(t, post(t, e.srv, "req-1", aliceBuy), http.StatusAccepted)
	if n := e.count(); n != 1 {
		t.Fatalf("%d records on orders, want 1", n)
	}
}

func TestValkeyDownIs503(t *testing.T) {
	e := newEnv(t)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { dead.Close() })
	down := e.serve(kafkatest.NewClient(t, gateway.ProducerClientOpts(produceTimeout)...), dead)
	requireStatus(t, post(t, down, "req-1", aliceBuy), http.StatusServiceUnavailable)
	if n := e.count(); n != 0 {
		t.Fatalf("%d records on orders, want 0", n)
	}
}

func TestOrderIDIsStableAndDistinct(t *testing.T) {
	if gateway.OrderID("alice", "k") != gateway.OrderID("alice", "k") {
		t.Fatal("not stable")
	}
	// Without a separator, ("ab","c") and ("a","bc") would hash the same input.
	if gateway.OrderID("ab", "c") == gateway.OrderID("a", "bc") {
		t.Fatal("ambiguous concatenation")
	}
	if id := gateway.OrderID("alice", "k"); len(id) != 34 || len(id) > messages.MaxOrderIDLen {
		t.Fatalf("order id %q", id)
	}
}
