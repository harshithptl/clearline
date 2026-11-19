package balance_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/messages"
	"github.com/harshithptl/clearline/internal/redistest"
)

func TestHubDropsSlowSubscriber(t *testing.T) {
	h := balance.NewHub(2)
	slow, _ := h.Subscribe()
	fast, unsubscribe := h.Subscribe()
	defer unsubscribe()

	var got []string
	for i := range 3 {
		h.Publish(messages.LedgerEvent{EventID: string(rune('a' + i))})
		got = append(got, (<-fast).EventID) // fast keeps up
	}
	if strings.Join(got, "") != "abc" {
		t.Fatalf("fast subscriber got %v", got)
	}
	// slow never read: it holds the first 2 and was dropped on the 3rd.
	n := 0
	for range slow {
		n++
	}
	if n != 2 || h.Subscribers() != 1 {
		t.Fatalf("slow got %d events, %d subscribers left; want 2 and 1", n, h.Subscribers())
	}
}

func TestHubUnsubscribeIsIdempotent(t *testing.T) {
	h := balance.NewHub(1)
	ch, unsubscribe := h.Subscribe()
	unsubscribe()
	unsubscribe()
	if _, open := <-ch; open || h.Subscribers() != 0 {
		t.Fatal("unsubscribe did not close the channel")
	}
	h.Publish(messages.LedgerEvent{EventID: "x"}) // no panic on a closed subscriber
}

func TestListBalances(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, ev := range []messages.LedgerEvent{
		event("T1:carol", "carol", "USD", "10", "AAPL", "-1"),
		event("T1:alice", "alice", "USD", "-10", "AAPL", "1"),
		event("T2:alice", "alice", "USD", "-5.5", "AAPL", "1"),
	} {
		if _, err := s.Apply(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(balance.Handler(s, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/balances")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body balance.ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	a := body.Accounts
	if len(a) != 2 || a[0].AccountID != "alice" || a[1].AccountID != "carol" {
		t.Fatalf("accounts %+v, want alice then carol", a)
	}
	if a[0].Balances["USD"] != "-15.5" || a[0].Balances["AAPL"] != "2" || a[0].Events != 2 {
		t.Fatalf("alice %+v", a[0])
	}
}

// Listing reads the account index, not the keyspace: other keys (here a
// stand-in for idempotency entries) are never touched, and accounts written
// before the index existed come back after IndexAccounts.
func TestAccountIndex(t *testing.T) {
	rdb, prefix := redistest.New(t)
	s := balance.NewStore(rdb, prefix)
	ctx := context.Background()
	for i := range 50 {
		rdb.Set(ctx, prefix+"idem:{x}:k"+strconv.Itoa(i), "v", 0)
	}
	if _, err := s.Apply(ctx, event("T1:bob", "bob", "USD", "1")); err != nil {
		t.Fatal(err)
	}
	// An account whose balance predates the index.
	rdb.HSet(ctx, prefix+"{alice}:bal", "USD", "5")

	got, err := s.Accounts(ctx, 10)
	if err != nil || !slices.Equal(got, []string{"bob"}) {
		t.Fatalf("before backfill: %v %v, want [bob]", got, err)
	}
	if n, err := s.IndexAccounts(ctx); err != nil || n != 2 {
		t.Fatalf("IndexAccounts = %d, %v; want 2", n, err)
	}
	if got, _ := s.Accounts(ctx, 10); !slices.Equal(got, []string{"alice", "bob"}) {
		t.Fatalf("after backfill: %v", got)
	}
	if got, _ := s.Accounts(ctx, 1); !slices.Equal(got, []string{"alice"}) {
		t.Fatalf("limit 1: %v", got)
	}
}

// The stream speaks SSE: event name, id, JSON data, blank line.
func TestEventsStream(t *testing.T) {
	s := newStore(t)
	hub := balance.NewHub(8)
	srv := httptest.NewServer(balance.Handler(s, hub))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	kafkatest.Eventually(t, 5*time.Second, "subscribed", func() bool { return hub.Subscribers() == 1 })

	want := event("T9:alice", "alice", "USD", "-1", "AAPL", "1")
	hub.Publish(want)

	sc := bufio.NewScanner(resp.Body)
	fields := map[string]string{}
	for sc.Scan() {
		line := sc.Text()
		if line == "" && fields["event"] != "" {
			break
		}
		if k, v, ok := strings.Cut(line, ": "); ok {
			fields[k] = v
		}
	}
	if fields["event"] != "ledger" || fields["id"] != "T9:alice" {
		t.Fatalf("frame %v", fields)
	}
	var got messages.LedgerEvent
	if err := json.Unmarshal([]byte(fields["data"]), &got); err != nil || got.TradeID != want.TradeID || len(got.Changes) != 2 {
		t.Fatalf("data %q: %v", fields["data"], err)
	}

	cancel() // client goes away: the handler unsubscribes
	kafkatest.Eventually(t, 5*time.Second, "unsubscribed", func() bool { return hub.Subscribers() == 0 })
}
