package balance_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/messages"
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
