package balance_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/balance"
	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/messages"
)

// Events delivered twice and one garbage record: balances are exact, the
// garbage is counted and skipped, and the read API serves the result.
func TestConsumerAppliesOnceAndServes(t *testing.T) {
	s := newStore(t)
	topic := kafkatest.NewTopic(t, "ledger-events", 3)
	prod := kafkatest.NewClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	ctx := context.Background()

	produce := func(key string, v []byte) {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for i := range 20 {
		id := "T" + strconv.Itoa(i)
		for _, ev := range []messages.LedgerEvent{
			event(messages.LedgerEventID(id, "alice"), "alice", "USD", "-2.5", "AAPL", "1"),
			event(messages.LedgerEventID(id, "bob"), "bob", "USD", "2.5", "AAPL", "-1"),
		} {
			b, _ := json.Marshal(ev)
			produce(ev.AccountID, b)
			produce(ev.AccountID, b) // redelivery
			total += 2
		}
	}
	produce("alice", []byte(`garbage`))
	total++

	cl := kafkatest.NewClient(t, balance.ConsumerClientOpts(kafkatest.GroupID(t), topic)...)
	c := balance.NewConsumer(cl, s, slog.New(slog.DiscardHandler))
	var notified atomic.Int64
	c.OnApplied(func(messages.LedgerEvent) { notified.Add(1) })
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = c.Run(runCtx) }()
	defer func() { cancel(); wg.Wait() }()

	kafkatest.Eventually(t, 30*time.Second, "all records committed", func() bool {
		return c.Stats().Committed.Load() == int64(total)
	})
	st := c.Stats()
	if st.Applied.Load() != 40 || st.Duplicates.Load() != 40 || st.Malformed.Load() != 1 {
		t.Fatalf("applied=%d duplicates=%d malformed=%d, want 40 40 1",
			st.Applied.Load(), st.Duplicates.Load(), st.Malformed.Load())
	}
	if notified.Load() != 40 {
		t.Fatalf("OnApplied called %d times, want 40 (duplicates must not notify)", notified.Load())
	}
	requireBalance(t, s, "alice", "USD", "-50", 20)
	requireBalance(t, s, "bob", "AAPL", "-20", 20)

	srv := httptest.NewServer(balance.Handler(s, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/balances/alice")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body balance.BalancesResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || body.Balances["USD"] != "-50" || body.Balances["AAPL"] != "20" || body.Events != 20 {
		t.Fatalf("GET /balances/alice = %d %+v", resp.StatusCode, body)
	}
}
