package topics_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/kafkatest"
	"github.com/harshithptl/clearline/internal/topics"
)

// Ensure on an existing topic must succeed and change nothing: cmd/topics runs
// on every deploy.
func TestEnsureIsIdempotent(t *testing.T) {
	name := kafkatest.NewTopic(t, "ensure", 3)
	adm := kadm.NewClient(kafkatest.NewClient(t))
	ctx := context.Background()

	// Ask for 5 partitions the second time: an existing topic is left alone.
	if err := topics.Ensure(ctx, adm, 1, topics.Spec{Name: name, Partitions: 5}); err != nil {
		t.Fatalf("second Ensure: %v", err)
	}
	details, err := adm.ListTopics(ctx, name)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := len(details[name].Partitions); got != 3 {
		t.Fatalf("partitions = %d, want 3", got)
	}
}

// The broker must refuse to auto-create topics, so a typo in a topic name
// fails instead of silently writing to a new topic nobody reads.
func TestAutoCreateDisabled(t *testing.T) {
	cl := kafkatest.NewClient(t, kgo.UnknownTopicRetries(1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	missing := "does-not-exist-" + kafkatest.GroupID(t)
	err := cl.ProduceSync(ctx, &kgo.Record{Topic: missing, Value: []byte("x")}).FirstErr()
	if err == nil {
		t.Fatal("produce to missing topic succeeded; auto-create must be off")
	}

	details, lerr := kadm.NewClient(cl).ListTopics(context.Background(), missing)
	if lerr != nil {
		t.Fatalf("list: %v", lerr)
	}
	if details[missing].Err == nil {
		t.Fatalf("topic %s exists after produce", missing)
	}
}

func TestAllTopicsDefined(t *testing.T) {
	want := map[string]bool{topics.Orders: true, topics.Trades: true, topics.LedgerEvents: true, topics.TradesDLQ: true}
	for _, s := range topics.All {
		if !want[s.Name] {
			t.Errorf("unexpected topic %s", s.Name)
		}
		if s.Partitions < 1 {
			t.Errorf("%s has %d partitions", s.Name, s.Partitions)
		}
		delete(want, s.Name)
	}
	if len(want) != 0 {
		t.Errorf("topics missing from All: %v", want)
	}
}
