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

// Configs apply on create and are re-applied to an existing topic.
func TestEnsureAppliesConfigs(t *testing.T) {
	adm := kadm.NewClient(kafkatest.NewClient(t))
	ctx := context.Background()
	name := kafkatest.NewTopic(t, "configs", 1) // created without configs
	spec := topics.Spec{Name: name, Partitions: 1, Configs: map[string]string{"retention.ms": topics.Forever}}
	if err := topics.Ensure(ctx, adm, 1, spec); err != nil {
		t.Fatal(err)
	}
	cfgs, err := adm.DescribeTopicConfigs(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	c, err := cfgs.On(name, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range c.Configs {
		if kv.Key == "retention.ms" {
			if kv.Value == nil || *kv.Value != topics.Forever {
				t.Fatalf("retention.ms = %v, want %s", kv.MaybeValue(), topics.Forever)
			}
			return
		}
	}
	t.Fatal("retention.ms not described")
}

func TestOrdersKeptForever(t *testing.T) {
	for _, s := range topics.All {
		if s.Name == topics.Orders && s.Configs["retention.ms"] != topics.Forever {
			t.Fatalf("orders retention = %q; replay needs every order", s.Configs["retention.ms"])
		}
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
