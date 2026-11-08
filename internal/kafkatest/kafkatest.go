// Package kafkatest gives each test its own Kafka topics, like pgtest does for
// databases. Topics get a random suffix and are deleted when the test ends.
//
// Connects to CLEARLINE_TEST_KAFKA_BROKERS (default localhost:9092, the
// compose broker). Requires compose Kafka to be running.
package kafkatest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/topics"
)

// Brokers returns the broker addresses tests should use.
func Brokers() []string {
	if b := os.Getenv("CLEARLINE_TEST_KAFKA_BROKERS"); b != "" {
		return strings.Split(b, ",")
	}
	return []string{"localhost:9092"}
}

// NewClient returns a client for the test brokers, closed on cleanup.
func NewClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(Brokers()...)}, opts...)...)
	if err != nil {
		t.Fatalf("kafkatest: new client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// NewTopic creates a uniquely named topic with the given partition count and
// deletes it on cleanup. base is a readable prefix, e.g. "trades".
func NewTopic(t *testing.T, base string, partitions int32) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name := base + "-test-" + randomSuffix(t)
	adm := kadm.NewClient(NewClient(t))
	if err := topics.Ensure(ctx, adm, 1, topics.Spec{Name: name, Partitions: partitions}); err != nil {
		t.Fatalf("kafkatest: %v (is compose Kafka up?)", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if _, err := adm.DeleteTopics(cctx, name); err != nil {
			t.Errorf("kafkatest: delete topic %s: %v", name, err)
		}
	})
	return name
}

// GroupID returns a unique consumer group name so tests never share offsets.
func GroupID(t *testing.T) string {
	t.Helper()
	return "test-group-" + randomSuffix(t)
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("kafkatest: random: %v", err)
	}
	return hex.EncodeToString(b)
}
