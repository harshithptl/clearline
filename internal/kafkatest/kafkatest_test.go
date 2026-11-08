package kafkatest_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/kafkatest"
)

// The delivery contract the services rely on, checked against the real broker:
//   - acks=all produce succeeds
//   - records with one key stay in order (same partition)
//   - offsets advance only when we commit them explicitly
func TestRoundTripWithManualCommit(t *testing.T) {
	topic := kafkatest.NewTopic(t, "roundtrip", 3)
	group := kafkatest.GroupID(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prod := kafkatest.NewClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	const n = 10
	for i := range n {
		r := &kgo.Record{Topic: topic, Key: []byte("AAPL"), Value: []byte(fmt.Sprintf("T%d", i))}
		if err := prod.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	// First consumer: read everything but commit only the first 4 records.
	got := consume(t, ctx, topic, group, n, 4)
	for i, v := range got {
		if v != fmt.Sprintf("T%d", i) {
			t.Fatalf("record %d = %s, want T%d (order broken)", i, v, i)
		}
	}

	// Second consumer in the same group resumes after the last commit: the 6
	// uncommitted records are redelivered. This is at-least-once delivery, the
	// reason every consumer dedupes.
	again := consume(t, ctx, topic, group, n-4, 0)
	if again[0] != "T4" || again[len(again)-1] != "T9" {
		t.Fatalf("redelivered %v, want T4..T9", again)
	}
}

// consume reads want records as a member of group, commits the first
// commitN of them, and closes the client.
func consume(t *testing.T, ctx context.Context, topic, group string, want, commitN int) []string {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkatest.Brokers()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cl.Close()

	var recs []*kgo.Record
	for len(recs) < want {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("timed out with %d/%d records", len(recs), want)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch: %v", errs[0].Err)
		}
		recs = append(recs, fetches.Records()...)
	}
	if commitN > 0 {
		if err := cl.CommitRecords(ctx, recs[:commitN]...); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Value)
	}
	return out
}
