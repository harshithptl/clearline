// Package topics defines Clearline's Kafka topics (SPEC 3.3) and creates them.
//
// Brokers run with auto-create disabled, so a topic exists only if Ensure
// created it. A producer typo fails loudly instead of creating a stray topic.
package topics

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Topic names. Keys are noted per SPEC 3.3; ordering is per partition, so the
// key decides what stays in order.
const (
	Orders       = "orders"        // key: symbol
	Trades       = "trades"        // key: symbol
	LedgerEvents = "ledger-events" // key: account
	TradesDLQ    = "trades.dlq"    // key: original key
)

// Spec describes one topic to create.
type Spec struct {
	Name       string
	Partitions int32
}

// All is every Clearline topic. Partition counts can grow later but never
// shrink, and growing one remaps keys to partitions, so start modest.
var All = []Spec{
	{Orders, 3},
	{Trades, 3},
	{LedgerEvents, 6},
	{TradesDLQ, 1},
}

// Ensure creates each topic that does not exist yet, then waits until every
// topic has all partitions with a leader. Existing topics are left untouched,
// so it is safe to run on every deploy.
//
// The wait matters: topic creation returns before the metadata has
// propagated, and a client that produces in that window gets
// UNKNOWN_TOPIC_OR_PARTITION.
func Ensure(ctx context.Context, adm *kadm.Client, replicationFactor int16, specs ...Spec) error {
	names := make([]string, 0, len(specs))
	for _, s := range specs {
		_, err := adm.CreateTopic(ctx, s.Partitions, replicationFactor, nil, s.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
		names = append(names, s.Name)
	}
	return waitReady(ctx, adm, names)
}

const readyPoll = 100 * time.Millisecond

func waitReady(ctx context.Context, adm *kadm.Client, names []string) error {
	for {
		details, err := adm.ListTopics(ctx, names...)
		if err == nil && allReady(details, names) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("topics %v not ready: %w", names, ctx.Err())
		case <-time.After(readyPoll):
		}
	}
}

// allReady reports whether every named topic exists with at least one
// partition and every partition has a leader.
func allReady(details kadm.TopicDetails, names []string) bool {
	for _, n := range names {
		d, ok := details[n]
		if !ok || d.Err != nil || len(d.Partitions) == 0 {
			return false
		}
		for _, p := range d.Partitions {
			if p.Err != nil || p.Leader < 0 {
				return false
			}
		}
	}
	return true
}
