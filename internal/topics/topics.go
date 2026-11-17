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
	// Configs are topic-level settings, applied on create and re-applied to
	// existing topics on every Ensure.
	Configs map[string]string
}

// Forever is the retention.ms value meaning "never delete".
const Forever = "-1"

// All is every Clearline topic. Partition counts can grow later but never
// shrink, and growing one remaps keys to partitions, so start modest.
//
// orders is kept forever: the matching engine holds its books in memory, and
// the matcher adapter rebuilds them by replaying orders from offset 0
// (docs/decisions/0002-matcher-replay.md). trades.dlq is kept forever so
// nothing waiting for a fix and replay ages out.
var All = []Spec{
	{Name: Orders, Partitions: 3, Configs: map[string]string{"retention.ms": Forever}},
	{Name: Trades, Partitions: 3},
	{Name: LedgerEvents, Partitions: 6},
	{Name: TradesDLQ, Partitions: 1, Configs: map[string]string{"retention.ms": Forever}},
}

// Ensure creates each topic that does not exist yet, sets each Spec's
// Configs, then waits until every topic has all partitions with a leader.
// Partition counts of existing topics are left untouched, so it is safe to
// run on every deploy.
//
// The wait matters: topic creation returns before the metadata has
// propagated, and a client that produces in that window gets
// UNKNOWN_TOPIC_OR_PARTITION.
func Ensure(ctx context.Context, adm *kadm.Client, replicationFactor int16, specs ...Spec) error {
	names := make([]string, 0, len(specs))
	for _, s := range specs {
		configs := make(map[string]*string, len(s.Configs))
		for k, v := range s.Configs {
			configs[k] = &v
		}
		_, err := adm.CreateTopic(ctx, s.Partitions, replicationFactor, configs, s.Name)
		if errors.Is(err, kerr.TopicAlreadyExists) {
			err = alterConfigs(ctx, adm, s)
		}
		if err != nil {
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
		names = append(names, s.Name)
	}
	return waitReady(ctx, adm, names)
}

// alterConfigs sets s.Configs on an existing topic, leaving other settings alone.
func alterConfigs(ctx context.Context, adm *kadm.Client, s Spec) error {
	if len(s.Configs) == 0 {
		return nil
	}
	alters := make([]kadm.AlterConfig, 0, len(s.Configs))
	for k, v := range s.Configs {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &v})
	}
	resps, err := adm.AlterTopicConfigs(ctx, alters, s.Name)
	if err != nil {
		return fmt.Errorf("alter configs: %w", err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return fmt.Errorf("alter configs: %w", r.Err)
		}
	}
	return nil
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
