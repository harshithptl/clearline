// Package dlq builds dead-letter records and replays them (SPEC F7).
//
// A dead-letter record is the original record (same key, value, and headers)
// on the DLQ topic, plus headers saying where it came from and why it failed.
// Replay republishes it to the original topic. That is safe because every
// consumer dedupes on event_id: replaying a message that did get processed
// is a no-op.
package dlq

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Header keys added to dead-letter records.
const (
	HeaderTopic     = "clearline-dlq-topic"
	HeaderPartition = "clearline-dlq-partition"
	HeaderOffset    = "clearline-dlq-offset"
	HeaderError     = "clearline-dlq-error"
	HeaderAttempts  = "clearline-dlq-attempts"
)

// NewRecord wraps a failed record for the DLQ topic.
func NewRecord(dlqTopic string, r *kgo.Record, cause error, attempts int) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(r.Headers)+5)
	for _, h := range r.Headers {
		if !isDLQHeader(h.Key) {
			headers = append(headers, h)
		}
	}
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderTopic, Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: HeaderPartition, Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: HeaderOffset, Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: HeaderError, Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: HeaderAttempts, Value: []byte(strconv.Itoa(attempts))},
	)
	return &kgo.Record{Topic: dlqTopic, Key: r.Key, Value: r.Value, Headers: headers}
}

// Header returns the value of header key on r, if present.
func Header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func isDLQHeader(k string) bool {
	switch k {
	case HeaderTopic, HeaderPartition, HeaderOffset, HeaderError, HeaderAttempts:
		return true
	}
	return false
}

// ErrNoOrigin: a DLQ record without HeaderTopic, so there is nowhere to
// replay it. Replay stops rather than guessing.
var ErrNoOrigin = errors.New("dlq record has no origin topic")

// Replay republishes every record currently on dlqTopic that group has not
// replayed yet, then returns how many it republished. Records are produced to
// their original topic (acks=all) before their DLQ offset is committed, so a
// crash mid-replay republishes some records twice, which consumers dedupe.
//
// cl must be a consumer of dlqTopic in group with auto-commit disabled (see
// ReplayClientOpts). Records that arrive after Replay starts are left for the
// next run.
func Replay(ctx context.Context, cl *kgo.Client, dlqTopic, group string) (int, error) {
	pending, err := pendingEnds(ctx, kadm.NewClient(cl), dlqTopic, group)
	if err != nil {
		return 0, err
	}
	n := 0
	for len(pending) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return n, err
		}
		if err := fetches.Err(); err != nil {
			return n, fmt.Errorf("dlq replay: fetch: %w", err)
		}
		var done []*kgo.Record
		for _, r := range fetches.Records() {
			end, ok := pending[r.Partition]
			if !ok || r.Offset >= end {
				continue // arrived after we started; leave for the next run
			}
			out, err := replayRecord(r)
			if err != nil {
				return n, err
			}
			if err := cl.ProduceSync(ctx, out).FirstErr(); err != nil {
				return n, fmt.Errorf("dlq replay: produce to %s: %w", out.Topic, err)
			}
			done = append(done, r)
			n++
			if r.Offset+1 >= end {
				delete(pending, r.Partition)
			}
		}
		if len(done) > 0 {
			if err := cl.CommitRecords(ctx, done...); err != nil {
				return n, fmt.Errorf("dlq replay: commit: %w", err)
			}
		}
	}
	return n, nil
}

// ReplayClientOpts are the client options Replay needs.
func ReplayClientOpts(dlqTopic, group string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(dlqTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
}

func replayRecord(r *kgo.Record) (*kgo.Record, error) {
	topic, ok := Header(r, HeaderTopic)
	if !ok || topic == "" {
		return nil, fmt.Errorf("%w: partition %d offset %d", ErrNoOrigin, r.Partition, r.Offset)
	}
	headers := make([]kgo.RecordHeader, 0, len(r.Headers))
	for _, h := range r.Headers {
		if !isDLQHeader(h.Key) {
			headers = append(headers, h)
		}
	}
	return &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value, Headers: headers}, nil
}

// pendingEnds returns, per partition with unreplayed records, the end offset
// as of now. A partition is unreplayed if the group's committed offset (or
// the log start, if none) is below the end.
func pendingEnds(ctx context.Context, adm *kadm.Client, topic, group string) (map[int32]int64, error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("dlq replay: end offsets: %w", err)
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("dlq replay: start offsets: %w", err)
	}
	committed, err := adm.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		return nil, fmt.Errorf("dlq replay: committed offsets: %w", err)
	}
	pending := map[int32]int64{}
	for p, end := range ends[topic] {
		if end.Err != nil {
			return nil, fmt.Errorf("dlq replay: partition %d: %w", p, end.Err)
		}
		from := int64(0)
		if s, ok := starts.Lookup(topic, p); ok && s.Err == nil {
			from = s.Offset
		}
		if c, ok := committed.Lookup(topic, p); ok && c.Err == nil && c.At >= 0 && c.At > from {
			from = c.At
		}
		if from < end.Offset {
			pending[p] = end.Offset
		}
	}
	return pending, nil
}
