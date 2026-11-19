package messages

import (
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Timing headers carry the start of each SPEC 7.1 latency through the
// pipeline, so the service at the end can measure it:
//
//	order to ledger:   HeaderOrderTime  gateway produce -> adapter copies to trade -> settlement commit
//	balance freshness: HeaderCommitTime outbox row commit -> relay copies to event -> balance in Valkey
//
// Values are Unix microseconds in decimal. Both ends read the same host's
// clock in the compose and single-node deployments; across hosts the
// measurement includes clock skew (negative values are dropped).
const (
	HeaderOrderTime  = "clearline-order-time"
	HeaderCommitTime = "clearline-commit-time"
)

// TimeHeader encodes t for a header value.
func TimeHeader(key string, t time.Time) kgo.RecordHeader {
	return kgo.RecordHeader{Key: key, Value: []byte(strconv.FormatInt(t.UnixMicro(), 10))}
}

// HeaderTime returns the time in header key, or the zero time if absent or
// malformed.
func HeaderTime(headers []kgo.RecordHeader, key string) time.Time {
	for _, h := range headers {
		if h.Key != key {
			continue
		}
		us, err := strconv.ParseInt(string(h.Value), 10, 64)
		if err != nil || us <= 0 {
			return time.Time{}
		}
		return time.UnixMicro(us)
	}
	return time.Time{}
}

// WithHeader returns headers with key set to h, replacing any existing value.
func WithHeader(headers []kgo.RecordHeader, h kgo.RecordHeader) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(headers)+1)
	for _, x := range headers {
		if x.Key != h.Key {
			out = append(out, x)
		}
	}
	return append(out, h)
}
