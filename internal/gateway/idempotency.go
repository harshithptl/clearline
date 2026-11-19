package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Idempotency stores one entry per (account, Idempotency-Key) in Valkey:
//
//	pending  a request holds the key and is producing to Kafka (short TTL)
//	done     the order is in Kafka; the stored response is replayed (long TTL)
//
// It is a cache of responses, not the correctness guarantee. If an entry is
// lost (expiry, Valkey failover, a gateway crash while pending), a retry
// produces the order again with the same order_id and the matcher adapter
// drops the copy. What this store adds is the same response for every retry
// and no second Kafka write in the common case.
type Idempotency struct {
	rdb        *redis.Client
	prefix     string
	pendingTTL time.Duration
	doneTTL    time.Duration
}

// NewIdempotency returns a store with keys under prefix. pendingTTL bounds
// how long a crashed request blocks retries; it must exceed the produce
// timeout so a slow but successful request still finds its own entry.
func NewIdempotency(rdb *redis.Client, prefix string, pendingTTL, doneTTL time.Duration) *Idempotency {
	return &Idempotency{rdb: rdb, prefix: prefix, pendingTTL: pendingTTL, doneTTL: doneTTL}
}

// Entry states.
const (
	statePending = "pending"
	stateDone    = "done"
)

// entry is the stored JSON value.
type entry struct {
	State string `json:"state"`
	// Fingerprint identifies the request body, so reusing a key with a
	// different order is detected instead of silently replayed.
	Fingerprint string `json:"fingerprint"`
	// Token makes a pending entry unique to the request that claimed it, so
	// only that request can complete or release it.
	Token  string          `json:"token,omitempty"`
	Status int             `json:"status,omitempty"`
	Body   json.RawMessage `json:"body,omitempty"`
}

// claim is a pending entry this request owns.
type claim struct {
	key   string
	value string
}

// key is <prefix>idem:{<account>}:<idempotency key>. The braces are a cluster
// hash tag, like the balance keys.
func (s *Idempotency) key(account, idemKey string) string {
	return s.prefix + "idem:{" + account + "}:" + idemKey
}

// errClaimRace: the entry kept appearing and disappearing under us.
var errClaimRace = errors.New("idempotency entry changed during claim")

// Claim takes the key for this request, or returns the entry already there.
// Exactly one of the results is non-nil when err is nil.
func (s *Idempotency) Claim(ctx context.Context, account, idemKey, fingerprint string) (*claim, *entry, error) {
	k := s.key(account, idemKey)
	tok := make([]byte, 16)
	if _, err := rand.Read(tok); err != nil {
		return nil, nil, err
	}
	v, err := json.Marshal(entry{State: statePending, Fingerprint: fingerprint, Token: hex.EncodeToString(tok)})
	if err != nil {
		return nil, nil, err
	}
	// The entry can expire between a failed SET NX and the GET; try again.
	for range 3 {
		ok, err := s.rdb.SetNX(ctx, k, v, s.pendingTTL).Result()
		if err != nil {
			return nil, nil, fmt.Errorf("idempotency claim: %w", err)
		}
		if ok {
			return &claim{key: k, value: string(v)}, nil, nil
		}
		raw, err := s.rdb.Get(ctx, k).Bytes()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("idempotency read: %w", err)
		}
		var e entry
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, nil, fmt.Errorf("idempotency entry %s: %w", k, err)
		}
		return nil, &e, nil
	}
	return nil, nil, errClaimRace
}

// Compare-and-set scripts: act only if the key still holds our pending value.
// Atomic in Valkey, so a request whose entry expired and was reclaimed by a
// retry cannot overwrite or delete the retry's entry.
var (
	completeScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return 1
end
return 0`)
	releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)
)

// Complete replaces the pending entry with the final response. It reports
// false if the entry was no longer ours (it expired first).
func (s *Idempotency) Complete(ctx context.Context, c *claim, fingerprint string, status int, body []byte) (bool, error) {
	v, err := json.Marshal(entry{State: stateDone, Fingerprint: fingerprint, Status: status, Body: body})
	if err != nil {
		return false, err
	}
	n, err := completeScript.Run(ctx, s.rdb, []string{c.key}, c.value, string(v), s.doneTTL.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("idempotency complete: %w", err)
	}
	return n == 1, nil
}

// Release deletes the pending entry so the client can retry right away.
func (s *Idempotency) Release(ctx context.Context, c *claim) error {
	if err := releaseScript.Run(ctx, s.rdb, []string{c.key}, c.value).Err(); err != nil {
		return fmt.Errorf("idempotency release: %w", err)
	}
	return nil
}
