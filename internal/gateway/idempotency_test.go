package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/harshithptl/clearline/internal/redistest"
)

// The claim protocol in isolation, including the case that makes the
// compare-and-set scripts necessary:
//
//	A claims, stalls past the pending TTL
//	entry expires, B claims
//	A wakes up: Complete and Release must not touch B's entry
func TestClaimLifecycle(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := redistest.New(t)
	s := NewIdempotency(rdb, prefix, 300*time.Millisecond, time.Hour)

	a, existing, err := s.Claim(ctx, "alice", "k1", "fp")
	if err != nil || a == nil || existing != nil {
		t.Fatalf("first claim = %v, %v, %v", a, existing, err)
	}
	_, existing, err = s.Claim(ctx, "alice", "k1", "fp")
	if err != nil || existing == nil || existing.State != statePending {
		t.Fatalf("second claim while pending = %+v, %v", existing, err)
	}

	time.Sleep(400 * time.Millisecond) // A stalls; its entry expires
	b, _, err := s.Claim(ctx, "alice", "k1", "fp")
	if err != nil || b == nil {
		t.Fatalf("claim after expiry = %v, %v", b, err)
	}
	ok, err := s.Complete(ctx, a, "fp", 202, []byte(`{"from":"a"}`))
	if err != nil || ok {
		t.Fatalf("stale Complete = %v, %v; want false", ok, err)
	}
	if err := s.Release(ctx, a); err != nil {
		t.Fatal(err)
	}
	_, existing, _ = s.Claim(ctx, "alice", "k1", "fp")
	if existing == nil || existing.State != statePending {
		t.Fatalf("stale Release removed the new owner's entry: %+v", existing)
	}

	ok, err = s.Complete(ctx, b, "fp", 202, []byte(`{"from":"b"}`))
	if err != nil || !ok {
		t.Fatalf("owner Complete = %v, %v", ok, err)
	}
	_, existing, _ = s.Claim(ctx, "alice", "k1", "fp")
	if existing == nil || existing.State != stateDone || existing.Status != 202 || string(existing.Body) != `{"from":"b"}` {
		t.Fatalf("done entry = %+v", existing)
	}
	ttl := rdb.PTTL(ctx, s.key("alice", "k1")).Val()
	if ttl < 59*time.Minute {
		t.Fatalf("done entry TTL %s, want about 1h", ttl)
	}
}

func TestReleaseLetsClientRetry(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := redistest.New(t)
	s := NewIdempotency(rdb, prefix, time.Minute, time.Hour)
	c, _, err := s.Claim(ctx, "alice", "k", "fp")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, c); err != nil {
		t.Fatal(err)
	}
	if c2, _, err := s.Claim(ctx, "alice", "k", "fp"); err != nil || c2 == nil {
		t.Fatalf("claim after release = %v, %v", c2, err)
	}
}
