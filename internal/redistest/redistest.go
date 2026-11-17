// Package redistest gives each test its own Valkey key prefix, like pgtest
// does for databases. Keys under the prefix are deleted when the test ends.
//
// Connects to CLEARLINE_TEST_REDIS_ADDR (default localhost:6379, the compose
// Valkey).
package redistest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Addr returns the Valkey address tests should use.
func Addr() string {
	if a := os.Getenv("CLEARLINE_TEST_REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

// New returns a client and a unique key prefix. Keys under the prefix are
// deleted and the client closed on cleanup.
func New(t *testing.T) (*redis.Client, string) {
	t.Helper()
	cl := redis.NewClient(&redis.Options{Addr: Addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Fatalf("redistest: ping %s (is compose Valkey up?): %v", Addr(), err)
	}
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	prefix := "test-" + hex.EncodeToString(b) + ":"
	t.Cleanup(func() {
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		iter := cl.Scan(ctx, 0, prefix+"*", 500).Iterator()
		for iter.Next(ctx) {
			cl.Del(ctx, iter.Val())
		}
		if err := iter.Err(); err != nil {
			t.Errorf("redistest: cleanup %s: %v", prefix, err)
		}
	})
	return cl, prefix
}
