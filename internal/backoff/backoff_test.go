package backoff_test

import (
	"context"
	"testing"
	"time"

	"github.com/harshithptl/clearline/internal/backoff"
)

func TestDelayGrowsAndCaps(t *testing.T) {
	p := backoff.Policy{Base: 100 * time.Millisecond, Max: time.Second}
	cases := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{4, 400 * time.Millisecond, 800 * time.Millisecond},
		{5, 500 * time.Millisecond, time.Second},
		{60, 500 * time.Millisecond, time.Second},
	}
	for _, c := range cases {
		for range 100 {
			if d := p.Delay(c.attempt); d < c.min || d > c.max {
				t.Fatalf("Delay(%d) = %v, want [%v, %v]", c.attempt, d, c.min, c.max)
			}
		}
	}
}

func TestSleepCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backoff.Sleep(ctx, time.Hour); err != context.Canceled {
		t.Fatalf("Sleep = %v, want Canceled", err)
	}
}
