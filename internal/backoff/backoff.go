// Package backoff computes retry delays and sleeps them, honoring context
// cancellation.
package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

// Policy is exponential backoff with jitter: attempt n waits roughly
// Base * 2^(n-1), capped at Max. Half the delay is random so many failing
// consumers do not retry in lockstep.
type Policy struct {
	Base time.Duration
	Max  time.Duration
}

// Delay returns the wait before retry number attempt (1-based).
func (p Policy) Delay(attempt int) time.Duration {
	d := p.Max
	if attempt < 1 {
		attempt = 1
	}
	if attempt < 31 {
		if exp := p.Base << (attempt - 1); exp > 0 && exp < p.Max {
			d = exp
		}
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1)
}

// Sleep waits for d or until ctx is done, returning ctx.Err() in that case.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
