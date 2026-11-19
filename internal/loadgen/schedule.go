// Package loadgen drives the gateway at a fixed order rate and summarizes
// the result.
//
// The schedule is open loop: request i is due at start + i/rate whether or
// not earlier requests have returned, and its latency is measured from that
// due time, not from when it was actually sent. A closed-loop tester (send,
// wait, send) slows down exactly when the system does and so hides the queue
// it would have built: "coordinated omission". Measuring from the due time
// charges that queueing to the system, as a real client would see it.
package loadgen

import (
	"context"
	"sync"
	"time"
)

// Sample is the outcome of one scheduled request.
type Sample struct {
	Due     time.Duration // offset from the run start when it was scheduled
	Latency time.Duration // from due time to response (includes any wait for a send slot)
	OK      bool
}

// SendFunc performs request i. It returns true on success.
type SendFunc func(ctx context.Context, i int) bool

// Schedule describes one constant-rate run.
type Schedule struct {
	Rate     float64       // requests per second
	Duration time.Duration // how long to keep scheduling
	// MaxInFlight bounds concurrent requests. When it is reached, due
	// requests wait for a slot and that wait counts as latency.
	MaxInFlight int
}

// Run executes s, calling send for each request, and returns one sample per
// scheduled request, in schedule order.
func Run(ctx context.Context, s Schedule, send SendFunc) []Sample {
	n := int(s.Rate * s.Duration.Seconds())
	if n <= 0 {
		return nil
	}
	interval := time.Duration(float64(time.Second) / s.Rate)
	samples := make([]Sample, n)
	slots := make(chan struct{}, max(1, s.MaxInFlight))
	var wg sync.WaitGroup
	start := time.Now()

	for i := range n {
		due := time.Duration(i) * interval
		if wait := time.Until(start.Add(due)); wait > 0 {
			select {
			case <-ctx.Done():
				wg.Wait()
				return samples[:i]
			case <-time.After(wait):
			}
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return samples[:i]
		}
		wg.Add(1)
		go func(i int, due time.Duration) {
			defer wg.Done()
			ok := send(ctx, i)
			samples[i] = Sample{Due: due, Latency: time.Since(start.Add(due)), OK: ok}
			<-slots
		}(i, due)
	}
	wg.Wait()
	return samples
}
