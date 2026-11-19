package balance

import (
	"sync"

	"github.com/harshithptl/clearline/internal/messages"
)

// Hub fans applied ledger events out to live subscribers (the /events
// stream). It is a notification channel, not a source of truth: balances are
// always read from the Store.
//
// Publish never blocks the consumer. A subscriber whose buffer is full is
// dropped (its channel closed); a browser reconnects and refetches, so a slow
// tab costs itself a refresh instead of costing everyone consumer lag.
type Hub struct {
	mu   sync.Mutex
	subs map[chan messages.LedgerEvent]struct{}
	buf  int
}

// NewHub returns a hub whose subscribers buffer up to buf events.
func NewHub(buf int) *Hub {
	return &Hub{subs: map[chan messages.LedgerEvent]struct{}{}, buf: buf}
}

// Subscribe returns a channel of events applied from now on and a function
// that unsubscribes. The channel is closed on unsubscribe or if the
// subscriber falls more than buf events behind.
func (h *Hub) Subscribe() (<-chan messages.LedgerEvent, func()) {
	ch := make(chan messages.LedgerEvent, h.buf)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() { h.drop(ch) }
}

// Publish delivers ev to every subscriber that has room and drops the rest.
func (h *Hub) Publish(ev messages.LedgerEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// Subscribers returns the number of live subscribers.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

func (h *Hub) drop(ch chan messages.LedgerEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
}
