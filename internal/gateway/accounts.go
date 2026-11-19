package gateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Accounts answers "does this account exist?" from Postgres.
//
// Why the gateway checks: an order from an unknown account would match, and
// settlement would then fail the whole trade (ErrUnknownAccount, then DLQ),
// taking the real counterparty's fill with it. Rejecting at the edge keeps a
// bad client from poisoning someone else's trade.
//
// Only positive answers are cached. Accounts are never deleted (ledger rows
// reference them), so "exists" cannot go stale, while "missing" can change
// the moment an account is created.
type Accounts struct {
	pool *pgxpool.Pool

	mu    sync.RWMutex
	known map[string]struct{}
}

// maxKnownAccounts bounds the cache; past it, lookups go to Postgres.
const maxKnownAccounts = 100_000

// NewAccounts returns a checker backed by pool.
func NewAccounts(pool *pgxpool.Pool) *Accounts {
	return &Accounts{pool: pool, known: map[string]struct{}{}}
}

// Exists reports whether account id exists.
func (a *Accounts) Exists(ctx context.Context, id string) (bool, error) {
	a.mu.RLock()
	_, ok := a.known[id]
	a.mu.RUnlock()
	if ok {
		return true, nil
	}
	var exists bool
	err := a.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM accounts WHERE account_id = $1)`, id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("account lookup: %w", err)
	}
	if exists {
		a.mu.Lock()
		if len(a.known) < maxKnownAccounts {
			a.known[id] = struct{}{}
		}
		a.mu.Unlock()
	}
	return exists, nil
}
