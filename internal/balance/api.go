package balance

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// BalancesResponse is the body of GET /balances/{account} and one entry of
// GET /balances.
type BalancesResponse struct {
	AccountID string            `json:"account_id"`
	Balances  map[string]string `json:"balances"` // asset -> decimal string
	Events    int64             `json:"events"`   // ledger events applied so far
}

// ListResponse is the body of GET /balances.
type ListResponse struct {
	Accounts []BalancesResponse `json:"accounts"`
}

// MaxListAccounts caps GET /balances.
const MaxListAccounts = 500

// keepAlive is how often an idle /events stream sends a comment line, so
// proxies and load balancers do not close it.
const keepAlive = 15 * time.Second

// Handler serves the read API:
//
//	GET /balances            every cached account (up to MaxListAccounts)
//	GET /balances/{account}  one account
//	GET /events              Server-Sent Events: one "ledger" event per applied
//	                         ledger event (only if hub is non-nil)
//	GET /healthz             Valkey reachable
func Handler(store *Store, hub *Hub) *http.ServeMux {
	h := &api{store: store, hub: hub}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /balances", h.list)
	mux.HandleFunc("GET /balances/{account}", h.balances)
	if hub != nil {
		mux.HandleFunc("GET /events", h.events)
	}
	mux.HandleFunc("GET /healthz", h.healthz)
	return mux
}

type api struct {
	store *Store
	hub   *Hub
}

func (h *api) balances(w http.ResponseWriter, r *http.Request) {
	account := r.PathValue("account")
	snap, err := h.store.Get(r.Context(), account)
	if err != nil {
		slog.Error("read balances", "account", account, "err", err)
		http.Error(w, "balances unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, toResponse(account, snap))
}

func (h *api) list(w http.ResponseWriter, r *http.Request) {
	accounts, err := h.store.Accounts(r.Context(), MaxListAccounts)
	if err == nil {
		var snaps map[string]Snapshot
		snaps, err = h.store.GetMany(r.Context(), accounts)
		if err == nil {
			resp := ListResponse{Accounts: make([]BalancesResponse, 0, len(accounts))}
			for _, a := range accounts {
				resp.Accounts = append(resp.Accounts, toResponse(a, snaps[a]))
			}
			writeJSON(w, resp)
			return
		}
	}
	slog.Error("list balances", "err", err)
	http.Error(w, "balances unavailable", http.StatusServiceUnavailable)
}

// events streams applied ledger events as Server-Sent Events. If this client
// falls behind, the hub drops it and the stream ends; EventSource reconnects
// on its own after the advertised retry delay, and the page refetches.
func (h *api) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch, unsubscribe := h.hub.Subscribe()
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // tell nginx not to buffer the stream
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "retry: 2000\n\n")
	flusher.Flush()

	tick := time.NewTicker(keepAlive)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, open := <-ch:
			if !open {
				return // dropped for falling behind
			}
			b, err := json.Marshal(ev)
			if err != nil {
				slog.Error("encode ledger event", "err", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "event: ledger\nid: %s\ndata: %s\n\n", ev.EventID, b); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (h *api) healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.store.rdb.Ping(r.Context()).Err(); err != nil {
		http.Error(w, "valkey unreachable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func toResponse(account string, snap Snapshot) BalancesResponse {
	resp := BalancesResponse{AccountID: account, Balances: map[string]string{}, Events: snap.Events}
	for asset, v := range snap.Balances {
		resp.Balances[asset] = v.String()
	}
	return resp
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("write response", "err", err)
	}
}
