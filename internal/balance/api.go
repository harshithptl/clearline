package balance

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// BalancesResponse is the body of GET /balances/{account}.
type BalancesResponse struct {
	AccountID string            `json:"account_id"`
	Balances  map[string]string `json:"balances"` // asset -> decimal string
	Events    int64             `json:"events"`   // ledger events applied so far
}

// Handler serves the read API:
//
//	GET /balances/{account}  current cached balances
//	GET /healthz             Valkey reachable
func Handler(store *Store) http.Handler {
	h := &api{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /balances/{account}", h.balances)
	mux.HandleFunc("GET /healthz", h.healthz)
	return mux
}

type api struct{ store *Store }

func (h *api) balances(w http.ResponseWriter, r *http.Request) {
	account := r.PathValue("account")
	snap, err := h.store.Get(r.Context(), account)
	if err != nil {
		slog.Error("read balances", "account", account, "err", err)
		http.Error(w, "balances unavailable", http.StatusServiceUnavailable)
		return
	}
	resp := BalancesResponse{AccountID: account, Balances: map[string]string{}, Events: snap.Events}
	for asset, v := range snap.Balances {
		resp.Balances[asset] = v.String()
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Warn("write response", "err", err)
	}
}

func (h *api) healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.store.rdb.Ping(r.Context()).Err(); err != nil {
		http.Error(w, "valkey unreachable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
