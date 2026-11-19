// Package gateway is the order gateway (SPEC F1, 3.2): a REST endpoint that
// validates orders and publishes them to the orders topic.
//
//	POST /orders   Idempotency-Key: <client chosen, 1-255 printable ASCII>
//	{"account_id":"alice","symbol":"AAPL","side":"buy","quantity":"10","price":"150.25"}
//
//	202  accepted: the order is durably in Kafka (acks=all)
//	400  malformed request or missing Idempotency-Key
//	409  the same key is still being processed; retry shortly
//	422  invalid order, unknown account, or key reused with a different order
//	503  Kafka, Valkey or Postgres unavailable; nothing was accepted, retry
//	     with the same key
//
// Retrying with the same Idempotency-Key never creates a second order, at two
// levels:
//
//  1. Valkey (fast path): the first request claims the key; retries get the
//     stored response back with Idempotent-Replayed: true.
//  2. The log (guarantee): order_id is derived from (account, key), so even
//     if the Valkey entry is lost, or a produce timed out but actually
//     landed, every copy carries the same order_id and the matcher adapter
//     matches only the first.
//
// Keys are scoped per account: two accounts using the same key do not collide.
package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/matcher"
	"github.com/harshithptl/clearline/internal/messages"
)

// HeaderIdempotencyKey is the request header carrying the client's key.
const HeaderIdempotencyKey = "Idempotency-Key"

// HeaderReplayed is set to "true" on responses served from the idempotency
// store instead of a new Kafka write.
const HeaderReplayed = "Idempotent-Replayed"

const (
	maxIdempotencyKeyLen = 255
	maxBodyBytes         = 8 << 10
	// cleanupTimeout bounds Valkey writes made after the request context may
	// already be gone (client hung up).
	cleanupTimeout = 2 * time.Second
)

// AccountChecker reports whether an account exists. *Accounts implements it.
type AccountChecker interface {
	Exists(ctx context.Context, id string) (bool, error)
}

// Config configures a Gateway.
type Config struct {
	// Producer writes to OrdersTopic. Create it with ProducerClientOpts.
	Producer    *kgo.Client
	OrdersTopic string
	Idempotency *Idempotency
	Accounts    AccountChecker
	Symbols     []matcher.SymbolSpec
	// ProduceTimeout bounds the Kafka write. Must be below the idempotency
	// pending TTL.
	ProduceTimeout time.Duration
	Logger         *slog.Logger
}

// Gateway serves the order API. Create with New.
type Gateway struct {
	cfg     Config
	symbols map[string]matcher.SymbolSpec
}

// New validates cfg.
func New(cfg Config) (*Gateway, error) {
	if cfg.Producer == nil || cfg.OrdersTopic == "" || cfg.Idempotency == nil || cfg.Accounts == nil {
		return nil, errors.New("gateway: Producer, OrdersTopic, Idempotency, Accounts are required")
	}
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("gateway: no symbols")
	}
	if cfg.ProduceTimeout <= 0 || cfg.ProduceTimeout >= cfg.Idempotency.pendingTTL {
		return nil, fmt.Errorf("gateway: produce timeout %s must be positive and below the pending TTL %s",
			cfg.ProduceTimeout, cfg.Idempotency.pendingTTL)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	g := &Gateway{cfg: cfg, symbols: map[string]matcher.SymbolSpec{}}
	for _, s := range cfg.Symbols {
		g.symbols[s.Symbol] = s
	}
	return g, nil
}

// ProducerClientOpts are the Kafka options the gateway relies on: acks=all
// (also the default, pinned here), idempotent producer (default), no linger,
// and records failed after timeout if they were never sent.
func ProducerClientOpts(timeout time.Duration) []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.RecordDeliveryTimeout(timeout),
	}
}

// Handler returns the HTTP routes.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", g.postOrder)
	mux.HandleFunc("GET /healthz", healthz)
	return mux
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

// OrderRequest is the POST /orders body. Numbers are decimal strings, never
// JSON numbers, so no client library rounds them through a float.
type OrderRequest struct {
	AccountID string `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Quantity  string `json:"quantity"`
	Price     string `json:"price"`
}

// OrderResponse is the 202 body. Partition and Offset locate the order in
// the orders log.
type OrderResponse struct {
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	AccountID string `json:"account_id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Quantity  string `json:"quantity"`
	Price     string `json:"price"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// ErrorResponse is the body of every non-2xx response.
type ErrorResponse struct {
	Error string `json:"error"`
}

func (g *Gateway) postOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idemKey := r.Header.Get(HeaderIdempotencyKey)
	if err := validIdempotencyKey(idemKey); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req, err := decodeRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	order, err := g.validate(req)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	order.OrderID = OrderID(order.AccountID, idemKey)

	exists, err := g.cfg.Accounts.Exists(ctx, order.AccountID)
	if err != nil {
		g.unavailable(w, "account lookup", err)
		return
	}
	if !exists {
		writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf("unknown account %q", order.AccountID))
		return
	}

	fp := fingerprint(order)
	c, existing, err := g.cfg.Idempotency.Claim(ctx, order.AccountID, idemKey, fp)
	if err != nil {
		g.unavailable(w, "idempotency", err)
		return
	}
	if existing != nil {
		replay(w, existing, fp)
		return
	}

	value, err := messages.EncodeOrder(order)
	if err != nil {
		g.release(c)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	pctx, cancel := context.WithTimeout(ctx, g.cfg.ProduceTimeout)
	rec := &kgo.Record{Topic: g.cfg.OrdersTopic, Key: []byte(order.Symbol), Value: value}
	err = g.cfg.Producer.ProduceSync(pctx, rec).FirstErr()
	cancel()
	if err != nil {
		// Possibly written anyway (timeout after send). Releasing is still
		// safe: a retry writes the same order_id and the adapter drops it.
		g.release(c)
		g.unavailable(w, "produce", err)
		return
	}

	body, err := json.Marshal(OrderResponse{
		OrderID: order.OrderID, Status: "accepted",
		AccountID: order.AccountID, Symbol: order.Symbol, Side: order.Side,
		Quantity: order.Quantity.String(), Price: order.Price.String(),
		Partition: rec.Partition, Offset: rec.Offset,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// The order is durable; the response is 202 whatever happens to the
	// cache write. A lost entry costs a duplicate write that the adapter drops.
	cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	ok, err := g.cfg.Idempotency.Complete(cctx, c, fp, http.StatusAccepted, body)
	ccancel()
	if err != nil || !ok {
		g.cfg.Logger.Warn("idempotency entry not completed", "order_id", order.OrderID, "owned", ok, "err", err)
	}
	writeJSON(w, http.StatusAccepted, body)
}

// validate parses req and applies the symbol's market rules.
func (g *Gateway) validate(req OrderRequest) (messages.ParsedOrder, error) {
	if err := ledger.ValidAccountID(req.AccountID); err != nil {
		return messages.ParsedOrder{}, fmt.Errorf("account_id %q must match %s", req.AccountID, ledger.AccountIDPattern)
	}
	spec, ok := g.symbols[req.Symbol]
	if !ok {
		return messages.ParsedOrder{}, fmt.Errorf("unknown symbol %q", req.Symbol)
	}
	if req.Side != messages.SideBuy && req.Side != messages.SideSell {
		return messages.ParsedOrder{}, fmt.Errorf("side %q must be %q or %q", req.Side, messages.SideBuy, messages.SideSell)
	}
	qty, err := decimal.NewFromString(req.Quantity)
	if err != nil {
		return messages.ParsedOrder{}, fmt.Errorf("quantity %q is not a decimal", req.Quantity)
	}
	price, err := decimal.NewFromString(req.Price)
	if err != nil {
		return messages.ParsedOrder{}, fmt.Errorf("price %q is not a decimal", req.Price)
	}
	o := messages.ParsedOrder{AccountID: req.AccountID, Symbol: req.Symbol, Side: req.Side, Quantity: qty, Price: price}
	if _, _, err := spec.CheckOrder(o); err != nil {
		return messages.ParsedOrder{}, err
	}
	return o, nil
}

// OrderID derives the order id from the account and the client's key, so
// every retry of one request carries the same id: "o-" + 128 bits of
// SHA-256(account NUL key). NUL cannot appear in either part, so distinct
// pairs never hash the same input.
func OrderID(account, idemKey string) string {
	sum := sha256.Sum256([]byte(account + "\x00" + idemKey))
	return "o-" + hex.EncodeToString(sum[:16])
}

// fingerprint identifies the order's content. Decimals are normalized
// (150.00 and 150 are the same price), so formatting differences in a retry
// do not count as a different request.
func fingerprint(o messages.ParsedOrder) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		o.AccountID, o.Symbol, o.Side, o.Quantity.String(), o.Price.String(),
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

// replay answers from an existing idempotency entry.
func replay(w http.ResponseWriter, e *entry, fp string) {
	switch {
	case e.Fingerprint != fp:
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key was already used for a different order")
	case e.State == statePending:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "a request with this Idempotency-Key is in progress")
	default:
		w.Header().Set(HeaderReplayed, "true")
		writeJSON(w, e.Status, e.Body)
	}
}

func (g *Gateway) release(c *claim) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := g.cfg.Idempotency.Release(ctx, c); err != nil {
		// The pending entry expires on its own; retries get 409 until then.
		g.cfg.Logger.Warn("idempotency release failed", "err", err)
	}
}

func (g *Gateway) unavailable(w http.ResponseWriter, what string, err error) {
	g.cfg.Logger.Warn("order not accepted", "stage", what, "err", err)
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, what+" unavailable; order not accepted, retry with the same Idempotency-Key")
}

func validIdempotencyKey(k string) error {
	if k == "" || len(k) > maxIdempotencyKeyLen {
		return fmt.Errorf("%s header is required (1 to %d characters)", HeaderIdempotencyKey, maxIdempotencyKeyLen)
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return fmt.Errorf("%s must be printable ASCII without spaces", HeaderIdempotencyKey)
		}
	}
	return nil
}

// decodeRequest reads exactly one JSON object with known fields only, so a
// typo like "qty" fails loudly instead of defaulting to empty.
func decodeRequest(w http.ResponseWriter, r *http.Request) (OrderRequest, error) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	var req OrderRequest
	if err := dec.Decode(&req); err != nil {
		return OrderRequest{}, fmt.Errorf("body: %w", err)
	}
	if dec.More() {
		return OrderRequest{}, errors.New("body: want exactly one JSON object")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return OrderRequest{}, errors.New("body: want exactly one JSON object")
	}
	return req, nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	b, _ := json.Marshal(ErrorResponse{Error: msg})
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body) //nolint:gosec // G705: JSON from json.Marshal, served as application/json with nosniff, never HTML
	_, _ = io.WriteString(w, "\n")
}
