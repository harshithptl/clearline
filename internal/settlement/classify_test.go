package settlement

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/harshithptl/clearline/internal/ledger"
	"github.com/harshithptl/clearline/internal/messages"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want failureClass
	}{
		{"invalid trade", fmt.Errorf("x: %w", ledger.ErrInvalidTrade), permanent},
		{"malformed", fmt.Errorf("x: %w", messages.ErrMalformed), permanent},
		{"unknown account", fmt.Errorf("x: %w", ErrUnknownAccount), messageLevel},
		{"numeric overflow", fmt.Errorf("x: %w", &pgconn.PgError{Code: "22003"}), messageLevel},
		{"check violation", fmt.Errorf("x: %w", &pgconn.PgError{Code: "23514"}), messageLevel},
		{"admin shutdown", fmt.Errorf("x: %w", &pgconn.PgError{Code: "57P01"}), infrastructure},
		{"connection refused", errors.New("dial tcp: connection refused"), infrastructure},
		{"deadline", context.DeadlineExceeded, infrastructure},
	}
	for _, c := range cases {
		if got := classify(c.err); got != c.want {
			t.Errorf("%s: classify = %d, want %d", c.name, got, c.want)
		}
	}
}
