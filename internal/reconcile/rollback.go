package reconcile

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// rollback is for defer: it undoes tx unless it was committed. After a
// commit it returns pgx.ErrTxClosed, and any other error means the
// connection is already broken, which the caller has reported.
func rollback(ctx context.Context, tx pgx.Tx) {
	_ = tx.Rollback(ctx)
}
