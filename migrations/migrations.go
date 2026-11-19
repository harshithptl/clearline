// Package migrations embeds the SQL schema migrations and applies them with
// goose, which records applied versions in the goose_db_version table. Apply
// only runs migrations the database has not seen, so it is safe to call on
// every startup.
//
// Rule: never edit a migration that has been applied anywhere. Add a new,
// higher-numbered file instead.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed *.sql
var files embed.FS

// Apply runs all pending migrations and returns the resulting schema version.
//
// It holds a Postgres advisory lock (goose's session locker) while it runs,
// so concurrent callers serialize. On Kubernetes every app pod runs migrate
// as an init container; the first one applies, and the rest wait, then find
// nothing to do.
func Apply(ctx context.Context, db *sql.DB) (int64, error) {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return 0, fmt.Errorf("migrations: locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, files, goose.WithSessionLocker(locker))
	if err != nil {
		return 0, fmt.Errorf("migrations: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return 0, fmt.Errorf("migrations: up: %w", err)
	}
	v, err := p.GetDBVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("migrations: version: %w", err)
	}
	return v, nil
}
