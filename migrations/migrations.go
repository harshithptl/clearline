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
)

//go:embed *.sql
var files embed.FS

// Apply runs all pending migrations and returns the resulting schema version.
func Apply(ctx context.Context, db *sql.DB) (int64, error) {
	p, err := goose.NewProvider(goose.DialectPostgres, db, files)
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
