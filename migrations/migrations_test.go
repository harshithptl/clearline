package migrations_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/migrations"
)

// Latest is the highest migration number. Bump it when adding a migration.
const latest = 3

// pgtest.NewDB has already applied every migration. Applying again must be a
// no-op that reports the same version: services call Apply on every start.
func TestApplyIsIdempotent(t *testing.T) {
	conn := pgtest.NewDB(t)
	db := stdlib.OpenDB(*conn.Config())
	defer db.Close()

	for range 2 {
		v, err := migrations.Apply(context.Background(), db)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if v != latest {
			t.Fatalf("version = %d, want %d", v, latest)
		}
	}
}

// goose records each applied version exactly once.
func TestVersionsRecorded(t *testing.T) {
	conn := pgtest.NewDB(t)
	var n int
	err := conn.QueryRow(context.Background(),
		`SELECT count(*) FROM goose_db_version WHERE version_id > 0 AND is_applied`).Scan(&n)
	if err != nil {
		t.Fatalf("query goose_db_version: %v", err)
	}
	if n != latest {
		t.Fatalf("%d versions recorded, want %d", n, latest)
	}
}
