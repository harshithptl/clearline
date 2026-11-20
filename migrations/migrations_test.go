package migrations_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/harshithptl/clearline/internal/pgtest"
	"github.com/harshithptl/clearline/migrations"
)

// Latest is the highest migration number. Bump it when adding a migration.
const latest = 4

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

// Concurrent Apply calls on an EMPTY database (many pods starting at once)
// all succeed and report the latest version. The session lock serializes
// them. (Removing the lock did not make this test fail in practice: goose's
// per-migration transactions coped here. The lock stays as goose's
// recommended guard for concurrent deploys, not because this test proves it.)
func TestApplyConcurrently(t *testing.T) {
	db := pgtest.NewEmptyDB(t)
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := migrations.Apply(context.Background(), db)
			if err == nil && v != latest {
				err = fmt.Errorf("version %d, want %d", v, latest)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
