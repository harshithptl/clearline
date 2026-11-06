// Package migrations embeds the SQL schema migrations and applies them in order.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
)

//go:embed *.sql
var files embed.FS

// Apply runs every embedded migration in filename order, each in its own
// transaction. It is meant for fresh databases (tests, first boot); there is
// no version tracking yet.
func Apply(ctx context.Context, conn *pgx.Conn) error {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	sort.Strings(names)

	for _, name := range names {
		sql, err := files.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, string(sql))
			return err
		}); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}
