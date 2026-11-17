// Command migrate applies pending schema migrations to a Postgres database.
//
//	go run ./cmd/migrate
//
// The database URL comes from CLEARLINE_DATABASE_URL, defaulting to the local
// compose database.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/harshithptl/clearline/migrations"
)

const defaultURL = "postgres://clearline:clearline@127.0.0.1:5432/clearline?sslmode=disable"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run() error {
	url := os.Getenv("CLEARLINE_DATABASE_URL")
	if url == "" {
		url = defaultURL
	}
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to %s@%s/%s: %w", cfg.User, cfg.Host, cfg.Database, err)
	}

	v, err := migrations.Apply(ctx, db)
	if err != nil {
		return err
	}
	fmt.Printf("schema at version %d\n", v)
	return nil
}
