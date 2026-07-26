package store

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestStore starts a throwaway Postgres container, applies the same
// migration used in production (migrations/0001_init.sql), and returns a
// Store backed by it. Requires a local Docker (or compatible) daemon.
func setupTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()

	schema, err := os.ReadFile(migrationPath(t))
	if err != nil {
		t.Fatalf("read migration file: %v", err)
	}

	pgContainer, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("messagebroker"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	return New(pool)
}

// migrationPath resolves migrations/0001_init.sql relative to this source
// file rather than the test binary's working directory, so it works
// regardless of how `go test` is invoked.
func migrationPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller for migration path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "0001_init.sql")
}
