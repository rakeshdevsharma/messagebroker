package broker

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"messagebroker/internal/store"
	pb "messagebroker/proto"
)

const bufSize = 1024 * 1024

// testServer bundles a running broker gRPC server — backed by a real,
// throwaway Postgres container — with a client dialed to it over an
// in-memory bufconn listener. Using bufconn instead of a real TCP port
// means these tests exercise the actual wire protocol (proto encoding,
// stream framing, status codes) without any port/binding flakiness.
type testServer struct {
	Client pb.BrokerClient
	Store  *store.Store
	Server *Server
}

// pollInterval is deliberately short so stream tests don't have to wait
// long for the dispatch loop's ticker to notice a newly ready message.
const testPollInterval = 25 * time.Millisecond

func setupTestServer(t *testing.T) *testServer {
	t.Helper()

	st := newTestStore(t)
	srv := NewServer(st, 30, testPollInterval)

	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testServer{
		Client: pb.NewBrokerClient(conn),
		Store:  st,
		Server: srv,
	}
}

// newTestStore starts a throwaway Postgres container and applies the
// production migration, mirroring internal/store's own test setup (kept
// separate since it lives in a different package's _test.go files).
func newTestStore(t *testing.T) *store.Store {
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

	return store.New(pool)
}

func migrationPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller for migration path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "0001_init.sql")
}
