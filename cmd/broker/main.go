package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"messagebroker/internal/broker"
	"messagebroker/internal/store"
	pb "messagebroker/proto"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := getenv("BROKER_DATABASE_URL", "postgres://localhost:5432/messagebroker")
	listenAddr := getenv("BROKER_LISTEN_ADDR", ":50051")

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	st := store.New(pool)
	srv := broker.NewServer(st, 30 /* lease seconds */, 300*time.Millisecond /* poll interval */)

	go srv.RunLeaseReaper(ctx, 5*time.Second)
	go srv.RunMessageCleanup(ctx, time.Minute)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBrokerServer(grpcServer, srv)

	go func() {
		<-ctx.Done()
		log.Println("shutting down gRPC server")
		grpcServer.GracefulStop()
	}()

	log.Printf("broker listening on %s", listenAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server error: %v", err)
	}
}
