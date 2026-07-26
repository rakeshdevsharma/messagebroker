# Message Broker — Implementation Plan

See [`DESIGN.md`](./DESIGN.md) for the architecture and rationale. This document tracks
how it was built and what's left.

## Status: v1 implemented

| # | Step | Files | Status |
|---|---|---|---|
| 1 | Go module + project layout | `go.mod` | Done |
| 2 | Proto definitions + generated stubs | `proto/broker.proto`, `proto/*.pb.go` | Done |
| 3 | Schema migration | `migrations/0001_init.sql` | Done |
| 4 | Data access layer (atomic claim/ack/release/reap/cleanup) | `internal/store/*.go` | Done |
| 5 | gRPC server + Consume stream + background loops | `internal/broker/*.go` | Done |
| 6 | Process wiring | `cmd/broker/main.go` | Done |
| 7 | Store-level regression tests | `internal/store/store_test.go`, `testutil_test.go` | Done (compiles; not yet run — no Docker in dev sandbox) |
| 8 | Local Postgres for dev/manual testing | `docker-compose.yml` (`postgres` service) | Done |
| 9 | Containerize the broker itself | `Dockerfile`, `docker-compose.yml` (`broker` service) | Done |

## File-by-file breakdown

- **`proto/broker.proto`** — service `Broker`: `CreateTopic`, `CreateConsumerGroup`,
  `CreateSubscription`, `Publish` (unary), `Consume` (bidi stream). No `CreateConsumer` —
  consumers self-register on the first `Consume` stream message.
- **`migrations/0001_init.sql`** — the six tables from DESIGN.md §3, plus indexes
  supporting the claim query (`consumer_group_id, state, message_id`) and the reaper
  (`state, lease_expires_at` partial index) and a seeded `default` consumer group.
- **`internal/store/store.go`** — one `Store` type wrapping a `*pgxpool.Pool`. All the
  concurrency-sensitive SQL lives here: `Publish` (atomic insert + fan-out), `Claim`
  (`FOR UPDATE SKIP LOCKED`), `Ack` (owner-scoped delete), `ReleaseByConsumer` (disconnect
  fast path), `ReapExpiredLeases` (slow-path safety net), `CleanupMessages` (7-day / fully-
  acked sweep).
- **`internal/broker/server.go`** — thin gRPC handlers mapping proto requests to `Store`
  calls for the unary RPCs.
- **`internal/broker/consume.go`** — the `Consume` stream handler: reads the initial
  `Register`, upserts the `consumer_group`/`consumer` rows, then runs a poll-and-push loop
  (ticker → `Claim` → `Send`) concurrently with a receive loop (`Recv` → `Ack`), releasing
  the consumer's claims on disconnect.
- **`internal/broker/reaper.go`**, **`cleanup.go`** — the two background tickers started
  from `main.go`.
- **`cmd/broker/main.go`** — reads `BROKER_DATABASE_URL` / `BROKER_LISTEN_ADDR`, opens the
  pgx pool, starts the gRPC server and the two background loops, shuts down gracefully on
  `SIGTERM`/interrupt.

## Testing

- **Unit/integration** (`internal/store/store_test.go`): uses `testcontainers-go` to spin
  up a real `postgres:16-alpine` per test run and applies `migrations/0001_init.sql`
  directly, so these exercise real Postgres locking behavior rather than a mock. Covers:
  - Concurrent `Claim` calls across many consumers never double-deliver a message.
  - A stale `Ack` (arriving after reassignment) is a no-op and doesn't disturb the new
    owner's row.
  - `CleanupMessages` keeps unacked messages and removes fully-acked ones.
  - `ReapExpiredLeases` recovers a message whose lease passed with no ack.
  - `CreateSubscription` with no group name falls back to `default`.
  - **Caveat**: these compile and pass `go vet`, but have not been executed in this
    environment — there is no Docker/Podman/Colima daemon available here. Run
    `go test ./...` on a machine with Docker to actually execute them.
- **Manual/smoke**: confirmed `cmd/broker` starts, binds its listen address, and its
  background loops fail gracefully (not a panic) when Postgres is unreachable.
- **Not yet done**: an end-to-end test driving the actual `Consume` gRPC stream (as
  opposed to the `store` layer directly) — e.g. two concurrent stream clients in one
  consumer group split a batch of published messages, and killing one mid-flight causes
  its in-flight message to reappear on the other.

## Deployment

`docker-compose.yml` runs both services:
- `postgres` — applies `migrations/0001_init.sql` via `docker-entrypoint-initdb.d` on
  first boot.
- `broker` — built from `Dockerfile` (multi-stage: `golang:1.25-alpine` build,
  `alpine:3.20` runtime), waits on Postgres's healthcheck, exposes `50051`.

Not yet run end-to-end in this environment (no Docker daemon available here) — verify with
`docker-compose up --build` on a machine that has Docker.

## Open follow-ups (not required for v1, flagged in DESIGN.md §6)

- `LISTEN/NOTIFY`-driven push instead of fixed-interval polling, if latency matters.
- Lease renewal/heartbeat RPC for long-running message processing.
- `delivery_count`-based dead-letter handling for poison messages.
- An optional `--migrate` startup flag on `cmd/broker` so the schema doesn't have to be
  applied out-of-band when not using `docker-compose`.
