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
| 10 | Producer and consumer CLIs | `cmd/producer/main.go`, `cmd/consumer/main.go` | Done |

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
- **`cmd/producer/main.go`** — dials the broker, optionally ensures the topic exists
  (`-ensure-topic`, tolerates `AlreadyExists`), then publishes either a single `-message` or
  newline-delimited stdin. Run multiple instances concurrently to exercise "multiple
  producers publishing simultaneously."
- **`cmd/consumer/main.go`** — dials the broker, ensures the subscription exists, opens the
  `Consume` stream, sends the initial `Register` (with an auto-generated `hostname-pid`
  name if `-name` isn't given, so N instances can join the same `-group` without manual
  provisioning), then loops printing each delivery and acking it. Handles Ctrl+C /
  `SIGTERM` for a clean disconnect (which triggers the broker's fast-path release).

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
- **Manual/smoke**: confirmed `cmd/broker`, `cmd/producer`, and `cmd/consumer` all start
  and fail gracefully (not a panic) when the broker/Postgres is unreachable — e.g.
  `producer`/`consumer` against a closed port return a clean `Unavailable` gRPC error and
  exit 1.
- **Not yet done**: actually running the end-to-end scenario against a live broker +
  Postgres — `cmd/producer`/`cmd/consumer` now exist and are the intended driver for it
  (e.g. two `cmd/consumer` instances with the same `-group` splitting messages published
  by `cmd/producer`, then killing one mid-flight and confirming its in-flight message
  reappears on the other), but it hasn't been executed here because there is no
  Docker/Podman/Colima daemon in this dev sandbox to run Postgres against. This needs to
  be run on a machine with Docker (see Deployment below).

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
