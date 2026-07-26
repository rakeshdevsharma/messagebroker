# Message Broker — High-Level Design

## 1. Goal

A gRPC message broker that supports:

- Multiple producers publishing concurrently to a topic.
- Multiple independent consumers subscribing to a topic, each seeing every message
  (fan-out via **subscriptions**).
- Competing consumers within a **consumer group**, where a message is delivered to only
  one consumer in the group (fan-out + load-balancing combined, Kafka-style).
- At-least-once delivery: a consumer keeps getting a message until it acks it.
- Horizontal scaling of a single logical consumer: any number of processes can join the
  same consumer group and safely compete for messages.
- Producers and consumers hold a persistent TCP connection to the broker.

## 2. Architecture

```
 Producer(s) ──gRPC unary Publish──▶ ┌──────────────┐
                                      │              │
 Consumer(s) ◀─gRPC bidi Consume────▶│    Broker    │◀── background: lease reaper
                                      │  (Go, single │◀── background: message cleanup
                                      │    process)  │
                                      └──────┬───────┘
                                             │  pgx pool
                                             ▼
                                        PostgreSQL
                       (topics, messages, consumer_group, consumer,
                        subscriptions, message_queue)
```

The broker is a single stateless Go process (`cmd/broker`); all durable state and all
delivery coordination lives in Postgres. Statelessness lets the broker itself be
horizontally scaled behind a gRPC load balancer without any additional coordination —
Postgres row locking is the only place concurrency is arbitrated.

## 3. Data Model

| Table | Purpose |
|---|---|
| `topics` | Named publish target. |
| `messages` | Immutable message body + topic + `created_at` (drives the 7-day retention). |
| `consumer_group` | A named group of competing consumers. Seeded with a `default` row. |
| `consumer` | One row per registered process instance within a group. |
| `subscriptions` | `(topic_id, consumer_group_id)` — at most one subscription per group per topic. |
| `message_queue` | One row per `(message, consumer_group)` — the delivery/ack ledger. |

`message_queue` is the core of the design: publishing a message inserts **one row per
subscribed consumer group** (not per consumer), and a message is *claimed* by exactly one
consumer in that group by writing its `consumer_id` into that row. This is what gives
"every subscribed group gets every message" + "only one consumer per group gets any given
message" simultaneously.

Key columns on `message_queue`:
- `state`: `ready` (unclaimed) → `unacked` (claimed, awaiting ack) → row deleted (acked).
- `consumer_id`: who currently holds the message; `NULL` when `ready`.
- `lease_expires_at`: a visibility timeout. If a claim isn't acked before this passes, the
  row is automatically returned to `ready` — this is what makes "at least once" hold even
  if a consumer crashes mid-processing.
- `delivery_count`: incremented on every claim; a message redelivered after a crash will
  have `delivery_count > 1`, which is expected under at-least-once semantics.

`ON DELETE CASCADE` from `message_queue.message_id → messages.id` means force-deleting a
message (7-day expiry) also clears any outstanding queue rows for it in one statement.

## 4. Core Flows

### Publish
One transaction: insert the message, then fan out a `ready` queue row to every current
subscription of that topic. Doing both in one transaction matters — otherwise a
concurrently-running cleanup sweep could see a message with zero queue rows (because the
fan-out insert hadn't committed yet) and delete it before any subscriber ever got it.

### Claim (a consumer pulling a message)
```sql
SELECT ... WHERE consumer_group_id = $1 AND state = 'ready'
  ORDER BY message_id LIMIT 1 FOR UPDATE SKIP LOCKED;
UPDATE ... SET consumer_id = $2, state = 'unacked', lease_expires_at = now() + lease;
```
`FOR UPDATE SKIP LOCKED` is what makes horizontal scaling of a consumer group correct: many
processes can run this query concurrently against the same group and each will lock (and
claim) a *different* row instead of two of them racing for the same one.

### Ack
```sql
DELETE FROM message_queue
WHERE message_id = $1 AND consumer_group_id = $2 AND consumer_id = $3 AND state = 'unacked';
```
Scoped to the caller's own `consumer_id` so a late ack from a consumer that has since been
reassigned (see below) can't delete the new owner's row. Zero rows affected means the ack
is stale — logged, not an error, since at-least-once delivery means duplicate/late acks are
expected.

### Dead-consumer recovery
Two complementary mechanisms, because relying on either alone is insufficient:
- **Fast path**: when a consumer's gRPC stream disconnects, the broker immediately
  releases anything still checked out to that `consumer_id`.
- **Slow path (safety net)**: a background reaper sweeps `message_queue` every few
  seconds and releases any row whose `lease_expires_at` has passed. This also catches a
  consumer that is still connected but hung and simply never acking — the fast path alone
  would never notice that case.

### Delivery transport
`Consume` is a bidirectional streaming gRPC RPC, not request/response polling from the
client's perspective: the consumer opens one stream, registers once, and then receives
`MessageDelivery` pushes and sends `Ack`s on that same stream for as long as it stays
connected. This is what makes the "persistent TCP connection" requirement load-bearing —
the stream's lifetime is the fast-path signal the broker uses to detect a dead consumer,
not just an incidental transport choice. Internally the broker still polls Postgres on an
interval per open stream (see §6 for the tradeoff).

### Cleanup
A message is deleted once it's older than 7 days, or once it has no remaining
`message_queue` rows (i.e. every subscribed group has acked it) — whichever happens first.

## 5. gRPC API

```proto
service Broker {
  rpc CreateTopic(CreateTopicRequest) returns (Topic);
  rpc CreateConsumerGroup(CreateConsumerGroupRequest) returns (ConsumerGroup);
  rpc CreateSubscription(CreateSubscriptionRequest) returns (Subscription);
  rpc Publish(PublishRequest) returns (PublishResponse);
  rpc Consume(stream ConsumeClientMsg) returns (stream ConsumeServerMsg);
}
```

There is deliberately no `CreateConsumer` RPC and no `Get`/`Ack` as separate unary calls:
- A consumer process registers itself (upsert by `consumer_group + name`) as the first
  message on the `Consume` stream. This is what lets N horizontally-scaled process
  instances just start up with the same group and distinct names, with no separate
  provisioning step per instance.
- `messages` intentionally has no Update/Delete API — only publish (create) and delivery
  (read). Exposing arbitrary mutation would conflict with the cleanup-driven deletion model
  and with consumers relying on the content they already read being immutable.

## 6. Known Tradeoffs / Follow-ups

- **Polling, not push**: each open `Consume` stream re-runs the claim query on a fixed
  interval (default 300ms) rather than being woken by `LISTEN/NOTIFY` on publish. Simpler
  and always correct; adds up to one poll interval of latency and scales DB load linearly
  with open streams. Chosen deliberately for v1 to keep the moving parts down; `LISTEN/
  NOTIFY`-driven push is the natural fast-follow if delivery latency needs to improve.
- **No lease renewal**: a claimed message's lease is fixed at claim time; a consumer doing
  long-running processing has no way to extend it, so it may be reaped and redelivered to
  another consumer while still legitimately in progress. Acceptable under at-least-once
  semantics but worth a heartbeat/renew RPC if processing times are long and variable.
- **No poison-message handling**: a message whose processing always crashes its consumer
  will cycle `ready → unacked → ready` indefinitely at the head of its group's queue, with
  no `max_delivery_count` / dead-letter escape hatch.
- **Single Postgres instance**: all coordination assumes one primary Postgres; no sharding
  or partitioning story if `message_queue` becomes a bottleneck at scale.
