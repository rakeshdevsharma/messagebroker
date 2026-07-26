// Package store contains all SQL access for the broker. Every query that
// touches message_queue concurrently (Claim, Ack, ReleaseByConsumer,
// ReapExpiredLeases) is written to be safe under many competing consumer
// processes polling the same consumer group at once.
package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type Topic struct {
	ID   int64
	Name string
}

type ConsumerGroup struct {
	ID   int64
	Name string
}

type Consumer struct {
	ID              int64
	ConsumerGroupID int64
	Name            string
}

type Subscription struct {
	TopicID         int64
	ConsumerGroupID int64
}

const DefaultConsumerGroupName = "default"

func (s *Store) CreateTopic(ctx context.Context, name string) (Topic, error) {
	var t Topic
	err := s.pool.QueryRow(ctx,
		`INSERT INTO topics (name) VALUES ($1) RETURNING id, name`, name,
	).Scan(&t.ID, &t.Name)
	return t, err
}

func (s *Store) GetTopicByName(ctx context.Context, name string) (Topic, error) {
	var t Topic
	err := s.pool.QueryRow(ctx,
		`SELECT id, name FROM topics WHERE name = $1`, name,
	).Scan(&t.ID, &t.Name)
	if err != nil {
		return t, wrapNotFound(err)
	}
	return t, nil
}

func (s *Store) CreateConsumerGroup(ctx context.Context, name string) (ConsumerGroup, error) {
	var g ConsumerGroup
	err := s.pool.QueryRow(ctx,
		`INSERT INTO consumer_group (name) VALUES ($1) RETURNING id, name`, name,
	).Scan(&g.ID, &g.Name)
	return g, err
}

// EnsureConsumerGroupByName gets or creates a consumer group by name. Used
// both for CreateSubscription (falls back to "default") and for consumer
// registration at the start of Consume.
func (s *Store) EnsureConsumerGroupByName(ctx context.Context, name string) (ConsumerGroup, error) {
	var g ConsumerGroup
	err := s.pool.QueryRow(ctx,
		`INSERT INTO consumer_group (name) VALUES ($1)
		 ON CONFLICT (name) DO UPDATE SET name = excluded.name
		 RETURNING id, name`, name,
	).Scan(&g.ID, &g.Name)
	return g, err
}

// EnsureConsumer gets or creates a consumer row for (group, name). Each
// horizontally-scaled process instance should register with a distinct
// name (e.g. hostname+pid) within the same group.
func (s *Store) EnsureConsumer(ctx context.Context, groupID int64, name string) (Consumer, error) {
	var c Consumer
	err := s.pool.QueryRow(ctx,
		`INSERT INTO consumer (consumer_group_id, name) VALUES ($1, $2)
		 ON CONFLICT (consumer_group_id, name) DO UPDATE SET name = excluded.name
		 RETURNING id, consumer_group_id, name`, groupID, name,
	).Scan(&c.ID, &c.ConsumerGroupID, &c.Name)
	return c, err
}

func (s *Store) CreateSubscription(ctx context.Context, topicName, consumerGroupName string) (Subscription, error) {
	if consumerGroupName == "" {
		consumerGroupName = DefaultConsumerGroupName
	}

	topic, err := s.GetTopicByName(ctx, topicName)
	if err != nil {
		return Subscription{}, err
	}
	group, err := s.EnsureConsumerGroupByName(ctx, consumerGroupName)
	if err != nil {
		return Subscription{}, err
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO subscriptions (topic_id, consumer_group_id) VALUES ($1, $2)
		 ON CONFLICT (topic_id, consumer_group_id) DO NOTHING`,
		topic.ID, group.ID,
	)
	if err != nil {
		return Subscription{}, err
	}
	return Subscription{TopicID: topic.ID, ConsumerGroupID: group.ID}, nil
}

// Publish inserts the message and fans it out to every current subscription
// of the topic in a single transaction, so a concurrent cleanup sweep can
// never observe a message with zero queue rows before its subscribers'
// rows exist (fix for the publish/cleanup race).
func (s *Store) Publish(ctx context.Context, topicName string, content []byte) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var topicID, messageID int64
	err = tx.QueryRow(ctx, `SELECT id FROM topics WHERE name = $1`, topicName).Scan(&topicID)
	if err != nil {
		return 0, wrapNotFound(err)
	}

	err = tx.QueryRow(ctx,
		`INSERT INTO messages (topic_id, content) VALUES ($1, $2) RETURNING id`,
		topicID, content,
	).Scan(&messageID)
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO message_queue (message_id, consumer_group_id, state)
		 SELECT $1, s.consumer_group_id, 'ready'
		 FROM subscriptions s WHERE s.topic_id = $2`,
		messageID, topicID,
	)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return messageID, nil
}

type ClaimedMessage struct {
	MessageID     int64
	TopicName     string
	Content       []byte
	DeliveryCount int32
}

// Claim atomically finds the oldest ready message for the given consumer
// group and assigns it to consumerID. FOR UPDATE SKIP LOCKED means many
// consumer processes can call Claim concurrently against the same group
// and each lock a distinct row instead of racing on the same one — this is
// what makes horizontal scaling of a consumer group safe.
func (s *Store) Claim(ctx context.Context, groupID, consumerID int64, leaseSeconds int) (*ClaimedMessage, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var queueRowID, messageID int64
	err = tx.QueryRow(ctx,
		`SELECT id, message_id FROM message_queue
		 WHERE consumer_group_id = $1 AND state = 'ready'
		 ORDER BY message_id ASC
		 LIMIT 1
		 FOR UPDATE SKIP LOCKED`,
		groupID,
	).Scan(&queueRowID, &messageID)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, err
	}

	var deliveryCount int32
	err = tx.QueryRow(ctx,
		`UPDATE message_queue
		 SET consumer_id = $1, state = 'unacked',
		     delivery_count = delivery_count + 1,
		     lease_expires_at = now() + ($2 || ' seconds')::interval
		 WHERE id = $3
		 RETURNING delivery_count`,
		consumerID, leaseSeconds, queueRowID,
	).Scan(&deliveryCount)
	if err != nil {
		return nil, err
	}

	var content []byte
	var topicName string
	err = tx.QueryRow(ctx,
		`SELECT m.content, t.name FROM messages m
		 JOIN topics t ON t.id = m.topic_id
		 WHERE m.id = $1`,
		messageID,
	).Scan(&content, &topicName)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &ClaimedMessage{
		MessageID:     messageID,
		TopicName:     topicName,
		Content:       content,
		DeliveryCount: deliveryCount,
	}, nil
}

// Ack deletes the queue row only if it is still owned by consumerID. If the
// row was already reassigned (lease expired, or the consumer's stream was
// detected as dead), zero rows are affected and owned is false — this is a
// stale/duplicate ack under at-least-once delivery, not an error.
func (s *Store) Ack(ctx context.Context, messageID, groupID, consumerID int64) (owned bool, err error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM message_queue
		 WHERE message_id = $1 AND consumer_group_id = $2 AND consumer_id = $3 AND state = 'unacked'`,
		messageID, groupID, consumerID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ReleaseByConsumer is the fast path run when a consumer's stream
// disconnects: it immediately frees any message still checked out to that
// consumer instead of waiting for the lease to expire.
func (s *Store) ReleaseByConsumer(ctx context.Context, consumerID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE message_queue
		 SET state = 'ready', consumer_id = NULL, lease_expires_at = NULL
		 WHERE consumer_id = $1 AND state = 'unacked'`,
		consumerID,
	)
	return err
}

// ReapExpiredLeases is the slow-path safety net: it recovers messages held
// by a consumer that is still connected but hung and never acking, which
// stream-disconnect detection alone would miss.
func (s *Store) ReapExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE message_queue
		 SET state = 'ready', consumer_id = NULL, lease_expires_at = NULL
		 WHERE state = 'unacked' AND lease_expires_at < now()`,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// CleanupMessages deletes messages older than 7 days, or that have already
// been fully acked by every subscription (no remaining queue rows).
// ON DELETE CASCADE on message_queue.message_id clears any remaining queue
// rows for a message deleted under the 7-day branch.
func (s *Store) CleanupMessages(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM messages
		 WHERE created_at < now() - interval '7 days'
		    OR id NOT IN (SELECT DISTINCT message_id FROM message_queue)`,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
