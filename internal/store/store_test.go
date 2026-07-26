package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestClaim_ConcurrentConsumersGetDistinctMessages is the regression test for
// fix #1: many consumer processes claiming concurrently against the same
// group must never both walk away with the same message.
func TestClaim_ConcurrentConsumersGetDistinctMessages(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "orders")
	requireNoError(t, err)
	group, err := st.CreateConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)

	const numMessages = 30
	for i := 0; i < numMessages; i++ {
		_, err := st.Publish(ctx, topic.Name, []byte(fmt.Sprintf("msg-%d", i)))
		requireNoError(t, err)
	}

	const numConsumers = 6
	consumers := make([]Consumer, numConsumers)
	for i := range consumers {
		c, err := st.EnsureConsumer(ctx, group.ID, fmt.Sprintf("worker-%d", i))
		requireNoError(t, err)
		consumers[i] = c
	}

	var mu sync.Mutex
	claimedBy := make(map[int64]int64) // messageID -> consumerID
	var wg sync.WaitGroup

	for _, c := range consumers {
		wg.Add(1)
		go func(c Consumer) {
			defer wg.Done()
			for {
				msg, err := st.Claim(ctx, group.ID, c.ID, 30)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if msg == nil {
					return
				}
				mu.Lock()
				if prev, dup := claimedBy[msg.MessageID]; dup {
					t.Errorf("message %d claimed by both consumer %d and consumer %d", msg.MessageID, prev, c.ID)
				}
				claimedBy[msg.MessageID] = c.ID
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	if len(claimedBy) != numMessages {
		t.Fatalf("expected exactly %d messages claimed, got %d", numMessages, len(claimedBy))
	}
}

// TestAck_StaleAckIgnoredAfterReassignment is the regression test for fix
// #6: an ack from a consumer that no longer owns the row (because it was
// reassigned after a disconnect) must not delete the new owner's row.
func TestAck_StaleAckIgnoredAfterReassignment(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "t")
	requireNoError(t, err)
	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)
	msgID, err := st.Publish(ctx, topic.Name, []byte("x"))
	requireNoError(t, err)

	consumerA, err := st.EnsureConsumer(ctx, group.ID, "a")
	requireNoError(t, err)
	consumerB, err := st.EnsureConsumer(ctx, group.ID, "b")
	requireNoError(t, err)

	claimed, err := st.Claim(ctx, group.ID, consumerA.ID, 30)
	requireNoError(t, err)
	if claimed == nil || claimed.MessageID != msgID {
		t.Fatalf("expected consumer A to claim message %d, got %+v", msgID, claimed)
	}

	// Simulate A disconnecting (or its lease being reaped).
	requireNoError(t, st.ReleaseByConsumer(ctx, consumerA.ID))

	claimed2, err := st.Claim(ctx, group.ID, consumerB.ID, 30)
	requireNoError(t, err)
	if claimed2 == nil || claimed2.MessageID != msgID {
		t.Fatalf("expected consumer B to claim reassigned message %d, got %+v", msgID, claimed2)
	}

	// A's ack finally arrives after reassignment: must be a no-op.
	owned, err := st.Ack(ctx, msgID, group.ID, consumerA.ID)
	requireNoError(t, err)
	if owned {
		t.Fatalf("expected stale ack from consumer A to not be owned")
	}

	// B's row must be untouched and still ackable.
	owned2, err := st.Ack(ctx, msgID, group.ID, consumerB.ID)
	requireNoError(t, err)
	if !owned2 {
		t.Fatalf("expected consumer B's ack to succeed")
	}
}

// TestCleanupMessages_RespectsOutstandingQueueRows is the regression test
// for fix #3/#4: a message with an outstanding (unacked) queue row must
// survive cleanup; once fully acked, it must be removed.
func TestCleanupMessages_RespectsOutstandingQueueRows(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "t")
	requireNoError(t, err)
	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)
	msgID, err := st.Publish(ctx, topic.Name, []byte("x"))
	requireNoError(t, err)

	_, err = st.CleanupMessages(ctx)
	requireNoError(t, err)
	if !messageExists(t, st, msgID) {
		t.Fatalf("expected unacked message %d to survive cleanup", msgID)
	}

	consumer, err := st.EnsureConsumer(ctx, group.ID, "c")
	requireNoError(t, err)
	claimed, err := st.Claim(ctx, group.ID, consumer.ID, 30)
	requireNoError(t, err)
	if claimed == nil {
		t.Fatalf("expected to claim message %d", msgID)
	}
	owned, err := st.Ack(ctx, msgID, group.ID, consumer.ID)
	requireNoError(t, err)
	if !owned {
		t.Fatalf("expected ack to succeed")
	}

	_, err = st.CleanupMessages(ctx)
	requireNoError(t, err)
	if messageExists(t, st, msgID) {
		t.Fatalf("expected fully-acked message %d to be removed by cleanup", msgID)
	}
}

// TestReapExpiredLeases is the regression test for fix #5: a message held
// by a consumer that never acks becomes claimable again once its lease
// expires, without relying on any TCP-disconnect signal.
func TestReapExpiredLeases(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "t")
	requireNoError(t, err)
	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)
	msgID, err := st.Publish(ctx, topic.Name, []byte("x"))
	requireNoError(t, err)

	consumerA, err := st.EnsureConsumer(ctx, group.ID, "a")
	requireNoError(t, err)
	claimed, err := st.Claim(ctx, group.ID, consumerA.ID, 1) // 1-second lease
	requireNoError(t, err)
	if claimed == nil {
		t.Fatalf("expected to claim message %d", msgID)
	}

	time.Sleep(2 * time.Second)

	n, err := st.ReapExpiredLeases(ctx)
	requireNoError(t, err)
	if n != 1 {
		t.Fatalf("expected 1 lease reaped, got %d", n)
	}

	consumerB, err := st.EnsureConsumer(ctx, group.ID, "b")
	requireNoError(t, err)
	claimed2, err := st.Claim(ctx, group.ID, consumerB.ID, 30)
	requireNoError(t, err)
	if claimed2 == nil || claimed2.MessageID != msgID {
		t.Fatalf("expected message %d to be reclaimable after reap, got %+v", msgID, claimed2)
	}
}

// TestCreateSubscription_DefaultsToDefaultGroup covers the "consumer group
// during subscription is optional" requirement.
func TestCreateSubscription_DefaultsToDefaultGroup(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "t")
	requireNoError(t, err)

	sub, err := st.CreateSubscription(ctx, topic.Name, "")
	requireNoError(t, err)

	defaultGroup, err := st.EnsureConsumerGroupByName(ctx, DefaultConsumerGroupName)
	requireNoError(t, err)
	if sub.ConsumerGroupID != defaultGroup.ID {
		t.Fatalf("expected subscription to use default group %d, got %d", defaultGroup.ID, sub.ConsumerGroupID)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func messageExists(t *testing.T, st *Store, messageID int64) bool {
	t.Helper()
	var exists bool
	err := st.pool.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM messages WHERE id = $1)`, messageID,
	).Scan(&exists)
	requireNoError(t, err)
	return exists
}
