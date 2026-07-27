package store

import (
	"context"
	"errors"
	"testing"
)

// TestCreateTopic_DuplicateReturnsAlreadyExists covers the error-mapping fix:
// a second CreateTopic with the same name must surface as ErrAlreadyExists,
// not a raw Postgres unique-violation error.
func TestCreateTopic_DuplicateReturnsAlreadyExists(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	_, err := st.CreateTopic(ctx, "orders")
	requireNoError(t, err)

	_, err = st.CreateTopic(ctx, "orders")
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

// TestCreateConsumerGroup_DuplicateReturnsAlreadyExists mirrors the topic
// case for consumer groups.
func TestCreateConsumerGroup_DuplicateReturnsAlreadyExists(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	_, err := st.CreateConsumerGroup(ctx, "workers")
	requireNoError(t, err)

	_, err = st.CreateConsumerGroup(ctx, "workers")
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

// TestGetTopicByName_NotFound covers the not-found mapping used by Publish
// and CreateSubscription when the topic doesn't exist.
func TestGetTopicByName_NotFound(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	_, err := st.GetTopicByName(ctx, "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestPublish_UnknownTopicReturnsNotFound covers Publish's own topic lookup,
// which runs inside its transaction rather than delegating to
// GetTopicByName.
func TestPublish_UnknownTopicReturnsNotFound(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	_, err := st.Publish(ctx, "does-not-exist", []byte("x"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestPublish_FansOutToEveryCurrentSubscription covers the atomic
// publish/fan-out fix: one Publish call must produce a ready queue row for
// every consumer group currently subscribed to the topic, and none for
// unrelated groups.
func TestPublish_FansOutToEveryCurrentSubscription(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "orders")
	requireNoError(t, err)

	groupA, err := st.CreateConsumerGroup(ctx, "a")
	requireNoError(t, err)
	groupB, err := st.CreateConsumerGroup(ctx, "b")
	requireNoError(t, err)
	groupC, err := st.CreateConsumerGroup(ctx, "c") // not subscribed
	requireNoError(t, err)

	_, err = st.CreateSubscription(ctx, topic.Name, groupA.Name)
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, groupB.Name)
	requireNoError(t, err)

	msgID, err := st.Publish(ctx, topic.Name, []byte("payload"))
	requireNoError(t, err)

	consumerA, err := st.EnsureConsumer(ctx, groupA.ID, "ca")
	requireNoError(t, err)
	claimedA, err := st.Claim(ctx, groupA.ID, consumerA.ID, 30)
	requireNoError(t, err)
	if claimedA == nil || claimedA.MessageID != msgID {
		t.Fatalf("expected group A to have a ready copy of message %d, got %+v", msgID, claimedA)
	}

	consumerB, err := st.EnsureConsumer(ctx, groupB.ID, "cb")
	requireNoError(t, err)
	claimedB, err := st.Claim(ctx, groupB.ID, consumerB.ID, 30)
	requireNoError(t, err)
	if claimedB == nil || claimedB.MessageID != msgID {
		t.Fatalf("expected group B to have a ready copy of message %d, got %+v", msgID, claimedB)
	}

	consumerC, err := st.EnsureConsumer(ctx, groupC.ID, "cc")
	requireNoError(t, err)
	claimedC, err := st.Claim(ctx, groupC.ID, consumerC.ID, 30)
	requireNoError(t, err)
	if claimedC != nil {
		t.Fatalf("expected group C (not subscribed) to have nothing to claim, got %+v", claimedC)
	}
}

// TestPublish_NoSubscribersIsCleanedUpImmediately: publishing to a topic
// with zero subscriptions produces zero message_queue rows, so the message
// is immediately eligible for cleanup rather than lingering forever.
func TestPublish_NoSubscribersIsCleanedUpImmediately(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "unsubscribed-topic")
	requireNoError(t, err)

	msgID, err := st.Publish(ctx, topic.Name, []byte("nobody wants this"))
	requireNoError(t, err)

	n, err := st.CleanupMessages(ctx)
	requireNoError(t, err)
	if n != 1 {
		t.Fatalf("expected cleanup to remove 1 message, removed %d", n)
	}
	if messageExists(t, st, msgID) {
		t.Fatalf("expected message %d with no subscribers to be cleaned up", msgID)
	}
}

// TestCreateSubscription_IsIdempotent: creating the same (topic, group)
// subscription twice must not error and must not double the fan-out on
// publish.
func TestCreateSubscription_IsIdempotent(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "t")
	requireNoError(t, err)
	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)

	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)

	msgID, err := st.Publish(ctx, topic.Name, []byte("x"))
	requireNoError(t, err)

	consumer, err := st.EnsureConsumer(ctx, group.ID, "c")
	requireNoError(t, err)

	first, err := st.Claim(ctx, group.ID, consumer.ID, 30)
	requireNoError(t, err)
	if first == nil || first.MessageID != msgID {
		t.Fatalf("expected to claim message %d, got %+v", msgID, first)
	}

	second, err := st.Claim(ctx, group.ID, consumer.ID, 30)
	requireNoError(t, err)
	if second != nil {
		t.Fatalf("expected only one queue row for message %d, but claimed a second: %+v", msgID, second)
	}
}

// TestEnsureConsumerGroupByName_IsIdempotent: repeated calls with the same
// name must return the same group ID, not create duplicates (relied on by
// Consume's per-stream registration).
func TestEnsureConsumerGroupByName_IsIdempotent(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	g1, err := st.EnsureConsumerGroupByName(ctx, "workers")
	requireNoError(t, err)
	g2, err := st.EnsureConsumerGroupByName(ctx, "workers")
	requireNoError(t, err)

	if g1.ID != g2.ID {
		t.Fatalf("expected same group ID across calls, got %d and %d", g1.ID, g2.ID)
	}
}

// TestEnsureConsumer_IsIdempotent: repeated registration by the same
// (group, name) pair — e.g. a consumer process reconnecting — must reuse
// the same consumer row rather than erroring or duplicating.
func TestEnsureConsumer_IsIdempotent(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)

	c1, err := st.EnsureConsumer(ctx, group.ID, "worker-1")
	requireNoError(t, err)
	c2, err := st.EnsureConsumer(ctx, group.ID, "worker-1")
	requireNoError(t, err)

	if c1.ID != c2.ID {
		t.Fatalf("expected same consumer ID across reconnects, got %d and %d", c1.ID, c2.ID)
	}
}

// TestClaim_ReturnsNilWhenNothingReady covers the empty-queue case: Claim
// must return (nil, nil), not an error, when there's nothing to deliver.
func TestClaim_ReturnsNilWhenNothingReady(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)
	consumer, err := st.EnsureConsumer(ctx, group.ID, "c")
	requireNoError(t, err)

	claimed, err := st.Claim(ctx, group.ID, consumer.ID, 30)
	requireNoError(t, err)
	if claimed != nil {
		t.Fatalf("expected nil claim on empty queue, got %+v", claimed)
	}
}

// TestClaim_FIFOOrderWithinAGroup: a single consumer claiming repeatedly
// must receive messages in publish order.
func TestClaim_FIFOOrderWithinAGroup(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	topic, err := st.CreateTopic(ctx, "t")
	requireNoError(t, err)
	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)
	_, err = st.CreateSubscription(ctx, topic.Name, group.Name)
	requireNoError(t, err)

	var ids []int64
	for i := 0; i < 5; i++ {
		id, err := st.Publish(ctx, topic.Name, []byte{byte(i)})
		requireNoError(t, err)
		ids = append(ids, id)
	}

	consumer, err := st.EnsureConsumer(ctx, group.ID, "c")
	requireNoError(t, err)

	for _, want := range ids {
		got, err := st.Claim(ctx, group.ID, consumer.ID, 30)
		requireNoError(t, err)
		if got == nil || got.MessageID != want {
			t.Fatalf("expected FIFO claim of message %d, got %+v", want, got)
		}
	}
}

// TestClaim_DeliveryCountIncrementsOnRedelivery: delivery_count must track
// how many times a message has been (re)delivered, which is what lets a
// consumer detect and treat a poison message specially.
func TestClaim_DeliveryCountIncrementsOnRedelivery(t *testing.T) {
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
	first, err := st.Claim(ctx, group.ID, consumerA.ID, 30)
	requireNoError(t, err)
	if first == nil || first.DeliveryCount != 1 {
		t.Fatalf("expected first delivery_count=1, got %+v", first)
	}

	requireNoError(t, st.ReleaseByConsumer(ctx, consumerA.ID))

	consumerB, err := st.EnsureConsumer(ctx, group.ID, "b")
	requireNoError(t, err)
	second, err := st.Claim(ctx, group.ID, consumerB.ID, 30)
	requireNoError(t, err)
	if second == nil || second.MessageID != msgID || second.DeliveryCount != 2 {
		t.Fatalf("expected redelivery of %d with delivery_count=2, got %+v", msgID, second)
	}
}

// TestAck_UnknownMessageIsNotOwned covers acking a message_id that was
// never claimed by this consumer (typo, replay, or a message from a
// different group) — must report owned=false, not error.
func TestAck_UnknownMessageIsNotOwned(t *testing.T) {
	st := setupTestStore(t)
	ctx := context.Background()

	group, err := st.CreateConsumerGroup(ctx, "g")
	requireNoError(t, err)
	consumer, err := st.EnsureConsumer(ctx, group.ID, "c")
	requireNoError(t, err)

	owned, err := st.Ack(ctx, 999999, group.ID, consumer.ID)
	requireNoError(t, err)
	if owned {
		t.Fatalf("expected owned=false for an unknown message")
	}
}

// TestReapExpiredLeases_LeavesLiveLeasesAlone: the reaper must only touch
// rows whose lease has actually expired, not ones still within their
// window.
func TestReapExpiredLeases_LeavesLiveLeasesAlone(t *testing.T) {
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

	consumer, err := st.EnsureConsumer(ctx, group.ID, "c")
	requireNoError(t, err)
	claimed, err := st.Claim(ctx, group.ID, consumer.ID, 300) // long lease
	requireNoError(t, err)
	if claimed == nil || claimed.MessageID != msgID {
		t.Fatalf("expected to claim message %d, got %+v", msgID, claimed)
	}

	n, err := st.ReapExpiredLeases(ctx)
	requireNoError(t, err)
	if n != 0 {
		t.Fatalf("expected 0 leases reaped while lease is still live, got %d", n)
	}

	owned, err := st.Ack(ctx, msgID, group.ID, consumer.ID)
	requireNoError(t, err)
	if !owned {
		t.Fatalf("expected original owner's ack to still succeed after a no-op reap")
	}
}
