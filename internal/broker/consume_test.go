package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "messagebroker/proto"
)

// openAndRegister opens a Consume stream and sends the initial Register
// message every stream must start with.
func openAndRegister(t *testing.T, ctx context.Context, client pb.BrokerClient, group, name string) pb.Broker_ConsumeClient {
	t.Helper()
	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := stream.Send(&pb.ConsumeClientMsg{
		Kind: &pb.ConsumeClientMsg_Register{
			Register: &pb.RegisterRequest{ConsumerGroupName: group, ConsumerName: name},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	return stream
}

// TestConsume_SingleConsumerReceivesAndAcksMessage is the end-to-end happy
// path: publish, receive over the real stream, ack, and confirm the ack
// actually cleared the queue row (nothing left to claim).
func TestConsume_SingleConsumerReceivesAndAcksMessage(t *testing.T) {
	ts := setupTestServer(t)
	ctx := ctxTimeout(t)

	if _, err := ts.Client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "orders"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := ts.Client.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{TopicName: "orders", ConsumerGroupName: "workers"})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	pubResp, err := ts.Client.Publish(ctx, &pb.PublishRequest{TopicName: "orders", Content: []byte("hello")})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream := openAndRegister(t, ctx, ts.Client, "workers", "consumer-1")

	serverMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	delivery := serverMsg.GetMessage()
	if delivery == nil || delivery.GetMessageId() != pubResp.GetMessageId() || string(delivery.GetContent()) != "hello" {
		t.Fatalf("unexpected delivery: %+v", delivery)
	}
	if delivery.GetDeliveryCount() != 1 {
		t.Fatalf("expected delivery_count=1, got %d", delivery.GetDeliveryCount())
	}

	if err := stream.Send(&pb.ConsumeClientMsg{
		Kind: &pb.ConsumeClientMsg_Ack{Ack: &pb.AckRequest{MessageId: delivery.GetMessageId()}},
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// The ack is processed by a goroutine reading the stream server-side;
	// give it a moment, then confirm the queue row is really gone by trying
	// (and failing) to claim it again from a fresh consumer in the group.
	time.Sleep(300 * time.Millisecond)
	other, err := ts.Store.EnsureConsumer(ctx, sub.GetConsumerGroupId(), "consumer-2")
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	claimed, err := ts.Store.Claim(ctx, sub.GetConsumerGroupId(), other.ID, 30)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected nothing left to claim after ack, got %+v", claimed)
	}
}

// TestConsume_CompetingConsumersSplitMessagesNoDuplicates is the stream-level
// version of the store-level concurrent-claim test: two consumer processes
// in the same group, talking over real Consume streams, must never both
// receive the same message, and together must receive every message.
func TestConsume_CompetingConsumersSplitMessagesNoDuplicates(t *testing.T) {
	ts := setupTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := ts.Client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "orders"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := ts.Client.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{TopicName: "orders", ConsumerGroupName: "workers"}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}

	const numMessages = 20
	published := make(map[int64]bool, numMessages)
	for i := 0; i < numMessages; i++ {
		resp, err := ts.Client.Publish(ctx, &pb.PublishRequest{TopicName: "orders", Content: []byte{byte(i)}})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		published[resp.GetMessageId()] = true
	}

	streamA := openAndRegister(t, ctx, ts.Client, "workers", "consumer-a")
	streamB := openAndRegister(t, ctx, ts.Client, "workers", "consumer-b")

	results := make(chan int64, numMessages*2)
	var wg sync.WaitGroup
	consume := func(stream pb.Broker_ConsumeClient) {
		defer wg.Done()
		for {
			serverMsg, err := stream.Recv()
			if err != nil {
				return
			}
			delivery := serverMsg.GetMessage()
			if delivery == nil {
				continue
			}
			results <- delivery.GetMessageId()
			if err := stream.Send(&pb.ConsumeClientMsg{
				Kind: &pb.ConsumeClientMsg_Ack{Ack: &pb.AckRequest{MessageId: delivery.GetMessageId()}},
			}); err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go consume(streamA)
	go consume(streamB)

	seen := make(map[int64]bool, numMessages)
	deadline := time.After(15 * time.Second)
loop:
	for len(seen) < numMessages {
		select {
		case id := <-results:
			if seen[id] {
				t.Fatalf("message %d delivered more than once", id)
			}
			if !published[id] {
				t.Fatalf("received unexpected message id %d", id)
			}
			seen[id] = true
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d messages", len(seen), numMessages)
			break loop
		}
	}

	cancel() // end both streams so the consume goroutines return
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("consume goroutines did not exit after context cancellation")
	}
}

// TestConsume_DisconnectFastPathReleasesInFlightMessage is the stream-level
// regression test for the dead-consumer fast path: closing a consumer's
// stream without acking must make its in-flight message immediately
// claimable by another consumer, without waiting for the lease to expire.
func TestConsume_DisconnectFastPathReleasesInFlightMessage(t *testing.T) {
	ts := setupTestServer(t)
	setupCtx := ctxTimeout(t)

	if _, err := ts.Client.CreateTopic(setupCtx, &pb.CreateTopicRequest{Name: "orders"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := ts.Client.CreateSubscription(setupCtx, &pb.CreateSubscriptionRequest{TopicName: "orders", ConsumerGroupName: "workers"}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	pubResp, err := ts.Client.Publish(setupCtx, &pb.PublishRequest{TopicName: "orders", Content: []byte("x")})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumerACtx, cancelA := context.WithCancel(context.Background())
	streamA := openAndRegister(t, consumerACtx, ts.Client, "workers", "consumer-a")

	serverMsg, err := streamA.Recv()
	if err != nil {
		t.Fatalf("Recv (A): %v", err)
	}
	delivery := serverMsg.GetMessage()
	if delivery == nil || delivery.GetMessageId() != pubResp.GetMessageId() {
		t.Fatalf("expected consumer A to receive message %d, got %+v", pubResp.GetMessageId(), delivery)
	}

	// Simulate consumer A disconnecting (crash/network loss) without acking.
	cancelA()
	time.Sleep(300 * time.Millisecond) // let the server observe the disconnect and release the row

	streamB := openAndRegister(t, setupCtx, ts.Client, "workers", "consumer-b")
	serverMsg2, err := streamB.Recv()
	if err != nil {
		t.Fatalf("Recv (B): %v", err)
	}
	delivery2 := serverMsg2.GetMessage()
	if delivery2 == nil || delivery2.GetMessageId() != pubResp.GetMessageId() {
		t.Fatalf("expected consumer B to receive reclaimed message %d, got %+v", pubResp.GetMessageId(), delivery2)
	}
	if delivery2.GetDeliveryCount() != 2 {
		t.Fatalf("expected delivery_count=2 on redelivery, got %d", delivery2.GetDeliveryCount())
	}
}

// TestConsume_RequiresRegisterFirst covers the InvalidArgument guard: a
// stream that sends anything other than a Register as its first message
// must be rejected.
func TestConsume_RequiresRegisterFirst(t *testing.T) {
	ts := setupTestServer(t)
	ctx := ctxTimeout(t)

	stream, err := ts.Client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := stream.Send(&pb.ConsumeClientMsg{
		Kind: &pb.ConsumeClientMsg_Ack{Ack: &pb.AckRequest{MessageId: 1}},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	_, err = stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

// TestConsume_EmptyGroupNameUsesDefaultGroup covers Register's fallback to
// the default consumer group, matching CreateSubscription's own fallback.
func TestConsume_EmptyGroupNameUsesDefaultGroup(t *testing.T) {
	ts := setupTestServer(t)
	ctx := ctxTimeout(t)

	if _, err := ts.Client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "orders"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := ts.Client.CreateSubscription(ctx, &pb.CreateSubscriptionRequest{TopicName: "orders"}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	pubResp, err := ts.Client.Publish(ctx, &pb.PublishRequest{TopicName: "orders", Content: []byte("x")})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stream := openAndRegister(t, ctx, ts.Client, "", "consumer-1")

	serverMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	delivery := serverMsg.GetMessage()
	if delivery == nil || delivery.GetMessageId() != pubResp.GetMessageId() {
		t.Fatalf("expected delivery of message %d via the default group, got %+v", pubResp.GetMessageId(), delivery)
	}
}
