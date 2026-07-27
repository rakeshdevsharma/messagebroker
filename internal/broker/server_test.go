package broker

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "messagebroker/proto"
)

func ctxTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCreateTopic_Success(t *testing.T) {
	ts := setupTestServer(t)

	resp, err := ts.Client.CreateTopic(ctxTimeout(t), &pb.CreateTopicRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if resp.GetName() != "orders" || resp.GetId() == 0 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestCreateTopic_DuplicateReturnsAlreadyExists(t *testing.T) {
	ts := setupTestServer(t)

	_, err := ts.Client.CreateTopic(ctxTimeout(t), &pb.CreateTopicRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err = ts.Client.CreateTopic(ctxTimeout(t), &pb.CreateTopicRequest{Name: "orders"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestCreateConsumerGroup_Success(t *testing.T) {
	ts := setupTestServer(t)

	resp, err := ts.Client.CreateConsumerGroup(ctxTimeout(t), &pb.CreateConsumerGroupRequest{Name: "workers"})
	if err != nil {
		t.Fatalf("CreateConsumerGroup: %v", err)
	}
	if resp.GetName() != "workers" || resp.GetId() == 0 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestCreateConsumerGroup_DuplicateReturnsAlreadyExists(t *testing.T) {
	ts := setupTestServer(t)

	_, err := ts.Client.CreateConsumerGroup(ctxTimeout(t), &pb.CreateConsumerGroupRequest{Name: "workers"})
	if err != nil {
		t.Fatalf("CreateConsumerGroup: %v", err)
	}

	_, err = ts.Client.CreateConsumerGroup(ctxTimeout(t), &pb.CreateConsumerGroupRequest{Name: "workers"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestCreateSubscription_UnknownTopicReturnsNotFound(t *testing.T) {
	ts := setupTestServer(t)

	_, err := ts.Client.CreateSubscription(ctxTimeout(t), &pb.CreateSubscriptionRequest{
		TopicName:         "does-not-exist",
		ConsumerGroupName: "workers",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestCreateSubscription_DefaultsToDefaultGroupOverGRPC(t *testing.T) {
	ts := setupTestServer(t)

	_, err := ts.Client.CreateTopic(ctxTimeout(t), &pb.CreateTopicRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	sub, err := ts.Client.CreateSubscription(ctxTimeout(t), &pb.CreateSubscriptionRequest{
		TopicName: "orders",
		// ConsumerGroupName intentionally left empty.
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if sub.GetConsumerGroupId() == 0 {
		t.Fatalf("expected subscription to resolve to the default group, got %+v", sub)
	}
}

func TestPublish_Success(t *testing.T) {
	ts := setupTestServer(t)

	_, err := ts.Client.CreateTopic(ctxTimeout(t), &pb.CreateTopicRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	resp, err := ts.Client.Publish(ctxTimeout(t), &pb.PublishRequest{TopicName: "orders", Content: []byte("hello")})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.GetMessageId() == 0 {
		t.Fatalf("expected a non-zero message id, got %+v", resp)
	}
}

func TestPublish_UnknownTopicReturnsNotFound(t *testing.T) {
	ts := setupTestServer(t)

	_, err := ts.Client.Publish(ctxTimeout(t), &pb.PublishRequest{TopicName: "does-not-exist", Content: []byte("x")})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}
