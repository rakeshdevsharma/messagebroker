// Package broker implements the gRPC Broker service on top of internal/store.
package broker

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "messagebroker/proto"

	"messagebroker/internal/store"
)

// toGRPCError translates store-level sentinel errors into the gRPC status
// codes clients are expected to handle (e.g. a CLI treating a duplicate
// CreateTopic call as success).
func toGRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

type Server struct {
	pb.UnimplementedBrokerServer

	Store *store.Store

	// LeaseSeconds is how long a claimed-but-unacked message is held by a
	// consumer before the reaper makes it ready again.
	LeaseSeconds int
	// PollInterval is how often each open Consume stream re-checks for a
	// ready message in its consumer group.
	PollInterval time.Duration
}

func NewServer(st *store.Store, leaseSeconds int, pollInterval time.Duration) *Server {
	return &Server{Store: st, LeaseSeconds: leaseSeconds, PollInterval: pollInterval}
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	t, err := s.Store.CreateTopic(ctx, req.GetName())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.Topic{Id: t.ID, Name: t.Name}, nil
}

func (s *Server) CreateConsumerGroup(ctx context.Context, req *pb.CreateConsumerGroupRequest) (*pb.ConsumerGroup, error) {
	g, err := s.Store.CreateConsumerGroup(ctx, req.GetName())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.ConsumerGroup{Id: g.ID, Name: g.Name}, nil
}

func (s *Server) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*pb.Subscription, error) {
	sub, err := s.Store.CreateSubscription(ctx, req.GetTopicName(), req.GetConsumerGroupName())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.Subscription{TopicId: sub.TopicID, ConsumerGroupId: sub.ConsumerGroupID}, nil
}

func (s *Server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	id, err := s.Store.Publish(ctx, req.GetTopicName(), req.GetContent())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.PublishResponse{MessageId: id}, nil
}
