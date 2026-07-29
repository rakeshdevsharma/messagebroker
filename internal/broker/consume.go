package broker

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "messagebroker/proto"
	"messagebroker/internal/store"
)

// Consume is a bidirectional stream: the client registers once as a
// (consumer_group, consumer_name) pair, then the broker pushes claimed
// messages down the stream on a poll interval while concurrently reading
// Acks back on the same stream. The stream's lifetime is the broker's
// primary (fast-path) signal that a consumer has gone away — on
// disconnect, any message still checked out to this consumer is released
// immediately rather than waiting for its lease to expire.
func (s *Server) Consume(stream pb.Broker_ConsumeServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	reg := first.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first message on Consume stream must be a Register")
	}

	groupName := reg.GetConsumerGroupName()
	if groupName == "" {
		groupName = store.DefaultConsumerGroupName
	}

	group, err := s.Store.EnsureConsumerGroupByName(ctx, groupName)
	if err != nil {
		return toGRPCError(err)
	}
	consumer, err := s.Store.EnsureConsumer(ctx, group.ID, reg.GetConsumerName())
	if err != nil {
		return toGRPCError(err)
	}
	log.Printf("Consume connected group=%q (id=%d) consumer=%q (id=%d)", groupName, group.ID, consumer.Name, consumer.ID)

	recvErrCh := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			if ack := msg.GetAck(); ack != nil {
				owned, err := s.Store.Ack(ctx, ack.GetMessageId(), group.ID, consumer.ID)
				if err != nil {
					log.Printf("ack error: consumer=%d message=%d: %v", consumer.ID, ack.GetMessageId(), err)
				} else if !owned {
					// Stale/duplicate ack under at-least-once delivery: the
					// message was already reassigned (lease expired or a
					// prior disconnect). Not an error.
					log.Printf("stale ack ignored: consumer=%d message=%d", consumer.ID, ack.GetMessageId())
				}
			}
		}
	}()

	ticker := time.NewTicker(s.PollInterval)
	defer ticker.Stop()

	release := func(reason string) {
		log.Printf("Consume disconnecting consumer=%d group=%d: %s", consumer.ID, group.ID, reason)
		if err := s.Store.ReleaseByConsumer(context.Background(), consumer.ID); err != nil {
			log.Printf("release on disconnect failed: consumer=%d: %v", consumer.ID, err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			release("stream context canceled")
			return ctx.Err()

		case err := <-recvErrCh:
			if errors.Is(err, io.EOF) {
				release("client closed stream")
				return nil
			}
			release("recv error: " + err.Error())
			return err

		case <-ticker.C:
			claimed, err := s.Store.Claim(ctx, group.ID, consumer.ID, s.LeaseSeconds)
			if err != nil {
				log.Printf("claim error: consumer=%d group=%d: %v", consumer.ID, group.ID, err)
				continue
			}
			if claimed == nil {
				continue
			}
			log.Printf("delivering message id=%d topic=%q consumer=%d delivery_count=%d", claimed.MessageID, claimed.TopicName, consumer.ID, claimed.DeliveryCount)
			err = stream.Send(&pb.ConsumeServerMsg{
				Kind: &pb.ConsumeServerMsg_Message{
					Message: &pb.MessageDelivery{
						MessageId:     claimed.MessageID,
						TopicName:     claimed.TopicName,
						Content:       claimed.Content,
						DeliveryCount: claimed.DeliveryCount,
					},
				},
			})
			if err != nil {
				release("send failed: " + err.Error())
				return err
			}
		}
	}
}
