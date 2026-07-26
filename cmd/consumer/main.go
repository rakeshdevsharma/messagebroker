// Command consumer subscribes to a broker topic as a member of a consumer
// group and prints/acks each message it's delivered. Run any number of
// instances with the same -group (and distinct, or auto-generated, -name)
// to exercise horizontal scaling: the broker guarantees each message in
// the group goes to exactly one of them.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "messagebroker/proto"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "broker gRPC address")
	topic := flag.String("topic", "", "topic to subscribe to (required)")
	group := flag.String("group", "", "consumer group name (empty = the default group)")
	name := flag.String("name", "", "consumer name within the group (empty = auto-generated from hostname+pid)")
	ensureSubscription := flag.Bool("ensure-subscription", true, "create the subscription first if it doesn't already exist")
	flag.Parse()

	if *topic == "" {
		log.Fatal("-topic is required")
	}
	if *name == "" {
		*name = defaultConsumerName()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewBrokerClient(conn)

	if *ensureSubscription {
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := client.CreateSubscription(subCtx, &pb.CreateSubscriptionRequest{
			TopicName:         *topic,
			ConsumerGroupName: *group,
		})
		cancel()
		if err != nil {
			log.Fatalf("create subscription: %v", err)
		}
	}

	stream, err := client.Consume(ctx)
	if err != nil {
		log.Fatalf("open consume stream: %v", err)
	}

	if err := stream.Send(&pb.ConsumeClientMsg{
		Kind: &pb.ConsumeClientMsg_Register{
			Register: &pb.RegisterRequest{ConsumerGroupName: *group, ConsumerName: *name},
		},
	}); err != nil {
		log.Fatalf("register: %v", err)
	}

	log.Printf("consumer %q joined group %q on topic %q, waiting for messages (Ctrl+C to stop)",
		*name, groupLabel(*group), *topic)

	for {
		serverMsg, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("shutting down: %v", ctx.Err())
				return
			}
			log.Fatalf("recv: %v", err)
		}

		delivery := serverMsg.GetMessage()
		if delivery == nil {
			continue
		}

		fmt.Printf("[%s] message_id=%d topic=%s delivery_count=%d content=%q\n",
			*name, delivery.GetMessageId(), delivery.GetTopicName(), delivery.GetDeliveryCount(), delivery.GetContent())

		if err := stream.Send(&pb.ConsumeClientMsg{
			Kind: &pb.ConsumeClientMsg_Ack{
				Ack: &pb.AckRequest{MessageId: delivery.GetMessageId()},
			},
		}); err != nil {
			log.Fatalf("ack: %v", err)
		}
	}
}

func defaultConsumerName() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

func groupLabel(group string) string {
	if group == "" {
		return "default"
	}
	return group
}
