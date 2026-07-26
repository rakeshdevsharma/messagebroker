// Command producer publishes messages to a broker topic. Run repeatedly
// (any number of processes, in parallel) to exercise the "multiple
// producers publishing concurrently" requirement.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "messagebroker/proto"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "broker gRPC address")
	topic := flag.String("topic", "", "topic to publish to (required)")
	message := flag.String("message", "", "single message to publish; if empty, reads newline-delimited messages from stdin")
	ensureTopic := flag.Bool("ensure-topic", true, "create the topic first if it doesn't already exist")
	flag.Parse()

	if *topic == "" {
		log.Fatal("-topic is required")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	client := pb.NewBrokerClient(conn)

	if *ensureTopic {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: *topic})
		cancel()
		if err != nil && status.Code(err) != codes.AlreadyExists {
			log.Fatalf("create topic %q: %v", *topic, err)
		}
	}

	publish := func(content []byte) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.Publish(ctx, &pb.PublishRequest{TopicName: *topic, Content: content})
		if err != nil {
			log.Fatalf("publish: %v", err)
		}
		fmt.Printf("published message_id=%d\n", resp.GetMessageId())
	}

	if *message != "" {
		publish([]byte(*message))
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		publish([]byte(line))
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("read stdin: %v", err)
	}
}
