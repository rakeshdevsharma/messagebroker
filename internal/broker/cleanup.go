package broker

import (
	"context"
	"log"
	"time"
)

// RunMessageCleanup periodically deletes messages older than 7 days or
// already fully acked by every subscription. Because Publish inserts the
// message and its message_queue fan-out rows in a single transaction, "no
// remaining queue rows" here only ever means fully-acked, never mid-publish.
func (s *Server) RunMessageCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.Store.CleanupMessages(ctx)
			if err != nil {
				log.Printf("message cleanup error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("message cleanup: deleted %d message(s)", n)
			}
		}
	}
}
