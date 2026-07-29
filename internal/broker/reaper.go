package broker

import (
	"context"
	"log"
	"time"
)

// RunLeaseReaper is the slow-path safety net for dead-consumer detection: it
// periodically frees any message whose lease has expired, recovering
// messages held by a consumer that is still connected but hung and never
// acking — a case stream-disconnect detection alone would miss.
func (s *Server) RunLeaseReaper(ctx context.Context, interval time.Duration) {
	log.Printf("lease reaper started interval=%s", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("lease reaper stopped")
			return
		case <-ticker.C:
			n, err := s.Store.ReapExpiredLeases(ctx)
			if err != nil {
				log.Printf("lease reaper error: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("lease reaper: released %d expired message(s)", n)
			}
		}
	}
}
