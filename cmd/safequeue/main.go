// Command safequeue runs the durable job queue as an HTTP service.
//
// Usage:
//
//	safequeue -addr :8080 -data ./data -visibility 30s -max-attempts 5
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/cognis-digital/safequeue/httpapi"
	"github.com/cognis-digital/safequeue/queue"
)

func main() {
	var (
		addr        = flag.String("addr", ":8080", "HTTP listen address")
		dataDir     = flag.String("data", "./data", "directory for the durable queue log")
		visibility  = flag.Duration("visibility", 30*time.Second, "lease visibility timeout")
		maxAttempts = flag.Int("max-attempts", 5, "delivery attempts before dead-lettering")
		baseBackoff = flag.Duration("base-backoff", time.Second, "first retry backoff delay")
		maxBackoff  = flag.Duration("max-backoff", 5*time.Minute, "maximum retry backoff delay")
	)
	flag.Parse()

	q, err := queue.Open(*dataDir, queue.Options{
		VisibilityTimeout: *visibility,
		MaxAttempts:       *maxAttempts,
		BaseBackoff:       *baseBackoff,
		MaxBackoff:        *maxBackoff,
	})
	if err != nil {
		log.Printf("safequeue: failed to open queue: %v", err)
		os.Exit(1)
	}
	defer q.Close()

	srv := httpapi.New(q)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("safequeue listening on %s (data dir: %s)", *addr, *dataDir)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("safequeue: server error: %v", err)
		os.Exit(1)
	}
}
