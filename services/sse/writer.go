package sse

import (
	"fmt"
	"net/http"
	"sync"
)

// sseWriter serializes writes to a single http.ResponseWriter. Gin does
// not promise concurrent-safe writes, and the handler + keepalive
// goroutines both write to the same writer — a mutex keeps framing
// intact. The SSE service unwinds writers via request-context
// cancellation rather than an explicit close(): a canceled ctx exits
// the handler loop, the deferred Unregister cancels the per-client ctx,
// and any concurrent fanOut send falls through the ctx.Done() arm.
type sseWriter struct {
	mu sync.Mutex
	w  http.ResponseWriter
	f  http.Flusher
}

func (s *sseWriter) write(payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := fmt.Fprint(s.w, payload); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}
