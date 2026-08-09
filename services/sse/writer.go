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

// writeNoFlush writes a frame WITHOUT flushing. It exists so a burst of
// per-tx status frames (the manager unfans one Kafka message into ~50 frames)
// can be written back-to-back and drained with a single flush() rather than
// one flush per frame. The caller MUST call flush() after the burst to push
// the buffered frames onto the wire.
func (s *sseWriter) writeNoFlush(payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := fmt.Fprint(s.w, payload); err != nil {
		return err
	}
	return nil
}

// flush flushes any buffered frames to the wire. http.Flusher.Flush has no
// error return, so this always returns nil; the signature carries an error to
// stay symmetric with write/writeNoFlush at call sites.
func (s *sseWriter) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.f.Flush()
	return nil
}
