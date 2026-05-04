package api_server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// sseClient is a single connected SSE consumer. The handler goroutine drains
// `ch` and writes frames to the wire; the manager pushes onto `ch` from its
// fan-out goroutine. token narrows the stream to txids associated with that
// callback token; empty means unfiltered.
//
// ctx is derived from the manager's parent context so we can broadcast
// "client is gone" to fan-out without racing the consumer-side close. The
// consumer (handleEventsSSE) calls cancel() via unregister on disconnect,
// which causes any concurrent fan-out send to fall through the ctx.Done()
// arm of its select rather than blocking or panicking. The channel is
// intentionally NOT closed: senders always race the close otherwise (F-020).
// The buffered channel is left to the GC once no goroutine references it.
type sseClient struct {
	id    int64
	token string
	ch    chan *models.TransactionStatus
	// ctx and cancel intentionally live on the struct: this is the
	// per-client cancel signal that fan-out selects on to avoid sending
	// onto a no-longer-drained channel (F-020). The standard "don't
	// store contexts" guidance doesn't apply to long-lived cancellation
	// handles owned by a registry entry.
	ctx    context.Context    //nolint:containedctx // see comment above
	cancel context.CancelFunc //nolint:containedctx // paired with ctx above
}

// sseManager owns the per-pod registry of SSE clients listening on /events.
// It calls events.Publisher.Subscribe ONCE at startup and fans every update
// out to every registered client. New clients register at /events connect
// time; deregister on disconnect.
//
// Token-based filtering happens in the fan-out path: every event is checked
// against each client's token via store.GetSubmissionsByToken — matching the
// old arcade's per-event lookup semantics. The implementation is O(clients ×
// submissions) per event, which is acceptable for the workloads this
// service targets; a follow-up could cache token→txid mappings if hot.
type sseManager struct {
	publisher events.Publisher
	store     store.Store
	logger    *zap.Logger

	// parentCtx is the long-lived context that owns the manager goroutine.
	// Per-client contexts are derived from it so canceling the manager
	// also cancels every registered client's fan-out path.
	parentCtx context.Context //nolint:containedctx // long-lived registry root

	nextClientID atomic.Int64

	mu      sync.RWMutex
	clients map[int64]*sseClient
}

// newSSEManager constructs the manager and starts a single subscriber goroutine
// that runs for the lifetime of ctx. Returns (nil, nil) only when publisher
// is nil — that signals "no fan-out wired" so the handler returns 503; it's
// not an error condition. Callers must check for nil before storing.
func newSSEManager(ctx context.Context, publisher events.Publisher, st store.Store, logger *zap.Logger) (*sseManager, error) {
	if publisher == nil {
		return nil, nil //nolint:nilnil // intentional: nil manager means "no fan-out wired"
	}
	m := &sseManager{
		publisher: publisher,
		store:     st,
		logger:    logger.Named("sse"),
		parentCtx: ctx,
		clients:   make(map[int64]*sseClient),
	}
	ch, err := publisher.Subscribe(ctx, "sse")
	if err != nil {
		return nil, fmt.Errorf("subscribing to events publisher: %w", err)
	}
	go m.run(ctx, ch)
	return m, nil
}

// run drains the upstream subscription and fans every update out to clients.
// Exits when ctx is canceled or the upstream channel closes.
func (m *sseManager) run(ctx context.Context, in <-chan *models.TransactionStatus) {
	for {
		select {
		case <-ctx.Done():
			return
		case status, ok := <-in:
			if !ok {
				return
			}
			if status == nil {
				continue
			}
			m.fanOut(ctx, status)
		}
	}
}

// fanOut delivers a status update to every interested client. A non-empty
// client.token causes a per-client check that the txid actually belongs to a
// submission registered under that token (mirrors the old arcade's
// txBelongsToToken behavior). Sends are non-blocking — slow consumers drop
// the event and recover via Last-Event-ID catchup on reconnect.
//
// Concurrency contract (F-020): we snapshot the client list under RLock and
// release the lock before sending. A client may unregister between the
// snapshot and the send. Each client owns a context that unregister cancels;
// the send selects on ctx.Done() so a canceled-and-gone client takes the
// drop arm instead of blocking or panicking. The send channel is never
// closed by the manager — closing would race this exact send. Slow clients
// fall through `default` and increment the dropped-by-reason counter.
func (m *sseManager) fanOut(ctx context.Context, status *models.TransactionStatus) {
	m.mu.RLock()
	clients := make([]*sseClient, 0, len(m.clients))
	for _, c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	for _, c := range clients {
		// Quick out for already-gone clients: avoids the token lookup work
		// for connections we know are unwinding. The send-site re-checks
		// ctx.Done() to handle the race where cancel happens between here
		// and the channel select.
		if c.ctx.Err() != nil {
			metrics.APISSEDroppedTotal.WithLabelValues("client_gone").Inc()
			continue
		}
		if c.token != "" && !m.txBelongsToToken(ctx, status.TxID, c.token) {
			continue
		}
		select {
		case c.ch <- status:
		case <-c.ctx.Done():
			// Client unregistered concurrently. Drop without sending —
			// the channel may already be unreferenced by the consumer.
			metrics.APISSEDroppedTotal.WithLabelValues("client_gone").Inc()
		default:
			// Buffer is full: consumer is slow. Drop and let
			// Last-Event-ID catchup recover on reconnect.
			metrics.APISSEDroppedTotal.WithLabelValues("slow_client").Inc()
			m.logger.Warn("dropping update for slow SSE client",
				zap.Int64("client_id", c.id),
				zap.String("txid", status.TxID),
			)
		}
	}
}

// txBelongsToToken reports whether txid was submitted with the given callback
// token. Mirrors old arcade behavior: per-event submissions lookup; cached
// only by the database layer.
func (m *sseManager) txBelongsToToken(ctx context.Context, txid, token string) bool {
	if m.store == nil {
		return false
	}
	subs, err := m.store.GetSubmissionsByToken(ctx, token)
	if err != nil {
		m.logger.Warn("submission lookup failed",
			zap.String("token", token),
			zap.Error(err),
		)
		return false
	}
	for _, s := range subs {
		if s.TxID == txid {
			return true
		}
	}
	return false
}

// register adds a client to the registry.
func (m *sseManager) register(c *sseClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[c.id] = c
}

// unregister removes a client and signals any in-flight fan-out send to drop
// rather than push onto the channel. We deliberately do NOT close c.ch:
// closing would race with concurrent fanOut sends (F-020). The consumer
// goroutine in handleEventsSSE selects on its request ctx (which is what
// drives the unregister) so it exits without needing a channel-close signal;
// the unreferenced channel is reclaimed by the GC.
func (m *sseManager) unregister(id int64) {
	m.mu.Lock()
	c, ok := m.clients[id]
	if ok {
		delete(m.clients, id)
	}
	m.mu.Unlock()
	if ok {
		c.cancel()
	}
}

// newClient assembles a client with a fresh id, buffered channel, and a
// per-client cancel handle. The client context is derived from the
// manager's parent ctx so a manager shutdown propagates to every client.
func (m *sseManager) newClient(token string) *sseClient {
	parent := m.parentCtx
	if parent == nil {
		parent = context.Background()
	}
	// The cancel func is stored on sseClient and invoked by unregister;
	// the linter can't trace that flow, so silence the warning explicitly.
	ctx, cancel := context.WithCancel(parent) //nolint:gosec // cancel called by sseManager.unregister
	return &sseClient{
		id:     m.nextClientID.Add(1),
		token:  token,
		ch:     make(chan *models.TransactionStatus, 64),
		ctx:    ctx,
		cancel: cancel,
	}
}

// handleEventsSSE serves GET /events?callbackToken=<token>. Streams
// transaction status updates as Server-Sent Events. Optional Last-Event-ID
// request header triggers a catchup pass that emits any updates that occurred
// after the supplied nanosecond timestamp (matches the old arcade exactly).
func (s *Server) handleEventsSSE(c *gin.Context) {
	if s.sse == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "events stream not enabled"})
		return
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "streaming unsupported"})
		return
	}

	token := c.Query("callbackToken")
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	writer := &sseWriter{w: c.Writer, f: flusher}
	ctx := c.Request.Context()

	// Register the live client BEFORE replay so any update published during
	// the replay lands in the client's buffer rather than being dropped.
	// Replay can deliver the same status the live feed will deliver shortly
	// after — that's a tolerable duplicate (each frame carries an `id:` of
	// the status timestamp, so clients dedupe by latest-per-txid).
	client := s.sse.newClient(token)
	s.sse.register(client)
	defer s.sse.unregister(client.id)

	// Initial-state replay: when a callbackToken is set, emit the current
	// status of every txid registered under that token. With no Last-Event-ID
	// (since == zero) every status is emitted; with one set, only statuses
	// strictly newer than that timestamp are. Without a token there's no way
	// to scope the replay, so it's skipped — that path remains live-only.
	if token != "" {
		var since time.Time
		if lastEventID := c.GetHeader("Last-Event-ID"); lastEventID != "" {
			if ns, err := strconv.ParseInt(lastEventID, 10, 64); err == nil {
				since = time.Unix(0, ns)
			}
		}
		s.sendSSECatchup(ctx, writer, token, since)
	}

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	// The channel is intentionally never closed by the manager (see F-020),
	// so the loop exits via the request ctx — that's what triggers the
	// deferred unregister, which cancels the client context. Reads from a
	// non-closed, no-longer-written channel just block until ctx.Done()
	// wins the select.
	for {
		select {
		case <-ctx.Done():
			return
		case status := <-client.ch:
			if status == nil {
				continue
			}
			if err := writeSSEStatus(writer, status); err != nil {
				return
			}
		case <-keepalive.C:
			if err := writer.write(": keepalive\n\n"); err != nil {
				return
			}
		}
	}
}

// sendSSECatchup replays the current persisted status of every txid registered
// under the supplied token. When `since` is non-zero only statuses with a
// timestamp strictly after `since` are emitted (the Last-Event-ID reconnect
// contract). When `since` is zero every status is emitted — used as the
// initial-state replay on a fresh connect, so a client that connects after
// arcade has already published events still sees the current state.
func (s *Server) sendSSECatchup(ctx context.Context, w *sseWriter, token string, since time.Time) {
	subs, err := s.store.GetSubmissionsByToken(ctx, token)
	if err != nil {
		return
	}
	for _, sub := range subs {
		status, err := s.store.GetStatus(ctx, sub.TxID)
		if err != nil || status == nil {
			continue
		}
		if !since.IsZero() && !status.Timestamp.After(since) {
			continue
		}
		if err := writeSSEStatus(w, status); err != nil {
			return
		}
	}
}

// writeSSEStatus emits one status frame in the old arcade's exact wire
// format. Event id is the timestamp in nanoseconds so clients can use it as
// Last-Event-ID on reconnect.
func writeSSEStatus(w *sseWriter, status *models.TransactionStatus) error {
	data, err := json.Marshal(sseStatusPayload{
		TxID:      status.TxID,
		TxStatus:  string(status.Status),
		Timestamp: status.Timestamp.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	frame := fmt.Sprintf("id: %d\nevent: status\ndata: %s\n\n", status.Timestamp.UnixNano(), data)
	return w.write(frame)
}

// sseStatusPayload is the JSON shape inside a `data:` field. Field order and
// names mirror the old arcade: txid, txStatus, timestamp.
type sseStatusPayload struct {
	TxID      string `json:"txid"`
	TxStatus  string `json:"txStatus"`
	Timestamp string `json:"timestamp"`
}
