package sse

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

// Client is a single connected SSE consumer. The handler goroutine
// drains `ch` and writes frames to the wire; the Manager pushes onto
// `ch` from its fan-out goroutine. token narrows the stream to txids
// associated with that callback token; empty means unfiltered.
//
// ctx is derived from the manager's parent context so we can broadcast
// "client is gone" to fan-out without racing the consumer-side close.
// The consumer (handleEvents) calls cancel() via Manager.unregister on
// disconnect, which causes any concurrent fan-out send to fall through
// the ctx.Done() arm of its select rather than blocking or panicking.
// The channel is intentionally NOT closed: senders always race the
// close otherwise (F-020).
type Client struct {
	ID    int64
	Token string
	Ch    chan *models.TransactionStatus
	// Ctx and Cancel intentionally live on the struct: this is the
	// per-client cancel signal that fan-out selects on (F-020). The
	// standard "don't store contexts" guidance doesn't apply to
	// long-lived cancellation handles owned by a registry entry.
	Ctx    context.Context    //nolint:containedctx // see comment above
	Cancel context.CancelFunc //nolint:containedctx // paired with Ctx above
}

// Manager owns the per-pod registry of SSE clients listening on
// /events. It calls events.Publisher.Subscribe ONCE at startup and fans
// every update out to every registered client. New clients register at
// /events connect time; deregister on disconnect.
//
// Token-based filtering happens in the fan-out path: every event is
// checked against each client's token via store.GetSubmissionsByToken.
// The implementation is O(clients × submissions) per event, which is
// acceptable for the workloads this service targets; a follow-up could
// cache token→txid mappings if hot.
type Manager struct {
	publisher events.Publisher
	store     store.Store
	logger    *zap.Logger

	// parentCtx is the long-lived context that owns the manager
	// goroutine. Per-client contexts are derived from it so canceling
	// the manager also cancels every registered client's fan-out path.
	parentCtx context.Context //nolint:containedctx // long-lived registry root

	nextClientID atomic.Int64

	mu      sync.RWMutex
	clients map[int64]*Client
}

// newManager constructs the manager and starts a single subscriber
// goroutine that runs for the lifetime of ctx. Returns (nil, nil) only
// when publisher is nil — that signals "no fan-out wired" so the
// handler returns 503; it's not an error condition.
func newManager(ctx context.Context, publisher events.Publisher, st store.Store, logger *zap.Logger) (*Manager, error) {
	if publisher == nil {
		return nil, nil //nolint:nilnil // intentional: nil manager means "no fan-out wired"
	}
	m := &Manager{
		publisher: publisher,
		store:     st,
		logger:    logger.Named("sse"),
		parentCtx: ctx,
		clients:   make(map[int64]*Client),
	}
	ch, err := publisher.Subscribe(ctx, "sse")
	if err != nil {
		return nil, fmt.Errorf("subscribing to events publisher: %w", err)
	}
	go m.run(ctx, ch)
	return m, nil
}

// run drains the upstream subscription and fans every update out to
// clients. Exits when ctx is canceled or the upstream channel closes.
func (m *Manager) run(ctx context.Context, in <-chan *models.TransactionStatus) {
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

// fanOut delivers a status update to every interested client. A
// non-empty client.Token causes a per-client check that the txid
// actually belongs to a submission registered under that token. Sends
// are non-blocking — slow consumers drop the event and recover via
// Last-Event-ID catchup on reconnect.
//
// Concurrency contract (F-020): we snapshot the client list under
// RLock and release the lock before sending. A client may unregister
// between the snapshot and the send. Each client owns a context that
// unregister cancels; the send selects on Ctx.Done() so a
// canceled-and-gone client takes the drop arm instead of blocking or
// panicking. The send channel is never closed by the manager — closing
// would race this exact send.
func (m *Manager) fanOut(ctx context.Context, status *models.TransactionStatus) {
	m.mu.RLock()
	clients := make([]*Client, 0, len(m.clients))
	for _, c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	for _, c := range clients {
		if c.Ctx.Err() != nil {
			metrics.APISSEDroppedTotal.WithLabelValues("client_gone").Inc()
			continue
		}
		if c.Token != "" && !m.txBelongsToToken(ctx, status.TxID, c.Token) {
			continue
		}
		select {
		case c.Ch <- status:
		case <-c.Ctx.Done():
			metrics.APISSEDroppedTotal.WithLabelValues("client_gone").Inc()
		default:
			metrics.APISSEDroppedTotal.WithLabelValues("slow_client").Inc()
			m.logger.Warn("dropping update for slow SSE client",
				zap.Int64("client_id", c.ID),
				zap.String("txid", status.TxID),
			)
		}
	}
}

// txBelongsToToken reports whether txid was submitted with the given
// callback token. Per-event submissions lookup; cached only by the
// database layer.
func (m *Manager) txBelongsToToken(ctx context.Context, txid, token string) bool {
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

// Register adds a client to the registry.
func (m *Manager) Register(c *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[c.ID] = c
}

// Unregister removes a client and signals any in-flight fan-out send to
// drop rather than push onto the channel. We deliberately do NOT close
// c.Ch: closing would race with concurrent fanOut sends (F-020). The
// consumer goroutine in handleEvents selects on its request ctx (which
// is what drives the unregister) so it exits without needing a
// channel-close signal; the unreferenced channel is reclaimed by the GC.
func (m *Manager) Unregister(id int64) {
	m.mu.Lock()
	c, ok := m.clients[id]
	if ok {
		delete(m.clients, id)
	}
	m.mu.Unlock()
	if ok {
		c.Cancel()
	}
}

// NewClient assembles a client with a fresh id, buffered channel, and
// per-client cancel handle. The client context is derived from the
// manager's parent ctx so a manager shutdown propagates to every client.
func (m *Manager) NewClient(token string) *Client {
	parent := m.parentCtx
	if parent == nil {
		parent = context.Background()
	}
	// cancel is stored on Client and invoked by Manager.Unregister — its
	// lifetime is the client connection, not this function.
	ctx, cancel := context.WithCancel(parent) //nolint:gosec // see comment above
	return &Client{
		ID:     m.nextClientID.Add(1),
		Token:  token,
		Ch:     make(chan *models.TransactionStatus, 64),
		Ctx:    ctx,
		Cancel: cancel,
	}
}

// handleEvents serves GET /events?callbackToken=<token>. Streams
// transaction status updates as Server-Sent Events. Optional
// Last-Event-ID request header triggers a catchup pass that emits any
// updates that occurred after the supplied nanosecond timestamp.
func (s *Service) handleEvents(c *gin.Context) {
	if s.manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "events stream not enabled"})
		return
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "streaming unsupported"})
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

	client := s.manager.NewClient(token)
	s.manager.Register(client)
	defer s.manager.Unregister(client.ID)

	if token != "" {
		var since time.Time
		if lastEventID := c.GetHeader("Last-Event-ID"); lastEventID != "" {
			if ns, err := strconv.ParseInt(lastEventID, 10, 64); err == nil {
				since = time.Unix(0, ns)
			}
		}
		s.sendCatchup(ctx, writer, token, since)
	}

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case status := <-client.Ch:
			if status == nil {
				continue
			}
			if err := writeStatus(writer, status); err != nil {
				return
			}
		case <-keepalive.C:
			if err := writer.write(": keepalive\n\n"); err != nil {
				return
			}
		}
	}
}

// sendCatchup replays the current persisted status of every txid
// registered under the supplied token. When `since` is non-zero only
// statuses with a timestamp strictly after `since` are emitted (the
// Last-Event-ID reconnect contract). When `since` is zero every status
// is emitted — used as the initial-state replay on a fresh connect.
func (s *Service) sendCatchup(ctx context.Context, w *sseWriter, token string, since time.Time) {
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
		if err := writeStatus(w, status); err != nil {
			return
		}
	}
}

// writeStatus emits one status frame. Event id is the timestamp in
// nanoseconds so clients can use it as Last-Event-ID on reconnect.
func writeStatus(w *sseWriter, status *models.TransactionStatus) error {
	data, err := json.Marshal(statusPayload{
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

// statusPayload is the JSON shape inside a `data:` field. Field order
// and names mirror the legacy wire format: txid, txStatus, timestamp.
type statusPayload struct {
	TxID      string `json:"txid"`
	TxStatus  string `json:"txStatus"`
	Timestamp string `json:"timestamp"`
}
