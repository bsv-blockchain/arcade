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
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// sseClient is a single connected SSE consumer. The handler goroutine drains
// `ch` and writes frames to the wire; the manager pushes onto `ch` from its
// fan-out goroutine. token narrows the stream to txids associated with that
// callback token; empty means unfiltered.
type sseClient struct {
	id    int64
	token string
	ch    chan *models.TransactionStatus
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
		clients:   make(map[int64]*sseClient),
	}
	ch, err := publisher.Subscribe(ctx)
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
func (m *sseManager) fanOut(ctx context.Context, status *models.TransactionStatus) {
	m.mu.RLock()
	clients := make([]*sseClient, 0, len(m.clients))
	for _, c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	for _, c := range clients {
		if c.token != "" && !m.txBelongsToToken(ctx, status.TxID, c.token) {
			continue
		}
		select {
		case c.ch <- status:
		default:
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

// register adds a client to the registry and returns its assigned id.
func (m *sseManager) register(c *sseClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[c.id] = c
}

// unregister removes a client and closes its channel.
func (m *sseManager) unregister(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[id]; ok {
		delete(m.clients, id)
		close(c.ch)
	}
}

// newClient assembles a client with a fresh id and buffered channel.
func (m *sseManager) newClient(token string) *sseClient {
	return &sseClient{
		id:    m.nextClientID.Add(1),
		token: token,
		ch:    make(chan *models.TransactionStatus, 64),
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

	for {
		select {
		case <-ctx.Done():
			return
		case status, ok := <-client.ch:
			if !ok {
				return
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
