package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// ErrUnexpectedSSEStatus is returned when SSE returns an unexpected status code.
var ErrUnexpectedSSEStatus = errors.New("unexpected SSE status code")

// sseManager manages SSE connections and fan-out to subscribers.
type sseManager struct {
	client *Client

	mu          sync.RWMutex
	connections map[string]*sseConnection // key is callbackToken (empty string for all events)
	subscribers map[<-chan *models.TransactionStatus]*subscriber
	nextSubID   int
}

// sseConnection represents an active SSE connection.
type sseConnection struct {
	token       string
	ctx         context.Context //nolint:containedctx // context needed for connection lifecycle
	cancel      context.CancelFunc
	lastEventID string
	subscribers map[int]*subscriber
	mu          sync.RWMutex
}

// subscriber represents a single subscriber to status updates.
type subscriber struct {
	id     int
	ch     chan *models.TransactionStatus
	ctx    context.Context //nolint:containedctx // context needed for subscriber lifecycle
	cancel context.CancelFunc
	token  string
}

// newSSEManager creates a new SSE manager.
func newSSEManager(client *Client) *sseManager {
	return &sseManager{
		client:      client,
		connections: make(map[string]*sseConnection),
		subscribers: make(map[<-chan *models.TransactionStatus]*subscriber),
	}
}

// subscribe creates a new subscription to status updates.
func (m *sseManager) subscribe(ctx context.Context, callbackToken string) (<-chan *models.TransactionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create subscriber
	subCtx, subCancel := context.WithCancel(ctx)
	sub := &subscriber{
		id:     m.nextSubID,
		ch:     make(chan *models.TransactionStatus, 100),
		ctx:    subCtx,
		cancel: subCancel,
		token:  callbackToken,
	}
	m.nextSubID++
	m.subscribers[sub.ch] = sub

	// Get or create connection for this token
	conn, exists := m.connections[callbackToken]
	if !exists {
		conn = m.createConnection(callbackToken)
		m.connections[callbackToken] = conn
	}

	// Add subscriber to connection
	conn.mu.Lock()
	conn.subscribers[sub.id] = sub
	conn.mu.Unlock()

	// Clean up when subscriber context is done
	go func() {
		<-subCtx.Done()
		m.removeSubscriber(sub)
	}()

	return sub.ch, nil
}

// unsubscribe removes a subscription.
func (m *sseManager) unsubscribe(ch <-chan *models.TransactionStatus) {
	m.mu.Lock()
	sub, exists := m.subscribers[ch]
	m.mu.Unlock()

	if exists {
		sub.cancel()
	}
}

// removeSubscriber removes a subscriber and closes its connection if no more subscribers.
func (m *sseManager) removeSubscriber(sub *subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.subscribers, sub.ch)
	close(sub.ch)

	// Remove from connection
	conn, exists := m.connections[sub.token]
	if !exists {
		return
	}

	conn.mu.Lock()
	delete(conn.subscribers, sub.id)
	remaining := len(conn.subscribers)
	conn.mu.Unlock()

	// Close connection if no more subscribers
	if remaining == 0 {
		conn.cancel()
		delete(m.connections, sub.token)
	}
}

// createConnection creates a new SSE connection for the given token.
func (m *sseManager) createConnection(token string) *sseConnection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &sseConnection{
		token:       token,
		ctx:         ctx,
		cancel:      cancel,
		subscribers: make(map[int]*subscriber),
	}

	go m.runConnection(conn)

	return conn
}

// runConnection manages the SSE connection lifecycle with reconnection.
func (m *sseManager) runConnection(conn *sseConnection) {
	backoff := time.Second

	for {
		select {
		case <-conn.ctx.Done():
			return
		default:
		}

		err := m.connectSSE(conn)
		if err != nil {
			// Exponential backoff with max 30 seconds
			select {
			case <-conn.ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}

		// Reset backoff on successful connection
		backoff = time.Second
	}
}

// connectSSE establishes an SSE connection and processes events.
//
//nolint:gocyclo // complex SSE connection handling logic
func (m *sseManager) connectSSE(conn *sseConnection) error {
	// Build URL
	url := m.client.baseURL + "/events"
	if conn.token != "" {
		url += "/" + conn.token
	}

	req, err := http.NewRequestWithContext(conn.ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Send Last-Event-ID for catchup
	if conn.lastEventID != "" {
		req.Header.Set("Last-Event-ID", conn.lastEventID)
	}

	resp, err := m.client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return ErrUnexpectedSSEStatus
	}

	// Process SSE events
	scanner := bufio.NewScanner(resp.Body)
	var currentEvent sseEvent

	for scanner.Scan() {
		line := scanner.Text()

		// Empty line marks end of event
		if line == "" {
			if currentEvent.data != "" {
				m.processEvent(conn, currentEvent)
			}
			currentEvent = sseEvent{}
			continue
		}

		// Parse SSE fields
		if strings.HasPrefix(line, "id:") {
			currentEvent.id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		} else if strings.HasPrefix(line, "event:") {
			currentEvent.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			currentEvent.data = strings.TrimPrefix(line, "data:")
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("connection error: %w", err)
	}

	return nil
}

// sseEvent represents a single SSE event.
type sseEvent struct {
	id    string
	event string
	data  string
}

// processEvent processes a single SSE event and fans out to subscribers.
func (m *sseManager) processEvent(conn *sseConnection, event sseEvent) {
	// Update last event ID
	if event.id != "" {
		conn.lastEventID = event.id
	}

	// Only process status events
	if event.event != "status" {
		return
	}

	// Parse status
	var status models.TransactionStatus
	if err := json.Unmarshal([]byte(event.data), &status); err != nil {
		return
	}

	// Fan out to subscribers
	conn.mu.RLock()
	subs := make([]*subscriber, 0, len(conn.subscribers))
	for _, sub := range conn.subscribers {
		subs = append(subs, sub)
	}
	conn.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.ch <- &status:
		case <-sub.ctx.Done():
		default:
			// Drop if channel is full
		}
	}
}

// close closes all connections and subscribers.
func (m *sseManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Cancel all connections
	for _, conn := range m.connections {
		conn.cancel()
	}
	m.connections = make(map[string]*sseConnection)

	// Close all subscriber channels
	for ch, sub := range m.subscribers {
		sub.cancel()
		close(sub.ch)
		delete(m.subscribers, ch)
	}
}
