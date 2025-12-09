package memory

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
)

// InMemoryPublisher implements events.Publisher using Go channels
type InMemoryPublisher struct {
	subscribers []chan *models.TransactionStatus
	mu          sync.RWMutex
	bufferSize  int
}

// NewInMemoryPublisher creates a new in-memory event publisher
func NewInMemoryPublisher(bufferSize int) events.Publisher {
	return &InMemoryPublisher{
		subscribers: make([]chan *models.TransactionStatus, 0),
		bufferSize:  bufferSize,
	}
}

// Publish sends a status update to all subscribers
func (p *InMemoryPublisher) Publish(ctx context.Context, status *models.TransactionStatus) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, sub := range p.subscribers {
		select {
		case sub <- status:
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Subscriber is slow, skip this update to prevent blocking
		}
	}

	return nil
}

// Subscribe returns a channel that receives all status updates
func (p *InMemoryPublisher) Subscribe(ctx context.Context) (<-chan *models.TransactionStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch := make(chan *models.TransactionStatus, p.bufferSize)
	p.subscribers = append(p.subscribers, ch)

	return ch, nil
}

// Close closes the publisher and all subscriptions
func (p *InMemoryPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, sub := range p.subscribers {
		close(sub)
	}
	p.subscribers = nil

	return nil
}
