package kafka

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

// NewProducerForTest constructs a Producer backed by an in-memory broker.
// Intended for tests — exposes no external requirements.
func NewProducerForTest() (*Producer, Broker) {
	b := NewMemoryBroker(1000)
	return NewProducer(b), b
}

// nopLogger returns a no-op logger for tests that don't assert on log output.
func nopLogger() *zap.Logger {
	return zap.NewNop()
}

// RecordingBroker is a minimal Broker that records every produce call so
// tests can assert on call counts and payloads. It does not implement
// Subscribe — use NewMemoryBroker when a test needs publish/consume.
type RecordingBroker struct {
	mu sync.Mutex

	// Sends captures every Send / SendAsync call in order.
	Sends []RecordedSend
	// Batches captures each SendBatch call as a slice of KeyValue entries.
	Batches [][]KeyValue
	// BatchCalls is the total number of SendBatch invocations.
	BatchCalls int

	// Errors — set these before the call to force a specific failure mode.
	SendErr  error
	BatchErr error
}

// RecordedSend is a single Send / SendAsync observation.
type RecordedSend struct {
	Topic string
	Key   string
	Value []byte
}

// Send records the call and returns SendErr if set.
func (b *RecordingBroker) Send(_ context.Context, topic, key string, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.SendErr != nil {
		return b.SendErr
	}
	b.Sends = append(b.Sends, RecordedSend{Topic: topic, Key: key, Value: value})
	return nil
}

// SendAsync mirrors Send for tests that don't distinguish sync vs. async.
func (b *RecordingBroker) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	return b.Send(ctx, topic, key, value)
}

// SendBatch records the batch and returns BatchErr if set.
func (b *RecordingBroker) SendBatch(_ context.Context, _ string, msgs []KeyValue) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.BatchCalls++
	if b.BatchErr != nil {
		return b.BatchErr
	}
	// Copy the slice so later caller mutation can't race with assertions.
	cp := make([]KeyValue, len(msgs))
	copy(cp, msgs)
	b.Batches = append(b.Batches, cp)
	return nil
}

// Subscribe is not implemented — callers that need real delivery should use
// NewMemoryBroker instead.
func (b *RecordingBroker) Subscribe(string, []string) (Subscription, error) {
	panic("RecordingBroker: Subscribe not supported — use NewMemoryBroker for consume tests")
}

// PartitionCount returns 1 since tests don't exercise multi-partition topics.
func (b *RecordingBroker) PartitionCount(_ string) (int, error) { return 1, nil }

// Close is a no-op.
func (b *RecordingBroker) Close() error { return nil }

// MessageCount returns the total number of messages recorded across all
// SendBatch calls — mirrors the old Sarama mock's flat-list behavior so
// tests that counted messages across batches port with minimal change.
func (b *RecordingBroker) MessageCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, batch := range b.Batches {
		n += len(batch)
	}
	return n
}

// Lock / Unlock expose the broker's mutex so tests can inspect fields
// without a data race. Used when poking at Sends / Batches directly.
func (b *RecordingBroker) Lock()   { b.mu.Lock() }
func (b *RecordingBroker) Unlock() { b.mu.Unlock() }
