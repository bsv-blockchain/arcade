package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
)

// fakeClaim is a Claim that tracks whether MarkMessage was invoked. Tests
// assert on Marked to verify that processOne preserves the offset on DLQ
// publish failure.
type fakeClaim struct {
	ctx    context.Context
	ch     chan *Message
	marked atomic.Int32
}

func newFakeClaim() *fakeClaim {
	return &fakeClaim{ctx: context.Background(), ch: make(chan *Message, 1)}
}

func (c *fakeClaim) Messages() <-chan *Message { return c.ch }
func (c *fakeClaim) Context() context.Context  { return c.ctx }
func (c *fakeClaim) MarkMessage(_ *Message)    { c.marked.Add(1) }
func (c *fakeClaim) Marked() int               { return int(c.marked.Load()) }

func TestProcessWithRetry_BackoffDelaysBetweenAttempts(t *testing.T) {
	attempts := 0
	handler := func(_ context.Context, _ *Message) error {
		attempts++
		return errors.New("fail")
	}

	c := &ConsumerGroup{
		handler:    handler,
		maxRetries: 4,
		logger:     zap.NewNop(),
	}

	msg := &Message{Topic: "test"}

	start := time.Now()
	err := c.processWithRetry(context.Background(), msg)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if attempts != 4 {
		t.Errorf("expected 4 attempts, got %d", attempts)
	}

	if elapsed < 500*time.Millisecond {
		t.Errorf("expected backoff delays totaling ~600ms, but retries completed in %v", elapsed)
	}
}

func TestProcessWithRetry_BackoffRespectsContextCancellation(t *testing.T) {
	attempts := 0
	handler := func(_ context.Context, _ *Message) error {
		attempts++
		return errors.New("fail")
	}

	c := &ConsumerGroup{
		handler:    handler,
		maxRetries: 10,
		logger:     zap.NewNop(),
	}

	msg := &Message{Topic: "test"}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = c.processWithRetry(ctx, msg)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("expected early exit on context cancellation, but took %v", elapsed)
	}
	if attempts >= 10 {
		t.Errorf("expected fewer than 10 attempts due to context cancellation, got %d", attempts)
	}
}

func TestProcessWithRetry_SuccessOnFirstAttempt_NoDelay(t *testing.T) {
	handler := func(_ context.Context, _ *Message) error {
		return nil
	}

	c := &ConsumerGroup{
		handler:    handler,
		maxRetries: 5,
		logger:     zap.NewNop(),
	}

	msg := &Message{Topic: "test"}

	start := time.Now()
	err := c.processWithRetry(context.Background(), msg)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("successful first attempt should be instant, took %v", elapsed)
	}
}

// TestProcessOne_DLQPublishFailureDoesNotMark — when handler retries are
// exhausted AND the DLQ publish itself fails, the offset MUST NOT be
// committed. Otherwise a transient DLQ outage silently loses the message
// (issue #69 / F-011). The DLQ-failure metric is also asserted.
func TestProcessOne_DLQPublishFailureDoesNotMark(t *testing.T) {
	const topic = "test.dlq-publish-fails"

	handler := func(_ context.Context, _ *Message) error {
		return errors.New("permanent handler failure")
	}

	// RecordingBroker with SendErr forces every Send (including the DLQ
	// publish via Producer.SendRaw) to fail.
	rec := &RecordingBroker{SendErr: errors.New("dlq topic offline")}
	producer := NewProducer(rec)

	c := &ConsumerGroup{
		handler:    handler,
		maxRetries: 2,
		logger:     zap.NewNop(),
		producer:   producer,
	}

	claim := newFakeClaim()
	msg := &Message{Topic: topic, Partition: 0, Offset: 42, Value: []byte("payload")}

	before := testutil.ToFloat64(metrics.KafkaDLQPublishFailures.WithLabelValues(topic))

	c.processOne(claim, msg)

	if claim.Marked() != 0 {
		t.Fatalf("MarkMessage was called %d times; expected 0 so Kafka redelivers", claim.Marked())
	}

	after := testutil.ToFloat64(metrics.KafkaDLQPublishFailures.WithLabelValues(topic))
	if after-before != 1 {
		t.Errorf("KafkaDLQPublishFailures delta = %v, want 1", after-before)
	}
}

// TestProcessOne_DLQPublishSuccessMarks — when handler retries are
// exhausted but DLQ publish succeeds, MarkMessage must run so we don't
// reprocess the poison message forever.
func TestProcessOne_DLQPublishSuccessMarks(t *testing.T) {
	const topic = "test.dlq-publish-ok"

	handler := func(_ context.Context, _ *Message) error {
		return errors.New("permanent handler failure")
	}

	rec := &RecordingBroker{} // no error → DLQ publish succeeds
	producer := NewProducer(rec)

	c := &ConsumerGroup{
		handler:    handler,
		maxRetries: 2,
		logger:     zap.NewNop(),
		producer:   producer,
	}

	claim := newFakeClaim()
	msg := &Message{Topic: topic, Partition: 0, Offset: 7, Value: []byte("payload")}

	before := testutil.ToFloat64(metrics.KafkaDLQPublishFailures.WithLabelValues(topic))

	c.processOne(claim, msg)

	if claim.Marked() != 1 {
		t.Fatalf("MarkMessage was called %d times; expected 1 (DLQ succeeded)", claim.Marked())
	}

	rec.Lock()
	sends := len(rec.Sends)
	var sentTopic string
	if sends > 0 {
		sentTopic = rec.Sends[0].Topic
	}
	rec.Unlock()
	if sends != 1 {
		t.Errorf("expected 1 DLQ Send, got %d", sends)
	}
	if sentTopic != DLQTopic(topic) {
		t.Errorf("DLQ send topic = %q, want %q", sentTopic, DLQTopic(topic))
	}

	// DLQ-failure counter must NOT increment when the publish succeeded.
	after := testutil.ToFloat64(metrics.KafkaDLQPublishFailures.WithLabelValues(topic))
	if after != before {
		t.Errorf("KafkaDLQPublishFailures changed by %v on success; expected 0", after-before)
	}
}

// TestProcessOne_HappyPathMarks — handler succeeds on first attempt: no
// DLQ activity, MarkMessage is called once.
func TestProcessOne_HappyPathMarks(t *testing.T) {
	const topic = "test.happy-path"

	handler := func(_ context.Context, _ *Message) error {
		return nil
	}

	rec := &RecordingBroker{}
	producer := NewProducer(rec)

	c := &ConsumerGroup{
		handler:    handler,
		maxRetries: 5,
		logger:     zap.NewNop(),
		producer:   producer,
	}

	claim := newFakeClaim()
	msg := &Message{Topic: topic, Partition: 0, Offset: 99, Value: []byte("payload")}

	c.processOne(claim, msg)

	if claim.Marked() != 1 {
		t.Fatalf("MarkMessage was called %d times; expected 1", claim.Marked())
	}

	rec.Lock()
	sends := len(rec.Sends)
	rec.Unlock()
	if sends != 0 {
		t.Errorf("expected 0 Sends on happy path, got %d", sends)
	}
}
