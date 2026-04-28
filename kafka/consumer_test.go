package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestProcessWithRetry_BackoffDelaysBetweenAttempts(t *testing.T) {
	attempts := 0
	handler := func(ctx context.Context, msg *Message) error {
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
	handler := func(ctx context.Context, msg *Message) error {
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
	handler := func(ctx context.Context, msg *Message) error {
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
