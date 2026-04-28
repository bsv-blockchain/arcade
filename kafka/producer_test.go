package kafka

import (
	"errors"
	"testing"
)

func TestProducer_SendBatch_Success(t *testing.T) {
	rb := &RecordingBroker{}
	p := NewProducer(rb)

	msgs := []KeyValue{
		{Key: "tx1", Value: map[string]string{"txid": "tx1"}},
		{Key: "tx2", Value: map[string]string{"txid": "tx2"}},
		{Key: "tx3", Value: map[string]string{"txid": "tx3"}},
	}

	if err := p.SendBatch("test-topic", msgs); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if rb.BatchCalls != 1 {
		t.Errorf("expected 1 batch call, got %d", rb.BatchCalls)
	}
	if len(rb.Batches) != 1 || len(rb.Batches[0]) != 3 {
		t.Errorf("expected 1 batch with 3 messages, got %v", rb.Batches)
	}
}

func TestProducer_SendBatch_EmptyReturnsNil(t *testing.T) {
	rb := &RecordingBroker{}
	p := NewProducer(rb)

	if err := p.SendBatch("test-topic", nil); err != nil {
		t.Fatalf("expected no error for empty batch, got: %v", err)
	}
}

func TestProducer_SendBatch_ErrorPropagation(t *testing.T) {
	rb := &RecordingBroker{BatchErr: errors.New("broker unavailable")}
	p := NewProducer(rb)

	msgs := []KeyValue{{Key: "tx1", Value: map[string]string{"txid": "tx1"}}}

	if err := p.SendBatch("test-topic", msgs); err == nil {
		t.Fatal("expected error, got nil")
	}
}
