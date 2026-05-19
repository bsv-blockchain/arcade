package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
)

// stubPartitionBroker is the smallest Broker implementation that can
// answer PartitionCount with caller-controlled values. The other
// methods panic — these tests don't exercise the produce/consume path.
type stubPartitionBroker struct {
	counts map[string]int
	err    error
}

func (b *stubPartitionBroker) Subscribe(string, []string) (Subscription, error) {
	panic("stubPartitionBroker: Subscribe not implemented")
}

func (b *stubPartitionBroker) Send(context.Context, string, string, []byte) error {
	panic("stubPartitionBroker: Send not implemented")
}

func (b *stubPartitionBroker) SendAsync(context.Context, string, string, []byte) error {
	panic("stubPartitionBroker: SendAsync not implemented")
}

func (b *stubPartitionBroker) SendBatch(context.Context, string, []KeyValue) error {
	panic("stubPartitionBroker: SendBatch not implemented")
}

func (b *stubPartitionBroker) Close() error { return nil }

func (b *stubPartitionBroker) PartitionCount(topic string) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if n, ok := b.counts[topic]; ok {
		return n, nil
	}
	return 0, ErrTopicNotFound
}

// TestCheckExactPartitions_MatchOK pins the success contract: when the
// broker reports the exact requested partition count, no error.
func TestCheckExactPartitions_MatchOK(t *testing.T) {
	rb := &RecordingBroker{} // always reports 1 partition
	if err := CheckExactPartitions(rb, TopicPropagation, 1, zap.NewNop()); err != nil {
		t.Fatalf("expected nil error for matching partition count, got %v", err)
	}
}

// TestCheckExactPartitions_MismatchFailsStartup pins the correctness
// guard: a TopicPropagation with > 1 partition must fail startup hard,
// because the dep-aware dispatcher's single-goroutine state ownership
// relies on total order at the topic level.
func TestCheckExactPartitions_MismatchFailsStartup(t *testing.T) {
	br := &stubPartitionBroker{counts: map[string]int{TopicPropagation: 3}}
	err := CheckExactPartitions(br, TopicPropagation, 1, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for 3-partition propagation topic, got nil — dispatcher correctness rule must be enforced at startup")
	}
	if !strings.Contains(err.Error(), "3 partitions") {
		t.Errorf("error message %q should report observed partition count", err.Error())
	}
	if !strings.Contains(err.Error(), "exactly 1") {
		t.Errorf("error message %q should state the required value", err.Error())
	}
}

// TestCheckExactPartitions_TopicMissing_FailsStartup pins the hard-fail
// contract: a missing topic is a startup error for correctness-
// constrained topics. Auto-create on first publish would use the
// broker's default partition count, silently breaking the dispatcher's
// single-partition invariant (see CheckExactPartitions doc and the
// call site in app/app.go that propagates the error to abort startup).
func TestCheckExactPartitions_TopicMissing_FailsStartup(t *testing.T) {
	br := &stubPartitionBroker{} // no entries → ErrTopicNotFound
	err := CheckExactPartitions(br, TopicPropagation, 1, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for missing correctness-constrained topic, got nil")
	}
	if !strings.Contains(err.Error(), "not found on broker") {
		t.Errorf("error %q should reference the not-found state", err.Error())
	}
	if !strings.Contains(err.Error(), "correctness requirement") {
		t.Errorf("error %q should flag this as a correctness-requirement violation", err.Error())
	}
}

// TestCheckExactPartitions_BrokerError_PropagatesError pins the
// fail-loud contract: when the broker can't answer the question for a
// non-not-found reason, startup must fail rather than silently proceed.
func TestCheckExactPartitions_BrokerError_PropagatesError(t *testing.T) {
	br := &stubPartitionBroker{err: errors.New("broker unreachable")}
	err := CheckExactPartitions(br, TopicPropagation, 1, zap.NewNop())
	if err == nil {
		t.Fatal("expected error when broker fails, got nil")
	}
	if !strings.Contains(err.Error(), "broker unreachable") {
		t.Errorf("error %q should wrap the underlying broker error", err.Error())
	}
}
