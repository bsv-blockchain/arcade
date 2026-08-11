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

func (b *stubPartitionBroker) Subscribe(string, []string, StartOffset) (Subscription, error) {
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

// TestCheckMinPartitions_ExactMatchOK pins the success contract: when
// the broker reports exactly the requested minimum, no error.
func TestCheckMinPartitions_ExactMatchOK(t *testing.T) {
	rb := &RecordingBroker{} // always reports 1 partition
	if err := CheckMinPartitions(rb, TopicPropagation, 1, zap.NewNop()); err != nil {
		t.Fatalf("expected nil error for matching partition count, got %v", err)
	}
}

// TestCheckMinPartitions_MorePartitionsOK pins the contract change from
// the retired exact-1 rule (#295): a propagation topic with MORE
// partitions than configured is fine — family partition keys preserve
// parent-before-child order per partition, and extra partitions only
// cost idle dispatchers.
func TestCheckMinPartitions_MorePartitionsOK(t *testing.T) {
	br := &stubPartitionBroker{counts: map[string]int{TopicPropagation: 8}}
	if err := CheckMinPartitions(br, TopicPropagation, 3, zap.NewNop()); err != nil {
		t.Fatalf("expected nil error when actual (8) >= configured (3), got %v", err)
	}
}

// TestCheckMinPartitions_FewerPartitionsFailsStartup pins the fail-closed
// guard: fewer partitions than configured means producers hash over a
// smaller space than the deployment was sized for and some consumers
// would sit idle. Startup must abort so config and topology can't drift.
func TestCheckMinPartitions_FewerPartitionsFailsStartup(t *testing.T) {
	br := &stubPartitionBroker{counts: map[string]int{TopicPropagation: 1}}
	err := CheckMinPartitions(br, TopicPropagation, 3, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for 1-partition topic with configured minimum 3, got nil")
	}
	if !strings.Contains(err.Error(), "1 partition") {
		t.Errorf("error message %q should report observed partition count", err.Error())
	}
	if !strings.Contains(err.Error(), "at least 3") {
		t.Errorf("error message %q should state the required minimum", err.Error())
	}
}

// TestCheckMinPartitions_TopicMissing_FailsStartup pins the hard-fail
// contract: a missing topic is a startup error even when the configured
// minimum is 1. Auto-create on first publish would use the broker's
// default partition count, silently deciding the key→partition mapping
// the dispatcher's ordering guarantees are built on.
func TestCheckMinPartitions_TopicMissing_FailsStartup(t *testing.T) {
	br := &stubPartitionBroker{} // no entries → ErrTopicNotFound
	err := CheckMinPartitions(br, TopicPropagation, 1, zap.NewNop())
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

// TestCheckMinPartitions_BrokerError_PropagatesError pins the fail-loud
// contract: when the broker can't answer the question for a
// non-not-found reason, startup must fail rather than silently proceed.
func TestCheckMinPartitions_BrokerError_PropagatesError(t *testing.T) {
	br := &stubPartitionBroker{err: errors.New("broker unreachable")}
	err := CheckMinPartitions(br, TopicPropagation, 1, zap.NewNop())
	if err == nil {
		t.Fatal("expected error when broker fails, got nil")
	}
	if !strings.Contains(err.Error(), "broker unreachable") {
		t.Errorf("error %q should wrap the underlying broker error", err.Error())
	}
}

// TestCheckMinPartitions_NonPositiveWantTreatedAsOne pins the floor:
// want <= 0 is not "skip the check" — the topic must still exist with
// at least one partition. This is what keeps the default config
// (propagation.partitions unset → 0 via a zero-value struct) fail-closed
// on a missing topic.
func TestCheckMinPartitions_NonPositiveWantTreatedAsOne(t *testing.T) {
	br := &stubPartitionBroker{} // topic missing
	if err := CheckMinPartitions(br, TopicPropagation, 0, zap.NewNop()); err == nil {
		t.Fatal("expected missing-topic error even with want=0, got nil")
	}
	ok := &stubPartitionBroker{counts: map[string]int{TopicPropagation: 1}}
	if err := CheckMinPartitions(ok, TopicPropagation, 0, zap.NewNop()); err != nil {
		t.Fatalf("expected nil error for existing topic with want=0, got %v", err)
	}
}
