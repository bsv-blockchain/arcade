package kafka

import "testing"

func TestSetTopicPrefix(t *testing.T) {
	t.Cleanup(func() { SetTopicPrefix(DefaultTopicPrefix) })

	SetTopicPrefix("arcade")
	if TopicPropagation != "arcade.propagation" {
		t.Errorf("TopicPropagation = %q, want %q", TopicPropagation, "arcade.propagation")
	}
	if TopicStatusUpdateDLQ != "arcade.tx_status.dlq" {
		t.Errorf("TopicStatusUpdateDLQ = %q, want %q", TopicStatusUpdateDLQ, "arcade.tx_status.dlq")
	}

	SetTopicPrefix("")
	if TopicBlockProcessed != "bsv-broadcaster.block_processed" {
		t.Errorf("empty prefix: TopicBlockProcessed = %q, want the default-prefixed name", TopicBlockProcessed)
	}
	if TopicBlockProcessedDLQ != "bsv-broadcaster.block_processed.dlq" {
		t.Errorf("empty prefix: TopicBlockProcessedDLQ = %q, want the default-prefixed name", TopicBlockProcessedDLQ)
	}
}
