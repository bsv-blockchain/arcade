package kafka

const (
	// TopicBlock is retained only for the p2p_client scaffold (which is not
	// registered in main). It is not created by AllTopics() and has no consumers.
	TopicBlock          = "arcade.block"
	TopicBlockProcessed = "arcade.block_processed"
	TopicTransaction    = "arcade.transaction"
	TopicPropagation    = "arcade.propagation"

	// Dead-letter queue topics
	TopicBlockProcessedDLQ = "arcade.block_processed.dlq"
	TopicTransactionDLQ    = "arcade.transaction.dlq"
	TopicPropagationDLQ    = "arcade.propagation.dlq"
)

// AllTopics returns all primary topics created/managed by arcade.
func AllTopics() []string {
	return []string{
		TopicBlockProcessed,
		TopicTransaction,
		TopicPropagation,
	}
}

// DLQTopic returns the dead-letter topic name for a given primary topic.
func DLQTopic(topic string) string {
	return topic + ".dlq"
}
