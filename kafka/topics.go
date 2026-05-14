package kafka

const (
	// TopicBlock is retained only for the p2p_client scaffold (which is not
	// registered in main). It is not created by AllTopics() and has no consumers.
	TopicBlock          = "arcade.block"
	TopicBlockProcessed = "arcade.block_processed"
	TopicTransaction    = "arcade.transaction"
	TopicPropagation    = "arcade.propagation"
	// TopicStatusUpdate carries transaction status mutations from every
	// service that writes to the store (validator, propagation, bump-builder,
	// api-server). It exists so the api-server SSE handler and webhook
	// service can fan out updates to clients without depending on which pod
	// the mutation originated from.
	TopicStatusUpdate = "arcade.tx_status"

	// Dead-letter queue topics
	TopicBlockProcessedDLQ = "arcade.block_processed.dlq"
	TopicTransactionDLQ    = "arcade.transaction.dlq"
	TopicPropagationDLQ    = "arcade.propagation.dlq"
	TopicStatusUpdateDLQ   = "arcade.tx_status.dlq"
)

// AllTopics returns all primary topics created/managed by arcade.
func AllTopics() []string {
	return []string{
		TopicBlockProcessed,
		TopicTransaction,
		TopicPropagation,
		TopicStatusUpdate,
	}
}

// DLQTopic returns the dead-letter topic name for a given primary topic.
func DLQTopic(topic string) string {
	return topic + ".dlq"
}
