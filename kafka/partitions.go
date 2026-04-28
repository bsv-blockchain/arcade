package kafka

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// CheckPartitions verifies that every topic in `topics` exists on the broker
// and has at least `minPartitions` partitions. Topics that don't exist yet
// (e.g. lazily-created on first publish) are reported as warnings, not
// errors, since Sarama/Kafka will auto-create them on publish.
//
// Returns an error only when an existing topic has fewer partitions than
// minPartitions, because that is an unrecoverable misconfiguration for a
// horizontally-scaled deployment — more pods than partitions means some
// pods will never receive messages. Used at startup from cmd/arcade so
// operators see the problem before traffic arrives.
func CheckPartitions(broker Broker, topics []string, minPartitions int, logger *zap.Logger) error {
	if minPartitions <= 1 {
		return nil
	}
	for _, topic := range topics {
		count, err := broker.PartitionCount(topic)
		if errors.Is(err, ErrTopicNotFound) {
			logger.Warn("topic not found on broker — will be auto-created on first publish; ensure partition count matches deployment size",
				zap.String("topic", topic),
				zap.Int("min_partitions", minPartitions),
			)
			continue
		}
		if err != nil {
			return fmt.Errorf("querying partition count for %s: %w", topic, err)
		}
		if count < minPartitions {
			return fmt.Errorf("topic %s has %d partitions, need at least %d for horizontal scaling", topic, count, minPartitions)
		}
		logger.Info("topic partition count ok",
			zap.String("topic", topic),
			zap.Int("partitions", count),
		)
	}
	return nil
}
