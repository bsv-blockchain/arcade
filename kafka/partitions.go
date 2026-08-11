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
			logger.Warn(
				"topic not found on broker — will be auto-created on first publish; ensure partition count matches deployment size",
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
		logger.Info(
			"topic partition count ok",
			zap.String("topic", topic),
			zap.Int("partitions", count),
		)
	}
	return nil
}

// CheckMinPartitions verifies that `topic` exists on the broker with at
// least `want` partitions. Used for topics whose partition count is a
// correctness-adjacent deployment constraint: producers key
// TopicPropagation by dependency family, so any partition count
// preserves parent-before-child order per partition, but a topic with
// fewer partitions than the deployment was sized for leaves consumers
// idle and means config and topology have drifted apart. `want < 1` is
// treated as 1 — the check is never a no-op.
//
// Missing topics are hard errors regardless of `want`: allowing
// auto-creation on first publish would let the broker default decide
// the partition count (and therefore the key→partition mapping) instead
// of the operator.
func CheckMinPartitions(broker Broker, topic string, want int, logger *zap.Logger) error {
	if want < 1 {
		want = 1
	}
	count, err := broker.PartitionCount(topic)
	if errors.Is(err, ErrTopicNotFound) {
		return fmt.Errorf(
			"topic %s not found on broker; create it before startup with at least %d partitions (correctness requirement)",
			topic,
			want,
		)
	}
	if err != nil {
		return fmt.Errorf("querying partition count for %s: %w", topic, err)
	}
	if count < want {
		return fmt.Errorf("topic %s has %d partition(s), want at least %d (propagation.partitions)", topic, count, want)
	}
	logger.Info(
		"topic partition count meets required minimum",
		zap.String("topic", topic),
		zap.Int("partitions", count),
		zap.Int("min_required", want),
	)
	return nil
}
