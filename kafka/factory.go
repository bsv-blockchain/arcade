package kafka

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/arcade/config"
)

// NewBroker dispatches on cfg.Backend:
//   - "sarama" (default): real Kafka via IBM Sarama. Requires cfg.Brokers.
//   - "memory": in-process broker. Zero external dependencies.
//
// topicPartitions widens selected topics on the memory backend (absent = 1
// partition), so standalone mode reproduces the multi-partition
// key→partition→dispatcher topology of a real deployment (#295 — the
// caller passes propagation.partitions for TopicPropagation). The sarama
// backend ignores it: real topics are provisioned broker-side and
// validated at startup via CheckMinPartitions.
//
// The returned Broker is shared across all services in the process — main.go
// constructs it once and hands it to Producer + every ConsumerGroup.
func NewBroker(cfg config.Kafka, topicPartitions map[string]int) (Broker, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = "sarama"
	}
	switch backend {
	case "sarama":
		if len(cfg.Brokers) == 0 {
			return nil, fmt.Errorf("kafka.brokers is required when kafka.backend=sarama")
		}
		return NewSaramaBroker(cfg.Brokers, cfg.ConsumerGroup)
	case "memory":
		return NewMemoryBrokerWithPartitions(cfg.BufferSize, time.Duration(cfg.SendTimeoutMs)*time.Millisecond, topicPartitions), nil
	default:
		return nil, fmt.Errorf("unknown kafka backend %q", backend)
	}
}
