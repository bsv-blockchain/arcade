package kafka

import (
	"fmt"

	"github.com/bsv-blockchain/arcade/config"
)

// NewBroker dispatches on cfg.Backend:
//   - "sarama" (default): real Kafka via IBM Sarama. Requires cfg.Brokers.
//   - "memory": in-process broker. Zero external dependencies.
//
// The returned Broker is shared across all services in the process — main.go
// constructs it once and hands it to Producer + every ConsumerGroup.
func NewBroker(cfg config.Kafka) (Broker, error) {
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
		return NewMemoryBroker(cfg.BufferSize), nil
	default:
		return nil, fmt.Errorf("unknown kafka backend %q", backend)
	}
}
