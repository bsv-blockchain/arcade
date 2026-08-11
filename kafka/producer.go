package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/arcade/metrics"
)

// Producer is the service-facing convenience wrapper over Broker. It takes Go
// values (any), JSON-marshals them, and hands the bytes to the configured
// broker. Services depend on *Producer rather than Broker directly because
// every caller today passes a struct — not doing the marshal here would force
// boilerplate in every call site.
type Producer struct {
	broker Broker
}

// NewProducer wraps a Broker. Construction is cheap — the broker owns the
// real connection pools.
func NewProducer(broker Broker) *Producer {
	return &Producer{broker: broker}
}

// Send JSON-marshals value and publishes synchronously. ctx is forwarded to
// the broker — callers should pass the request/handler context so an active
// span (server, consumer, ...) becomes the parent of the Kafka producer span
// and its trace context propagates onto the message headers (see
// InjectTraceContext). A context with no active span costs nothing extra:
// the inject/span-creation paths are no-ops in that case.
func (p *Producer) Send(ctx context.Context, topic, key string, value any) error {
	data, err := marshalValue(value)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}
	if err := p.broker.Send(ctx, topic, key, data); err != nil {
		if errors.Is(err, ErrBrokerBackpressure) {
			metrics.KafkaBackpressureTotal.WithLabelValues(topic).Inc()
		} else {
			metrics.KafkaProduceErrors.WithLabelValues(topic).Inc()
		}
		return err
	}
	metrics.KafkaMessagesTotal.WithLabelValues(topic, "produce").Inc()
	metrics.KafkaMessageBytes.WithLabelValues(topic, "produce").Observe(float64(len(data)))
	return nil
}

// SendAsync JSON-marshals value and publishes fire-and-forget. See Send for
// the ctx/trace-propagation contract.
func (p *Producer) SendAsync(ctx context.Context, topic, key string, value any) error {
	data, err := marshalValue(value)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}
	if err := p.broker.SendAsync(ctx, topic, key, data); err != nil {
		metrics.KafkaProduceErrors.WithLabelValues(topic).Inc()
		return err
	}
	metrics.KafkaMessagesTotal.WithLabelValues(topic, "produce").Inc()
	metrics.KafkaMessageBytes.WithLabelValues(topic, "produce").Observe(float64(len(data)))
	return nil
}

// SendBatch publishes multiple values to the same topic. Each KeyValue.Value
// is JSON-marshaled before the batch is forwarded to the broker. See Send for
// the ctx/trace-propagation contract; every message in the batch is tagged
// with the same parent trace context.
func (p *Producer) SendBatch(ctx context.Context, topic string, msgs []KeyValue) error {
	if err := p.broker.SendBatch(ctx, topic, msgs); err != nil {
		if errors.Is(err, ErrBrokerBackpressure) {
			metrics.KafkaBackpressureTotal.WithLabelValues(topic).Inc()
		} else {
			metrics.KafkaProduceErrors.WithLabelValues(topic).Inc()
		}
		return err
	}
	metrics.KafkaMessagesTotal.WithLabelValues(topic, "produce").Add(float64(len(msgs)))
	return nil
}

// SendRaw publishes pre-marshaled bytes. Used by consumer DLQ routing so we
// don't double-encode. See Send for the ctx/trace-propagation contract.
func (p *Producer) SendRaw(ctx context.Context, topic, key string, value []byte) error {
	if err := p.broker.Send(ctx, topic, key, value); err != nil {
		if errors.Is(err, ErrBrokerBackpressure) {
			metrics.KafkaBackpressureTotal.WithLabelValues(topic).Inc()
		} else {
			metrics.KafkaProduceErrors.WithLabelValues(topic).Inc()
		}
		return err
	}
	metrics.KafkaMessagesTotal.WithLabelValues(topic, "produce").Inc()
	metrics.KafkaMessageBytes.WithLabelValues(topic, "produce").Observe(float64(len(value)))
	return nil
}

// Broker returns the underlying broker, used by ConsumerGroup to Subscribe.
func (p *Producer) Broker() Broker {
	return p.broker
}

// Close tears down the underlying broker.
func (p *Producer) Close() error {
	return p.broker.Close()
}

// marshalValue JSON-encodes a Go value for transport. Extracted so batch and
// single paths share the same behavior.
func marshalValue(value any) ([]byte, error) {
	if raw, ok := value.([]byte); ok {
		return raw, nil
	}
	return json.Marshal(value)
}
