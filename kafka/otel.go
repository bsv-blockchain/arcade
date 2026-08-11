package kafka

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies this package's spans/tracer to the OTEL SDK. Kept as
// the package import path per OTEL convention (mirrors how otelhttp/otelgin
// name their own instrumentation scopes).
const tracerName = "github.com/bsv-blockchain/arcade/kafka"

// headerCarrier adapts a Kafka header map (as used by both the neutral
// Message type and Sarama's []RecordHeader, once translated) to
// propagation.TextMapCarrier so the standard W3C tracecontext/baggage
// propagators can inject into and extract from it directly — no
// intermediate http.Header or map[string]string copy.
type headerCarrier map[string][]byte

// Get implements propagation.TextMapCarrier.
func (c headerCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	return string(v)
}

// Set implements propagation.TextMapCarrier.
func (c headerCarrier) Set(key, value string) {
	c[key] = []byte(value)
}

// Keys implements propagation.TextMapCarrier.
func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTraceContext returns the Kafka message headers carrying ctx's trace
// context, or nil when ctx has no valid span.
//
// The nil-vs-empty-map distinction is the hot-path guard: when telemetry is
// disabled (or ctx simply carries no active span — e.g. an internal
// fire-and-forget publish), this returns nil without allocating anything,
// so every produce call on that path costs one interface-boxed trace.SpanContext
// comparison and nothing else. See BenchmarkInjectTraceContext_Disabled and
// the AllocsPerRun assertion in otel_test.go.
func InjectTraceContext(ctx context.Context) map[string][]byte {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}
	headers := make(map[string][]byte, 2)
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(headers))
	return headers
}

// ExtractTraceContext returns ctx annotated with the trace context carried
// by headers. When headers is nil or empty (the producer never injected —
// telemetry disabled end-to-end, or a message produced before this feature
// shipped) ctx is returned unchanged with no propagator call.
func ExtractTraceContext(ctx context.Context, headers map[string][]byte) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier(headers))
}
