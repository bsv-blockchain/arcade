package kafka

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// BenchmarkInjectTraceContext_Disabled measures the hot/disabled produce
// path: ctx carries no valid span, so InjectTraceContext must return nil
// without allocating. Run with -benchmem; allocs/op must be 0 — this is the
// same guarantee TestInjectTraceContext_NoValidSpan pins via AllocsPerRun.
func BenchmarkInjectTraceContext_Disabled(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = InjectTraceContext(ctx)
	}
}

// BenchmarkInjectTraceContext_Enabled measures the cost of a real inject —
// a valid recording span plus a real W3C propagator — as the reference point
// the disabled benchmark above is compared against.
func BenchmarkInjectTraceContext_Enabled(b *testing.B) {
	restoreOTELGlobals(b)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	b.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("kafka-otel-bench").Start(context.Background(), "produce")
	defer span.End()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = InjectTraceContext(ctx)
	}
}

// newBenchSaramaBroker builds a saramaBroker over a no-op fake sync producer
// and a Sarama async-producer mock (only present to satisfy
// newSaramaBrokerFromProducers' signature and drain goroutines — these
// benchmarks only exercise the synchronous Send path). No network, no real
// Kafka.
func newBenchSaramaBroker(b *testing.B) *saramaBroker {
	b.Helper()
	broker := newSaramaBrokerFromProducers(&fakeSyncProducer{}, newAsyncProducerMock(b), nil, "", zap.NewNop())
	b.Cleanup(func() { _ = broker.Close() })
	return broker
}

// BenchmarkSaramaBroker_Send_Disabled measures the full produce hot path
// (span creation via the no-op tracer + header injection guard) with no
// active span — the default, telemetry-disabled shape of every produce call
// in the codebase today.
func BenchmarkSaramaBroker_Send_Disabled(b *testing.B) {
	broker := newBenchSaramaBroker(b)
	ctx := context.Background()
	value := []byte("benchmark-value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.Send(ctx, "bench-topic", "k", value); err != nil {
			b.Fatalf("Send: %v", err)
		}
	}
}

// BenchmarkSaramaBroker_Send_RecordingSpan measures the same produce path
// with a real recording parent span and a real propagator installed —
// the cost of actually propagating trace context onto the wire, for
// comparison against the disabled benchmark above.
func BenchmarkSaramaBroker_Send_RecordingSpan(b *testing.B) {
	restoreOTELGlobals(b)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	b.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("kafka-otel-bench").Start(context.Background(), "produce")
	defer span.End()

	broker := newBenchSaramaBroker(b)
	value := []byte("benchmark-value")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.Send(ctx, "bench-topic", "k", value); err != nil {
			b.Fatalf("Send: %v", err)
		}
	}
}
