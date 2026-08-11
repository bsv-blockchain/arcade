package kafka

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestMemoryBroker_TraceContextEndToEnd is the end-to-end round trip through
// standalone mode's in-process broker: a producer publishes with a
// span-carrying ctx, and the ConsumerGroup handler on the other side must
// observe a context whose trace ID matches the producer's span — proving
// InjectTraceContext (memoryBroker.publish) and ExtractTraceContext
// (ConsumerGroup.processOne) round-trip correctly outside of Sarama too.
//
// Only the global TextMapPropagator is configured (via SetTextMapPropagator,
// snapshot/restored like telemetry/telemetry_test.go's restoreOTELGlobals):
// the producer span comes from a local, real SDK TracerProvider wired to a
// tracetest.InMemoryExporter, so this test proves the propagation contract
// without needing to install anything as the global TracerProvider.
func TestMemoryBroker_TraceContextEndToEnd(t *testing.T) {
	restoreOTELGlobals(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	broker := NewMemoryBroker(16)
	defer func() { _ = broker.Close() }()
	producer := NewProducer(broker)

	received := make(chan context.Context, 1)
	cg, err := NewConsumerGroup(ConsumerConfig{
		Broker:  broker,
		GroupID: "trace-e2e",
		Topics:  []string{"trace-topic"},
		Handler: func(ctx context.Context, _ *Message) error {
			received <- ctx
			return nil
		},
		Producer: producer,
		Logger:   nopLogger(),
	})
	if err != nil {
		t.Fatalf("consumer group: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = cg.Run(runCtx) }()
	<-cg.Ready()

	produceCtx, span := tp.Tracer("producer-test").Start(context.Background(), "produce-op")
	wantTraceID := span.SpanContext().TraceID()
	span.End()

	if err := producer.Send(produceCtx, "trace-topic", "k", map[string]string{"hello": "world"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case gotCtx := <-received:
		gotSC := trace.SpanContextFromContext(gotCtx)
		if !gotSC.IsValid() {
			t.Fatal("handler ctx carries no valid span context")
		}
		if gotSC.TraceID() != wantTraceID {
			t.Errorf("handler trace ID = %s, want %s", gotSC.TraceID(), wantTraceID)
		}
		if !gotSC.IsRemote() {
			t.Error("extracted span context should be marked remote — it crossed the broker boundary")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	// The producer span was a real, sampled span (not a no-op stand-in) —
	// confirm it actually got exported.
	if spans := exporter.GetSpans(); len(spans) == 0 {
		t.Error("expected the producer span to have been exported")
	}
}

// TestMemoryBroker_TraceContextDisabled is the counterpart to the end-to-end
// test above: with no span on the producer ctx (the default, telemetry-off
// shape), the published message must carry no trace headers and the
// consumer handler's ctx must carry no valid span. Guards against a
// regression where InjectTraceContext/ExtractTraceContext silently invent a
// trace when there was never one to propagate.
func TestMemoryBroker_TraceContextDisabled(t *testing.T) {
	broker := NewMemoryBroker(16)
	defer func() { _ = broker.Close() }()
	producer := NewProducer(broker)

	received := make(chan context.Context, 1)
	cg, err := NewConsumerGroup(ConsumerConfig{
		Broker:  broker,
		GroupID: "trace-disabled",
		Topics:  []string{"trace-topic-disabled"},
		Handler: func(ctx context.Context, _ *Message) error {
			received <- ctx
			return nil
		},
		Producer: producer,
		Logger:   nopLogger(),
	})
	if err != nil {
		t.Fatalf("consumer group: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = cg.Run(runCtx) }()
	<-cg.Ready()

	if err := producer.Send(context.Background(), "trace-topic-disabled", "k", map[string]string{"hello": "world"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case gotCtx := <-received:
		if got := trace.SpanContextFromContext(gotCtx); got.IsValid() {
			t.Errorf("handler ctx carries a span context (%s) though the producer never had one", got.TraceID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}
