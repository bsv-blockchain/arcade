package kafka

import (
	"context"
	"sort"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// restoreOTELGlobals snapshots the global tracer provider and text-map
// propagator, restoring them when the test finishes. Replicated from
// telemetry/telemetry_test.go's helper of the same name (unexported there,
// so not importable from this package) — see merkleservice/tracing_test.go
// for the same pattern.
func restoreOTELGlobals(t testing.TB) {
	t.Helper()
	tp := otel.GetTracerProvider()
	prop := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != tp {
			otel.SetTracerProvider(tp)
		}
		otel.SetTextMapPropagator(prop)
	})
}

// TestHeaderCarrier_GetSetKeys pins the propagation.TextMapCarrier contract
// headerCarrier implements: Get/Set do an exact []byte<->string conversion
// (no encoding, no truncation) and Keys enumerates every key present.
func TestHeaderCarrier_GetSetKeys(t *testing.T) {
	c := headerCarrier{}

	c.Set("traceparent", "00-aaaa-bbbb-01")
	c.Set("tracestate", "vendor=value")

	if got := c.Get("traceparent"); got != "00-aaaa-bbbb-01" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "00-aaaa-bbbb-01")
	}
	if got := c.Get("tracestate"); got != "vendor=value" {
		t.Errorf("Get(tracestate) = %q, want %q", got, "vendor=value")
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty string", got)
	}

	// Underlying storage is exactly the []byte form of what was Set — no
	// hidden encoding.
	if string(c["traceparent"]) != "00-aaaa-bbbb-01" {
		t.Errorf("stored bytes = %q, want exact string bytes", c["traceparent"])
	}

	keys := c.Keys()
	sort.Strings(keys)
	want := []string{"traceparent", "tracestate"}
	if len(keys) != len(want) {
		t.Fatalf("Keys() = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("Keys()[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}

// TestInjectTraceContext_NoValidSpan asserts the hot-path guard: a ctx with
// no valid span context (telemetry disabled, or simply no active span at
// the produce call site — e.g. context.Background()) returns nil, not an
// empty map, and does so with ZERO allocations. This is the guarantee the
// whole feature's "zero-cost when disabled" requirement rests on for the
// produce path — every saramaBroker.Send/SendAsync/SendBatch call runs this
// check before doing anything else.
func TestInjectTraceContext_NoValidSpan(t *testing.T) {
	if hdrs := InjectTraceContext(context.Background()); hdrs != nil {
		t.Fatalf("InjectTraceContext(no span) = %v, want nil", hdrs)
	}

	// A context carrying only a non-recording/invalid span (e.g. one that
	// went through the no-op tracer without ever inheriting a real parent)
	// must also short-circuit to nil.
	ctx := trace.ContextWithSpanContext(context.Background(), trace.SpanContext{})
	if hdrs := InjectTraceContext(ctx); hdrs != nil {
		t.Fatalf("InjectTraceContext(invalid span context) = %v, want nil", hdrs)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_ = InjectTraceContext(context.Background())
	})
	if allocs != 0 {
		t.Errorf("InjectTraceContext(no span) allocs/op = %v, want 0", allocs)
	}
}

// TestExtractTraceContext_NilOrEmptyHeaders asserts the consume-side mirror
// of the same guard: nil or empty headers (the producer never injected —
// telemetry disabled end-to-end, or a message produced before this feature
// shipped) must return ctx completely unchanged — not merely
// value-equivalent, but the exact same context — with no propagator call.
func TestExtractTraceContext_NilOrEmptyHeaders(t *testing.T) {
	type ctxKey struct{}
	base := context.WithValue(context.Background(), ctxKey{}, "marker")

	if got := ExtractTraceContext(base, nil); got != base {
		t.Error("ExtractTraceContext(nil headers) did not return ctx unchanged")
	}
	if got := ExtractTraceContext(base, map[string][]byte{}); got != base {
		t.Error("ExtractTraceContext(empty headers) did not return ctx unchanged")
	}
}

// TestInjectExtractTraceContext_RoundTrip is the core correctness guarantee:
// Inject with a recording span's context, then Extract from the resulting
// headers, must yield a context whose trace ID and span ID exactly match the
// original span — including the raw []byte<->string carrier round trip (no
// mangling of the traceparent header value).
//
// Only the global TextMapPropagator is touched (via SetTextMapPropagator);
// the span itself comes from a local, non-global TracerProvider, so this
// test does not depend on — and does not need to configure — the global
// TracerProvider.
func TestInjectExtractTraceContext_RoundTrip(t *testing.T) {
	restoreOTELGlobals(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("kafka-otel-test").Start(context.Background(), "produce")
	defer span.End()
	want := span.SpanContext()
	if !want.IsValid() {
		t.Fatal("test setup: local tracer produced an invalid span context")
	}

	headers := InjectTraceContext(ctx)
	if headers == nil {
		t.Fatal("InjectTraceContext returned nil for a valid recording span context")
	}
	raw, ok := headers["traceparent"]
	if !ok {
		t.Fatalf("headers missing traceparent key: %v", keysOf(headers))
	}
	// The carrier's Get/Set is an exact []byte<->string conversion — the
	// traceparent header's on-wire bytes must be exactly the propagator's
	// string representation, byte for byte.
	if string(raw) != (headerCarrier(headers)).Get("traceparent") {
		t.Errorf("traceparent bytes %q do not exactly match carrier.Get %q", raw, (headerCarrier(headers)).Get("traceparent"))
	}

	got := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), headers))
	if !got.IsValid() {
		t.Fatal("ExtractTraceContext produced an invalid span context")
	}
	if got.TraceID() != want.TraceID() {
		t.Errorf("extracted trace ID = %s, want %s", got.TraceID(), want.TraceID())
	}
	if got.SpanID() != want.SpanID() {
		t.Errorf("extracted span ID = %s, want %s", got.SpanID(), want.SpanID())
	}
}

func keysOf(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
