package telemetry

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// recordedSpanContext returns a context carrying a real, recorded span
// context from an SDK tracer (valid trace/span IDs, no exporter attached).
func recordedSpanContext(t *testing.T) context.Context {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("telemetry-test").Start(context.Background(), "op")
	t.Cleanup(func() { span.End() })
	return ctx
}

// TestFields_WithRecordedSpan asserts the exact field names the design
// requires: trace_id and span_id, matching the span's IDs.
func TestFields_WithRecordedSpan(t *testing.T) {
	ctx := recordedSpanContext(t)
	sc := spanContextFromCtx(ctx, t)

	fields := Fields(ctx)
	if len(fields) != 2 {
		t.Fatalf("Fields returned %d fields, want 2 (trace_id, span_id): %+v", len(fields), fields)
	}

	byKey := map[string]zap.Field{}
	for _, f := range fields {
		byKey[f.Key] = f
	}

	tf, ok := byKey["trace_id"]
	if !ok {
		t.Fatal("missing trace_id field")
	}
	if got, want := tf.String, sc.TraceID().String(); got != want {
		t.Errorf("trace_id = %q, want %q", got, want)
	}

	sf, ok := byKey["span_id"]
	if !ok {
		t.Fatal("missing span_id field")
	}
	if got, want := sf.String, sc.SpanID().String(); got != want {
		t.Errorf("span_id = %q, want %q", got, want)
	}
}

// TestFields_WithoutSpan asserts nil (not empty-but-non-nil) so callers can
// cheaply check len(Fields(ctx)) == 0 without an intermediate allocation.
func TestFields_WithoutSpan(t *testing.T) {
	if fields := Fields(context.Background()); fields != nil {
		t.Fatalf("Fields on a plain context = %+v, want nil", fields)
	}
}

// TestLoggerWith_WithRecordedSpan asserts the returned logger actually emits
// trace_id/span_id on every subsequent log line.
func TestLoggerWith_WithRecordedSpan(t *testing.T) {
	ctx := recordedSpanContext(t)
	sc := spanContextFromCtx(ctx, t)

	core, logs := observer.New(zap.InfoLevel)
	base := zap.New(core)

	enriched := LoggerWith(ctx, base)
	if enriched == base {
		t.Fatal("LoggerWith returned the base logger unchanged despite a valid span context")
	}
	enriched.Info("hello")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("got %d log entries, want 1", len(entries))
	}
	ctxMap := entries[0].ContextMap()
	if got := ctxMap["trace_id"]; got != sc.TraceID().String() {
		t.Errorf("logged trace_id = %v, want %q", got, sc.TraceID().String())
	}
	if got := ctxMap["span_id"]; got != sc.SpanID().String() {
		t.Errorf("logged span_id = %v, want %q", got, sc.SpanID().String())
	}
}

// TestLoggerWith_WithoutSpan asserts the base logger is returned unchanged
// (same pointer) when ctx carries no span, and that nil base stays nil.
func TestLoggerWith_WithoutSpan(t *testing.T) {
	base := zap.NewNop()
	if got := LoggerWith(context.Background(), base); got != base {
		t.Fatal("LoggerWith without a span context should return base unchanged")
	}

	if got := LoggerWith(context.Background(), nil); got != nil {
		t.Fatalf("LoggerWith(ctx, nil) = %v, want nil", got)
	}
}

func spanContextFromCtx(ctx context.Context, t *testing.T) trace.SpanContext {
	t.Helper()
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatal("test context does not carry a valid span context")
	}
	return sc
}
