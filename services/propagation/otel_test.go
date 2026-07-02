package propagation

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// restoreOTELTracerProvider snapshots the global tracer provider and restores
// it when the test finishes, so these tests stay order-independent. Mirrors
// telemetry/telemetry_test.go's restoreOTELGlobals (unexported there).
func restoreOTELTracerProvider(t *testing.T) {
	t.Helper()
	tp := otel.GetTracerProvider()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != tp {
			otel.SetTracerProvider(tp)
		}
	})
}

// newRecordingTracerProvider installs a real SDK tracer provider wired to an
// in-memory exporter as the global, with the SDK's own per-span link limit
// raised well above batchSpanMaxLinks so that any truncation observed in the
// exported spans is attributable to startBroadcastSpan's cap, not the SDK's.
func newRecordingTracerProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	restoreOTELTracerProvider(t)
	exporter := tracetest.NewInMemoryExporter()
	limits := sdktrace.NewSpanLimits()
	limits.LinkCountLimit = 100000
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSyncer(exporter),
		sdktrace.WithRawSpanLimits(limits),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	return exporter
}

// validSpanContext mints a distinct, valid remote SpanContext for a batch
// message — standing in for a producer span extracted from Kafka headers.
func validSpanContext(t *testing.T, traceHex string) trace.SpanContext {
	t.Helper()
	tid, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatalf("TraceIDFromHex(%q): %v", traceHex, err)
	}
	sid, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

func broadcastSpan(t *testing.T, spans tracetest.SpanStubs) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == "propagation.broadcast" {
			return s
		}
	}
	t.Fatalf("no propagation.broadcast span exported (got %d spans)", len(spans))
	return tracetest.SpanStub{}
}

func attrInt(t *testing.T, s tracetest.SpanStub, key string) (int64, bool) {
	t.Helper()
	for _, a := range s.Attributes {
		if string(a.Key) == key {
			return a.Value.AsInt64(), true
		}
	}
	return 0, false
}

// TestStartBroadcastSpan_LinksOnlyValidContexts exercises the batch-span link
// logic: a batch mixing messages that carry a valid stashed producer
// spanCtx with ones that carry none must (a) create the broadcast span,
// (b) link exactly the valid contexts, (c) filter the invalid/zero ones, and
// record the batch_size/linked_count attributes.
func TestStartBroadcastSpan_LinksOnlyValidContexts(t *testing.T) {
	exporter := newRecordingTracerProvider(t)

	valid1 := validSpanContext(t, "4bf92f3577b34da6a3ce929d0e0e4736")
	valid2 := validSpanContext(t, "0af7651916cd43dd8448eb211c80319c")

	batch := []propagationMsg{
		{TXID: "tx-with-span-1", spanCtx: valid1},
		{TXID: "tx-no-span"},                                        // zero-value spanCtx (invalid)
		{TXID: "tx-with-span-2", spanCtx: valid2},                   // valid
		{TXID: "tx-explicit-invalid", spanCtx: trace.SpanContext{}}, // invalid
	}

	p := &Propagator{}
	_, end := p.startBroadcastSpan(context.Background(), batch)
	end()

	span := broadcastSpan(t, exporter.GetSpans())

	if len(span.Links) != 2 {
		t.Fatalf("broadcast span has %d links, want 2 (only the valid contexts)", len(span.Links))
	}
	gotTraces := map[trace.TraceID]bool{}
	for _, l := range span.Links {
		if !l.SpanContext.IsValid() {
			t.Error("broadcast span carries an invalid link — invalid contexts should have been filtered")
		}
		gotTraces[l.SpanContext.TraceID()] = true
	}
	if !gotTraces[valid1.TraceID()] || !gotTraces[valid2.TraceID()] {
		t.Errorf("links %v missing one of the expected trace IDs %s / %s", gotTraces, valid1.TraceID(), valid2.TraceID())
	}
	if got, ok := attrInt(t, span, "broadcast.batch_size"); !ok || got != int64(len(batch)) {
		t.Errorf("broadcast.batch_size = %d (present=%v), want %d", got, ok, len(batch))
	}
	if got, ok := attrInt(t, span, "broadcast.linked_count"); !ok || got != 2 {
		t.Errorf("broadcast.linked_count = %d (present=%v), want 2", got, ok)
	}
}

// TestStartBroadcastSpan_CapsLinks pins the batchSpanMaxLinks cap: given more
// than 128 valid producer contexts, exactly 128 links are attached and the
// linked_count attribute reflects the truncation against batch_size. The SDK
// link limit is raised in newRecordingTracerProvider so 128 can only come
// from startBroadcastSpan's own cap, and DroppedLinks staying 0 confirms
// AddLink was called exactly 128 times (not 130 with the SDK dropping two).
func TestStartBroadcastSpan_CapsLinks(t *testing.T) {
	exporter := newRecordingTracerProvider(t)

	const total = batchSpanMaxLinks + 5
	batch := make([]propagationMsg, 0, total)
	for i := 0; i < total; i++ {
		// Distinct trace IDs so each is a genuinely separate link.
		tid := trace.TraceID{}
		tid[0] = byte(i + 1)
		tid[15] = byte(i + 1)
		sid := trace.SpanID{}
		sid[0] = byte(i + 1)
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     sid,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		})
		batch = append(batch, propagationMsg{spanCtx: sc})
	}

	p := &Propagator{}
	_, end := p.startBroadcastSpan(context.Background(), batch)
	end()

	span := broadcastSpan(t, exporter.GetSpans())
	if len(span.Links) != batchSpanMaxLinks {
		t.Fatalf("broadcast span has %d links, want the cap of %d", len(span.Links), batchSpanMaxLinks)
	}
	if span.DroppedLinks != 0 {
		t.Errorf("SDK DroppedLinks = %d, want 0 — startBroadcastSpan should cap AddLink calls itself, not rely on the SDK dropping", span.DroppedLinks)
	}
	if got, ok := attrInt(t, span, "broadcast.linked_count"); !ok || got != int64(batchSpanMaxLinks) {
		t.Errorf("broadcast.linked_count = %d (present=%v), want %d", got, ok, batchSpanMaxLinks)
	}
	if got, ok := attrInt(t, span, "broadcast.batch_size"); !ok || got != int64(total) {
		t.Errorf("broadcast.batch_size = %d (present=%v), want %d", got, ok, total)
	}
}

// TestStartBroadcastSpan_Disabled confirms the disabled/no-op path: with no
// real tracer provider installed (the default global), startBroadcastSpan
// returns a usable ctx + end func, nothing is exported, and end() is safe to
// call. Guards the "effectively free when telemetry is off" contract for the
// hot processBatch path.
func TestStartBroadcastSpan_Disabled(t *testing.T) {
	restoreOTELTracerProvider(t)
	// Do NOT install a real provider — exercise the default no-op global.

	batch := []propagationMsg{
		{TXID: "tx1", spanCtx: validSpanContext(t, "4bf92f3577b34da6a3ce929d0e0e4736")},
	}
	p := &Propagator{}
	ctx, end := p.startBroadcastSpan(context.Background(), batch)
	if ctx == nil {
		t.Fatal("startBroadcastSpan returned a nil context")
	}
	end() // must not panic

	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		t.Errorf("disabled path produced a valid span context %s; expected none", sc.TraceID())
	}
}
