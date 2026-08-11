package merkleservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// restoreOTELGlobals snapshots the global tracer provider and text-map
// propagator, restoring them when the test finishes. Replicated from
// telemetry/telemetry_test.go's helper of the same name (unexported there,
// so not importable from this package).
func restoreOTELGlobals(t *testing.T) {
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

// TestClientRegister_TraceparentPropagation pins outbound tracing on the
// merkle-service client: Register must carry a W3C traceparent header when
// called with a context holding an active, sampled span AND a real
// propagator installed (telemetry.Init's contract — it always sets both
// together), and must NOT invent one when telemetry was never enabled (the
// OTEL globals are left at their SDK defaults: no-op tracer provider, no-op
// propagator).
//
// Order matters: the "disabled" case must run before anything in this
// process ever calls otel.SetTextMapPropagator/SetTracerProvider. OTEL's
// global propagator/tracer-provider are one-time-upgradeable delegates
// (internal/global) — once a real value is installed, restoring the
// snapshot afterward makes GetTextMapPropagator() return the delegate again,
// but that delegate permanently forwards to the first real value it was
// ever given. Running "disabled" first (before any Set call in this test
// binary) is the only way to observe true no-op behavior.
func TestClientRegister_TraceparentPropagation(t *testing.T) {
	t.Run("disabled: no span, no propagator installed -> no traceparent", func(t *testing.T) {
		// Precondition: this case only means anything while the global
		// propagator is still the default no-op delegate. If some earlier
		// test in this binary already called otel.SetTextMapPropagator, the
		// delegate permanently forwards to that real propagator and we can no
		// longer observe true no-op behaviour — skip rather than fail with a
		// confusing red. The default/no-op propagator advertises no Fields;
		// a real W3C propagator advertises "traceparent".
		if len(otel.GetTextMapPropagator().Fields()) != 0 {
			t.Skip("a real text-map propagator is already installed globally; cannot observe no-op behaviour in this run")
		}

		var gotHeader string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHeader = r.Header.Get("traceparent")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := NewClient(server.URL, "", 0)
		if err := client.Register(context.Background(), "abc123", "http://callback/url", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotHeader != "" {
			t.Errorf("traceparent header = %q, want empty when telemetry was never enabled", gotHeader)
		}
	})

	t.Run("enabled: active span + real propagator -> traceparent injected", func(t *testing.T) {
		restoreOTELGlobals(t)

		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
		t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))

		var gotHeader string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHeader = r.Header.Get("traceparent")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		// Constructed after the globals are set, matching production
		// ordering (telemetry.Init runs before app.Bootstrap wires up the
		// merkle-service client) — otelhttp.NewTransport captures the
		// propagator at construction time.
		client := NewClient(server.URL, "", 0)

		ctx, span := tp.Tracer("merkleservice-test").Start(context.Background(), "test-op")
		defer span.End()

		if err := client.Register(ctx, "abc123", "http://callback/url", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotHeader == "" {
			t.Fatal("expected a traceparent header on the outbound request, got none")
		}

		wantTraceID := span.SpanContext().TraceID().String()
		if !containsTraceID(gotHeader, wantTraceID) {
			t.Errorf("traceparent header = %q, want it to contain trace id %q", gotHeader, wantTraceID)
		}
	})
}

// containsTraceID reports whether a W3C traceparent header value
// ("version-traceid-spanid-flags") carries the given trace ID.
func containsTraceID(traceparent, traceID string) bool {
	// traceparent format: "00-<32 hex trace id>-<16 hex span id>-<2 hex flags>"
	const versionPrefixLen = 3 // "00-"
	if len(traceparent) < versionPrefixLen+len(traceID) {
		return false
	}
	return traceparent[versionPrefixLen:versionPrefixLen+len(traceID)] == traceID
}
