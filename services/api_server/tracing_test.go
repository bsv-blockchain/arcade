package api_server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
)

// restoreOTELTracerProvider snapshots the global tracer provider and restores
// it when the test finishes, mirroring telemetry/telemetry_test.go's
// restoreOTELGlobals helper (unexported there, so replicated here scoped to
// just the tracer provider this test touches).
func restoreOTELTracerProvider(t *testing.T) {
	t.Helper()
	tp := otel.GetTracerProvider()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != tp {
			otel.SetTracerProvider(tp)
		}
	})
}

// newTestRouter builds a Server with an observed logger and wires it through
// newRouter — the same middleware assembly Start() uses (otelgin,
// CustomRecovery, requestLogger) — so tests exercise the real middleware
// stack without binding a socket.
func newTestRouter(logger *zap.Logger) *gin.Engine {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg: &config.Config{
			Telemetry: config.TelemetryConfig{ServiceName: "arcade-tracing-test"},
		},
		logger: logger,
	}
	return srv.newRouter()
}

// TestNewRouter_CreatesServerSpanWithRoutePattern asserts otelgin.Middleware
// is wired into the engine Start() builds: a request through a traced route
// produces exactly one server span named after the matched gin route
// pattern, and the request log line (emitted by requestLogger, which runs
// inside otelgin) carries the matching trace_id.
func TestNewRouter_CreatesServerSpanWithRoutePattern(t *testing.T) {
	restoreOTELTracerProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	core, logs := observer.New(zap.DebugLevel)
	router := newTestRouter(zap.New(core))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", w.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans for GET /, want 1: %+v", len(spans), spans)
	}
	span := spans[0]
	if want := "GET /"; span.Name != want {
		t.Errorf("span name = %q, want %q (route pattern as name)", span.Name, want)
	}
	if !span.SpanContext.IsValid() {
		t.Fatal("recorded span has an invalid span context")
	}

	// The request log line must carry the same trace_id so logs and traces
	// correlate in the downstream backend.
	var requestEntry *observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Message == "request" {
			entry := e
			requestEntry = &entry
			break
		}
	}
	if requestEntry == nil {
		t.Fatal("no \"request\" log line emitted")
	}
	ctxMap := requestEntry.ContextMap()
	wantTraceID := span.SpanContext.TraceID().String()
	if got := ctxMap["trace_id"]; got != wantTraceID {
		t.Errorf("request log trace_id = %v, want %q", got, wantTraceID)
	}
	if got := ctxMap["span_id"]; got != span.SpanContext.SpanID().String() {
		t.Errorf("request log span_id = %v, want %q", got, span.SpanContext.SpanID().String())
	}
}

// TestNewRouter_PanicLogCarriesTraceID pins the middleware ordering contract
// documented on newRouter: otelgin sits OUTSIDE CustomRecovery, so when a
// handler panics the span is still live in the request context when
// recoverPanic runs. Asserts (a) the "panic in handler" log line carries the
// trace_id/span_id of the exported span, (b) the client still gets a 500,
// and (c) a server span for the panicking route is exported and marked as an
// error (otelgin ends it after recovery wrote the 500).
func TestNewRouter_PanicLogCarriesTraceID(t *testing.T) {
	restoreOTELTracerProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	core, logs := observer.New(zap.DebugLevel)
	router := newTestRouter(zap.New(core))
	router.GET("/panic-test", func(_ *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic-test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("GET /panic-test status = %d, want 500", w.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans for GET /panic-test, want 1: %+v", len(spans), spans)
	}
	span := spans[0]
	if want := "GET /panic-test"; span.Name != want {
		t.Errorf("span name = %q, want %q", span.Name, want)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error (recovered panic returned 500)", span.Status.Code)
	}

	var panicEntry *observer.LoggedEntry
	for _, e := range logs.All() {
		if e.Message == "panic in handler" {
			entry := e
			panicEntry = &entry
			break
		}
	}
	if panicEntry == nil {
		t.Fatal("no \"panic in handler\" log line emitted")
	}
	ctxMap := panicEntry.ContextMap()
	if got := ctxMap["trace_id"]; got != span.SpanContext.TraceID().String() {
		t.Errorf("panic log trace_id = %v, want %q (recovery must run inside the otelgin span)", got, span.SpanContext.TraceID().String())
	}
	if got := ctxMap["span_id"]; got != span.SpanContext.SpanID().String() {
		t.Errorf("panic log span_id = %v, want %q", got, span.SpanContext.SpanID().String())
	}
}

// TestNewRouter_HealthReadyMetricsAreNotTraced pins the otelgin filter: the
// three polling endpoints must never produce spans, however often infra
// probes them.
func TestNewRouter_HealthReadyMetricsAreNotTraced(t *testing.T) {
	restoreOTELTracerProvider(t)

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	otel.SetTracerProvider(tp)

	router := newTestRouter(zap.NewNop())

	for _, path := range []string{"/health", "/ready", "/metrics"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
	}

	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("expected no spans for /health, /ready, /metrics, got %d: %+v", len(spans), spans)
	}
}
