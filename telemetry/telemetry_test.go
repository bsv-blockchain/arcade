package telemetry

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/metrics"
)

// restoreOTELGlobals snapshots the global tracer provider, meter provider,
// and text-map propagator and restores them when the test finishes. Every
// test that calls Init with Enabled=true must call this first, so tests stay
// order-independent (TestInit_DisabledIsNoop compares before/after globals
// and would otherwise observe leftovers from a previously-run enabled test).
func restoreOTELGlobals(t *testing.T) {
	t.Helper()
	tp := otel.GetTracerProvider()
	mp := otel.GetMeterProvider()
	prop := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		// Guard each Set: restoring an identical value is a no-op at best
		// and, for the never-mutated default delegators, triggers the SDK's
		// self-delegation error handler. Providers are always pointers so !=
		// is safe; propagators need the type-checked helper because the
		// composite propagator's dynamic type is a non-comparable slice.
		if otel.GetTracerProvider() != tp {
			otel.SetTracerProvider(tp)
		}
		if otel.GetMeterProvider() != mp {
			otel.SetMeterProvider(mp)
		}
		if !propagatorsEqual(otel.GetTextMapPropagator(), prop) {
			otel.SetTextMapPropagator(prop)
		}
	})
}

// propagatorsEqual reports whether two propagator interface values are the
// same instance, guarding against non-comparable dynamic types (comparing two
// interfaces both holding slice-typed composites would panic).
func propagatorsEqual(a, b propagation.TextMapPropagator) bool {
	ta, tb := reflect.TypeOf(a), reflect.TypeOf(b)
	if ta != tb || ta == nil || !ta.Comparable() {
		return false
	}
	return a == b
}

// TestInit_DisabledIsNoop asserts the strict guarantee: with Enabled=false,
// Init sets nothing globally (the pre-call tracer/meter providers are left
// exactly as they were) and returns quickly with a shutdown func that never
// fails. This is what lets telemetry ship dormant in every build until an
// operator opts in.
func TestInit_DisabledIsNoop(t *testing.T) {
	beforeTP := otel.GetTracerProvider()
	beforeMP := otel.GetMeterProvider()

	start := time.Now()
	shutdown, err := Init(context.Background(), config.TelemetryConfig{Enabled: false}, Options{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Init with Enabled=false returned error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("Init with Enabled=false returned a nil shutdown func")
	}
	if elapsed > time.Second {
		t.Fatalf("Init with Enabled=false took %v, expected a near-instant no-op", elapsed)
	}

	if otel.GetTracerProvider() != beforeTP {
		t.Fatal("Init with Enabled=false changed the global tracer provider")
	}
	if otel.GetMeterProvider() != beforeMP {
		t.Fatal("Init with Enabled=false changed the global meter provider")
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("no-op shutdown returned error: %v", err)
	}
}

// capturingCollector records, per OTLP HTTP path, how many non-empty POST
// bodies arrived and their concatenated raw bytes, so the test can assert not
// just that something was exported but that a specific instrument's data made
// it into the payload.
type capturingCollector struct {
	mu     sync.Mutex
	hits   map[string]int
	bodies map[string][]byte
}

func newCapturingCollector() *capturingCollector {
	return &capturingCollector{
		hits:   make(map[string]int),
		bodies: make(map[string][]byte),
	}
}

func (c *capturingCollector) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		if len(body) > 0 {
			c.hits[r.URL.Path]++
			c.bodies[r.URL.Path] = append(c.bodies[r.URL.Path], body...)
		}
		c.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}
}

func (c *capturingCollector) hitsFor(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits[path]
}

func (c *capturingCollector) bodyFor(path string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bodies[path]
}

// TestInit_HTTPExportsTracesAndBridgedMetrics enables the HTTP/protobuf
// pipeline against a local httptest.Server, produces one span and increments
// one promauto counter, then asserts Shutdown flushed real data to BOTH
// /v1/traces and /v1/metrics.
//
// A non-empty /v1/metrics POST alone does NOT prove the Prometheus bridge:
// a producer-less PeriodicReader still exports a resource-only payload. So
// the test additionally asserts the bridged counter's name appears in the
// /v1/metrics body (metric names are embedded as plain strings in OTLP
// protobuf) — that byte sequence can only be there if the bridge producer
// (prometheusbridge.NewMetricProducer) read the Prometheus default registry
// and fed it to the OTLP exporter, since nothing in this test talks to the
// OTEL metric API directly.
func TestInit_HTTPExportsTracesAndBridgedMetrics(t *testing.T) {
	restoreOTELGlobals(t)

	collector := newCapturingCollector()
	server := httptest.NewServer(collector.handler())
	defer server.Close()

	endpoint := strings.TrimPrefix(server.URL, "http://")

	cfg := config.TelemetryConfig{
		Enabled:         true,
		Endpoint:        endpoint,
		Protocol:        "http",
		Insecure:        true,
		ServiceName:     "arcade-telemetry-test",
		Traces:          true,
		Metrics:         true,
		SampleRatio:     1.0,
		ExportTimeoutMs: 5000,
	}

	shutdown, err := Init(context.Background(), cfg, Options{Version: "test", Mode: "test"})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	_, span := otel.Tracer("telemetry-test").Start(context.Background(), "test-span")
	span.End()

	counter := promauto.With(prometheus.DefaultRegisterer).NewCounter(prometheus.CounterOpts{
		Name: "arcade_telemetry_test_total",
		Help: "Test-only counter incremented by TestInit_HTTPExportsTracesAndBridgedMetrics.",
	})
	// The default registry is process-global; without unregistering, a second
	// run of this test in the same process (go test -count=2) panics on
	// duplicate registration.
	t.Cleanup(func() { prometheus.DefaultRegisterer.Unregister(counter) })
	counter.Inc()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}

	if got := collector.hitsFor("/v1/traces"); got == 0 {
		t.Error("expected at least one non-empty POST to /v1/traces, got none")
	}
	if got := collector.hitsFor("/v1/metrics"); got == 0 {
		t.Error("expected at least one non-empty POST to /v1/metrics, got none")
	}
	if body := collector.bodyFor("/v1/metrics"); !bytes.Contains(body, []byte("arcade_telemetry_test_total")) {
		t.Errorf("/v1/metrics payload does not contain the bridged counter name %q — "+
			"the Prometheus bridge producer is not wired to the OTLP metric exporter (payload %d bytes)",
			"arcade_telemetry_test_total", len(body))
	}
}

// TestInit_InsecureAppliesToEnvSuppliedEndpoint guards against the
// WithInsecure hoist regression: cfg.Insecure must apply even when the
// endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT rather than cfg.Endpoint.
// Before the fix, WithInsecure was only appended inside the `cfg.Endpoint !=
// ""` branch, so an env-supplied endpoint silently dropped
// telemetry.insecure=true and the HTTP exporter defaulted to TLS — which
// fails outright against this plaintext httptest server, so a zero hit count
// here is exactly the symptom the fix prevents.
//
// The env value is deliberately a scheme-less "//host:port" network-path
// reference rather than server.URL's "http://host:port": the OTLP exporter's
// own env parsing (go.opentelemetry.io/otel/exporters/otlp/.../envconfig)
// infers Insecure from a "http"/"https" scheme on the endpoint URL, which
// would make an "http://..." env value auto-insecure independent of this
// package's cfg.Insecure plumbing and mask the bug under test. A scheme-less
// value still resolves to the right host:port but leaves the library
// defaulting to secure, so only this package's own (post-fix) WithInsecure
// call can make the export succeed.
func TestInit_InsecureAppliesToEnvSuppliedEndpoint(t *testing.T) {
	restoreOTELGlobals(t)

	collector := newCapturingCollector()
	server := httptest.NewServer(collector.handler())
	defer server.Close()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "//"+strings.TrimPrefix(server.URL, "http://"))

	cfg := config.TelemetryConfig{
		Enabled:         true,
		Protocol:        "http",
		Insecure:        true,
		ServiceName:     "arcade-telemetry-test",
		Traces:          true,
		SampleRatio:     1.0,
		ExportTimeoutMs: 5000,
	}

	shutdown, err := Init(context.Background(), cfg, Options{Version: "test", Mode: "test"})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	_, span := otel.Tracer("telemetry-test").Start(context.Background(), "test-span")
	span.End()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}

	if got := collector.hitsFor("/v1/traces"); got == 0 {
		t.Error("expected at least one non-empty POST to /v1/traces via the env-supplied endpoint, got none " +
			"(telemetry.insecure=true was not honored, so the exporter likely attempted TLS against a plaintext server)")
	}
}

// TestInit_EndpointFallsBackToEnv covers the two documented endpoint
// resolution rules: an empty telemetry.endpoint defers to the standard
// OTEL_EXPORTER_OTLP_ENDPOINT env var, and Init fails fast when neither is
// set (rather than silently doing nothing or hanging on export).
func TestInit_EndpointFallsBackToEnv(t *testing.T) {
	t.Run("env endpoint set", func(t *testing.T) {
		restoreOTELGlobals(t)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")

		cfg := config.TelemetryConfig{
			Enabled:         true,
			Protocol:        "grpc",
			Traces:          true,
			Metrics:         true,
			SampleRatio:     1.0,
			ExportTimeoutMs: 200,
		}
		shutdown, err := Init(context.Background(), cfg, Options{})
		if err != nil {
			t.Fatalf("Init should fall back to OTEL_EXPORTER_OTLP_ENDPOINT, got error: %v", err)
		}
		// Bounded internally by cfg.ExportTimeoutMs regardless of ctx; there
		// is no real collector listening, so this just exercises the timeout
		// path without hanging the test.
		_ = shutdown(context.Background())
	})

	t.Run("no endpoint anywhere", func(t *testing.T) {
		cfg := config.TelemetryConfig{
			Enabled:     true,
			Protocol:    "grpc",
			Traces:      true,
			Metrics:     true,
			SampleRatio: 1.0,
		}
		_, err := Init(context.Background(), cfg, Options{})
		if err == nil {
			t.Fatal("expected Init to error when no endpoint is configured anywhere")
		}
	})
}

// TestBuildResource_EnvOverridesServiceName is the resource-precedence
// guarantee required for Kubernetes deployments: OTEL_SERVICE_NAME /
// OTEL_RESOURCE_ATTRIBUTES are set by the platform and must win over
// telemetry.service_name / telemetry.namespace config defaults.
func TestBuildResource_EnvOverridesServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "env-supplied-service")

	cfg := config.TelemetryConfig{ServiceName: "cfg-supplied-service"}
	res, err := buildResource(context.Background(), cfg, Options{})
	if err != nil {
		t.Fatalf("buildResource returned error: %v", err)
	}

	val, ok := res.Set().Value(semconv.ServiceNameKey)
	if !ok {
		t.Fatal("resource missing service.name attribute")
	}
	if got := val.AsString(); got != "env-supplied-service" {
		t.Fatalf("service.name = %q, want env override %q", got, "env-supplied-service")
	}
}

// otelhttpAdvisedDurationBoundaries is the bucket boundary set otelhttp
// advises for http.client.request.duration at the pinned instrumentation
// version (internal/semconv/client.go passes these to
// metric.WithExplicitBucketBoundaries; httpconv declares the instrument with
// unit "s" and the transport records durationToSeconds, so they are seconds).
//
// The SDK treats an instrument advisory as the default aggregation's
// boundaries, which a View overrides. Asserting the pre-View baseline here is
// what makes the post-View assertion mean something, and it fails loudly if a
// dependency bump changes the advisory — in particular if it ever reverts to
// the SDK's millisecond-shaped default (0, 5, 10, … 10000), which against a
// seconds-valued instrument would leave every bucket but the first empty.
var otelhttpAdvisedDurationBoundaries = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
}

// collectMetrics drives one real request through an otelhttp-instrumented
// transport bound to a MeterProvider built with views (none, if views is
// empty) and the same Prometheus bridge producer initMetrics installs, then
// returns everything the reader collected — both the SDK-side instruments
// otelhttp created and the bridged `arcade_*` families.
func collectMetrics(t *testing.T, views ...sdkmetric.View) metricdata.ResourceMetrics {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(prometheusbridge.NewMetricProducer()))
	opts := []sdkmetric.Option{sdkmetric.WithReader(reader)}
	if len(views) > 0 {
		opts = append(opts, sdkmetric.WithView(views...))
	}
	mp := sdkmetric.NewMeterProvider(opts...)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	// A local MeterProvider, not the global one: this asserts against the
	// provider configuration under test without touching OTEL globals.
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithMeterProvider(mp)),
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("instrumented request failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	return rm
}

// findMetric returns the collected metric with the given name, failing the
// test if it is absent.
func findMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not present in collected data", name)
	return metricdata.Metrics{}
}

// histogramBounds returns the explicit bucket boundaries of the first data
// point of a float64 histogram metric.
func histogramBounds(t *testing.T, m metricdata.Metrics) []float64 {
	t.Helper()
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q has data type %T, want metricdata.Histogram[float64]", m.Name, m.Data)
	}
	if len(hist.DataPoints) == 0 {
		t.Fatalf("metric %q has no data points", m.Name)
	}
	return hist.DataPoints[0].Bounds
}

// promHistogramBounds reads the bucket boundaries a histogram family declares
// on the Prometheus side, so a bridged metric can be compared against its own
// promauto definition rather than against a copy of it hardcoded here.
func promHistogramBounds(t *testing.T, name string) []float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering from the Prometheus default registry: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			h := m.GetHistogram()
			if h == nil {
				continue
			}
			bounds := make([]float64, 0, len(h.GetBucket()))
			for _, b := range h.GetBucket() {
				bounds = append(bounds, b.GetUpperBound())
			}
			return bounds
		}
	}
	t.Fatalf("histogram %q not found in the Prometheus default registry", name)
	return nil
}

// TestMetricViews_HTTPClientRequestDuration asserts the View registered by
// metricViews actually reaches the instrument otelhttp creates — by running a
// request through a real otelhttp Transport rather than by inspecting the
// View value — and that the resulting histogram carries the intended
// boundaries.
//
// The baseline subtest pins what the instrumentation emits without the View,
// which is both the control for the assertion above and the guard on the unit
// question: boundaries in the 0.005..10 range can only be seconds for an
// instrument whose recorded values are seconds.
func TestMetricViews_HTTPClientRequestDuration(t *testing.T) {
	const name = "http.client.request.duration"

	t.Run("view narrows the boundary set", func(t *testing.T) {
		m := findMetric(t, collectMetrics(t, metricViews()...), name)
		if m.Unit != "s" {
			t.Errorf("%s unit = %q, want %q — the values in httpClientDurationBoundaries are "+
				"seconds and must be re-scaled if the instrument's unit changes", name, m.Unit, "s")
		}
		if got := histogramBounds(t, m); !slices.Equal(got, httpClientDurationBoundaries) {
			t.Errorf("%s boundaries = %v, want %v (the View did not reach the instrument)",
				name, got, httpClientDurationBoundaries)
		}
	})

	t.Run("baseline without the view", func(t *testing.T) {
		m := findMetric(t, collectMetrics(t), name)
		if got := histogramBounds(t, m); !slices.Equal(got, otelhttpAdvisedDurationBoundaries) {
			t.Errorf("%s boundaries without a View = %v, want the instrumentation advisory %v — "+
				"otelhttp changed what it advises; re-check the unit and the boundary choice in "+
				"httpClientDurationBoundaries", name, got, otelhttpAdvisedDurationBoundaries)
		}
		if len(otelhttpAdvisedDurationBoundaries) <= len(httpClientDurationBoundaries) {
			t.Errorf("the View sets %d boundaries against an advised %d — it is not reducing anything",
				len(httpClientDurationBoundaries), len(otelhttpAdvisedDurationBoundaries))
		}
	})
}

// TestMetricViews_LeaveBridgedArcadeMetricsAlone guards the blast radius. The
// `arcade_*` families reach the same reader through the Prometheus bridge
// producer, whose ScopeMetrics are appended at collection time and never pass
// through view resolution — and the View matches one wildcard-free instrument
// name regardless. This asserts the outcome of both: a bridged histogram
// keeps the buckets its promauto definition declares.
func TestMetricViews_LeaveBridgedArcadeMetricsAlone(t *testing.T) {
	const name = "arcade_teranode_request_duration_seconds"

	// A histogram vec has no children until something observes on it, so the
	// family would be absent from the bridge's output otherwise.
	metrics.TeranodeRequestDuration.WithLabelValues("submit_tx", "2xx").Observe(0.12)

	got := histogramBounds(t, findMetric(t, collectMetrics(t, metricViews()...), name))
	want := promHistogramBounds(t, name)
	if !slices.Equal(got, want) {
		t.Errorf("%s boundaries after bridging = %v, want its promauto definition %v — "+
			"a View is rewriting an arcade_* metric", name, got, want)
	}
	if slices.Equal(got, httpClientDurationBoundaries) {
		t.Errorf("%s boundaries = %v, which is the http.client.request.duration View's set — "+
			"the View is matching more than its named instrument", name, got)
	}
}
