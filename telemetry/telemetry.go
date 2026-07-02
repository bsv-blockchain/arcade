// Package telemetry owns the OpenTelemetry provider lifecycle for arcade:
// building the shared resource, wiring the OTLP trace/metric exporters, and
// bridging the existing Prometheus `arcade_*` metrics onto the OTLP metric
// pipeline. See docs/plans/otel-integration.md for the design.
//
// The package is off by default. With cfg.Enabled == false, Init installs no
// providers, sets no OTEL globals, and opens no network connections — runtime
// behaviour is identical to a build with no OTEL support at all.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
)

// defaultExportTimeout bounds Shutdown when cfg.ExportTimeoutMs is unset
// (zero). Matches the documented config default.
const defaultExportTimeout = 10 * time.Second

// Options carries the cross-cutting build metadata that TelemetryConfig
// itself intentionally omits (config/config.go keeps Telemetry free of
// process-identity fields so the block stays purely about the OTLP pipeline).
type Options struct {
	// Version is the arcade build version (version.Version), recorded as the
	// resource's service.version attribute.
	Version string
	// Mode is the running arcade mode (cfg.Mode: "all", "api-server", …),
	// recorded as the arcade.mode resource attribute.
	Mode string
	// InstanceID overrides the resource's service.instance.id. When empty,
	// Init prefers the POD_NAME environment variable, falling back to
	// os.Hostname().
	InstanceID string
	// Logger receives warnings about partial/best-effort failures (e.g. a
	// provider that didn't flush cleanly on shutdown). May be nil.
	Logger *zap.Logger
}

// noopShutdown is returned whenever telemetry is disabled or a signal is
// skipped: it does nothing and never fails.
func noopShutdown(context.Context) error { return nil }

// Init builds the OTEL resource and the trace/metric providers per cfg, sets
// them as the OTEL globals, installs the W3C TraceContext + Baggage
// propagator, and returns a Shutdown func that flushes and closes every
// exporter it created.
//
// When cfg.Enabled is false, Init sets nothing globally and returns a no-op
// Shutdown — no providers are built and no network connections are made, so
// runtime behaviour matches a build with telemetry compiled out entirely.
func Init(ctx context.Context, cfg config.TelemetryConfig, opts Options) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return noopShutdown, nil
	}

	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	timeout := time.Duration(cfg.ExportTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultExportTimeout
	}

	res, err := buildResource(ctx, cfg, opts)
	if err != nil {
		return noopShutdown, fmt.Errorf("build resource: %w", err)
	}

	// Build EVERY pipeline before mutating any OTEL global. If a later
	// pipeline fails, nothing has been published yet, so the process is not
	// left half-instrumented with an exporter goroutine nobody can shut
	// down; abort tears down whatever was already built.
	var shutdowns []func(context.Context) error
	var flushes []func(context.Context) error

	abort := func() {
		// WithoutCancel: the cleanup must run to completion (bounded by
		// timeout) even when the failure that triggered it was the caller's
		// ctx being cancelled mid-Init.
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		for _, shutdown := range shutdowns {
			if serr := shutdown(actx); serr != nil {
				logger.Warn("telemetry: cleanup of partially built pipeline failed", zap.Error(serr))
			}
		}
	}

	var tp *sdktrace.TracerProvider
	if cfg.Traces {
		var shutdown, flush func(context.Context) error
		var ierr error
		tp, shutdown, flush, ierr = initTraces(ctx, cfg, res)
		if ierr != nil {
			abort()
			return noopShutdown, ierr
		}
		shutdowns = append(shutdowns, shutdown)
		flushes = append(flushes, flush)
	}

	var mp *sdkmetric.MeterProvider
	if cfg.Metrics {
		var shutdown, flush func(context.Context) error
		var ierr error
		mp, shutdown, flush, ierr = initMetrics(ctx, cfg, res)
		if ierr != nil {
			abort()
			return noopShutdown, ierr
		}
		shutdowns = append(shutdowns, shutdown)
		flushes = append(flushes, flush)
	}

	if cfg.Logs {
		logger.Warn("otlp log export not implemented; logs ship via stdout")
	}

	// Every pipeline built successfully — only now publish the globals.
	if tp != nil {
		otel.SetTracerProvider(tp)
	}
	if mp != nil {
		otel.SetMeterProvider(mp)
	}
	// Only install the propagator when at least one signal pipeline was
	// built. With enabled=true but both traces and metrics off, nothing was
	// created, so leaving the global propagator untouched keeps the
	// disabled-is-noop posture rather than mutating a global for a fully
	// inert config.
	if tp != nil || mp != nil {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	}

	// Partial export/shutdown failures are logged, not surfaced as an error:
	// a slow or unreachable collector at process exit must never turn into a
	// non-zero exit code or a blocked deploy. Callers that want to know
	// whether everything flushed cleanly can inspect their own logs.
	shutdownFn := func(shutdownCtx context.Context) error {
		sctx, cancel := context.WithTimeout(shutdownCtx, timeout)
		defer cancel()

		for _, flush := range flushes {
			if ferr := flush(sctx); ferr != nil {
				logger.Warn("telemetry: force flush failed", zap.Error(ferr))
			}
		}
		for _, shutdown := range shutdowns {
			if serr := shutdown(sctx); serr != nil {
				logger.Warn("telemetry: provider shutdown failed", zap.Error(serr))
			}
		}
		return nil
	}

	return shutdownFn, nil
}

// buildResource constructs the shared OTEL resource from cfg/opts, then
// merges the environment-detected resource (OTEL_SERVICE_NAME /
// OTEL_RESOURCE_ATTRIBUTES) over it so k8s-deployment-set env values win over
// the config defaults. resource.Merge treats its second argument as the
// higher-priority side on key conflicts, so the env-derived resource must be
// passed second.
func buildResource(ctx context.Context, cfg config.TelemetryConfig, opts Options) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
	}
	if cfg.Namespace != "" {
		attrs = append(attrs, semconv.ServiceNamespace(cfg.Namespace))
	}
	if opts.Version != "" {
		attrs = append(attrs, semconv.ServiceVersion(opts.Version))
	}
	if id := resolveInstanceID(opts); id != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(id))
	}
	if opts.Mode != "" {
		attrs = append(attrs, attribute.String("arcade.mode", opts.Mode))
	}
	base := resource.NewSchemaless(attrs...)

	envRes, envErr := resource.New(ctx, resource.WithFromEnv())
	if envErr != nil && !errors.Is(envErr, resource.ErrPartialResource) {
		return nil, fmt.Errorf("detect resource from environment: %w", envErr)
	}

	merged, mergeErr := resource.Merge(base, envRes)
	if mergeErr != nil {
		return nil, fmt.Errorf("merge resource with environment: %w", mergeErr)
	}
	return merged, nil
}

// resolveInstanceID implements the service.instance.id precedence documented
// on Options.InstanceID: explicit override, then POD_NAME (set by the k8s
// downward API in every arcade Deployment), then os.Hostname().
func resolveInstanceID(opts Options) string {
	if opts.InstanceID != "" {
		return opts.InstanceID
	}
	if pod := os.Getenv("POD_NAME"); pod != "" {
		return pod
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return ""
}

// hasOTLPEndpointEnv reports whether the standard OTLP endpoint environment
// variables (generic or signal-specific) are set. signal is "TRACES" or
// "METRICS".
func hasOTLPEndpointEnv(signal string) bool {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		return true
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT") != ""
}

// initTraces builds the OTLP trace exporter, batch processor, and
// TracerProvider for cfg. It returns the provider along with separate
// Shutdown/ForceFlush funcs so Init can bound and sequence them uniformly
// across signals.
func initTraces(ctx context.Context, cfg config.TelemetryConfig, res *resource.Resource) (*sdktrace.TracerProvider, func(context.Context) error, func(context.Context) error, error) {
	if cfg.Endpoint == "" && !hasOTLPEndpointEnv("TRACES") {
		return nil, nil, nil, fmt.Errorf(
			"traces enabled but no OTLP endpoint configured " +
				"(set telemetry.endpoint or OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_TRACES_ENDPOINT)")
	}

	var exp *otlptrace.Exporter
	var err error
	switch cfg.Protocol {
	case "http":
		httpOpts := []otlptracehttp.Option{}
		if cfg.Endpoint != "" {
			httpOpts = append(httpOpts, otlptracehttp.WithEndpoint(cfg.Endpoint))
		}
		// WithInsecure applies regardless of whether the endpoint came from
		// cfg or the OTEL_EXPORTER_OTLP_*_ENDPOINT env var, so it must live
		// outside the endpoint guard.
		if cfg.Insecure {
			httpOpts = append(httpOpts, otlptracehttp.WithInsecure())
		}
		exp, err = otlptracehttp.New(ctx, httpOpts...)
	default: // "grpc" and any unvalidated value fall back to grpc
		grpcOpts := []otlptracegrpc.Option{}
		if cfg.Endpoint != "" {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Insecure {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithInsecure())
		}
		exp, err = otlptracegrpc.New(ctx, grpcOpts...)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build trace exporter: %w", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(exp)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		sdktrace.WithSpanProcessor(bsp),
	)

	return tp, tp.Shutdown, tp.ForceFlush, nil
}

// initMetrics builds the OTLP metric exporter, a PeriodicReader that also
// pulls from the Prometheus default registry via the OTEL contrib bridge
// (so every promauto arcade_* instrument in metrics/metrics.go flows through
// without call-site changes), and the resulting MeterProvider.
func initMetrics(ctx context.Context, cfg config.TelemetryConfig, res *resource.Resource) (*sdkmetric.MeterProvider, func(context.Context) error, func(context.Context) error, error) {
	if cfg.Endpoint == "" && !hasOTLPEndpointEnv("METRICS") {
		return nil, nil, nil, fmt.Errorf(
			"metrics enabled but no OTLP endpoint configured " +
				"(set telemetry.endpoint or OTEL_EXPORTER_OTLP_ENDPOINT / OTEL_EXPORTER_OTLP_METRICS_ENDPOINT)")
	}

	var exp sdkmetric.Exporter
	var err error
	switch cfg.Protocol {
	case "http":
		httpOpts := []otlpmetrichttp.Option{}
		if cfg.Endpoint != "" {
			httpOpts = append(httpOpts, otlpmetrichttp.WithEndpoint(cfg.Endpoint))
		}
		// WithInsecure applies regardless of whether the endpoint came from
		// cfg or the OTEL_EXPORTER_OTLP_*_ENDPOINT env var, so it must live
		// outside the endpoint guard.
		if cfg.Insecure {
			httpOpts = append(httpOpts, otlpmetrichttp.WithInsecure())
		}
		exp, err = otlpmetrichttp.New(ctx, httpOpts...)
	default: // "grpc" and any unvalidated value fall back to grpc
		grpcOpts := []otlpmetricgrpc.Option{}
		if cfg.Endpoint != "" {
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Insecure {
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithInsecure())
		}
		exp, err = otlpmetricgrpc.New(ctx, grpcOpts...)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build metric exporter: %w", err)
	}

	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithProducer(prometheusbridge.NewMetricProducer()))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)

	return mp, mp.Shutdown, mp.ForceFlush, nil
}
