# OpenTelemetry Integration

Plan to integrate OpenTelemetry (OTEL) into Arcade so that telemetry — traces,
metrics, and (optionally) logs — can be exported over OTLP to an **external,
OTEL-based monitoring service** owned by a downstream solution that consumes
Arcade. Today Arcade is observable only via a Prometheus scrape endpoint and
stdout JSON logs. This plan adds a first-class OTEL pipeline, migrates the
existing Prometheus metric surface onto it, and instruments the request paths
(HTTP server, outbound HTTP clients, Kafka, store) with distributed traces — all
behind config that keeps the current behaviour as the default until an operator
opts in.

The intended reader is the developer who will implement this. It is written to
be executable step by step, with explicit file targets, the design decisions
already made, and the trade-offs behind them.

---

## Problem

A downstream solution that embeds/consumes Arcade runs a centralized
**OTEL-based monitoring service** (an OTLP collector + backend). It expects the
components it operates to *push* telemetry to it over OTLP rather than be
scraped. Arcade currently cannot do this:

- **Metrics** are exposed only via a Prometheus **pull** model. `metrics/metrics.go`
  defines ~50 `promauto` instruments registered against the Prometheus default
  registry, and `services/health.go:45` (plus the API server) exposes them at
  `/metrics`. There is no way to push these to an OTLP endpoint.
- **Tracing does not exist.** The OTEL libraries are already in `go.mod` but only
  as **indirect/transitive** dependencies (pulled in by other modules) — there
  is no direct OTEL usage anywhere in the codebase. There are no spans, no trace
  context propagation across the HTTP → Kafka → HTTP hops, so the downstream
  service cannot correlate an Arcade request with the rest of a distributed
  trace.
- **Logs** are zap JSON to stdout (`cmd/arcade/main.go:newLogger`). They carry no
  trace/span IDs, so even once tracing exists logs can't be correlated to traces
  in the monitoring backend.

The goal of the downstream team is a single pane of glass: traces, metrics, and
logs from Arcade arriving at their collector, correlated, and attributed to the
right service/pod/mode.

## Goal

1. Stand up an OTEL SDK pipeline inside Arcade that exports over **OTLP** to a
   configurable endpoint (the external monitoring service's collector).
2. **Migrate the existing metric entries** so the metrics the team already relies
   on (`arcade_*`) flow to the OTLP endpoint, without losing the Prometheus
   scrape during transition.
3. Add **distributed tracing** across Arcade's request paths and propagate trace
   context across process boundaries (HTTP and Kafka).
4. Correlate **logs** with traces (trace_id/span_id on every log line), and
   optionally export logs over OTLP too.
5. Ship it **opt-in and off by default**: with no OTEL config, Arcade behaves
   exactly as it does today (Prometheus scrape, stdout logs, no exporter
   connections). Enabling it is a config/env change, not a redeploy of different
   code.

### Non-goals

- Replacing zap as the logging library.
- Removing the Prometheus `/metrics` endpoint in this change (it stays for
  backward compatibility; a later change can retire it once the team confirms the
  OTLP metric stream is sufficient — see *Phase 5*).
- Re-architecting the metrics taxonomy. Metric names, labels, and semantics in
  `metrics/metrics.go` are preserved 1:1.
- Auto-instrumenting every internal function. Tracing targets the boundaries and
  hot paths that the downstream service needs to correlate, not every call.

---

## Design decisions

These are settled; the implementer should follow them unless they hit a concrete
blocker, in which case raise it before diverging.

### D1 — Metrics: bridge first, native API later

There are two ways to get the existing `arcade_*` metrics onto OTLP:

- **(A) Prometheus → OTEL bridge.** Keep every `promauto` instrument exactly as
  written. Add the OTEL Prometheus *producer* bridge
  (`go.opentelemetry.io/contrib/bridges/prometheus`) which reads the Prometheus
  registry and feeds an OTEL `MeterProvider` whose reader is the OTLP metric
  exporter. The `/metrics` endpoint keeps working unchanged.
- **(B) Native rewrite.** Replace all ~50 `promauto.NewX` vars with OTEL
  `metric.Meter` instruments and rewrite every `.Inc()/.Observe()/.Set()` call
  site across the 24 consuming files.

**Decision: do (A) now.** It is low-risk, touches ~3 files instead of ~25,
preserves the scrape endpoint and the entire alerting/dashboard surface in
`metrics/README.md` during the transition, and gets metrics onto OTLP in one
phase. (B) is a large, error-prone mechanical change for marginal benefit (OTEL
exemplars / native histograms) and is explicitly deferred. The bridge means
metric points carry the resource attributes (service.name, mode, pod) on the
OTLP side, which is what the downstream service needs for attribution.

> The `contrib/bridges/prometheus` module is **not yet in `go.sum`** and will be
> added by `go get` (see *Dependencies*). The core `go.opentelemetry.io/otel/sdk/metric`
> *is* already present.

### D2 — OTLP transport: configurable, default gRPC

Export over OTLP. Support both gRPC and HTTP/protobuf, selected by config, since
the external collector's listener may be either. **Default to gRPC** (`4317`)
because it is the common collector default and the trace exporter we already
have transitively (`otlptracehttp`) has a gRPC sibling. Honour the standard
`OTEL_EXPORTER_OTLP_*` environment variables in addition to Arcade's own config
block, so the downstream team can configure Arcade the same way they configure
their other OTEL components.

> `otlptrace` + `otlptracehttp` are already in `go.sum`. The **metric** and
> **log** OTLP exporters, and the gRPC trace exporter, are **not** and will be
> added by `go get`.

### D3 — Single telemetry package, initialized in `main`

Add one new package, `telemetry/`, that owns provider construction and shutdown.
`cmd/arcade/main.go` initializes it immediately after config load (before
`app.Bootstrap`) and defers a bounded `Shutdown(ctx)` that flushes exporters.
Providers are set as the OTEL globals (`otel.SetTracerProvider`,
`otel.SetMeterProvider`, `global.SetLoggerProvider`, `otel.SetTextMapPropagator`)
so instrumentation libraries pick them up without plumbing handles through
`Deps`. This mirrors how `metrics/metrics.go` already uses package-level globals.

### D4 — Tracing scope: boundaries + hot paths

Instrument, in priority order:

1. **Inbound HTTP** (gin) — the API server and the merkle-service callback
   handler. Extract incoming trace context; start the server span.
2. **Outbound HTTP** — `teranode/client.go` and `merkleservice/client.go`
   `http.Client`s, so broadcasts and merkle registration are child spans and
   `traceparent` is injected for the receiving service.
3. **Kafka** — inject trace context into `Message.Headers` on produce and extract
   on consume, so the async propagation/bump-builder pipeline stays in the same
   trace as the originating submit. The neutral `Message.Headers map[string][]byte`
   (`kafka/broker.go:95`) already exists for exactly this.
4. **Store** — a span per `store.Store` operation on the hot path, attributes for
   backend + operation. Lower priority; can land in a later phase.

This is the order the downstream service cares about for correlation. Internal
fan-out goroutines (dispatcher, reaper) get spans only where they cross a
boundary that already emits a metric.

### D5 — Off by default, opt-in via config

A new `telemetry`/`otel` config block (default `enabled: false`). When disabled,
`telemetry.Init` installs **no-op** providers and opens **no** connections —
identical runtime behaviour to today. The Prometheus scrape endpoint is
independent of this flag and always on (until Phase 5).

### D6 — Resource attributes

Every signal carries a shared OTEL `Resource`:
`service.name=arcade`, `service.namespace` (configurable, e.g. the downstream
solution's name), `service.version` (from the build version `goreleaser`
injects), `service.instance.id` (pod name / hostname), and
`arcade.mode=<cfg.Mode>`. This lets the monitoring service slice by deployment
mode (`api-server`, `propagation`, `bump-builder`, …) which maps directly onto
the per-mode pods in `deploy/`.

---

## Changes

### 1. Dependencies (`go.mod` / `go.sum`)

Promote the existing indirect OTEL deps to direct, and add the exporters/bridge
that aren't present yet:

```
go get \
  go.opentelemetry.io/otel \
  go.opentelemetry.io/otel/sdk \
  go.opentelemetry.io/otel/sdk/metric \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc \
  go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc \
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp \
  go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp \
  go.opentelemetry.io/contrib/bridges/prometheus \
  go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin \
  go.opentelemetry.io/contrib/bridges/otelzap   # only if log export (Phase 4) is in scope
```

`otelhttp` and `otelgrpc` instrumentation are already present (indirect) and just
need promotion. Pin all OTEL core modules to the **same minor version** already
in the tree (`v1.44.0` for `otel`/`sdk`/exporters; matching `v0.69.0`-era for
`contrib`) to avoid the OTEL split-version incompatibilities. Run
`go mod tidy` and verify `metrics` still builds.

### 2. New config block (`config/config.go`)

Add a `Telemetry` struct and wire it into `Config` next to `Health`
(`config/config.go:118`). Follow the existing `mapstructure` + `viper.SetDefault`
+ `ARCADE_`-env conventions (`config/config.go:642`, `:693`).

```go
// Telemetry configures OpenTelemetry export to an external OTLP collector.
// Disabled by default: with Enabled=false, Init installs no-op providers and
// opens no connections, so runtime behaviour matches the pre-OTEL build.
type Telemetry struct {
    Enabled       bool   `mapstructure:"enabled"`
    Endpoint      string `mapstructure:"endpoint"`        // host:port of the OTLP collector
    Protocol      string `mapstructure:"protocol"`        // "grpc" (default) | "http"
    Insecure      bool   `mapstructure:"insecure"`        // skip TLS (in-cluster collector)
    ServiceName   string `mapstructure:"service_name"`    // default "arcade"
    Namespace     string `mapstructure:"namespace"`       // service.namespace, e.g. downstream solution name
    Traces        bool   `mapstructure:"traces"`          // default true when Enabled
    Metrics       bool   `mapstructure:"metrics"`         // default true when Enabled
    Logs          bool   `mapstructure:"logs"`            // default false (Phase 4)
    SampleRatio   float64 `mapstructure:"sample_ratio"`   // parent-based ratio sampler, default 1.0
    ExportTimeoutMs int  `mapstructure:"export_timeout_ms"` // default 10000
}
```

Defaults to add in the `SetDefaults`/`viper.SetDefault` section:

```go
viper.SetDefault("telemetry.enabled", false)
viper.SetDefault("telemetry.protocol", "grpc")
viper.SetDefault("telemetry.service_name", "arcade")
viper.SetDefault("telemetry.traces", true)
viper.SetDefault("telemetry.metrics", true)
viper.SetDefault("telemetry.logs", false)
viper.SetDefault("telemetry.sample_ratio", 1.0)
viper.SetDefault("telemetry.export_timeout_ms", 10000)
```

Also honour the standard `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_PROTOCOL`
env vars: if `telemetry.endpoint` is empty, fall back to `OTEL_EXPORTER_OTLP_ENDPOINT`
inside `telemetry.Init`. Add a `validate()` rule: if `telemetry.enabled` then
`endpoint` (or the env var) must be non-empty, and `protocol ∈ {grpc, http}`.

Document the block in `config.example.yaml` (after the `health:` block at
`config.example.yaml:114`) and `config.example.standalone.yaml`.

### 3. New `telemetry` package (`telemetry/telemetry.go`)

Owns provider lifecycle. Public surface:

```go
package telemetry

// Init builds the resource and the trace/metric/log providers per cfg, sets
// them as the OTEL globals, installs the W3C TraceContext + Baggage
// propagator, and returns a Shutdown func that flushes + closes all exporters.
// When cfg.Enabled is false it installs no-op providers and returns a no-op
// Shutdown — no network connections are made.
func Init(ctx context.Context, cfg config.Telemetry, version string, logger *zap.Logger) (Shutdown func(context.Context) error, err error)
```

Responsibilities:

- Build `resource.Resource` from D6 (service.name, namespace, version,
  instance.id from `os.Hostname()`, `arcade.mode`). `arcade.mode` is read from
  the top-level config — pass it in or add a field; keep `Telemetry` itself free
  of cross-cutting config.
- Construct the OTLP **trace** exporter (grpc/http per `Protocol`), wrap in a
  `BatchSpanProcessor`, build a `TracerProvider` with a `ParentBased(TraceIDRatioBased(SampleRatio))`
  sampler.
- Construct the OTLP **metric** exporter, a `PeriodicReader`, and a
  `MeterProvider`. Register the **Prometheus bridge** producer
  (`prometheus.NewMetricProducer`) on the reader so the existing default-registry
  metrics are read and exported (D1). No metric call sites change.
- (Phase 4) Construct the OTLP **log** exporter + `LoggerProvider`.
- `Shutdown` calls `ForceFlush` then `Shutdown` on each provider, bounded by
  `ExportTimeoutMs`, logging (not failing) on partial errors.

Add `telemetry/telemetry_test.go`: assert that `Init` with `Enabled=false`
returns a no-op shutdown and makes no connections, and that with `Enabled=true`
+ a bufconn/stub collector the providers are non-nil and `Shutdown` flushes.

### 4. Wire into startup (`cmd/arcade/main.go`)

After `newLogger` and before `app.Bootstrap` (`cmd/arcade/main.go:42-48`):

```go
tShutdown, err := telemetry.Init(ctx, cfg.Telemetry, version, logger)
if err != nil {
    return fmt.Errorf("telemetry init: %w", err)
}
defer func() {
    sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer scancel()
    _ = tShutdown(sctx)
}()
```

`version` is the build version string — thread through the value goreleaser sets
(check `.goreleaser.yml` ldflags; if none exists, add a `var version = "dev"`
package var and an ldflags entry). The deferred shutdown must run on the same
path as `logger.Sync()` so spans/metrics flush before exit.

### 5. Log/trace correlation (`cmd/arcade/main.go:newLogger` + middleware)

- Add trace correlation so every log line emitted within a span carries
  `trace_id` / `span_id`. Lowest-friction option: in the gin request path and in
  service handlers that already have a `context.Context`, derive a request logger
  via a small helper `telemetry.LoggerWith(ctx, base)` that pulls
  `trace.SpanContextFromContext(ctx)` and adds the two fields. Apply it in
  `services/api_server/server.go:requestLogger` (`:163`) where the request logger
  is built.
- (Phase 4, optional) If `telemetry.logs=true`, add the `otelzap` core to the
  zap logger in `newLogger` so logs are *also* exported over OTLP. Keep the
  stdout JSON core unconditionally — never lose local logs.

### 6. Inbound HTTP tracing (`services/api_server/`)

- Add `otelgin.Middleware(serviceName)` to the gin engine **before**
  `requestLogger` so a server span exists for the duration of the handler and
  the existing metrics/log middleware can read the span context. Find the engine
  setup in `services/api_server/server.go` / `routes.go` and register it in the
  same place other global middleware is added.
- The merkle-service **callback** handler is on the same gin engine, so it is
  covered automatically; verify the route group picks up the middleware.
- Keep `requestLogger` as-is for metrics — `otelgin` handles spans, the existing
  middleware handles `arcade_api_*` metrics and logs. They are complementary.

### 7. Outbound HTTP tracing (`teranode/client.go`, `merkleservice/client.go`)

Both build a bare `http.Client` (`teranode/client.go:204`,
`merkleservice/client.go:37`). Wrap their `Transport` with
`otelhttp.NewTransport(base)`:

```go
httpClient: &http.Client{
    Transport: otelhttp.NewTransport(newBroadcastTransport()),
    Timeout:   ...,
},
```

Because every outbound call already uses `http.NewRequestWithContext(ctx, …)`
(`teranode/client.go:649,709`; `merkleservice/client.go:77,150`), the transport
automatically continues the trace from `ctx` and injects `traceparent`. No
call-site changes. The existing `TeranodeRequestDuration` metric stays — it is
not replaced by the span.

### 8. Kafka context propagation (`kafka/`)

The producer and consumer already pass a neutral `Message.Headers map[string][]byte`
(`kafka/broker.go:95`, populated at `kafka/sarama_broker.go:308`). Add a tiny
`propagation.TextMapCarrier` adapter over `map[string][]byte` and:

- **Produce** (`kafka/producer.go` / `sarama_broker.go` send paths): inject the
  active span context from the message's `context.Context` into `Headers` before
  the sarama message is built (`sarama_broker.go:140,154,175`). Map `Headers` to
  `sarama.RecordHeader` (verify they're already copied across — extend if not).
- **Consume** (`kafka/consumer.go`): extract context from the inbound `Headers`
  and start a consumer span linked to the producer span, so the async
  propagation / bump-builder work stays in the same trace as the HTTP submit.

This is the highest-value tracing hop: it stitches the synchronous submit to the
asynchronous broadcast pipeline. Confirm the carrier handles the
`[]byte`↔`string` conversion for `traceparent`.

### 9. Store tracing (lower priority — can be its own phase)

Wrap the hot-path store methods (`UpdateStatus`, `InsertSubmission`, batch
helpers) in spans inside `store/aerospike/aerospike.go` and `store/pebble/pebble.go`,
or — cleaner — add a thin tracing decorator implementing `store.Store` that
wraps the configured backend in `storefactory.New`. Attributes:
`db.system=<backend>`, `db.operation`. The existing
`StoreUpdateStatusDuration` metric is retained.

### 10. Deployment manifests (`deploy/*.yaml`)

Add OTEL env to each Deployment's `env:` block (pattern at
`deploy/api-server.yaml:39`). Use `ARCADE_TELEMETRY_*` (Arcade config) **or** the
standard `OTEL_EXPORTER_OTLP_*` vars — pick one and document it; recommend the
`OTEL_*` standard so the downstream team configures Arcade like their other
components:

```yaml
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: "http://otel-collector.monitoring.svc.cluster.local:4317"
- name: ARCADE_TELEMETRY_ENABLED
  value: "true"
- name: ARCADE_TELEMETRY_NAMESPACE
  value: "<downstream-solution-name>"
- name: ARCADE_TELEMETRY_INSECURE
  value: "true"   # in-cluster collector, no TLS
```

`service.instance.id` should come from the pod name — add a downward-API env
(`valueFrom.fieldRef: metadata.name`) and have `telemetry.Init` prefer it over
`os.Hostname()`.

### 11. Documentation

- New `docs/observability.md` (or extend `metrics/README.md`): explain the OTLP
  pipeline, the config block, the env vars, the three signals, and that metrics
  are bridged from Prometheus (so the names in `metrics/README.md` are unchanged
  on the OTLP side). Cross-link from `README.md`.
- Note the dual-export window: `/metrics` scrape **and** OTLP metrics run
  simultaneously until Phase 5.

---

## Phasing

Land as a sequence of small, independently reviewable PRs. Each phase is safe to
deploy because the feature stays off until config enables it.

| Phase | Scope | Risk |
|---|---|---|
| **0** | Deps + `telemetry` package + config block + `main.go` wiring, **no-op when disabled**. Nothing instrumented yet. | Very low — dormant code. |
| **1** | Metrics bridge (D1). Enable in staging; confirm `arcade_*` arrive at the collector alongside the scrape. | Low. |
| **2** | Trace context propagator + inbound (otelgin) + outbound (otelhttp) HTTP spans. | Low. |
| **3** | Kafka context propagation (producer inject / consumer extract). | Medium — touches the hot publish path; guard behind the flag and load-test. |
| **4** | Log/trace correlation fields; optional OTLP log export. | Low. |
| **5** | Store spans; then, once the team confirms OTLP metrics are sufficient, a *separate* change to retire the `/metrics` endpoint (or keep both). | Low. |

---

## Testing & rollout

- **Unit:** `telemetry` package tests (no-op when disabled; providers built when
  enabled; shutdown flushes). Carrier round-trip test for the Kafka adapter
  (inject then extract yields the same span context).
- **Integration / local:** add an OTEL Collector service to `docker-compose.yaml`
  with a `debug`/`logging` exporter, set `ARCADE_TELEMETRY_ENABLED=true`, submit a
  tx, and confirm a single trace spans HTTP submit → Kafka → propagation →
  outbound broadcast, and that `arcade_*` metrics arrive.
- **Performance:** Phase 3 (Kafka) and the hot publish path must be load-tested
  with telemetry on. Use the existing `docs/100tps-propagation-results.md`
  harness as the baseline; sampling (`sample_ratio`) is the release valve if
  span volume is too high. Batch span/metric processors keep export off the hot
  path, but verify no added allocation in `processBatch`.
- **Rollout:** ship each phase off-by-default → enable in staging via env →
  validate in the downstream team's collector → enable in production. The kill
  switch is `ARCADE_TELEMETRY_ENABLED=false`, no redeploy of code required.

## Risks & mitigations

- **OTEL version skew.** The OTEL Go modules version independently and mismatched
  minors panic at init. *Mitigation:* pin every core module to the version
  already in the tree (`v1.44.0`), pin `contrib` to its matching release, and add
  a `go mod tidy` + build check to CI.
- **Exporter back-pressure / collector down.** A dead collector must not stall
  Arcade. *Mitigation:* batch processors with bounded queues drop on overflow
  (the OTEL SDK default) rather than block; bound export timeout via
  `ExportTimeoutMs`; never put export on the request goroutine.
- **Double counting metrics.** During the dual-export window both the scrape and
  the OTLP stream carry the same numbers. *Mitigation:* document it; the
  downstream service is the consumer of OTLP, Prometheus scrape is for local/k8s
  ops — they're separate consumers, so it's expected, not a bug.
- **Cardinality.** Spans must not carry txids as high-cardinality span *names*
  (attributes are fine). Keep span names to the operation (route pattern, op
  name) exactly as the metrics package already keeps labels low-cardinality
  (`metrics/metrics.go:10-12`).
- **Hot-path allocation.** Kafka header injection allocates per message.
  *Mitigation:* only inject when tracing is enabled and a valid span context is
  present; benchmark before/after.

## Open questions for the downstream team

1. Collector OTLP listener: **gRPC (4317)** or **HTTP (4318)**? Drives the
   `protocol` default and the deploy env.
2. Do they want **logs** over OTLP (Phase 4), or just metrics + traces with logs
   staying in their existing log pipeline?
3. Required **resource attributes / naming** conventions (their `service.name`
   scheme, any mandatory `deployment.environment` attribute)?
4. Expected **sampling** policy — head sampling ratio here, or tail sampling at
   their collector (in which case Arcade sends 100%)?
5. Should the `/metrics` Prometheus endpoint be **retired** once OTLP metrics are
   confirmed, or kept for their k8s/Prometheus ops indefinitely?
