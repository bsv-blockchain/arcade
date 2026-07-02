# Observability

Arcade ships two independent, complementary observability surfaces:

1. **OpenTelemetry (OTLP) traces + metrics** — off by default, opt-in via
   config/env. Push-based: arcade dials out to a collector, typically a
   node-local one reached over the Kubernetes downward API.
2. **Structured JSON logs to stdout** — always on, unaffected by the
   telemetry flag. Every transaction-lifecycle transition is logged with a
   canonical, snake_case field set shared with `merkle-service`, so a single
   Coralogix query can follow a `txid` or `block_hash` across both services.

**OTLP log export is intentionally not implemented.** Logs never leave the
process over OTLP; they ship stdout → your platform's log agent → filelog
receiver → Coralogix (or whatever backend ingests container stdout). Setting
`telemetry.logs: true` does not turn on log export — it only emits a one-time
startup warning saying so. See [`docs/plans/otel-integration.md`](plans/otel-integration.md)
for the original design and phasing.

This document covers what actually shipped: the OTLP pipeline, the full
config/env reference, the log field canon, the transaction-lifecycle log
lines, and some Coralogix search recipes.

## The OTLP pipeline

```
arcade (traces + metrics, OTLP/gRPC or OTLP/HTTP)
   │
   ▼
node-local OTLP collector  (http://$(HOST_IP):4317, one per k8s node)
   │
   ├─ traces ──────────────────────────────▶ Coralogix
   └─ metrics (bridged from Prometheus) ────▶ Coralogix
```

- **Traces and metrics** are pushed over OTLP. **Logs are not** — they stay
  on the stdout → filelog path described above.
- **Metrics are dual-exported.** The existing Prometheus `/metrics` scrape
  endpoint (see [`metrics/README.md`](../metrics/README.md)) keeps running
  unconditionally, independent of the telemetry flag. When
  `telemetry.enabled` and `telemetry.metrics` are both true, the **same**
  `arcade_*` instruments in `metrics/metrics.go` are *also* read via the
  OTEL contrib Prometheus bridge (`go.opentelemetry.io/contrib/bridges/prometheus`)
  and pushed over OTLP. No metric name, label, or semantic changes — the
  bridge just re-reads the Prometheus default registry on each OTLP export
  tick. Both exporters run simultaneously; this is expected, not a bug (see
  the dual-export note in the design doc).
- Arcade never talks to Coralogix directly. It only ever dials the
  collector endpoint in `telemetry.endpoint` / `OTEL_EXPORTER_OTLP_ENDPOINT`
  — how that collector gets data to Coralogix (or anywhere else) is outside
  arcade's concern. In the reference deployment that collector is
  node-local, reached via the pod's `HOST_IP` (Kubernetes downward API) on
  port 4317 (gRPC) — see `deploy/*.yaml` and the flux repo
  ([bsva-infra-flux#227](https://github.com/bsv-blockchain/bsva-infra-flux/pull/227))
  for the authoritative production wiring.

## Enabling telemetry

Telemetry is **off by default**. With `telemetry.enabled: false` (the
default), `telemetry.Init` builds no providers, sets no OTEL globals, and
opens no network connections — runtime behaviour is identical to a build
with no OTEL support at all. Turning it on is a config/env change, not a
redeploy of different code, and turning it back off is the kill switch.

### Config / env reference

All keys live under `telemetry:` in `config.example.yaml` and follow
arcade's standard `ARCADE_<SECTION>_<KEY>` env override convention
(`viper.SetEnvPrefix("ARCADE")` + `.` → `_`).

| Config key (`telemetry.*`) | Env override | Default | Notes |
|---|---|---|---|
| `enabled` | `ARCADE_TELEMETRY_ENABLED` | `false` | Master switch. `false` ⇒ no-op providers, no network I/O, nothing below applies. |
| `endpoint` | `ARCADE_TELEMETRY_ENDPOINT` | `""` | `host:port` of the OTLP collector — **no scheme**. A `http://`/`https://` prefix is rejected by `config.validate()` for *both* `protocol` values (the HTTP exporter's `WithEndpoint` also wants bare `host:port`; scheme is controlled by `insecure`, not the URL). Left empty, falls back to the standard `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT` env vars (resolved inside `telemetry.Init`, not `validate()`) — those *do* carry a scheme, per the OTEL env var spec. |
| `protocol` | `ARCADE_TELEMETRY_PROTOCOL` | `grpc` | `grpc` (port 4317) or `http` (port 4318). Selects which exporter package arcade constructs (`otlptracegrpc`/`otlpmetricgrpc` vs `otlptracehttp`/`otlpmetrichttp`) — arcade does **not** read the generic `OTEL_EXPORTER_OTLP_PROTOCOL` env var; this config/env key is the only way to choose transport. |
| `insecure` | `ARCADE_TELEMETRY_INSECURE` | `false` | Skip TLS when dialing the collector (typical for an in-cluster/node-local collector). Applies whenever `insecure: true`, regardless of whether the endpoint came from `telemetry.endpoint` or an `OTEL_EXPORTER_OTLP_*_ENDPOINT` env var. |
| `service_name` | `ARCADE_TELEMETRY_SERVICE_NAME` | `arcade` | `service.name` resource attribute. Overridden by `OTEL_SERVICE_NAME` if that env var is set (env wins — see Resource attributes below). |
| `namespace` | `ARCADE_TELEMETRY_NAMESPACE` | `""` | `service.namespace` resource attribute (e.g. the downstream solution's name). Omitted from the resource entirely when empty. |
| `traces` | `ARCADE_TELEMETRY_TRACES` | `true` | Enables the OTLP trace pipeline. Only consulted when `enabled: true`. |
| `metrics` | `ARCADE_TELEMETRY_METRICS` | `true` | Enables the OTLP metric pipeline (Prometheus bridge). Only consulted when `enabled: true`. |
| `logs` | `ARCADE_TELEMETRY_LOGS` | `false` | **Not implemented.** `true` only logs a startup warning; logs always ship via stdout JSON regardless of this flag. |
| `sample_ratio` | `ARCADE_TELEMETRY_SAMPLE_RATIO` | `1.0` | Ratio (0.0–1.0) fed to a `ParentBased(TraceIDRatioBased(...))` sampler. |
| `export_timeout_ms` | `ARCADE_TELEMETRY_EXPORT_TIMEOUT_MS` | `10000` | Bounds `Shutdown`'s `ForceFlush` + `Shutdown` of every provider at process exit. |

Standard OTEL environment variables layer on top of (and can override) the
config above:

| Env var | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Endpoint fallback when `telemetry.endpoint` is empty; applies to both traces and metrics. Must include a scheme (e.g. `http://collector:4317`) — this is a *different* contract from `telemetry.endpoint`, which must NOT have one. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Per-signal endpoint fallback, same scheme requirement, takes precedence over the generic var for that signal. |
| `OTEL_SERVICE_NAME` | Overrides `telemetry.service_name` in the merged resource. |
| `OTEL_RESOURCE_ATTRIBUTES` | Comma-separated `key=value` pairs merged into the resource; wins over every config-derived attribute on key conflicts. |
| `POD_NAME` | Set by the Kubernetes downward API (`fieldRef: metadata.name`). Preferred over `os.Hostname()` for the `service.instance.id` resource attribute — see `deploy/*.yaml`. |
| `HOST_IP` | Not read by arcade itself; used in `deploy/*.yaml` to build `OTEL_EXPORTER_OTLP_ENDPOINT=http://$(HOST_IP):4317` pointing at the node-local collector. Must be defined before its `$(HOST_IP)` use in the same `env:` list. |

### Resource attributes

Every signal (trace or metric) carries the same OTEL `Resource`:

| Attribute | Source |
|---|---|
| `service.name` | `telemetry.service_name` (default `arcade`), overridden by `OTEL_SERVICE_NAME` |
| `service.namespace` | `telemetry.namespace`, omitted when empty |
| `service.version` | the build version (`version.Version`) |
| `service.instance.id` | `POD_NAME` env, falling back to `os.Hostname()` |
| `arcade.mode` | the running `--mode` (`all`, `api-server`, `bump-builder`, `propagation`, `p2p-client`, `chaintracks`, `sse`, `watchdog`) |

`arcade.mode` is the arcade-specific attribute matching `merkle-service`'s
own `merkle.mode` — both let you slice traces/metrics by which per-mode pod
emitted them, mapping directly onto the Deployments in `deploy/`.
`OTEL_RESOURCE_ATTRIBUTES` / `OTEL_SERVICE_NAME` are merged in *after* these
config-derived values and win on conflict, so platform-injected env always
overrides config defaults.

## Trace instrumentation

- **Inbound HTTP**: `otelgin.Middleware` wraps the gin engine
  (`services/api_server/server.go`), installed unconditionally (cheap no-op
  when telemetry is disabled) and placed *outside* the panic-recovery
  middleware so a panic is still captured on a live span.
- **Outbound HTTP**: every arcade-owned `http.Client` (Teranode broadcast,
  merkle-service client, DataHub client, webhook delivery) wraps its
  transport with `otelhttp.NewTransport`, so client spans and `traceparent`
  injection happen automatically wherever the call already threads a
  `context.Context`.
- **Kafka**: `kafka/otel.go` injects the active span context into message
  headers on produce and extracts it on consume, so the async
  propagation/bump-builder pipeline stays in the same trace as the
  originating HTTP submit. Injection is a genuine no-op (nil map, zero
  allocation) when there is no valid span in context — i.e. whenever
  telemetry is disabled — verified by a benchmark asserting zero allocations
  on that path.
- **F-024 preserved**: the Kafka trace-propagation work does not change
  propagation's "register with merkle-service before broadcast" durability
  invariant (F-024) — trace context is metadata riding alongside the
  message, not part of the delivery-order logic.

## Logs

### Field canon

Every transaction/block-lifecycle log line uses the canonical, snake_case
zap fields from the `logfields` package (`logfields/logfields.go`) instead
of ad-hoc `zap.String`/`zap.Int` calls. **This field set is shared with
merkle-service**, so one Coralogix query (e.g. `txid:"<hex>"`) surfaces
every log line touching that transaction across both services, not just
arcade's.

| Field | Type | Meaning |
|---|---|---|
| `txid` | string | Single transaction id. |
| `txids` | array of strings | A batch of transaction ids (see bounded vs. chunked below). |
| `txid_count` | int | True size of a txid batch — always the real total, even when `txids` on the same line is capped. |
| `block_hash` | string | Block hash. |
| `block_height` | uint64 | Block height. |
| `subtree_hash` | string | Merkle subtree hash. Declared for parity with merkle-service's field canon; no arcade lifecycle line currently emits it (arcade's STUMP line logs `subtree_index`, not the hash). |
| `callback_url` | string | Webhook/callback URL. |
| `status` | string | Transaction status value (`RECEIVED`, `MINED`, ...). |
| `stage` | string | Pipeline stage a rejection occurred at: `intake`, `network`, or `cascade`. |
| `trace_id` | string | OTEL trace id, attached via `telemetry.Fields`/`telemetry.LoggerWith` when the log call has a live, sampled-in span in its `context.Context`. |
| `span_id` | string | OTEL span id, same source as `trace_id`. |

`trace_id`/`span_id` are only present on log lines that (a) have a live,
sampled-in span somewhere in their `context.Context` and (b) are logged
through `telemetry.LoggerWith(ctx, logger)` (or built from
`telemetry.Fields(ctx)`) rather than the bare component logger. This covers
more than just inbound HTTP requests: the MINED line below runs on the
`BLOCK_PROCESSED` Kafka consumer's message context, which
`kafka/consumer.go` re-hydrates from the producer's injected trace headers
into a new `kafka.consume` span before the handler runs — so MINED lines
*do* carry `trace_id`/`span_id` (linking back to whatever produced the
`BLOCK_PROCESSED` message) whenever that message was traced. Lines that
instead aggregate a flushed batch spanning many unrelated producer traces —
ACCEPTED_BY_NETWORK, the merkle-registration Debug line, PENDING_RETRY
requeues, REJECTED (network/cascade), and the webhook-delivered line —
deliberately use the plain component logger: there is no single trace to
attach, and for webhook delivery specifically there is no request-scoped
context at all (it runs on the delivery worker pool, fed by a Kafka status
event).

This is statically enforced: `logfields/enforce_test.go` walks every
non-test `.go` file outside the `logfields` package and fails the build if
any of `zap.String`/`Strings`/`Int`/`Uint64`/`Any`/`Reflect` is called with
one of the canonical field names above (or a retired ad-hoc variant:
`txids_sample`, `txid_total`, `blockHash`) as its first argument — i.e. you
cannot regress back to a hand-rolled `zap.String("txid", ...)` call; you
must go through `logfields.TxID(...)`.

Large txid batches are handled two ways:

- **`TxIDBatch`** — bounded, single line: `txid_count` always carries the
  true total, but the `txids` list itself is capped at 1000 entries. Use
  this where an operational preview is enough.
- **`ForEachTxIDChunk`** — full coverage, multiple lines: emits one line per
  1000-txid chunk (`chunk_index`/`chunk_total` fields), so every txid is
  guaranteed to appear somewhere in the log stream. Use this wherever a
  Coralogix txid search must never silently miss a transition.

### Transaction-lifecycle log lines

| Status / event | Message | Coverage | Level |
|---|---|---|---|
| RECEIVED (single-tx submit) | `"transaction received"` | sync, per-request, single `txid` | Info |
| RECEIVED (batch submit) | `"transactions received"` | sync, on the client's request path — bounded (`TxIDBatch`), not chunked, to keep unbounded work off request latency | Info |
| REJECTED (intake) | `"transaction rejected"` (`stage=intake`) | sync, per-tx | Info |
| ACCEPTED_BY_NETWORK | `"transactions accepted by network"` | async (post-broadcast batch), full coverage — chunked via `ForEachTxIDChunk` | Info |
| REJECTED (network) | `"transaction rejected"` (`stage=network`) | async, per-tx | Info |
| REJECTED (cascade — dependent child of a rejected parent) | `"transaction rejected"` (`stage=cascade`, `reason="parent rejected"`) | async, per-tx | Info |
| transactions requeued for retry (in-memory dispatcher retry, not the `PENDING_RETRY` store status) | `"transactions requeued for retry"` (`stage=network`) | async, bounded (`TxIDBatch`), fires once per requeue call — failure-path only, low volume | Info |
| merkle-service registration checkpoint | `"registered with merkle-service"` | async, bounded (`TxIDBatch`) | **Debug** — not a status-lattice transition, just an F-024 durability checkpoint; kept at Debug to hold Info volume flat while staying searchable by txid when Debug logging is enabled |
| SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES | `"transactions seen"` (`status=SEEN_ON_NETWORK` or `SEEN_MULTIPLE_NODES`) | async (merkle-service-driven callback, not the client's request), full coverage — chunked | Info |
| STUMP received | `"stump stored"` | sync, per-STUMP callback | Info |
| BLOCK_PROCESSED received | `"block_processed enqueued"` | sync, per-block callback | Info |
| MINED | `"transactions mined"` | async, full coverage — chunked (a 14k-tx block produces ~14 lines at the 1000-txid chunk size) | Info |
| webhook delivered | `"callback delivered"` | async, per-delivery (delivery worker pool, no per-request span) | Info |

Two statuses in the transaction lattice have **no active producer** and are
therefore never logged as a transition today:

- **`IMMUTABLE`** — defined in the status lattice (`models/transaction.go`)
  as the terminal sink reachable from `MINED`, but nothing in the current
  codebase ever writes it via a status update. If/when a reorg-confirmation
  path starts writing `IMMUTABLE`, it should get the same lifecycle-logging
  treatment as `MINED`.
- **`PENDING_RETRY` via the store** — `store.Store.SetPendingRetryFields` /
  `GetReadyRetries` / `ClearRetryState` are defined and implemented on every
  backend (Aerospike/Pebble/Postgres), but every non-test call site in the
  repo is in a `_test.go` file — there is no production caller. The retry
  behaviour that *does* run today (`requeueAfterDelay` in
  `services/propagation/propagator.go`) never writes `status=PENDING_RETRY`
  at all: the row stays at `RECEIVED` in the store while the tx is
  re-admitted through the in-memory dispatcher after a flat delay, and is
  logged separately (see "transactions requeued for retry" above).

## Coralogix search recipes

Because every lifecycle line uses the shared canonical fields, and the same
field set is used by merkle-service:

- **Full lifecycle of one transaction**: search `txid:"<hex>"` (or
  `txids:"<hex>"`, since chunked lines carry the array field). Returns
  RECEIVED → the merkle-registration Debug line (if Debug logging is on) →
  ACCEPTED_BY_NETWORK → SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES → MINED, plus
  any REJECTED line if it never made it that far — from arcade *and*
  merkle-service in one query, since both use `txid`.
- **Everything about one block**: search `block_hash:"<hex>"`. Surfaces the
  STUMP lines, `block_processed enqueued`, and the chunked `transactions
  mined` lines for that block.
- **Trace-correlated view of one request**: pull `trace_id` off any log line
  that has one (RECEIVED and other request-path lines), then search
  `trace_id:"<id>"` to pivot from logs to the full distributed trace (HTTP →
  Kafka → outbound broadcast) in the traces view, or vice versa — copy a
  `trace_id`/`span_id` off a trace and search logs for it.
- **Rejections by stage**: `stage:"network"` (or `intake`/`cascade`) narrows
  a REJECTED search to where in the pipeline it happened.

## Related docs

- [`docs/plans/otel-integration.md`](plans/otel-integration.md) — the
  original design doc (problem statement, phasing, trade-offs) this
  implements.
- [`metrics/README.md`](../metrics/README.md) — the Prometheus metric
  surface that's bridged onto OTLP; alerting/dashboard recipes.
- [`README.md`](../README.md) — top-level project docs.
