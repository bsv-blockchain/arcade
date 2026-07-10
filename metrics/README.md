# Metrics

Arcade exposes a Prometheus scrape endpoint at `/metrics` on both the health
server (port `health.port`, default `8081`) and the API server (port
`api.port`, default `8080`). Either is fine — pick the one your service mode
runs.

These same `arcade_*` metrics are also dual-exported over OTLP (bridged
straight from this Prometheus registry) when telemetry is enabled — see
[`docs/observability.md`](../docs/observability.md) for the OTLP pipeline,
config/env reference, and the structured-log field canon used for
transaction-lifecycle logging.

All metric names start with `arcade_` so they live in their own namespace.

## What to alert on

| Symptom | Metric | Suggested rule |
|---|---|---|
| Validation pipeline backed up | `arcade_tx_validator_pending_depth` | `> 1000 for 1m` |
| Propagation Kafka publish failing | `arcade_tx_validator_publish_carry_depth` | `> 0 for 30s` (any non-zero is a smell) |
| Validate flush slowing down | `histogram_quantile(0.95, rate(arcade_tx_validator_flush_duration_seconds_bucket[5m]))` | `> 1s` |
| Broadcast tail latency | `histogram_quantile(0.95, rate(arcade_propagation_broadcast_duration_seconds_bucket[5m]))` | `> 5s` |
| Reaper not running anywhere | `sum(arcade_propagation_reaper_lease_held)` | `< 1 for 5m` (failover stuck) |
| Reaper running on multiple replicas | `sum(arcade_propagation_reaper_lease_held)` | `> 1 for 30s` (split brain) |
| BUMP build failures | `arcade_bump_builder_build_duration_seconds_count` by `outcome` | copy-pasteable PromQL in the note below (a regex over the failure outcomes; its `\|` chars can't live in a table cell) |
| Blocks arriving with no STUMPs | `rate(arcade_bump_builder_empty_stump_blocks_total[5m])` | `> 0 for 15m` (see note) |
| Datahub endpoint flapping | `changes(arcade_teranode_endpoint_healthy[5m])` | `> 4` |
| All datahubs unhealthy | `sum(arcade_teranode_endpoint_healthy{}) == 0` | for 1m |
| API errors | `rate(arcade_api_request_duration_seconds_count{status_class="5xx"}[5m])` | `> 0` |
| Kafka publish failures | `rate(arcade_kafka_produce_errors_total[5m])` | `> 0 for 1m` |
| DLQ growth | `rate(arcade_kafka_messages_total{op="dlq"}[5m])` | `> 0` |

### Note on `arcade_bump_builder_build_duration_seconds_count{outcome=…}`

Alert on the failure outcomes, enumerated explicitly:

```promql
rate(arcade_bump_builder_build_duration_seconds_count{
  outcome=~"parse_failed|incomplete_stumps|fetch_failed|no_subtrees|build_failed|validation_failed|store_failed"
}[5m]) > 0
```

**Do not** write `{outcome!="success"}` — three outcomes are benign and would
fire it:

- `short_circuited` — a BUMP already existed, so a redelivered `BLOCK_PROCESSED`
  skipped the rebuild. Expected whenever `/reprocess` re-drives a block.
- `no_stumps` — the block contained no tracked txs. Expected on most blocks.
- `context_canceled` — shutdown.

`no_stumps` is benign *per block* but its rate is a real signal: it cannot be
distinguished from "merkle-service STUMP callbacks were dropped". Alert on
`arcade_bump_builder_empty_stump_blocks_total` separately, and read it next to
`arcade_bump_builder_build_duration_seconds_count{outcome="success"}` — a high
empty-stump rate with a near-zero success rate means STUMPs are not landing.

## Dashboard recipes

### Throughput overview

- `rate(arcade_kafka_messages_total{topic="transaction",op="produce"}[1m])` — submitted txs / sec
- `rate(arcade_tx_validator_outcome_total{outcome="accepted"}[1m])` — validated / sec
- `rate(arcade_propagation_outcome_total{outcome="accepted"}[1m])` — propagated / sec
- Diff between these surfaces where the pipeline is leaking.

### Latency budget

- `histogram_quantile(0.50, ...flush_duration_seconds_bucket)` — median validate time
- `histogram_quantile(0.95, ...flush_duration_seconds_bucket)` — p95 validate time
- `histogram_quantile(0.95, ...broadcast_duration_seconds_bucket{path="batch"})` — p95 batch broadcast
- `histogram_quantile(0.95, ...merkle_register_duration_seconds_bucket)` — merkle is often the bottleneck

### Datahub health board

- Per-endpoint `arcade_teranode_endpoint_healthy` (one row per `endpoint`)
- Stacked area: `arcade_teranode_endpoint_count{source}` over time

### Inline retries (commit 82c0cc7)

- `rate(arcade_propagation_inline_retry_total{outcome="recovered"}[5m])` — saved trips to PENDING_RETRY
- `rate(arcade_propagation_inline_retry_total{outcome="exhausted"}[5m])` — sustained downstream issues

### Reaper visibility

- `arcade_propagation_reaper_lease_held` per pod — exactly one should be 1
- `arcade_propagation_reaper_ready_depth` — sustained > 0 means rebroadcast is keeping up; sustained growth means it isn't
- `rate(arcade_propagation_reaper_tick_total{outcome="ran"}[5m])` should equal the configured tick rate on the leader

### BUMP builder

- `rate(arcade_bump_builder_blocks_processed_total[5m])` — blocks / sec
- `histogram_quantile(0.95, rate(arcade_bump_builder_build_duration_seconds_bucket[5m]))` — build wall time
- `histogram_quantile(0.95, rate(arcade_bump_builder_datahub_fetch_seconds_bucket[5m]))` — datahub fetch p95
- Stacked area of `arcade_bump_builder_build_duration_seconds_count` by `outcome` — surfaces fetch / validation / store failures

## Cardinality guard rails

- The only high-cardinality label currently emitted is `endpoint` on
  `arcade_teranode_endpoint_healthy` and `arcade_teranode_request_duration_seconds`.
  Cardinality is bounded by the size of the datahub fleet (typically <20). If
  the fleet grows large, drop the per-endpoint label on
  `request_duration_seconds` first — it's the higher-volume metric.
- `route` on API metrics uses Gin's `FullPath()` (the route pattern, not the
  resolved URL), so cardinality is bounded by the route table.
- Txids and Kafka offsets are never used as labels.

## Configuration

No metrics-specific configuration. Scrape the endpoint directly.

A minimal Prometheus scrape config (single-binary deployment):

```yaml
scrape_configs:
  - job_name: arcade
    scrape_interval: 15s
    static_configs:
      - targets: ['arcade:8081']
```

For Kubernetes, point your service monitor at the health port:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: arcade
spec:
  selector:
    matchLabels: {app: arcade}
  endpoints:
    - port: health
      path: /metrics
      interval: 15s
```
