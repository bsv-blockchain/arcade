// Package metrics defines the Prometheus metrics surface arcade exposes for
// scrape via the /metrics endpoint on the health server.
//
// Conventions
//
//   - Every metric is prefixed `arcade_` so a multi-tenant Prometheus can
//     filter on it cleanly.
//   - Counters end in `_total`. Histograms end in `_seconds` (latency) or
//     `_bytes` (sizes). Gauges end in a noun (e.g. `_depth`, `_count`).
//   - Labels are kept low-cardinality. Endpoint URLs are labeled because the
//     fleet is small (handful of datahubs); txids and Kafka offsets are
//     never used as labels.
//   - Buckets for latency histograms span 1ms..30s in coarse Fibonacci-ish
//     steps so a 1ms validate and a 30s reaper rebroadcast both land in
//     useful buckets.
//   - Buckets for size histograms span 1..10000 since that's the range we
//     see for batch sizes from a 1-tx single submit up to a 1000-tx flush.
//
// Service ownership
//
//   - propagation: batch size, broadcast latency per outcome, chunk count,
//     dispatcher pending depth, deferred-requeue gauge, reaper lease and
//     tick outcomes, narrowed-reaper rebroadcast depth, merkle registration
//     latency.
//   - bump_builder: build duration, blocks processed, BUMP outcomes, STUMP
//     and grace-window stats.
//   - api_server: request latency by route + status, in-flight gauge.
//   - teranode (HTTP client): per-endpoint request latency by op + status,
//     endpoint health gauge.
//   - kafka: produce/consume/DLQ counters, message size histogram.
//   - p2p_client: node_status messages received, datahub URL discovery
//     outcomes.
//
// Most metrics live as package-level vars so any service can update them
// without plumbing a registry through. Tests use the default registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// labelOutcome is the conventional label name for counters that partition
// their measurements by a coarse success/failure-class enum. Centralized so
// every metric uses the same label key (and goconst stays quiet).
const labelOutcome = "outcome"

// Standard latency buckets for histograms measuring durations from very
// short (DB lookup, validate) up to long (reaper tick, bump build).
var latencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
}

// Standard size buckets for histograms measuring batch sizes.
var sizeBuckets = []float64{
	1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
}

// Standard byte-size buckets for HTTP / message payloads.
var bytesBuckets = []float64{
	256, 1024, 4096, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024,
}

// ---------------------------------------------------------------------------
// propagation
// ---------------------------------------------------------------------------

// PropagationBatchSize measures how many txs landed in each processBatch call
// — i.e. the size at the entrypoint to the broadcast pipeline.
var PropagationBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_propagation_batch_size",
	Help:    "Number of txs in each processBatch call.",
	Buckets: sizeBuckets,
})

// PropagationBroadcastConsensus counts broadcasts by network-level outcome.
// "unanimous_reject" means every endpoint that responded returned non-2xx —
// the network agrees the tx is bad, so the slow-track breaker is NOT
// charged against the responding peers (they're behaving correctly). This
// metric is the diagnostic for the resilience tunable: if it's growing
// quickly, the tx generator is producing rejectable txs (double-spends,
// invalid signatures, insufficient fees, …) — not a peer-health problem.
var PropagationBroadcastConsensus = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_broadcast_consensus_total",
	Help: "Per-broadcast consensus outcome across all responding endpoints.",
}, []string{"verdict"}) // accepted, unanimous_reject, mixed, unreachable

// PropagationPendingDepth gauges how many propagationMsgs the dep-aware
// dispatcher is currently holding in its pendingMsgs accumulator awaiting
// the next flush. The dispatcher owns the slice on a single goroutine,
// so the gauge tracks its length at every mutation point. Sustained
// growth indicates downstream (teranode broadcast or merkle-service
// register) is not keeping up with ingest.
var PropagationPendingDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_pending_depth",
	Help: "Number of propagation messages buffered in the dispatcher awaiting flush.",
})

// PropagationPendingRequeues gauges how many delayed-requeue goroutines
// are currently parked waiting for their flat requeueDelay to elapse
// before pushing back onto the dispatcher. Each Teranode infra-failure
// (no peer reachable, parseable 500, per-slot PROCESSING) drives a
// new requeue, so a sustained high value points to upstream pressure
// — pair it with TeranodeEndpointHealth to confirm. Inc'd on entry,
// Dec'd via defer regardless of whether the goroutine exits via timer
// or ctx.Done.
var PropagationPendingRequeues = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_pending_requeues",
	Help: "Number of requeueAfterDelay goroutines currently awaiting their delay before re-admitting messages.",
})

// PropagationInflightBatches gauges how many flushBatch goroutines are
// currently running their register+broadcast pipeline. Capped at
// PropagationConfig.MaxConcurrentBatches; sustained saturation means the
// pipeline cannot keep up with the kafka drain rate and pendingMsgs will
// grow until backpressure forces the consumer to block.
var PropagationInflightBatches = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_inflight_batches",
	Help: "Number of propagation batches currently mid-pipeline.",
})

// PropagationBroadcastDuration measures end-to-end wall time of broadcasting
// a single chunk to all healthy endpoints (the inner /tx or /txs path).
var PropagationBroadcastDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_propagation_broadcast_duration_seconds",
	Help:    "Wall time of one chunk broadcast across all healthy endpoints.",
	Buckets: latencyBuckets,
}, []string{"path"}) // batch, single

// PropagationOutcomeTotal counts per-tx outcomes from the propagation step.
var PropagationOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_outcome_total",
	Help: "Per-tx propagation outcome counts.",
}, []string{labelOutcome}) // accepted, rejected, retryable, no_verdict

// PropagationChunkTotal counts how many chunk broadcasts were issued. Combined
// with PropagationBatchSize this surfaces whether teranode_max_batch_size is
// well-tuned.
var PropagationChunkTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_chunk_total",
	Help: "Number of chunk broadcasts issued, by fallback decision.",
}, []string{"fallback"}) // none

// PropagationMerkleRegisterDuration measures the merkle-service registration
// wall time for one flushBatch — a single bounded-concurrency fan-out over
// every tx in the batch, observed once per batch. Slow merkle calls are a
// common bottleneck; under burst ingest this histogram is the canonical
// p50/p99 signal.
var PropagationMerkleRegisterDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_propagation_merkle_register_duration_seconds",
	Help:    "Wall time of one batched merkle-service registration fan-out.",
	Buckets: latencyBuckets,
})

// PropagationMerkleRegisterFailures counts per-tx merkle-service registration
// failures by reason. Sustained values indicate the merkle service is
// unhealthy — without this metric a registration outage was previously
// masked by silent broadcast continuation. The label is kept open so future
// error-class splits (e.g. "timeout", "5xx", "auth") can be added without
// renaming the metric.
var PropagationMerkleRegisterFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_merkle_register_failures_total",
	Help: "Per-tx merkle-service Register failures, by reason.",
}, []string{"reason"})

// PropagationMerkleRegisterBatchOutcomeTotal counts each flushBatch's merkle
// registration result. "fully_ok" = every tx registered, "partial" = some
// succeeded and some routed to PENDING_RETRY, "all_failed" = nothing
// broadcast this flush. Lets dashboards distinguish a one-off RTT blip
// (single "partial" tick) from a sustained outage (rising "all_failed"
// rate) — a signal the per-tx failure counter alone obscures.
var PropagationMerkleRegisterBatchOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_merkle_register_batch_outcome_total",
	Help: "Per-batch merkle-service registration outcome.",
}, []string{labelOutcome}) // fully_ok, partial, all_failed

// PropagationReaperLease is 1 when this pod holds the reaper lease, 0 otherwise.
// In K8s, sum across pods should always equal 1 (or 0 during failover).
var PropagationReaperLease = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_reaper_lease_held",
	Help: "1 if this pod holds the reaper lease, 0 otherwise.",
})

// PropagationReaperTickTotal tracks reaper ticks by outcome.
var PropagationReaperTickTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_reaper_tick_total",
	Help: "Reaper tick outcomes.",
}, []string{labelOutcome}) // ran, skipped_no_leader, lease_error

// PropagationReaperReadyDepth is the count of stale SEEN_ON_NETWORK /
// SEEN_MULTIPLE_NODES rows the last reaper tick observed as candidates
// for rebroadcast. Set on every tick (including to 0 when the queue
// clears) so dashboards reflect current state, not the last non-zero
// scan. Sustained high values indicate a struggling downstream
// (datahubs flapping, merkle slow) blocking SEEN_ON_NETWORK txs from
// reaching ACCEPTED.
var PropagationReaperReadyDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_reaper_ready_depth",
	Help: "Number of stale SEEN_ON_NETWORK rows ready for rebroadcast at the last reaper tick.",
})

// ---------------------------------------------------------------------------
// bump_builder
// ---------------------------------------------------------------------------

// BumpBuilderBuildDuration measures end-to-end wall time from BLOCK_PROCESSED
// receipt (after grace window) to BUMP persisted.
var BumpBuilderBuildDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_bump_builder_build_duration_seconds",
	Help:    "Time to build and persist one compound BUMP, by outcome.",
	Buckets: latencyBuckets,
}, []string{labelOutcome}) // success, no_stumps, fetch_failed, validation_failed, store_failed

// BumpBuilderBlocksProcessedTotal counts BLOCK_PROCESSED messages handled.
var BumpBuilderBlocksProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_blocks_processed_total",
	Help: "BLOCK_PROCESSED messages handled by bump-builder.",
})

// BumpBuilderStumpCount is the histogram of how many STUMPs each block had.
// Useful for spotting blocks with unusual tracking patterns.
var BumpBuilderStumpCount = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_bump_builder_stump_count",
	Help:    "Number of STUMPs per block at BUMP construction time.",
	Buckets: sizeBuckets,
})

// BumpBuilderTxidsMined counts the txs marked MINED across all builds.
var BumpBuilderTxidsMinedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_txids_mined_total",
	Help: "Tracked transactions marked MINED via BUMP construction.",
})

// BumpBuilderDatahubFetchDuration measures the round-trip to the datahub for
// subtree hashes + coinbase BUMP + header merkle root.
var BumpBuilderDatahubFetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_bump_builder_datahub_fetch_seconds",
	Help:    "Datahub fetch latency for block data needed by BUMP construction.",
	Buckets: latencyBuckets,
})

// BumpBuilderGraceWaitTotal counts how often the grace window was respected.
// (Almost always; useful as a smoke metric.)
var BumpBuilderGraceWaitTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_grace_window_waits_total",
	Help: "BLOCK_PROCESSED handlers that waited the grace window before reading STUMPs.",
})

// BumpBuilderEmptyStumpBlocksTotal counts BLOCK_PROCESSED messages that
// arrived with zero stored STUMPs for the block. The "expected" case is a
// block with no tracked transactions, but a sustained non-zero rate while
// arcade has watched txs is a strong signal that STUMP callbacks are being
// lost upstream (merkle-service callback_dedup suppression, delivery DLQ,
// callback URL outage). Surfaces silent drops that would otherwise only show
// up as "tx stuck in SEEN_MULTIPLE_NODES" days later.
var BumpBuilderEmptyStumpBlocksTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_empty_stump_blocks_total",
	Help: "BLOCK_PROCESSED messages handled with zero STUMPs in the store for the block.",
})

// BumpBuilderShortCircuitTotal counts BLOCK_PROCESSED messages handled by the
// short-circuit path — the BUMP already exists in the store and this is a
// redelivery (typically from /reprocess re-emitting BLOCK_PROCESSED). Tracks
// how much work the short-circuit saves vs. re-fetching datahub.
var BumpBuilderShortCircuitTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_short_circuit_total",
	Help: "BLOCK_PROCESSED messages skipped because a compound BUMP already exists for the block.",
})

// ---------------------------------------------------------------------------
// watchdog (standalone service — block-processing recovery)
// ---------------------------------------------------------------------------

// WatchdogTickTotal tracks watchdog tick outcomes.
var WatchdogTickTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_watchdog_tick_total",
	Help: "Watchdog tick outcomes.",
}, []string{labelOutcome}) // ran, skipped_no_leader, lease_error

// WatchdogStaleCount is the number of stale block_processing rows the last
// tick observed (post-recency-window filter, pre-backoff filter).
var WatchdogStaleCount = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_watchdog_stale_count",
	Help: "Stale block_processing rows observed by the last watchdog tick.",
})

// WatchdogReprocessTotal counts /reprocess outcomes by reason.
var WatchdogReprocessTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_watchdog_reprocess_total",
	Help: "Watchdog /reprocess call outcomes.",
}, []string{labelOutcome}) // success, err_4xx, err_5xx, err_network

// WatchdogBackoffDepth is the size of the in-memory attempts map.
// Sustained growth implies blocks are persistently failing to recover —
// inspect logs for the 4xx/5xx outcome breakdown.
var WatchdogBackoffDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_watchdog_backoff_depth",
	Help: "Number of blocks currently held in the watchdog's in-memory backoff map.",
})

// ---------------------------------------------------------------------------
// api_server
// ---------------------------------------------------------------------------

// APIRequestDuration measures HTTP request latency by route and status class.
// route is the gin route pattern (not the resolved URL) so cardinality stays
// bounded.
var APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_api_request_duration_seconds",
	Help:    "API request latency by route + method + status class.",
	Buckets: latencyBuckets,
}, []string{"route", "method", "status_class"}) // status_class = 2xx, 3xx, 4xx, 5xx

// APISubmissionRecorderDropTotal counts submission rows dropped by the async
// recorder pool because its bounded queue was full. Non-zero is acceptable
// (recordSubmission is best-effort) but sustained drops mean the recorder
// pool is undersized relative to the inbound submit rate — raise worker
// count or queue depth in server.go.
var APISubmissionRecorderDropTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_api_submission_recorder_drop_total",
	Help: "Submission rows dropped because the async recorder queue was full.",
})

// APIRequestsInFlight tracks how many requests are currently being handled.
var APIRequestsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_api_requests_in_flight",
	Help: "API requests currently being handled.",
})

// APIRequestBytes tracks request body size — surfaces oversized clients early.
var APIRequestBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_api_request_bytes",
	Help:    "API request body size in bytes, by route.",
	Buckets: bytesBuckets,
}, []string{"route"})

// APISSEDroppedTotal counts SSE fan-out events that were dropped without
// being delivered to a client. Reasons:
//   - "slow_client": the client's send buffer was full (the consumer goroutine
//     wasn't draining it fast enough).
//   - "client_gone": the client was unregistering concurrently and its context
//     had already been canceled by the time fan-out reached it.
//
// A non-zero "client_gone" rate is normal under churn; a sustained
// "slow_client" rate indicates a consumer that can't keep up with the publish
// rate and may need a larger buffer or a backpressure strategy.
var APISSEDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_api_sse_dropped_total",
	Help: "SSE fan-out events dropped without delivery, by reason.",
}, []string{"reason"}) // slow_client, client_gone

// EventsSubscriberDroppedTotal counts events.Publisher.Subscribe channel
// drops, labeled by which caller's channel filled. The publisher emits a
// drop when the per-subscriber buffer is at capacity and the kafka handler
// goroutine can't enqueue the next message without blocking. A sustained
// non-zero rate on a particular caller (e.g. "webhook") points to that
// subscriber's downstream draining slower than the producer rate — typical
// causes are synchronous I/O in the channel reader or a CPU-pressured pod.
var EventsSubscriberDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_events_subscriber_dropped_total",
	Help: "events.Publisher subscriber-channel drops, labeled by caller (e.g. sse, webhook).",
}, []string{"caller"})

// WebhookPoolSaturatedTotal counts status updates that the webhook
// service dropped because its bounded delivery worker pool was full when
// the channel reader tried to enqueue them. A non-zero rate means
// MaxConcurrentDeliveries is too low for the current rate of slow
// callbacks: workers are blocked on http.Client.Do and incoming statuses
// pile up faster than the work channel can hold them. Distinguishes
// pool-pressure drops from upstream subscriber-channel drops
// (arcade_events_subscriber_dropped_total{caller="webhook"}) so the two
// failure modes can be tuned independently.
var WebhookPoolSaturatedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_webhook_pool_saturated_total",
	Help: "Status updates dropped by the webhook service because its delivery worker pool was full.",
})

// WebhookCASLostTotal counts claim-then-POST attempts that lost the CAS to
// another replica — the other pod already advanced LastDeliveredStatus for
// the same submission, so this pod silently skipped its POST. With N
// horizontally-scaled api-server replicas, this counter is expected to
// increase at roughly (N - 1) × deliveries: the winner POSTs, the (N - 1)
// losers count here. A flat zero on a multi-replica deployment means events
// are not actually flowing through CAS — either the schema migration is
// missing or only one pod is producing events.
var WebhookCASLostTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_webhook_cas_lost_total",
	Help: "Webhook deliveries skipped because another replica won the LastDeliveredStatus CAS.",
})

// WebhookCASErrorTotal counts CAS attempts that failed with a real infra
// error rather than a generation mismatch — surfaced separately so a flat
// WebhookCASLostTotal can't mask a backend that's silently failing every
// write. Only the Aerospike backend emits this today: its CAS path collapses
// gen-mismatch and infra errors into the same (false, nil) return shape, so
// the metric is the one observable signal that distinguishes them. Postgres
// and Pebble propagate infra errors through the function's `err` return and
// the caller already logs those.
var WebhookCASErrorTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_webhook_cas_error_total",
	Help: "Webhook CAS writes that failed with an infra error (distinct from generation mismatch).",
})

// WebhookReaperLease is 1 when this pod holds the webhook-reaper lease, 0
// otherwise. With N replicas, exactly one is expected to be 1 at any time.
var WebhookReaperLease = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_webhook_reaper_lease",
	Help: "1 when this pod holds the webhook-reaper lease, 0 otherwise.",
})

// WebhookReaperTickTotal tracks reaper ticks by outcome (ran /
// skipped_no_leader / lease_error). Skipped_no_leader is the expected steady
// state on N-1 replicas; a sustained lease_error rate points at the store
// backend being unhealthy.
var WebhookReaperTickTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_webhook_reaper_tick_total",
	Help: "Webhook reaper ticks by outcome (ran / skipped_no_leader / lease_error).",
}, []string{labelOutcome})

// WebhookReaperReadyDepth is the number of submissions the most recent reaper
// tick observed as ready-for-retry. A sustained non-zero value means the
// backlog of failed POSTs is growing faster than the reaper can drain it.
var WebhookReaperReadyDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_webhook_reaper_ready_depth",
	Help: "Submissions the last webhook-reaper tick observed as ready-for-retry.",
})

// ---------------------------------------------------------------------------
// teranode (HTTP client)
// ---------------------------------------------------------------------------

// TeranodeRequestDuration measures HTTP latency for outbound calls to a
// datahub endpoint, by op and status code class.
var TeranodeRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_teranode_request_duration_seconds",
	Help:    "HTTP request latency from arcade to a datahub endpoint, by op and status class.",
	Buckets: latencyBuckets,
}, []string{"op", "status_class"}) // op = submit_tx, submit_txs, probe; status_class = 2xx/4xx/5xx/transport_error

// TeranodeEndpointHealth is per-endpoint circuit-breaker state. 1 = healthy,
// 0 = unhealthy. Endpoint URL is the label so dashboards can per-endpoint
// alert; URL count is bounded by the size of the datahub fleet.
var TeranodeEndpointHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arcade_teranode_endpoint_healthy",
	Help: "1 if the endpoint is currently in the healthy set, 0 if circuit-breaker tripped.",
}, []string{"endpoint", "source"}) // source = configured, discovered

// TeranodeEndpointCount is the total count of registered endpoints, separated
// by source. Surfaces whether p2p discovery is finding peers.
var TeranodeEndpointCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arcade_teranode_endpoint_count",
	Help: "Number of registered datahub endpoints, by source.",
}, []string{"source"})

// ---------------------------------------------------------------------------
// kafka
// ---------------------------------------------------------------------------

// KafkaMessagesTotal counts produce / consume / DLQ events by topic.
var KafkaMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_kafka_messages_total",
	Help: "Kafka messages produced, consumed, or DLQ-routed, by topic and op.",
}, []string{"topic", "op"}) // op = produce, consume, dlq

// KafkaMessageBytes measures message payload size.
var KafkaMessageBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_kafka_message_bytes",
	Help:    "Kafka message size in bytes, by topic and op.",
	Buckets: bytesBuckets,
}, []string{"topic", "op"})

// KafkaBackpressureTotal counts Send() calls that returned
// ErrBrokerBackpressure. A sustained non-zero rate means a consumer is too
// slow to keep up with the producer at the broker's configured buffer/timeout
// — investigate the corresponding consumer's pending-depth gauge.
var KafkaBackpressureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_kafka_backpressure_total",
	Help: "Producer Send calls that returned ErrBrokerBackpressure, by topic.",
}, []string{"topic"})

// KafkaProduceErrors counts producer failures by topic.
var KafkaProduceErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_kafka_produce_errors_total",
	Help: "Kafka producer error count, by topic.",
}, []string{"topic"})

// KafkaDLQPublishFailures counts DLQ publish failures by original topic. A
// non-zero rate means the DLQ topic is rejecting publishes — investigate Kafka
// availability. The consumer leaves the offset uncommitted on these failures,
// so they correlate with rising consumer lag on the primary topic until
// publishing recovers.
var KafkaDLQPublishFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_kafka_dlq_publish_failures_total",
	Help: "Kafka DLQ publish failure count, by original topic.",
}, []string{"topic"})

// ---------------------------------------------------------------------------
// p2p_client
// ---------------------------------------------------------------------------

// P2PNodeStatusMessagesTotal counts node_status messages received from the
// teranode pubsub topic.
var P2PNodeStatusMessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_p2p_node_status_messages_total",
	Help: "node_status messages received from teranode peers.",
})

// P2PEndpointDiscoveryTotal counts datahub URL discovery outcomes.
var P2PEndpointDiscoveryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_p2p_endpoint_discovery_total",
	Help: "Datahub URL discovery outcomes from peer announcements.",
}, []string{labelOutcome}) // registered, invalid, no_url, error

// ---------------------------------------------------------------------------
// callback path — inbound merkle-service callbacks
// ---------------------------------------------------------------------------

// statusTransitionAgeBuckets covers RECEIVED→SEEN_ON_NETWORK style
// transitions. Wider than latencyBuckets because the tail can stretch into
// the multi-second range under merkle-service congestion (the slow case is
// the one we care most about catching with a histogram).
var statusTransitionAgeBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
}

// StatusTransitionAge measures the wall-clock age of the previous status
// row at the moment a new transition is applied. Wired into the SEEN_ON_NETWORK
// callback handler so {from="RECEIVED",to="SEEN_ON_NETWORK"} surfaces the
// time between arcade receiving a tx and the merkle-service callback
// landing in arcade's store + publish pipeline. Naturally extensible to
// other transitions (RECEIVED→REJECTED, SEEN_ON_NETWORK→MINED, ...) without
// new metric definitions.
var StatusTransitionAge = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_status_transition_age_seconds",
	Help:    "Wall-clock age of the previous status row at the moment a new transition is applied.",
	Buckets: statusTransitionAgeBuckets,
}, []string{"from", "to"})

// CallbackHandlerDuration measures end-to-end handler latency for one
// inbound /api/v1/merkle-service/callback request, partitioned by the
// callback type so a slow STUMP path doesn't get conflated with a slow
// SEEN_ON_NETWORK path. result ∈ {success, partial, error}.
var CallbackHandlerDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_callback_handler_duration_seconds",
	Help:    "End-to-end duration of one inbound merkle-service callback handler, by type.",
	Buckets: latencyBuckets,
}, []string{"type", "result"})

// CallbackBatchSize records len(TxIDs) per inbound callback so we can see
// how aggressively the upstream is batching. Informs whether bulk-publish
// optimizations are paying off.
var CallbackBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_callback_batch_size",
	Help:    "Number of txids in one inbound merkle-service callback, by type.",
	Buckets: sizeBuckets,
}, []string{"type"})

// CallbackUnknownTxIDTotal counts txids referenced by a callback that
// arcade's store has no record of. Already logged at WARN; the counter
// makes the rate scrapable so an operator can spot upstream drift.
var CallbackUnknownTxIDTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_callback_unknown_txid_total",
	Help: "Inbound callbacks that named a txid arcade's store doesn't know.",
}, []string{"type"})

// CallbackStaleTotal counts txids in inbound callbacks whose store row is
// already past the target status (lattice short-circuited the update). The
// underlying signal also lives in store_updatestatus_duration_seconds_count
// with outcome=skipped_lattice, but that histogram bakes in the duration
// label set; a dedicated counter is cheaper to alert on and makes
// "merkle-service is sending us stale callbacks" a first-class number.
// prev_status carries the lattice-blocked previous state (e.g. MINED) so
// operators can tell apart "callback for already-mined tx" from "duplicate
// SEEN_ON_NETWORK".
var CallbackStaleTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_callback_stale_total",
	Help: "Inbound callbacks whose target status was already eclipsed by the stored row's status.",
}, []string{"type", "prev_status"})

// ---------------------------------------------------------------------------
// store hot-path — per-call latency
// ---------------------------------------------------------------------------

// StoreUpdateStatusDuration decomposes the per-call UpdateStatus latency so
// we can tell whether a long callback handler is paying disk-write cost or
// publish cost. from_status/to_status label cardinality is bounded by the
// status lattice (~10 values each) so the matrix stays small.
var StoreUpdateStatusDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_store_updatestatus_duration_seconds",
	Help:    "Duration of one store.UpdateStatus call, by from/to status and outcome.",
	Buckets: latencyBuckets,
}, []string{"from_status", "to_status", labelOutcome}) // outcome: applied, skipped_lattice, not_found, error

// ---------------------------------------------------------------------------
// events publisher — Kafka send latency
// ---------------------------------------------------------------------------

// EventsPublishDuration measures the latency of one Publisher.Publish or
// PublishBulk call. Currently the path is dark — when the in-memory broker
// applies backpressure (a stalled consumer makes Send take 2s), nothing
// surfaces in metrics. kind ∈ {single, bulk}.
var EventsPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_events_publish_duration_seconds",
	Help:    "Duration of one Publisher.Publish/PublishBulk call.",
	Buckets: latencyBuckets,
}, []string{"kind", labelOutcome}) // outcome: success, error

// ObserveStatusClass returns the bucket label ("2xx", "3xx", "4xx", "5xx",
// "transport_error") for a given HTTP status code. Used by HTTP-latency
// histograms to keep label cardinality bounded.
func ObserveStatusClass(statusCode int) string {
	switch {
	case statusCode == 0:
		return "transport_error"
	case statusCode >= 200 && statusCode < 300:
		return "2xx"
	case statusCode >= 300 && statusCode < 400:
		return "3xx"
	case statusCode >= 400 && statusCode < 500:
		return "4xx"
	case statusCode >= 500 && statusCode < 600:
		return "5xx"
	default:
		return "other"
	}
}
