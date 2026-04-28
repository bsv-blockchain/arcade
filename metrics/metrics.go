// Package metrics defines the Prometheus metrics surface arcade exposes for
// scrape via the /metrics endpoint on the health server.
//
// Conventions
//
//   - Every metric is prefixed `arcade_` so a multi-tenant Prometheus can
//     filter on it cleanly.
//   - Counters end in `_total`. Histograms end in `_seconds` (latency) or
//     `_bytes` (sizes). Gauges end in a noun (e.g. `_depth`, `_count`).
//   - Labels are kept low-cardinality. Endpoint URLs are labelled because the
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
//   - tx_validator: pipeline depth, flush latency/size, parse fails, accept
//     vs reject vs duplicate counts.
//   - propagation: batch size, broadcast latency per outcome, chunk count,
//     pending-retry depth, reaper lease and tick outcomes, inline retries,
//     merkle registration latency.
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
// tx_validator
// ---------------------------------------------------------------------------

// TxValidatorPendingDepth is the size of the pending-validation slice. Sustained
// growth means handleMessage is queueing faster than flushValidations can
// process — likely time to increase tx_validator.parallelism or partition count.
var TxValidatorPendingDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_tx_validator_pending_depth",
	Help: "Number of raw tx messages buffered awaiting flush.",
})

// TxValidatorPublishCarryDepth is the size of the publish-carry slice. Non-zero
// means the propagation Kafka topic is rejecting publishes — investigate Kafka
// health.
var TxValidatorPublishCarryDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_tx_validator_publish_carry_depth",
	Help: "Number of validated propagation messages awaiting Kafka publish retry.",
})

// TxValidatorFlushDuration measures end-to-end wall time of one flush window
// (parse + dedup + validate + persist + publish).
var TxValidatorFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_tx_validator_flush_duration_seconds",
	Help:    "Wall time of one tx_validator flush window, by outcome.",
	Buckets: latencyBuckets,
}, []string{"outcome"}) // success, publish_failed

// TxValidatorFlushSize captures how many txs each flush processed. Tracking the
// distribution surfaces whether parallelism is being applied at all (a stuck
// at size=1 means we're effectively serial).
var TxValidatorFlushSize = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_tx_validator_flush_size",
	Help:    "Number of txs processed per flush window.",
	Buckets: sizeBuckets,
})

// TxValidatorOutcomeTotal counts per-tx outcomes from the validation phase.
var TxValidatorOutcomeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_tx_validator_outcome_total",
	Help: "Per-tx validation outcome counts.",
}, []string{"outcome"}) // accepted, rejected, duplicate, parse_fail, store_error

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
}, []string{"outcome"}) // accepted, rejected, retryable, no_verdict

// PropagationChunkTotal counts how many chunk broadcasts were issued. Combined
// with PropagationBatchSize this surfaces whether teranode_max_batch_size is
// well-tuned.
var PropagationChunkTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_chunk_total",
	Help: "Number of chunk broadcasts issued, by fallback decision.",
}, []string{"fallback"}) // none, per_tx_after_all_rejected

// PropagationInlineRetryTotal counts inline retry attempts (Fix #7) — how
// often a transient broadcast failure was caught at the validator step before
// going to durable PENDING_RETRY.
var PropagationInlineRetryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_inline_retry_total",
	Help: "Inline retry attempts on broadcastSingleToEndpoints.",
}, []string{"outcome"}) // recovered, exhausted

// PropagationMerkleRegisterDuration measures the merkle-service batch
// registration round-trip. Slow merkle calls are a common bottleneck.
var PropagationMerkleRegisterDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_propagation_merkle_register_duration_seconds",
	Help:    "Duration of merkle-service RegisterBatch calls.",
	Buckets: latencyBuckets,
})

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
}, []string{"outcome"}) // ran, skipped_no_leader, lease_error

// PropagationReaperReadyDepth is the count of PENDING_RETRY rows that the last
// reaper tick observed as ready. Sustained high values indicate a struggling
// downstream (datahubs flapping, merkle slow).
var PropagationReaperReadyDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_reaper_ready_depth",
	Help: "Number of PENDING_RETRY rows ready at the last reaper tick.",
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
}, []string{"outcome"}) // success, no_stumps, fetch_failed, validation_failed, store_failed

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

// KafkaProduceErrors counts producer failures by topic.
var KafkaProduceErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_kafka_produce_errors_total",
	Help: "Kafka producer error count, by topic.",
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
}, []string{"outcome"}) // registered, duplicate, invalid, no_url

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
