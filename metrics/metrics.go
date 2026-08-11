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
// dispatchers are currently holding in their pendingMsgs accumulators
// awaiting the next flush, summed across this pod's partition
// dispatchers (#295: one dispatcher per assigned partition claim; each
// delta-adds its own contribution and removes it on claim teardown).
// Sustained growth indicates downstream (teranode broadcast or
// merkle-service register) is not keeping up with ingest.
var PropagationPendingDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_pending_depth",
	Help: "Propagation messages buffered awaiting flush, summed across this pod's partition dispatchers.",
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

// PropagationClaimRevokedBatchesTotal counts processBatch pipelines aborted
// because their Kafka claim context was already canceled — the broker revoked
// the partition claim mid-batch (consumer-group rebalance) or the service is
// shutting down. Aborting is the durable path: nothing in the batch has been
// marked on the revoked claim's offset tracker, so every tx replays under the
// next claim. An occasional tick during a rolling restart is normal; a
// sustained rate means macro-batch cycles are overrunning the consumer-group
// session timeout and the group is thrashing.
var PropagationClaimRevokedBatchesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_propagation_claim_revoked_batches_total",
	Help: "Propagation batches aborted mid-pipeline because the Kafka claim was revoked; their txs stay uncommitted for replay.",
})

// PropagationInflightDepth gauges the dispatchers' full in-flight census:
// every tx admitted from Kafka that has not yet reached a terminal verdict —
// pending flush, mid-broadcast, held behind an in-flight parent, or parked in
// a delayed requeue — summed across this pod's partition dispatchers
// (#295 delta accounting, same as PropagationPendingDepth). Unlike
// PropagationPendingDepth, which admission backpressure caps at
// max_pending per dispatcher, this gauge is uncapped, so it is the one
// that shows a real backlog growing. Each in-flight entry pins its Kafka
// offset below its partition's commit watermark, so this depth also
// approximates the pod's uncommitted-offset backlog.
var PropagationInflightDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_propagation_inflight_depth",
	Help: "Transactions in the in-flight set (admitted but not yet terminal), summed across this pod's partition dispatchers; each pins an uncommitted Kafka offset.",
})

// PropagationConflictAttributionTotal counts conflict-family ("alien") Teranode
// failure lines by whether arcade could resolve them back to a submitted tx.
//
// Teranode renders the conflict family (UTXO_SPENT / TX_CONFLICTING /
// TX_INVALID_DOUBLE_SPEND, HTTP 409) as the deepest public cause, which names
// the SPENT OUTPOINT and the competing spender — never the submitted txid — so
// the line keys under a hash that is not in the batch. arcade holds the
// submitted raw transactions, so it can usually match "<outpoint>:<vout>"
// against a submitted tx's inputs and attribute the verdict (result=attributed
// → terminal REJECTED). result=unattributable means the outpoint matched no
// submitted input, or matched more than one (an intra-batch double spend), so
// the verdict is deliberately NOT guessed: implicit accepts stay withheld and
// the affected txs fall to the bounded requeue.
//
// A sustained unattributable rate is the signal that Teranode changed the
// conflict-line format (or is reporting on outpoints arcade never submitted) —
// exactly the shape that wedged the dev-ovh-1 propagation consumer on
// 2026-08-11 at 337,247 frozen messages of Kafka lag.
var PropagationConflictAttributionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_conflict_attribution_total",
	Help: "Conflict-family Teranode failure lines by whether the spent outpoint resolved to a submitted transaction.",
}, []string{"result"}) // attributed, unattributable

// PropagationMissingParentTotal counts Teranode missing-parent responses
// (TX_MISSING_PARENT / TX_NOT_FOUND) by how arcade resolved them. With a
// multi-partition arcade.propagation topic (#295) a child submitted in a
// later request than its parent can reach Teranode first; that condition is
// retryable, never a verdict on its own (#254).
//
// outcome="requeued": the parent wasn't terminally REJECTED in arcade's own
// store, so the child went back through the bounded requeue (and, on budget
// exhaustion, PENDING_RETRY + reaper). A short burst is expected during a
// partition-count change (key remap window); a sustained rate at steady
// state means family keying is not co-locating same-submission chains —
// check the intake path. outcome="rejected_ancestor": arcade's store showed
// a parent terminally REJECTED, so the child inherited REJECTED via the
// standard cascade reason — the cross-partition analog of the
// dispatcher's same-partition cascade.
var PropagationMissingParentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_propagation_missing_parent_total",
	Help: "Teranode missing-parent responses by resolution (requeued vs rejected via a REJECTED ancestor in arcade's store).",
}, []string{"outcome"}) // requeued, rejected_ancestor

// PropagationRequeueExhaustedTotal counts transactions parked at PENDING_RETRY
// because they burned their whole in-memory requeue budget
// (propagation.retry_max_attempts) without ever producing a network verdict.
//
// This is the poison-batch escape hatch. Before it existed a batch that
// permanently failed with no attributable per-tx verdict requeued forever:
// every tx stayed in the dispatcher's inFlight map, so its Kafka offset kept
// pinning LowestUnfinished() and the single-partition arcade.propagation topic
// could not commit. Observed live on dev-ovh-1 (2026-08-11, arcade v0.11.5):
// 337,247 messages of lag frozen for minutes, 4,679 txs in-flight, ~2,410
// UTXO_SPENT 409s in two minutes across 43 batches of which 42 requeued, while
// the leader pod burned 4.2 cores achieving nothing.
//
// Any non-zero rate here is worth an alert: it means transactions are getting
// no verdict from any peer. They are NOT lost — the row keeps its raw bytes and
// the reaper rebroadcasts PENDING_RETRY rows — but the fast path has given up.
var PropagationRequeueExhaustedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_propagation_requeue_exhausted_total",
	Help: "Transactions parked at PENDING_RETRY after exhausting the in-memory requeue budget with no network verdict.",
})

// APITxsSubmittedTotal counts individual transactions submitted through the
// API, by route and dedup result. Unlike the HTTP request histogram (one
// sample per request), this counts PER TRANSACTION — the /txs batch route
// submits many txs in one request, so request counts undercount submissions
// by the batch factor. result=new is the "unique txids submitted" series
// dashboards should use; duplicate = idempotent resubmit of a known txid;
// retry_rejected = resubmit of a previously rejected txid (re-enters the
// broadcast pipeline).
var APITxsSubmittedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_api_txs_submitted_total",
	Help: "Transactions submitted via the API by route and dedup result (new|duplicate|retry_rejected). Counted per transaction, not per request.",
}, []string{"route", "result"})

// APIFinalityRejectionsTotal counts transactions rejected at intake by the
// nLockTime/BIP113 finality gate, by route. These are terminal REJECTED
// verdicts with an actionable reason; the submitter is expected to resubmit
// once the lock expires (issue #245).
var APIFinalityRejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_api_finality_rejections_total",
	Help: "Transactions rejected at intake as non-final (nLockTime/BIP113), by route.",
}, []string{"route"})

// APIFinalityPrecheckUnavailableTotal counts finality pre-checks skipped
// because chain state (tip/headers via chaintracks) was unavailable. The
// gate fails open — teranode remains the authority — so a sustained rate
// here means non-final txs are once again bouncing off the network with
// generic errors instead of being caught at intake.
var APIFinalityPrecheckUnavailableTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_api_finality_precheck_unavailable_total",
	Help: "Finality pre-checks skipped (failed open) because chain state was unavailable.",
})

// StuckTransientTxs counts transactions sitting in a transient status
// (RECEIVED, ACCEPTED_BY_NETWORK) for longer than the stale-transient
// threshold (1h), within the reaper's 24h scan lookback. Unlike
// PropagationReaperReadyDepth it is uncapped, split per status, and covers
// ACCEPTED_BY_NETWORK (which the rebroadcast path deliberately ignores).
// Computed on the leader's reaper tick; non-leaders report 0, so aggregate
// with max() across pods. A sustained non-zero value means the SEEN
// state-transfer for those txs never arrived — the primary alerting signal
// for a stalled subtree-callback pipeline.
var StuckTransientTxs = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arcade_stuck_transient_txs",
	Help: "Transactions stuck in a transient status for over 1h (24h lookback), by status. Leader-computed; aggregate with max().",
}, []string{"status"})

// OldestTransientTxAge reports the age in seconds of the oldest transaction
// currently stuck in each transient status (0 when none). Same computation
// cadence and aggregation semantics as StuckTransientTxs.
var OldestTransientTxAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arcade_oldest_transient_tx_age_seconds",
	Help: "Age of the oldest transaction stuck in each transient status (seconds; 0 when none). Leader-computed; aggregate with max().",
}, []string{"status"})

// StuckTransientTxsByCallback breaks the StuckTransientTxs census down by the
// callback host of the submission(s) that registered each stuck tx, so an
// operator can see WHICH client's transactions are stuck (e.g. one overlay
// service dominating the ACCEPTED_BY_NETWORK backlog) rather than just the
// aggregate. callback_host is the hostname of the submission's CallbackURL, or
// "none" for txs submitted without a callback. Leader-computed and fully reset
// each tick (so a host that drains stops reporting); aggregate with max() and
// sum by (callback_host). Attribution is capped per tick
// (stuckAttributionCap) to bound the per-tick submission lookups — the
// uncapped totals stay on StuckTransientTxs.
var StuckTransientTxsByCallback = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arcade_stuck_transient_txs_by_callback",
	Help: "Stuck transient txs (see arcade_stuck_transient_txs) attributed to the submitting client's callback host. Leader-computed, reset each tick; aggregate with max().",
}, []string{"status", "callback_host"})

// ---------------------------------------------------------------------------
// bump_builder
// ---------------------------------------------------------------------------

// BumpBuilderBuildDuration measures end-to-end wall time from BLOCK_PROCESSED
// receipt to terminal disposition — including the grace-window wait when one
// was needed (completeness-first ordering skips the wait entirely when
// merkle's expected-STUMP set is already satisfied on arrival).
//
// Every terminal disposition of handleMessage lands here exactly once. Two
// outcomes mean a BUMP was built and persisted: `finalized_complete_no_grace`
// (expected-STUMP set complete on the first read — no grace wait) and
// `grace_waited` (built via the grace-window path; also stamped when the
// window is configured to 0). Three more are benign non-failures:
// `short_circuited` (a BUMP already existed, so a redelivery was skipped),
// `no_stumps` (block contains no tracked txs) and `context_canceled`
// (shutdown). The rest are failures:
//
//	parse_failed, deferred_incomplete, fetch_failed, no_subtrees,
//	build_failed, validation_failed, store_failed
//
// Alert by matching the failure outcomes positively — outcome=~"parse_failed|
// deferred_incomplete|fetch_failed|no_subtrees|build_failed|validation_failed|
// store_failed" (the list above). Do NOT negate the benign labels (e.g.
// outcome!~"finalized_complete_no_grace|grace_waited"): a negation also
// matches short_circuited/no_stumps/context_canceled, and it silently starts
// matching any new benign outcome someone adds. Keep this list and the rule
// in README.md in sync when adding an outcome.
var BumpBuilderBuildDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_bump_builder_build_duration_seconds",
	Help:    "Time to build and persist one compound BUMP, by outcome.",
	Buckets: latencyBuckets,
}, []string{labelOutcome})

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

// BumpBuilderBlockDataSourceTotal counts which source supplied the subtree
// hashes + coinbase BUMP + header merkle root used to build a compound BUMP:
// "callback" when merkle-service enriched BLOCK_PROCESSED (datahub-independent
// path, issue #195) or "datahub" when bump-builder fell back to fetching the
// block. A healthy fully-rolled-out deployment trends to source="callback"; a
// sustained source="datahub" rate signals an un-upgraded merkle-service or
// blocks whose enrichment fields couldn't be built/validated upstream.
var BumpBuilderBlockDataSourceTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_bump_builder_block_data_source_total",
	Help: "Block data source used for compound BUMP construction, by source (callback|datahub).",
}, []string{"source"})

// BumpBuilderGraceWaitTotal counts BLOCK_PROCESSED handlers that actually
// waited the grace window before re-reading STUMPs — i.e. completeness could
// not be verified from the first STUMP read (expected set unsatisfied, or
// absent with STUMPs present). Handlers whose expected-STUMP set was already
// complete on arrival skip the wait and never increment this, so the ratio of
// this counter to blocks_processed_total tracks how often the window still
// earns its latency cost.
var BumpBuilderGraceWaitTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_grace_window_waits_total",
	Help: "BLOCK_PROCESSED handlers that waited the grace window before re-reading STUMPs.",
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

// BumpBuilderIncompleteStumpsTotal counts BLOCK_PROCESSED messages whose
// merkle-supplied expected-STUMP set (CallbackMessage.ExpectedSubtreeIndices)
// was NOT fully satisfied by the STUMPs arcade had stored once the grace window
// elapsed. Such a block is deliberately left un-finalized (processed_at stays
// NULL) so the watchdog re-drives it via merkle's /reprocess, which re-emits the
// missing STUMPs and BLOCK_PROCESSED. On BumpBuilderBuildDuration these land
// as outcome="deferred_incomplete" (renamed from "incomplete_stumps" when
// completeness-first grace handling landed). This is the metric that makes the
// previously-silent partial-STUMP drop visible: unlike the all-missing case
// (empty_stump_blocks_total), a block missing only SOME of its STUMPs used to
// build a valid-looking BUMP and lose the absent subtree's txs with no signal.
var BumpBuilderIncompleteStumpsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_incomplete_stumps_total",
	Help: "BLOCK_PROCESSED messages left un-finalized because expected STUMPs were still missing after the grace window.",
})

// BumpBuilderShortCircuitTotal counts BLOCK_PROCESSED messages handled by the
// short-circuit path — the BUMP already exists in the store and this is a
// redelivery (typically from /reprocess re-emitting BLOCK_PROCESSED). Tracks
// how much work the short-circuit saves vs. re-fetching datahub.
var BumpBuilderShortCircuitTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_bump_builder_short_circuit_total",
	Help: "BLOCK_PROCESSED messages skipped because a compound BUMP already exists for the block.",
})

// BumpBuilderAnchorGuardDeniedTotal counts blocks the anchor guard refused
// to mark MINED because chaintracks' active chain has a DIFFERENT block at
// their height (issue #279 — same-height competition loser). path
// distinguishes the fresh-build path from the short-circuit redelivery
// path. Each denial routes the block into the anchor reconciler's queue.
var BumpBuilderAnchorGuardDeniedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_bump_builder_anchor_guard_denied_total",
	Help: "Blocks refused MINED anchoring because the active chain has a different block at their height.",
}, []string{"path"})

// ---------------------------------------------------------------------------
// anchor reconciler (bump-builder-hosted — reorg tx re-anchoring, issue #279)

// ReconcilerBlocksTotal counts orphaned blocks processed by the anchor
// reconciler, by terminal outcome: reanchored (all affected txs moved to a
// canonical block), reverted (all reverted to SEEN_ON_NETWORK), mixed,
// empty (nothing anchored to the orphan anymore), resurrected (the block is
// active again — stale orphan mark), deferred (waiting on the canonical
// block's BUMP), parked (canonical BUMP unavailable at the defer cap — txs
// left MINED, NOT reverted, awaiting a later canonical BUMP; issue #282),
// error.
var ReconcilerBlocksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_reconciler_blocks_total",
	Help: "Orphaned blocks processed by the anchor reconciler, by outcome.",
}, []string{"outcome"})

// ReconcilerTxsReanchoredTotal counts transactions the reconciler moved from
// an orphaned block to the active-chain block containing them.
var ReconcilerTxsReanchoredTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_reconciler_txs_reanchored_total",
	Help: "Transactions re-anchored from an orphaned block to a canonical block.",
})

// ReconcilerTxsRevertedTotal counts transactions the reconciler reverted to
// SEEN_ON_NETWORK because no canonical block contains them.
var ReconcilerTxsRevertedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_reconciler_txs_reverted_total",
	Help: "Transactions reverted to SEEN_ON_NETWORK because their only block was orphaned.",
})

// ReconcilerTxsParkedTotal counts transactions left MINED against an
// orphaned block because its canonical BUMP was unavailable at the defer cap
// (issue #282). Parked txs are NOT reverted to SEEN_ON_NETWORK — a later
// stored/rebuilt canonical BUMP re-anchors them through the normal mine
// path. A steady climb here means canonical BUMPs are not arriving (see the
// merkle-service /reprocess dependency, bsv-blockchain/merkle-service#208).
var ReconcilerTxsParkedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_reconciler_txs_parked_total",
	Help: "Transactions left MINED against an orphan because no canonical BUMP was available (not reverted).",
})

// ReconcilerBlockDuration observes wall time per reconciled block.
var ReconcilerBlockDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arcade_reconciler_block_duration_seconds",
	Help:    "Wall time spent reconciling one orphaned block's transactions.",
	Buckets: prometheus.ExponentialBuckets(0.01, 4, 8), // 10ms .. ~11m
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
}, []string{labelOutcome}) // success, err_auth, err_4xx, err_5xx, err_network

// WatchdogBackoffDepth is the size of the in-memory attempts map.
// Sustained growth implies blocks are persistently failing to recover —
// inspect logs for the 4xx/5xx outcome breakdown.
var WatchdogBackoffDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_watchdog_backoff_depth",
	Help: "Number of blocks currently held in the watchdog's in-memory backoff map.",
})

// WatchdogParkedTotal counts blocks the watchdog has parked (stopped
// re-driving) because they hit the reprocess cap. A non-zero value means
// blocks are permanently un-finalizable — inspect bump-builder for the
// underlying reason (e.g. incomplete STUMPs / merkle-root mismatch).
var WatchdogParkedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_watchdog_parked_total",
	Help: "Blocks parked by the watchdog (reprocessing stopped), by reason.",
}, []string{"reason"}) // max_attempts, max_age

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
//   - "slow_client": the client's send buffer was full. The label name is
//     historical and is KEPT for dashboard/alert compatibility, but it is NOT
//     a diagnosis — the buffer fills whenever production outruns drain, and
//     the fan-out goroutine itself has been the producer-side bottleneck.
//     Read it together with SSEFanoutDuration (is fan-out itself slow?) and
//     SSEConnectedClients before attributing fault to a consumer.
//   - "client_gone": the client was unregistering concurrently and its context
//     had already been canceled by the time fan-out reached it.
//
// A non-zero "client_gone" rate is normal under churn.
var APISSEDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_api_sse_dropped_total",
	Help: "SSE fan-out events dropped without delivery, by reason.",
}, []string{"reason"}) // slow_client, client_gone

// SSEFanoutDuration measures how long the manager's single fan-out goroutine
// spends on one event, split by kind:
//   - "single": one per-tx status event.
//   - "bulk": one bulk event (a block's MINED unfan), covering the batched
//     token resolution AND every per-tx delivery it expands to.
//
// This is THE signal for the fan-out's own throughput ceiling: the goroutine
// is serial, so sustained 1/p50 below the publish rate means events are
// queueing upstream and will eventually be dropped — regardless of how fast
// any consumer drains. It is the metric that was missing when 1.6M drops were
// attributed to a "slow SSE client" that was idle.
var SSEFanoutDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_sse_fanout_duration_seconds",
	Help:    "Time the SSE fan-out goroutine spent dispatching one event, by kind.",
	Buckets: latencyBuckets,
}, []string{"kind"}) // single, bulk

// SSEConnectedClients tracks how many /events connections are registered on
// this pod. Fan-out cost per event scales with this, and it disambiguates
// "one chronically slow consumer" from "many consumers".
var SSEConnectedClients = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_sse_connected_clients",
	Help: "SSE /events clients currently registered on this pod.",
})

// SSETokenCacheTotal counts txid→token-set cache outcomes on the fan-out
// path ("hit" / "miss"). Each miss is one store round-trip on the serial
// fan-out goroutine, so the miss rate multiplied by SSETokenLookupDuration is
// the store's contribution to fan-out latency. Bulk events bypass the cache
// by design and are not counted here.
var SSETokenCacheTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_sse_token_cache_total",
	Help: "SSE fan-out txid→token-set cache lookups, by result.",
}, []string{"result"}) // hit, miss

// SSETokenCacheEntries reports the cache's resident entry count. Sitting at
// the entry cap means the working set is larger than the cache and hits are
// being lost to eviction; sitting well below it while the byte budget binds
// means unusually large token strings.
var SSETokenCacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_sse_token_cache_entries",
	Help: "Entries resident in the SSE fan-out txid→token-set cache.",
})

// SSETokenLookupDuration measures one Store.TokensForTxIDs call from the
// fan-out path, by kind ("single" for a cache miss on one txid, "batch" for a
// bulk event's chunked resolution). This is the per-event probe latency that
// had to be measured out-of-band before.
var SSETokenLookupDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "arcade_sse_token_lookup_duration_seconds",
	Help:    "Store.TokensForTxIDs latency from the SSE fan-out path, by kind.",
	Buckets: latencyBuckets,
}, []string{"kind"}) // single, batch

// SSEMidstreamCatchupsTotal counts store-backed catchup rounds run on a LIVE
// /events connection after fan-out overflowed the client's send channel
// (reason="slow_client" drops above). One drop episode can take several
// rounds (each bounded by the per-round frame cap), so this counts rounds,
// not episodes. A sustained rate means a consumer chronically drains slower
// than the publish rate and is being served store-paced replay instead of
// live frames.
var SSEMidstreamCatchupsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_sse_midstream_catchups_total",
	Help: "Mid-stream SSE catchup rounds run after a client's send buffer overflowed.",
})

// SSEMidstreamCatchupFramesTotal counts frames replayed from the store by
// mid-stream catchup rounds. Includes duplicates of frames the client already
// received live (the replay window is deliberately rewound past the drop
// boundary; clients dedupe by txid+status).
var SSEMidstreamCatchupFramesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "arcade_sse_midstream_catchup_frames_total",
	Help: "Frames replayed from the store by mid-stream SSE catchup rounds.",
})

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

// MinedPushWithoutMerklePathTotal counts MINED (or IMMUTABLE) statuses pushed
// on a channel ("webhook" | "sse") whose body/frame went out without a
// merklePath despite enrichment being attempted. Because the compound BUMP is
// persisted before the MINED event is published, a nonzero value points to a
// cache-eviction/reorg race or a missing/unparseable BUMP — the proof is still
// recoverable via GET /tx/:txid, but a sustained rate warrants investigation.
var MinedPushWithoutMerklePathTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_mined_push_without_merkle_path_total",
	Help: "MINED/IMMUTABLE statuses pushed without a merklePath after enrichment, by channel.",
}, []string{"channel"})

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

// TeranodeEndpointRefreshRejectedTotal counts registry rows rejected by URL
// validation at refresh time (app.endpointSource). Nonzero means the shared
// DatahubEndpoint registry contains rows that predate discovery-time
// validation (or a peer's DNS broke after registration) — candidates for
// pruning.
var TeranodeEndpointRefreshRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "arcade_teranode_endpoint_refresh_rejected_total",
	Help: "Registry datahub endpoints rejected at refresh time by URL validation.",
}, []string{labelOutcome}) // blocked, invalid

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
}, []string{labelOutcome}) // registered, invalid, blocked, no_url, no_store, error

// P2PPeerBestHeight reports the best block height each datahub peer
// advertises in its node_status messages. Compared against
// arcade_chain_tip_height (arcade's own processed tip) this is the
// height-lag signal that was previously invisible: a reachable-but-stalled
// peer showed healthy on every existing metric while its chain view froze
// 50+ blocks behind (issue #254). Labelled by the peer's base_url — the
// small, stable datahub set — and never by libp2p peer id, whose
// restart-churn would grow the series set unboundedly; peers announcing no
// base_url are skipped.
var P2PPeerBestHeight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arcade_p2p_peer_best_height",
	Help: "Best block height advertised by a datahub peer via p2p node_status, by base_url.",
}, []string{"base_url"})

// ChainTipHeight reports arcade's own view of the active chain tip — the
// highest block_processing row marked active. Refreshed by the api-server's
// /health handler (which also returns it as blockHeight); alert on this
// flatlining, or on arcade_p2p_peer_best_height pulling ahead of it, to
// catch stale-chain-view conditions before they surface as non-final
// rejections (issue #254).
var ChainTipHeight = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "arcade_chain_tip_height",
	Help: "Arcade's active processed chain-tip height (max active block_processing row).",
})

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
