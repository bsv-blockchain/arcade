# Production Kafka topics

Arcade does **not** create Kafka topics itself — it relies on broker-side
auto-creation on first publish, which is typically disabled in production
clusters. One topic also has a hard correctness constraint (`arcade.propagation`
must have **at least `propagation.partitions`**) that arcade enforces at startup
with a fail-closed check. This guide lists every topic an operator needs to
pre-create, with partition, replication, and retention guidance.

This guide assumes a production deployment with `kafka.backend: sarama` pointed
at an external Kafka cluster. Standalone profiles (`kafka.backend: memory`)
don't need any of this — see [`getting-started.md`](getting-started.md).

## Topics arcade uses

| Topic                          | Partitions                | Consumer group(s)                  | Purpose                                                              |
| ------------------------------ | ------------------------- | ---------------------------------- | -------------------------------------------------------------------- |
| `arcade.propagation`           | **≥ `propagation.partitions`** (default 1) | `<consumer_group>-propagation`     | API server → propagation: txs to broadcast to datahubs.              |
| `arcade.block_processed`       | ≥ N (N = bump-builder replicas) | `<consumer_group>-bump-builder` | API server → bump-builder: BLOCK_PROCESSED signals from Merkle Service. |
| `arcade.tx_status`             | ≥ N (N = max concurrent SSE/webhook fan-outs) | ephemeral per-subscriber groups | All services → SSE/webhook: tx status mutations.                     |
| `arcade.transaction`           | 1 (currently unused)      | none                               | Defined in `AllTopics()` but retired with `tx_validator`. Create or skip. |
| `arcade.propagation.dlq`       | 1                         | none (operator-inspected)          | Failed propagation messages after max retries.                       |
| `arcade.block_processed.dlq`   | 1                         | none (operator-inspected)          | Failed BLOCK_PROCESSED messages after max retries.                   |
| `arcade.tx_status.dlq`         | 1                         | none (operator-inspected)          | Failed status-update messages after max retries.                     |
| `arcade.transaction.dlq`       | 1                         | none                               | DLQ for retired topic; safe to skip.                                 |

Consumer-group base name comes from `kafka.consumer_group` in config (default
`arcade`). Service-specific suffixes (`-propagation`, `-bump-builder`) are
applied automatically — operators only configure the base name.

## Partition counts

### `arcade.propagation` — at least `propagation.partitions`

The propagation service runs one dep-aware dispatcher per partition claim.
Each dispatcher owns its own in-memory dependency index, so a parent and its
child must land on the **same** partition for the child to be held behind its
parent.

Intake guarantees that for transactions submitted together: `POST /txs`
computes a partition key per **dependency family** — every tx in the request
connected through in-batch spends shares one key (the lexicographically
smallest member txid, so a shuffled resubmission maps to the same partition).
A whole family therefore lands on one partition, and per-partition order
delivers parents before children.

Chains that span requests have no such guarantee, and that is by design rather
than an oversight: a child can reach a datahub before its parent and draw
`TX_MISSING_PARENT`. That is treated as a **condition, not a verdict** — the tx
rides the bounded requeue, then parks at `PENDING_RETRY` for the reaper's
durable retry queue. The only path to `REJECTED` is arcade's own store showing
an ancestor terminally rejected. Note this means single-transaction `POST /tx`
submissions get no cross-request ordering help: they are keyed by their own
txid.

Arcade enforces the floor at startup with `kafka.CheckMinPartitions`. A missing
topic, or a topic with fewer partitions than `propagation.partitions`, aborts
startup. More partitions than configured is allowed — but note the producer
hashes over the topic's **actual** partition count, so extra partitions carry
real traffic and get their own dispatchers. `propagation.partitions` is a
fail-closed assertion, not a placement control.

Sizing: run at most `propagation.partitions` propagation replicas. Extra
replicas idle as standbys for failover. A single request whose transactions
are all one dependency family lands entirely on one partition, so batches that
are one long chain — or that contain a consolidation tx spending many
in-request outputs — serialize regardless of partition count.

### Widening an existing deployment

Partition counts can be increased in place. Do it **topic first**, so arcade is
never configured for more partitions than the topic has (that combination is a
boot-time hard fail):

```bash
# 1. Widen the topic.
rpk topic add-partitions arcade.propagation --num 3   # 1 -> 4 total

# 2. Raise propagation.partitions to the new count (config or
#    ARCADE_PROPAGATION_PARTITIONS), then roll the propagation pods.

# 3. Scale replicas up to at most the partition count.
kubectl scale deploy/propagation --replicas=4
```

Between steps 1 and 2 the producer already hashes over the wider topic, so a
family submitted before the widen and its continuation submitted after can map
to different partitions. That window is what the missing-parent safety net
exists for. During it, watch:

- `arcade_propagation_missing_parent_total{outcome="requeued"}` — a burst is
  expected; it should return to baseline once the remap window passes.
- `arcade_propagation_parked_depth` — a transient rise is normal; sustained
  growth means chains are not converging.
- `arcade_propagation_outcome_total{outcome="rejected"}` — should **not**
  move. A spike would mean ordering artifacts reached a verdict, which the
  condition-not-verdict rule is designed to make impossible.

For a zero-race alternative, drain the topic and recreate it at the target
width before rolling pods.

### `arcade.block_processed` and `arcade.tx_status` — match consumer scale

These topics fan out to consumer groups, so partition count should be
**≥ the maximum concurrent consumer pod count**. A topic with fewer
partitions than consumers in its group leaves some consumers permanently
idle.

Worked example: if you run 4 bump-builder replicas, create
`arcade.block_processed` with at least 4 partitions. Sizing higher than the
current pod count is fine and gives headroom to scale out without recreating
the topic. The same logic applies to `arcade.tx_status`, where each SSE/
webhook subscriber creates its own ephemeral group — partition count caps
the per-subscriber parallelism but does not block additional subscribers.

### Dead-letter topics — 1 partition is fine

DLQ topics receive failed messages after the in-process consumer exhausts
`kafka.max_retries` (default 5). Arcade never reads from them; operators
inspect manually. One partition is sufficient.

## Replication and durability

Arcade's sync producer uses `RequiredAcks: WaitForAll` — every produce
waits for all in-sync replicas. Under-replicated partitions will stall
writes, so size your replication factor and `min.insync.replicas` for
your cluster's availability target.

Suggested for a typical 3-broker production cluster:

- `--replication-factor 3`
- `min.insync.replicas=2` (tolerates one broker down without stalling writes)

## Retention

Arcade does not enforce or assume any retention policy. Suggested defaults:

- **Hot topics** (`arcade.propagation`, `arcade.block_processed`,
  `arcade.tx_status`) — 24h to 7d. These are operational queues; once
  consumed they have no replay value beyond troubleshooting.
- **DLQ topics** — 14d to 30d so you have a reasonable inspection window
  before failed messages age out.

`cleanup.policy=delete` (the broker default) is correct for all topics;
none of them are log-compacted.

## Creating the topics

### Using `rpk` (Redpanda CLI)

```bash
# Hot path
# --partitions must be >= propagation.partitions (default 1). To go wider
# later, see "Widening an existing deployment" above.
rpk topic create arcade.propagation \
  --partitions 1 \
  --replicas 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000   # 7 days

rpk topic create arcade.block_processed \
  --partitions 4 \
  --replicas 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

rpk topic create arcade.tx_status \
  --partitions 4 \
  --replicas 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000    # 1 day

# DLQs
for t in arcade.propagation.dlq arcade.block_processed.dlq arcade.tx_status.dlq; do
  rpk topic create "$t" \
    --partitions 1 \
    --replicas 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=2592000000   # 30 days
done
```

Adjust `--partitions 4` for `arcade.block_processed` / `arcade.tx_status` to
match your consumer-pod headroom.

### Using `kafka-topics.sh` (Apache Kafka CLI)

```bash
BS=localhost:9092

# Hot path
kafka-topics.sh --bootstrap-server "$BS" --create \
  --topic arcade.propagation \
  --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

kafka-topics.sh --bootstrap-server "$BS" --create \
  --topic arcade.block_processed \
  --partitions 4 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000

kafka-topics.sh --bootstrap-server "$BS" --create \
  --topic arcade.tx_status \
  --partitions 4 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000

# DLQs
for t in arcade.propagation.dlq arcade.block_processed.dlq arcade.tx_status.dlq; do
  kafka-topics.sh --bootstrap-server "$BS" --create \
    --topic "$t" \
    --partitions 1 --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=2592000000
done
```

## Consumer-group naming

Every arcade service derives its consumer group from the single
`kafka.consumer_group` config value, with a service-specific suffix:

- `<group>-propagation` reads `arcade.propagation`
- `<group>-bump-builder` reads `arcade.block_processed`
- `arcade.tx_status` subscribers (SSE manager, webhook service) use unique
  ephemeral group IDs (`arcade-events-<hex>`) so every subscriber sees every
  message.

Set `kafka.consumer_group` to a value unique per environment
(e.g. `arcade-mainnet-prod`, `arcade-testnet`) so groups don't collide if
multiple deployments share a Kafka cluster.

## What arcade checks at startup

- `kafka.CheckMinPartitions(arcade.propagation, propagation.partitions)` — **hard fail** if
  the topic is missing or has fewer partitions than `propagation.partitions`.
- `kafka.CheckPartitions(...)` — soft warning path used when
  `kafka.min_partitions > 1`. Existing topics with fewer partitions cause a
  startup error; missing topics only log a warning ("will be auto-created
  on first publish").

This means a forgotten `arcade.propagation` will fail loudly, but a
forgotten `arcade.block_processed` will silently auto-create with the
broker's default partition count on first publish. If your broker has
`auto.create.topics.enable=false` (recommended for production), forgotten
topics will instead surface as produce errors at first traffic. Either way,
pre-creating every topic in the table above is the only way to be sure
partition counts match your deployment.

## Message size (large transactions)

`TopicPropagation` carries the raw transaction base64-encoded inside a JSON
envelope, so a message is about 4/3 of the transaction size plus the envelope.
With the default 10 MiB `max_tx_size_policy` a valid submit can need ~14 MiB.
Three limits must agree, or large submits fail at produce time with
`failed to submit` (HTTP 500) while `GET /policy` still advertises them as
accepted:

| where | setting | default | arcade default |
|---|---|---|---|
| arcade producer | `kafka.max_message_bytes` (`Producer.MaxMessageBytes`) | 1 MiB (sarama) | 16 MiB |
| broker | Kafka `message.max.bytes` / Redpanda `kafka_batch_max_bytes` | ~1 MiB | 16 MiB (`compose/topic-init.sh`, `deploy/kafka.yaml`) |
| topic | `max.message.bytes` on every `arcade.*` topic | inherits broker | 16 MiB (`compose/topic-init.sh`) |

Production topics live in `bsva-infra-flux` (`topics.yaml`); set
`max.message.bytes` there to the same value.
