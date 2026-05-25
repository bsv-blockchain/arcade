# Production Kafka topics

Arcade does **not** create Kafka topics itself — it relies on broker-side
auto-creation on first publish, which is typically disabled in production
clusters. One topic also has a hard correctness constraint (`arcade.propagation`
must be **exactly 1 partition**) that arcade enforces at startup with a
fail-closed check. This guide lists every topic an operator needs to
pre-create, with partition, replication, and retention guidance.

This guide assumes a production deployment with `kafka.backend: sarama` pointed
at an external Kafka cluster. Standalone profiles (`kafka.backend: memory`)
don't need any of this — see [`getting-started.md`](getting-started.md).

## Topics arcade uses

| Topic                          | Partitions                | Consumer group(s)                  | Purpose                                                              |
| ------------------------------ | ------------------------- | ---------------------------------- | -------------------------------------------------------------------- |
| `arcade.propagation`           | **exactly 1** (mandatory) | `<consumer_group>-propagation`     | API server → propagation: txs to broadcast to datahubs.              |
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

### `arcade.propagation` — exactly 1 (mandatory)

The propagation service uses a dep-aware dispatcher whose single-goroutine
state ownership relies on **total order at the topic level**. Parent/child
txids landing on different partitions would bypass the in-memory dependency
index and reintroduce a "missing inputs" race. Arcade enforces this with a
hard check at startup (`kafka.CheckExactPartitions`); a missing topic or any
partition count other than 1 aborts startup.

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

- `kafka.CheckExactPartitions(arcade.propagation, 1)` — **hard fail** if
  the topic is missing or has any partition count other than 1.
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
