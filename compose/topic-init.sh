#!/usr/bin/env bash
# One-shot Kafka topic bootstrap for the BSV Broadcaster compose stack — compose
# analogue of bsva-infra-flux/apps/base/arcade-v2/_base/topics.yaml.
# Runs in the redpanda image (bash + rpk), gated on redpanda's healthcheck;
# every BSV Broadcaster service gates on this completing successfully.
#
# Idempotent: safe across `podman-compose up` re-runs and container restarts.
#
# Partition counts are load-bearing:
#   - bsv-broadcaster.propagation needs at least PROPAGATION_PARTITIONS partitions
#     (default 1). BSV Broadcaster enforces the same minimum fail-closed at startup
#     (kafka.CheckMinPartitions). Any count preserves ordering: producers key
#     messages by dependency family, so parent/child txs of one submission
#     share a partition (#295).
#   - block_processed(8) / tx_status(16) mirror production consumer fan-out.
# See docs/production-kafka.md.
set -euo pipefail

RPK=(rpk -X brokers=redpanda:9092 -X admin.hosts=redpanda:9644)

PROPAGATION_PARTITIONS="${PROPAGATION_PARTITIONS:-1}"

# merkle-service (all-in-one) provisions its own topics (subtree, block,
# stumps, ...) via client auto-creation; keep broker-side auto-create on for
# it (mirrors redpanda.WithAutoCreateTopics() in tests/e2e/harness). The
# BSV Broadcaster.* topics are still created explicitly below — auto-create would give
# them the wrong partition counts.
"${RPK[@]}" cluster config set auto_create_topics_enabled true

declare -A TOPICS=(
  [bsv-broadcaster.block_processed]=8
  [bsv-broadcaster.block_processed.dlq]=8
  [bsv-broadcaster.propagation]="$PROPAGATION_PARTITIONS"
  [bsv-broadcaster.propagation.dlq]="$PROPAGATION_PARTITIONS"
  [bsv-broadcaster.tx_status]=16
  [bsv-broadcaster.tx_status.dlq]=16
)

for topic in "${!TOPICS[@]}"; do
  partitions="${TOPICS[$topic]}"
  if "${RPK[@]}" topic describe "$topic" >/dev/null 2>&1; then
    echo "topic-init: $topic already exists"
  else
    "${RPK[@]}" topic create "$topic" --partitions "$partitions" --replicas 1
    echo "topic-init: created $topic (partitions=$partitions)"
  fi
done

# Grow an existing propagation topic in place when the target minimum rose
# between runs. Partitions can only be added, never removed — a topic wider
# than the target is fine (BSV Broadcaster's startup check is >=, and family keying
# keeps ordering correct at any width).
count="$("${RPK[@]}" topic list | awk '$1 == "bsv-broadcaster.propagation" { print $2 }')"
if [[ "$count" -lt "$PROPAGATION_PARTITIONS" ]]; then
  "${RPK[@]}" topic add-partitions bsv-broadcaster.propagation --num "$((PROPAGATION_PARTITIONS - count))"
  echo "topic-init: widened bsv-broadcaster.propagation ${count} -> ${PROPAGATION_PARTITIONS} partitions"
fi

echo "topic-init: all BSV Broadcaster topics present; bsv-broadcaster.propagation has >= ${PROPAGATION_PARTITIONS} partition(s)"
