#!/usr/bin/env bash
# One-shot Kafka topic bootstrap for the arcade compose stack — compose
# analogue of bsva-infra-flux/apps/base/arcade-v2/_base/topics.yaml.
# Runs in the redpanda image (bash + rpk), gated on redpanda's healthcheck;
# every arcade service gates on this completing successfully.
#
# Idempotent: safe across `podman-compose up` re-runs and container restarts.
#
# Partition counts are load-bearing:
#   - arcade.propagation needs at least PROPAGATION_PARTITIONS partitions
#     (default 1). arcade enforces the same minimum fail-closed at startup
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
# arcade.* topics are still created explicitly below — auto-create would give
# them the wrong partition counts.
"${RPK[@]}" cluster config set auto_create_topics_enabled true

# Message size: a policy-max 10 MiB tx is base64 (x4/3) inside the propagation
# envelope, so the broker default (1 MiB) rejects large submits at produce
# time. Keep in step with kafka.max_message_bytes (arcade-config.yaml) and
# the topic-level max.message.bytes set below.
MAX_MESSAGE_BYTES="${MAX_MESSAGE_BYTES:-16777216}"
"${RPK[@]}" cluster config set kafka_batch_max_bytes "$MAX_MESSAGE_BYTES"

declare -A TOPICS=(
  [arcade.block_processed]=8
  [arcade.block_processed.dlq]=8
  [arcade.propagation]="$PROPAGATION_PARTITIONS"
  [arcade.propagation.dlq]="$PROPAGATION_PARTITIONS"
  [arcade.tx_status]=16
  [arcade.tx_status.dlq]=16
)

for topic in "${!TOPICS[@]}"; do
  partitions="${TOPICS[$topic]}"
  if "${RPK[@]}" topic describe "$topic" >/dev/null 2>&1; then
    echo "topic-init: $topic already exists"
  else
    "${RPK[@]}" topic create "$topic" --partitions "$partitions" --replicas 1 \
      --topic-config "max.message.bytes=$MAX_MESSAGE_BYTES"
    echo "topic-init: created $topic (partitions=$partitions)"
  fi
  # Idempotent: raise the per-topic cap on topics that pre-date this setting.
  "${RPK[@]}" topic alter-config "$topic" --set "max.message.bytes=$MAX_MESSAGE_BYTES"
done

# Grow an existing propagation topic in place when the target minimum rose
# between runs. Partitions can only be added, never removed — a topic wider
# than the target is fine (arcade's startup check is >=, and family keying
# keeps ordering correct at any width).
count="$("${RPK[@]}" topic list | awk '$1 == "arcade.propagation" { print $2 }')"
if [[ "$count" -lt "$PROPAGATION_PARTITIONS" ]]; then
  "${RPK[@]}" topic add-partitions arcade.propagation --num "$((PROPAGATION_PARTITIONS - count))"
  echo "topic-init: widened arcade.propagation ${count} -> ${PROPAGATION_PARTITIONS} partitions"
fi

echo "topic-init: all arcade topics present; arcade.propagation has >= ${PROPAGATION_PARTITIONS} partition(s)"
