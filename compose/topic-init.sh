#!/usr/bin/env bash
# One-shot Kafka topic bootstrap for the arcade compose stack — compose
# analogue of bsva-infra-flux/apps/base/arcade-v2/_base/topics.yaml.
# Runs in the redpanda image (bash + rpk), gated on redpanda's healthcheck;
# every arcade service gates on this completing successfully.
#
# Idempotent: safe across `podman-compose up` re-runs and container restarts.
#
# Partition counts are load-bearing:
#   - arcade.propagation MUST have exactly 1 partition. arcade enforces this
#     fail-closed at startup in EVERY mode (kafka.CheckExactPartitions);
#     parent/child tx ordering relies on total order at the topic level.
#   - block_processed(8) / tx_status(16) mirror production consumer fan-out.
# See docs/production-kafka.md.
set -euo pipefail

RPK=(rpk -X brokers=redpanda:9092 -X admin.hosts=redpanda:9644)

# merkle-service (all-in-one) provisions its own topics (subtree, block,
# stumps, ...) via client auto-creation; keep broker-side auto-create on for
# it (mirrors redpanda.WithAutoCreateTopics() in tests/e2e/harness). The
# arcade.* topics are still created explicitly below — auto-create would give
# them the wrong partition counts.
"${RPK[@]}" cluster config set auto_create_topics_enabled true

declare -A TOPICS=(
  [arcade.block_processed]=8
  [arcade.block_processed.dlq]=8
  [arcade.propagation]=1
  [arcade.propagation.dlq]=1
  [arcade.tx_status]=16
  [arcade.tx_status.dlq]=16
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

count="$("${RPK[@]}" topic list | awk '$1 == "arcade.propagation" { print $2 }')"
if [[ "$count" != "1" ]]; then
  echo "topic-init: FATAL: arcade.propagation has partitions=$count, expected exactly 1" >&2
  echo "topic-init: delete the topic (rpk topic delete arcade.propagation) or wipe the redpanda-data volume" >&2
  exit 1
fi

echo "topic-init: all arcade topics present; arcade.propagation partition invariant OK"
