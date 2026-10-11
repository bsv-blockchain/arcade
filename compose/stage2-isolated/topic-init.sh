#!/usr/bin/env bash
# Isolated Stage-2 topic bootstrap. Protocol topic names match Arcade.
# The broker, volume, and consumer group are not the production broker.
set -euo pipefail

RPK=(rpk -X brokers=votari-stage2-kafka:9092 -X admin.hosts=votari-stage2-kafka:9644)

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

echo "topic-init: isolated arcade topics present"
