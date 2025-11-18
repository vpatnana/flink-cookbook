#!/bin/bash
# Script to submit the Flink SARIMAX job to the cluster

set -e

# Default arguments
USE_DATAGEN="${USE_DATAGEN:-true}"  # Use datagen by default (simpler!)
TOPIC="${TOPIC:-node-metrics}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-redpanda-1:9092}"
PARALLELISM="${PARALLELISM:-2}"
MAX_HISTORY="${MAX_HISTORY:-1440}"
MIN_HISTORY="${MIN_HISTORY:-288}"
DATAGEN_NODES="${DATAGEN_NODES:-5}"
DATAGEN_RATE="${DATAGEN_RATE:-2.0}"

echo "Submitting Flink SARIMAX job..."
echo "  Parallelism: $PARALLELISM"
echo ""

# Build command arguments
CMD_ARGS=(
  --parallelism "$PARALLELISM"
  --max-history "$MAX_HISTORY"
  --min-history "$MIN_HISTORY"
  --emit-every-n 5
  --order "1,1,1"
  --seasonal-order "0,1,1,288"
  --forecast-steps 1
  --alert-z-threshold 3.0
  --alert-pct-threshold 50.0
  --alert-min-baseline 1.0
)

if [ "$USE_DATAGEN" = "true" ]; then
  echo "  Using built-in datagen (no Kafka required)"
  echo "  Datagen nodes: $DATAGEN_NODES"
  echo "  Datagen rate: $DATAGEN_RATE msg/sec/node"
  CMD_ARGS+=(--use-datagen --datagen-nodes "$DATAGEN_NODES" --datagen-rate "$DATAGEN_RATE")
else
  echo "  Using Kafka source"
  echo "  Topic: $TOPIC"
  echo "  Bootstrap Servers: $BOOTSTRAP_SERVERS"
  CMD_ARGS+=(--topic "$TOPIC" --bootstrap-servers "$BOOTSTRAP_SERVERS")
fi

# Submit the job
docker exec -it jobmanager python3 /opt/flink/usrlib/flinkarima.py "${CMD_ARGS[@]}" "$@"

