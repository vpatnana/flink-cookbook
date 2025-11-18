#!/bin/bash
# Script to run the data generator

set -e

TOPIC="${TOPIC:-node-metrics}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:19092}"
NODES="${NODES:-5}"
RATE="${RATE:-2.0}"

echo "Starting data generator..."
echo "  Topic: $TOPIC"
echo "  Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "  Nodes: $NODES"
echo "  Rate: $RATE msg/sec/node"
echo ""

# Check if running inside container or locally
if [ -f /.dockerenv ] || [ -n "$FLINK_CONTAINER" ]; then
    # Running in container
    python3 /opt/flink/usrlib/datagen.py \
      --topic "$TOPIC" \
      --bootstrap-servers "$BOOTSTRAP_SERVERS" \
      --nodes "$NODES" \
      --rate "$RATE" \
      "$@"
else
    # Running locally - need kafka-python installed
    if ! command -v python3 &> /dev/null; then
        echo "Error: python3 not found"
        exit 1
    fi
    
    # Try to use local Python with kafka-python
    python3 src/datagen.py \
      --topic "$TOPIC" \
      --bootstrap-servers "$BOOTSTRAP_SERVERS" \
      --nodes "$NODES" \
      --rate "$RATE" \
      "$@"
fi

