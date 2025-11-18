# Flink SARIMAX Baseline & Alert System

A Flink streaming application that computes SARIMAX baselines for node CPU utilization and generates alerts when values deviate from expected patterns.

## Architecture

See [stream_architecture.md](stream_architecture.md) for a detailed diagram of the stream processing pipeline.

## Prerequisites

- Docker and Docker Compose
- Ports available: 8081 (Flink UI), 19092 (Kafka), 18081-18082 (Redpanda)

## Quick Start

### Option A: Using Built-in Datagen (Simplest - No Kafka Required!)

```bash
# 1. Build and start Flink only (no Kafka needed!)
docker compose up -d jobmanager taskmanager-1 taskmanager-2

# 2. Submit job with built-in datagen
docker exec -it jobmanager python3 /opt/flink/usrlib/flinkarima.py \
  --use-datagen \
  --datagen-nodes 5 \
  --datagen-rate 2.0 \
  --parallelism 2
```

That's it! The job will generate test data internally and process it.

### Option B: Using Kafka/Redpanda (Production-like)

```bash
# 1. Build and start all services
docker compose up -d

# 2. Create Kafka topic
docker exec -it redpanda-1 rpk topic create node-metrics --brokers localhost:9092

# 3. Start external data generator (optional - or use datagen above)
./run_datagen.sh

# 4. Submit Flink job
docker exec -it jobmanager python3 /opt/flink/usrlib/flinkarima.py \
  --topic node-metrics \
  --bootstrap-servers redpanda-1:9092 \
  --parallelism 2
```

### 5. Monitor

- **Flink Web UI**: http://localhost:8081
- **Job Logs**: `docker compose logs -f taskmanager-1`
- **Baselines & Alerts**: Check taskmanager logs for output

## Configuration

### Data Generator Options

```bash
python3 src/datagen.py \
  --topic node-metrics \
  --bootstrap-servers localhost:19092 \
  --nodes 5 \              # Number of nodes
  --rate 2.0 \             # Messages per second per node
  --duration 3600          # Run for 1 hour (optional)
```

### Flink Job Options

**Using Built-in Datagen (Recommended for Testing):**

```bash
python3 src/flinkarima.py \
  --use-datagen \           # Use built-in generator (no Kafka!)
  --datagen-nodes 5 \       # Number of nodes
  --datagen-rate 2.0 \      # Messages per second per node
  --parallelism 2 \
  --max-history 1440 \      # ~5 days of 5-min samples
  --min-history 288         # ~1 day minimum before fitting
```

**Using Kafka Source:**

```bash
python3 src/flinkarima.py \
  --topic node-metrics \
  --bootstrap-servers redpanda-1:9092 \
  --parallelism 2 \
  --max-history 1440 \      # ~5 days of 5-min samples
  --min-history 288 \       # ~1 day minimum before fitting
  --emit-every-n 5 \        # Emit baseline every 5 samples
  --order "1,1,1" \         # ARIMA order (p,d,q)
  --seasonal-order "0,1,1,288" \  # Seasonal order (P,D,Q,s)
  --alert-z-threshold 3.0 \ # Z-score alert threshold
  --alert-pct-threshold 50.0  # Percentage deviation threshold
```

## Data Format

### Input (Kafka Topic)

```json
{
  "node_id": "node-01",
  "cpu_utilization": 73.5,
  "timestamp": 1731883200000
}
```

### Baseline Output

```json
{
  "node_id": "node-01",
  "observed_cpu": 73.5,
  "baseline_cpu": 68.2,
  "history_size": 288,
  "event_time": 1731883200000,
  "running_mean": 65.3,
  "running_std": 12.4,
  "daily_avg_latest": 70.1,
  "daily_avg_last5": 68.9
}
```

### Alert Output

```json
{
  "node_id": "node-01",
  "alert_type": "cpu_deviation",
  "severity": "high",
  "observed_cpu": 95.2,
  "baseline_cpu": 68.2,
  "deviation": 27.0,
  "pct_deviation": 39.6,
  "z_score": 3.2,
  "alert_reason": "z_score=3.20 exceeds threshold=3.0",
  "event_time": 1731883200000,
  "baseline_event_time": 1731882900000
}
```

## Features

- **5-minute Aggregation**: Reduces noise by aggregating raw samples
- **SARIMAX Modeling**: Learns daily seasonality patterns (288 samples = 1 day at 5-min intervals)
- **Data Normalization**: Uses running statistics (Welford's algorithm) for scaling
- **Broadcast State**: Efficient baseline distribution to all alert operator instances
- **Dual Thresholds**: Alerts on both z-score (statistical) and percentage deviation
- **Fault Tolerance**: Checkpointing enabled for state recovery

## Troubleshooting

### Port Conflicts

If port 8081 is already in use:

```bash
# Find what's using the port
lsof -i :8081

# Stop conflicting service or change port in docker-compose.yml
```

### Job Not Starting

```bash
# Check Flink logs
docker compose logs jobmanager
docker compose logs taskmanager-1

# Check if Python dependencies are installed
docker exec -it jobmanager python3 -c "import statsmodels; import pyflink"
```

### No Data in Kafka

```bash
# Check Redpanda is running
docker compose ps redpanda

# List topics
docker exec -it redpanda-1 rpk topic list --brokers localhost:9092

# Check topic messages
docker exec -it redpanda-1 rpk topic consume node-metrics --brokers localhost:9092 -n 10
```

## Development

### Rebuild After Code Changes

```bash
docker compose build
docker compose up -d
```

### Run Tests Locally

```bash
# Install dependencies
pip install kafka-python

# Run data generator locally
python3 src/datagen.py --bootstrap-servers localhost:19092 --nodes 3 --rate 1.0
```

## Cleanup

```bash
# Stop all services
docker compose down

# Remove volumes (clears state)
docker compose down -v
```

