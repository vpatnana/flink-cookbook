#!/usr/bin/env python3
"""
Data generator for node CPU utilization metrics.

Generates realistic CPU utilization patterns with:
- Daily seasonality (higher during business hours)
- Random noise
- Occasional spikes for testing alerts
- Multiple nodes with different patterns

Usage:
    python datagen.py --topic node-metrics --bootstrap-servers localhost:9092 --nodes 5 --rate 10
"""

import argparse
import json
import random
import time
from datetime import datetime
from typing import List

from kafka import KafkaProducer


def generate_cpu_utilization(
    node_id: str,
    base_level: float,
    hour: int,
    minute: int,
    spike_probability: float = 0.01,
) -> float:
    """
    Generate CPU utilization with daily seasonality and occasional spikes.
    
    Args:
        node_id: Node identifier
        base_level: Base CPU level for this node (30-70%)
        hour: Current hour (0-23)
        minute: Current minute (0-59)
        spike_probability: Probability of a spike (default: 1%)
    
    Returns:
        CPU utilization percentage (0-100)
    """
    # Daily pattern: lower at night (0-6), higher during day (9-17), medium in evening
    if 0 <= hour < 6:
        time_factor = 0.4  # Night: 40% of base
    elif 6 <= hour < 9:
        time_factor = 0.6  # Morning ramp-up: 60%
    elif 9 <= hour < 17:
        time_factor = 1.2  # Business hours: 120%
    elif 17 <= hour < 22:
        time_factor = 0.8  # Evening: 80%
    else:
        time_factor = 0.5  # Late evening: 50%
    
    # Add some minute-level variation
    minute_variation = 1.0 + (minute % 15) * 0.02  # Slight variation every 15 minutes
    
    # Base value with time pattern
    cpu = base_level * time_factor * minute_variation
    
    # Add random noise (±5%)
    noise = random.uniform(-5, 5)
    cpu += noise
    
    # Occasional spike (for testing alerts)
    if random.random() < spike_probability:
        spike = random.uniform(30, 50)  # 30-50% spike
        cpu += spike
        print(f"[SPIKE] Node {node_id}: +{spike:.1f}% spike at {hour:02d}:{minute:02d}")
    
    # Clamp to valid range
    cpu = max(0.0, min(100.0, cpu))
    
    return round(cpu, 2)


def generate_metrics(
    producer: KafkaProducer,
    topic: str,
    nodes: List[str],
    rate: float,
    duration: int = None,
):
    """
    Generate and send metrics to Kafka.
    
    Args:
        producer: Kafka producer instance
        topic: Kafka topic name
        nodes: List of node IDs
        rate: Messages per second per node
        duration: Duration in seconds (None = infinite)
    """
    interval = 1.0 / rate if rate > 0 else 1.0
    start_time = time.time()
    message_count = 0
    
    # Assign base levels to nodes (different patterns)
    node_bases = {node: random.uniform(30, 70) for node in nodes}
    
    print(f"Starting data generation:")
    print(f"  Topic: {topic}")
    print(f"  Nodes: {len(nodes)}")
    print(f"  Rate: {rate} msg/sec/node ({rate * len(nodes)} total)")
    print(f"  Node base levels: {node_bases}")
    print()
    
    try:
        while True:
            current_time = time.time()
            if duration and (current_time - start_time) > duration:
                break
            
            now = datetime.now()
            hour = now.hour
            minute = now.minute
            
            for node_id in nodes:
                cpu = generate_cpu_utilization(
                    node_id,
                    node_bases[node_id],
                    hour,
                    minute,
                    spike_probability=0.01,  # 1% chance of spike
                )
                
                message = {
                    "node_id": node_id,
                    "cpu_utilization": cpu,
                    "timestamp": int(current_time * 1000),  # milliseconds
                }
                
                producer.send(topic, value=json.dumps(message).encode("utf-8"))
                message_count += 1
                
                if message_count % 100 == 0:
                    print(f"Sent {message_count} messages... (CPU: {cpu:.1f}% for {node_id})")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\nStopping data generation...")
    finally:
        producer.flush()
        elapsed = time.time() - start_time
        print(f"\nGenerated {message_count} messages in {elapsed:.1f} seconds")
        print(f"Average rate: {message_count / elapsed:.1f} msg/sec")


def main():
    parser = argparse.ArgumentParser(description="Generate node CPU utilization metrics")
    parser.add_argument("--topic", default="node-metrics", help="Kafka topic name")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=5,
        help="Number of nodes to generate metrics for",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Messages per second per node",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration in seconds (None = infinite)",
    )
    
    args = parser.parse_args()
    
    # Generate node IDs
    nodes = [f"node-{i:02d}" for i in range(1, args.nodes + 1)]
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: v,
        )
        print(f"Connected to Kafka at {args.bootstrap_servers}")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("Make sure Kafka/Redpanda is running and accessible")
        return
    
    # Generate metrics
    generate_metrics(producer, args.topic, nodes, args.rate, args.duration)


if __name__ == "__main__":
    main()

