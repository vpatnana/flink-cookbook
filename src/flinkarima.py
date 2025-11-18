#!/usr/bin/env python3
"""
Flink streaming job that learns SARIMAX baselines for node CPU utilization.

The job expects a Kafka topic that contains JSON messages shaped like:
{
  "node_id": "node-42",
  "cpu_utilization": 73.5,
  "timestamp": 1731883200000
}

For every node_id key the process function keeps a bounded history window and
fits a SARIMAX model to generate a baseline (forecast) for the most recent
observation frequency.  The baseline is emitted as a JSON string so it can be
consumed by downstream systems or sinks (Elastic, Influx, etc.).

Requirements (already installed in the Docker image):
  * pyflink>=1.20.0
  * statsmodels>=0.14
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Iterable, List, Optional
import math

from pyflink.common import Row, Types, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction, KeyedBroadcastProcessFunction, SourceFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows

from statsmodels.tsa.statespace.sarimax import SARIMAX

LOGGER = logging.getLogger("flinkarima")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))


class MetricGenerator(SourceFunction):
    """Flink source function that generates node CPU metrics with daily seasonality."""
    
    def __init__(self, num_nodes: int = 5, rate: float = 2.0, duration_seconds: int = None):
        super().__init__()
        self.num_nodes = num_nodes
        self.rate = rate  # messages per second per node
        self.duration_seconds = duration_seconds
        self.running = True
        
    def run(self, source_context):
        import random
        import time
        from datetime import datetime
        
        nodes = [f"node-{i:02d}" for i in range(1, self.num_nodes + 1)]
        node_bases = {node: random.uniform(30, 70) for node in nodes}
        # Interval should be 1.0 / rate since we emit num_nodes messages per iteration
        # This ensures rate messages per second per node
        interval = 1.0 / self.rate if self.rate > 0 else 1.0
        start_time = time.time()
        message_count = 0
        
        LOGGER.info(f"Starting metric generation: {self.num_nodes} nodes, {self.rate} msg/sec/node")
        
        while self.running:
            current_time = time.time()
            if self.duration_seconds and (current_time - start_time) > self.duration_seconds:
                break
            
            now = datetime.now()
            hour = now.hour
            minute = now.minute
            
            for node_id in nodes:
                if not self.running:
                    break
                    
                # Generate CPU with daily seasonality
                base = node_bases[node_id]
                if 0 <= hour < 6:
                    time_factor = 0.4
                elif 6 <= hour < 9:
                    time_factor = 0.6
                elif 9 <= hour < 17:
                    time_factor = 1.2
                elif 17 <= hour < 22:
                    time_factor = 0.8
                else:
                    time_factor = 0.5
                
                cpu = base * time_factor + random.uniform(-5, 5)
                if random.random() < 0.01:  # 1% spike probability
                    cpu += random.uniform(30, 50)
                cpu = max(0.0, min(100.0, cpu))
                
                metric = Row(
                    node_id=node_id,
                    cpu=round(cpu, 2),
                    event_time=int(current_time * 1000)
                )
                
                source_context.collect(metric)
                message_count += 1
                
                if message_count % 100 == 0:
                    LOGGER.debug(f"Generated {message_count} metrics")
            
            time.sleep(interval)
        
        LOGGER.info(f"Metric generation complete: {message_count} messages")
    
    def cancel(self):
        self.running = False


def parse_metric(raw: str) -> Row:
    """Parse an inbound metric JSON string into a Row."""
    payload = json.loads(raw)
    cpu_val = float(payload["cpu_utilization"])
    ts = int(payload.get("timestamp", 0))
    node_id = str(payload["node_id"])
    return Row(node_id=node_id, cpu=cpu_val, event_time=ts)


def _daily_trend_metrics(raw_history: List[float], season_len: int, days: int = 5):
    season_len = max(1, season_len)
    needed = season_len * max(1, days)
    if not raw_history:
        return {"latest_day_avg": None, "five_day_avg": None}
    trimmed = raw_history[-needed:] if len(raw_history) >= needed else raw_history[:]
    latest_day = trimmed[-season_len:] if len(trimmed) >= season_len else trimmed
    latest_avg = sum(latest_day) / len(latest_day) if latest_day else None
    five_day_avg = sum(trimmed) / len(trimmed) if trimmed else None
    return {
        "latest_day_avg": latest_avg,
        "five_day_avg": five_day_avg,
    }


class SarimaxBaselineFunction(KeyedProcessFunction):
    """Maintains per-node history and emits SARIMAX baselines."""

    def __init__(
        self,
        max_history: int = 1440,
        min_history: int = 288,
        emit_every_n: int = 5,
        order=(1, 1, 1),
        seasonal_order=(0, 1, 1, 288),
        forecast_steps: int = 1,
    ) -> None:
        super().__init__()
        self.max_history = max_history
        self.min_history = min_history
        self.emit_every_n = emit_every_n
        self.order = order
        self.seasonal_order = seasonal_order
        self.forecast_steps = forecast_steps
        self.history_state = None
        self.raw_history_state = None
        self.counter_state = None
        self.count_state = None
        self.mean_state = None
        self.m2_state = None

    def open(self, runtime_context):
        self.history_state = runtime_context.get_list_state(
            ListStateDescriptor("history", Types.DOUBLE())
        )
        self.raw_history_state = runtime_context.get_list_state(
            ListStateDescriptor("raw_history", Types.DOUBLE())
        )
        self.counter_state = runtime_context.get_state(
            ValueStateDescriptor("emit_counter", Types.INT())
        )
        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("sample_count", Types.LONG())
        )
        self.mean_state = runtime_context.get_state(
            ValueStateDescriptor("running_mean", Types.DOUBLE())
        )
        self.m2_state = runtime_context.get_state(
            ValueStateDescriptor("running_m2", Types.DOUBLE())
        )

    def process_element(self, value, ctx, out):
        history = list(self.history_state.get())
        raw_history = list(self.raw_history_state.get())
        count = self.count_state.value() or 0
        mean = self.mean_state.value() or 0.0
        m2 = self.m2_state.value() or 0.0
        std = math.sqrt(m2 / (count - 1)) if count > 1 and m2 > 0 else 0.0
        scaled_cpu = (float(value.cpu) - mean) / std if std > 0 else 0.0
        history.append(scaled_cpu)
        raw_history.append(float(value.cpu))
        if len(history) > self.max_history:
            history = history[-self.max_history :]
        if len(raw_history) > self.max_history:
            raw_history = raw_history[-self.max_history :]
        self.history_state.update(history)
        self.raw_history_state.update(raw_history)

        # update running stats with new raw observation
        count += 1
        delta = float(value.cpu) - mean
        mean += delta / count
        delta2 = float(value.cpu) - mean
        m2 += delta * delta2
        self.count_state.update(count)
        self.mean_state.update(mean)
        self.m2_state.update(m2)

        counter = self.counter_state.value()
        if counter is None:
            counter = 0
        counter = (counter + 1) % self.emit_every_n
        self.counter_state.update(counter)
        if counter != 0 or len(history) < self.min_history:
            return

        try:
            model = SARIMAX(
                history,
                order=self.order,
                seasonal_order=self.seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fitted = model.fit(disp=False)
            baseline_scaled = float(fitted.forecast(self.forecast_steps)[-1])
            std = math.sqrt(m2 / (count - 1)) if count > 1 and m2 > 0 else 0.0
            baseline = (
                baseline_scaled * std + mean if std > 0 else mean if count > 0 else 0.0
            )
            trend_metrics = _daily_trend_metrics(
                raw_history,
                season_len=self.seasonal_order[3],
                days=max(5, self.max_history // max(1, self.seasonal_order[3])),
            )
            result = {
                "node_id": value.node_id,
                "observed_cpu": value.cpu,
                "baseline_cpu": max(0.0, baseline),
                "history_size": len(history),
                "event_time": value.event_time,
                "running_mean": mean,
                "running_std": std,
                "daily_avg_latest": trend_metrics["latest_day_avg"],
                "daily_avg_last5": trend_metrics["five_day_avg"],
            }
            out.collect(json.dumps(result))
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Failed SARIMAX fit for %s: %s", value.node_id, exc)


class FiveMinuteAverager(ProcessWindowFunction):
    """Aggregates raw samples into 5-minute averages per node."""

    def process(self, key, ctx, elements, out):
        count = 0
        total = 0.0
        latest_ts = 0
        for element in elements:
            count += 1
            total += float(element.cpu)
            latest_ts = max(latest_ts, int(element.event_time))
        if count == 0:
            return
        avg = total / count
        out.collect(
            Row(
                node_id=key,
                cpu=avg,
                event_time=ctx.window().end if latest_ts == 0 else latest_ts,
            )
        )


class AlertFunction(KeyedBroadcastProcessFunction):
    """Joins raw data with baselines using broadcast state and emits alerts when deviation exceeds threshold."""

    # Broadcast state descriptor for baselines (node_id -> baseline JSON)
    BASELINE_DESCRIPTOR = MapStateDescriptor("baselines", Types.STRING(), Types.STRING())

    def __init__(
        self,
        z_score_threshold: float = 3.0,
        pct_threshold: float = 50.0,
        min_baseline: float = 1.0,
    ) -> None:
        super().__init__()
        self.z_score_threshold = z_score_threshold
        self.pct_threshold = pct_threshold
        self.min_baseline = min_baseline

    def process_element(self, value, ctx, out):
        """Process raw data stream (5-minute aggregates) - read baselines from broadcast state."""
        # value is a JSON string from the raw_stream
        raw_data = json.loads(value)
        node_id = raw_data.get("node_id", "")
        observed_cpu = float(raw_data.get("cpu", 0.0))
        event_time = raw_data.get("event_time", 0)

        # Read baseline from broadcast state
        broadcast_state = ctx.get_broadcast_state(self.BASELINE_DESCRIPTOR)
        baseline_json = broadcast_state.get(node_id)

        if baseline_json is None:
            # No baseline yet for this node, skip alerting
            LOGGER.debug("No baseline available yet for node %s", node_id)
            return

        baseline_data = json.loads(baseline_json)
        baseline_cpu = baseline_data.get("baseline_cpu", 0.0)
        running_std = baseline_data.get("running_std", 0.0)

        # Calculate deviations
        deviation = observed_cpu - baseline_cpu
        pct_deviation = (
            (deviation / baseline_cpu * 100.0) if baseline_cpu >= self.min_baseline else 0.0
        )
        z_score = (
            deviation / running_std if running_std > 0.0 else 0.0
        )

        # Check if alert should be triggered
        should_alert = False
        alert_reason = None

        if abs(z_score) >= self.z_score_threshold:
            should_alert = True
            alert_reason = f"z_score={z_score:.2f} exceeds threshold={self.z_score_threshold}"
        elif abs(pct_deviation) >= self.pct_threshold:
            should_alert = True
            alert_reason = f"pct_deviation={pct_deviation:.2f}% exceeds threshold={self.pct_threshold}%"

        if should_alert:
            alert = {
                "node_id": node_id,
                "alert_type": "cpu_deviation",
                "severity": "high" if abs(z_score) >= self.z_score_threshold * 2 else "medium",
                "observed_cpu": observed_cpu,
                "baseline_cpu": baseline_cpu,
                "deviation": deviation,
                "pct_deviation": pct_deviation,
                "z_score": z_score,
                "alert_reason": alert_reason,
                "event_time": event_time,
                "baseline_event_time": baseline_data.get("event_time", 0),
            }
            out.collect(json.dumps(alert))
            LOGGER.warning(
                "Alert for %s: %s (observed=%.2f, baseline=%.2f, z=%.2f)",
                node_id, alert_reason, observed_cpu, baseline_cpu, z_score
            )

    def process_broadcast_element(self, value, ctx, out):
        """Process baseline stream - update broadcast state with latest baseline."""
        baseline_json = value
        baseline_data = json.loads(baseline_json)
        node_id = baseline_data.get("node_id", "")

        if not node_id:
            LOGGER.warning("Received baseline without node_id, skipping")
            return

        # Update broadcast state with latest baseline for this node
        broadcast_state = ctx.get_broadcast_state(self.BASELINE_DESCRIPTOR)
        broadcast_state.put(node_id, baseline_json)
        
        LOGGER.debug("Updated baseline in broadcast state for node %s", node_id)


def build_consumer(topic: str, bootstrap_servers: str) -> FlinkKafkaConsumer:
    props = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": "flink-sarimax",
        "auto.offset.reset": "latest",
    }
    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=props,
    )


def run_job(args):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(args.checkpoint_ms)
    env.set_parallelism(args.parallelism)

    # Use datagen source if enabled, otherwise use Kafka
    if args.use_datagen:
        LOGGER.info("Using built-in datagen source (no Kafka required)")
        generator = MetricGenerator(
            num_nodes=args.datagen_nodes,
            rate=args.datagen_rate,
            duration_seconds=args.datagen_duration,
        )
        parsed = env.add_source(generator, output_type=Types.ROW_NAMED(
            ["node_id", "cpu", "event_time"],
            [Types.STRING(), Types.FLOAT(), Types.LONG()]
        )).name("datagen_metrics")
    else:
        LOGGER.info(f"Using Kafka source: {args.topic} @ {args.bootstrap_servers}")
        source = build_consumer(args.topic, args.bootstrap_servers)
        metric_stream = env.add_source(source).name("kafka_node_metrics")
        parsed = metric_stream.map(
            parse_metric,
            output_type=Types.ROW_NAMED(["node_id", "cpu", "event_time"], [Types.STRING(), Types.FLOAT(), Types.LONG()]),
        )

    keyed = parsed.key_by(lambda row: row.node_id, key_type=Types.STRING())

    five_minute = keyed.window(
        TumblingProcessingTimeWindows.of(Time.minutes(5))
    ).process(
        FiveMinuteAverager(),
        output_type=Types.ROW_NAMED(
            ["node_id", "cpu", "event_time"],
            [Types.STRING(), Types.FLOAT(), Types.LONG()],
        ),
    )

    sarimax = SarimaxBaselineFunction(
        max_history=args.max_history,
        min_history=args.min_history,
        emit_every_n=args.emit_every_n,
        order=tuple(args.order),
        seasonal_order=tuple(args.seasonal_order),
        forecast_steps=args.forecast_steps,
    )

    baseline_stream = five_minute.process(
        sarimax, output_type=Types.STRING()
    ).name("sarimax_baseline")

    # Create raw data stream (same 5-minute aggregates) for alerting
    raw_stream = five_minute.map(
        lambda row: json.dumps({
            "node_id": row.node_id,
            "cpu": row.cpu,
            "event_time": row.event_time,
        }),
        output_type=Types.STRING()
    ).name("raw_metrics")

    # Key raw stream by node_id for processing
    keyed_raw = raw_stream.key_by(
        lambda x: json.loads(x).get("node_id", ""),
        key_type=Types.STRING()
    )

    # Create broadcast stream from baseline stream
    baseline_broadcast = baseline_stream.broadcast(AlertFunction.BASELINE_DESCRIPTOR)

    # Connect raw stream with broadcast baseline stream
    alert_function = AlertFunction(
        z_score_threshold=args.alert_z_threshold,
        pct_threshold=args.alert_pct_threshold,
        min_baseline=args.alert_min_baseline,
    )

    keyed_raw.connect(baseline_broadcast).process(
        alert_function, output_type=Types.STRING()
    ).name("cpu_deviation_alerts").print()

    # Also print baselines for monitoring
    baseline_stream.print()

    env.execute("node-cpu-sarimax-baseline")


def _comma_int_list(value: str, expected: int) -> List[int]:
    parts = [int(x.strip()) for x in value.split(",")]
    if len(parts) != expected:
        raise argparse.ArgumentTypeError(
            f"Expected {expected} comma-separated ints, got {value}"
        )
    return parts


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute SARIMAX baselines for node CPU utilization."
    )
    parser.add_argument("--use-datagen", action="store_true", help="Use built-in datagen source instead of Kafka")
    parser.add_argument("--topic", default="node-metrics", help="Kafka topic (ignored if --use-datagen)")
    parser.add_argument("--bootstrap-servers", default="redpanda-1:9092", help="Kafka bootstrap servers (ignored if --use-datagen)")
    parser.add_argument("--datagen-nodes", type=int, default=5, help="Number of nodes for datagen source")
    parser.add_argument("--datagen-rate", type=float, default=2.0, help="Messages per second per node for datagen")
    parser.add_argument("--datagen-duration", type=int, default=None, help="Duration in seconds for datagen (None = infinite)")
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--checkpoint-ms", type=int, default=60000)
    parser.add_argument("--max-history", type=int, default=1440, help="Number of 5-minute samples to retain (~5 days).")
    parser.add_argument("--min-history", type=int, default=288, help="Minimum 5-minute samples before fitting (~1 day).")
    parser.add_argument("--emit-every-n", type=int, default=5)
    parser.add_argument(
        "--order",
        type=lambda v: _comma_int_list(v, 3),
        default="1,1,1",
        help="p,d,q order (comma separated)",
    )
    parser.add_argument(
        "--seasonal-order",
        type=lambda v: _comma_int_list(v, 4),
        default="0,1,1,288",
        help="Seasonal order P,D,Q,s where s=288 for 5-minute daily seasonality",
    )
    parser.add_argument("--forecast-steps", type=int, default=1)
    parser.add_argument(
        "--alert-z-threshold",
        type=float,
        default=3.0,
        help="Z-score threshold for alerting (default: 3.0 = 3 standard deviations)",
    )
    parser.add_argument(
        "--alert-pct-threshold",
        type=float,
        default=50.0,
        help="Percentage deviation threshold for alerting (default: 50%%)",
    )
    parser.add_argument(
        "--alert-min-baseline",
        type=float,
        default=1.0,
        help="Minimum baseline value to compute percentage deviation (default: 1.0)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_job(parse_args())

