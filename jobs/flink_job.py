"""PyFlink streaming job: Kafka -> sketches -> adaptive anomaly detection.

Pipeline
--------
    Kafka("network-events")
        -> parse (ts, src_ip)
        -> event-time watermarks
        -> tumbling event-time window (default 1s)
        -> ProcessAllWindowFunction:
              CountMinSketch (per-key frequency)
              HyperLogLog    (distinct sources)
              SpaceSaving    (heavy hitters)
              -> WindowSummary -> AdaptiveDetector -> Alert
        -> JSONL sink (Kafka "alerts" and/or stdout)

IMPORTANT
---------
PyFlink does not yet support Python 3.13. Run this from a Python 3.11 venv:

    pip install -r requirements-streaming.txt
    # local mini-cluster (no Flink cluster needed):
    python jobs/flink_job.py --local
    # submit to the Docker cluster:
    docker exec -it sketchstream-jobmanager flink run -py /opt/jobs/flink_job.py

The connector jar (matching your Flink version) must be on the classpath; see
README.md. The sketch/detector classes are reused verbatim from ``sketchstream``,
so results are directly comparable with the local pipeline.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build_env(args):
    from pathlib import Path

    from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(args.parallelism)
    for jar in args.jars:
        env.add_jars(Path(jar).resolve().as_uri())
    return env


def build_source(args):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )

    initial = (
        KafkaOffsetsInitializer.earliest()
        if args.from_earliest
        else KafkaOffsetsInitializer.latest()
    )
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_topics(args.topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(initial)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_sink(args):
    if not args.alerts_topic:
        return None
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import (
        KafkaRecordSerializationSchema,
        KafkaSink,
    )

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.alerts_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="network-events")
    parser.add_argument("--alerts-topic", default="alerts")
    parser.add_argument("--group-id", default="sketchstream-flink")
    parser.add_argument("--window-size", type=float, default=1.0)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--cms-width", type=int, default=2000)
    parser.add_argument("--cms-depth", type=int, default=5)
    parser.add_argument("--hll-precision", type=int, default=12)
    parser.add_argument("--hh-capacity", type=int, default=200)
    parser.add_argument("--z-threshold", type=float, default=4.0)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--jar", dest="jars", action="append", default=[],
                        help="path to flink-connector-kafka jar (repeatable)")
    parser.add_argument("--local", action="store_true",
                        help="run a local mini-cluster instead of submitting")
    parser.add_argument("--from-earliest", action="store_true",
                        help="start from the earliest offset (consume existing events)")
    parser.add_argument("--incremental", action="store_true",
                        help="use mergeable SketchState aggregation (O(1) window memory)")
    parser.add_argument("--out", default="", help="optional JSONL file sink")
    args = parser.parse_args(argv)

    from pyflink.common import Duration, Types, WatermarkStrategy
    from pyflink.common.time import Time

    env = build_env(args)
    source = build_source(args)

    stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "network-events"
    )

    def parse(value: str):
        rec = json.loads(value)
        return (float(rec["ts"]), str(rec["src_ip"]))

    event_type = Types.TUPLE([Types.DOUBLE(), Types.STRING()])
    parsed = stream.map(parse, output_type=event_type)

    from pyflink.common.watermark_strategy import TimestampAssigner

    class EventTimeAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return int(value[0] * 1000)  # seconds -> milliseconds

    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2))
        .with_idleness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTimeAssigner())
    )
    parsed = parsed.assign_timestamps_and_watermarks(watermark)

    from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction
    from pyflink.datastream.window import TumblingEventTimeWindows

    from sketchstream.detector import AdaptiveDetector
    from sketchstream.events import WindowSummary
    from sketchstream.sketches import CountMinSketch, HyperLogLog, SketchState, SpaceSaving

    class AnomalyWindow(ProcessAllWindowFunction):
        """One instance persists across windows -> detector state is retained."""

        def __init__(self, cms_width, cms_depth, hll_precision, hh_capacity,
                     top_k, z_threshold, warmup):
            self._cms_width = cms_width
            self._cms_depth = cms_depth
            self._hll_precision = hll_precision
            self._hh_capacity = hh_capacity
            self._top_k = top_k
            self._detector = AdaptiveDetector(warmup=warmup, z_threshold=z_threshold)

        def process(self, context, elements):
            cms = CountMinSketch(self._cms_width, self._cms_depth)
            hll = HyperLogLog(self._hll_precision)
            hh = SpaceSaving(self._hh_capacity)
            total = 0
            window = context.window()
            for ts, src_ip in elements:
                cms.add(src_ip)
                hll.add(src_ip)
                hh.add(src_ip)
                total += 1
            summary = WindowSummary(
                index=int(window.start // 1000),
                start=window.start / 1000.0,
                end=window.end / 1000.0,
                total=total,
                unique_sources=hll.estimate(),
                top_keys=hh.top_k(self._top_k),
                cms=cms,
                hll=hll,
                heavy=hh,
            )
            alert = self._detector.observe(summary)
            # PyFlink's ProcessAllWindowFunction returns an iterable of outputs.
            return [
                json.dumps(
                    {
                        "type": "window",
                        "start": summary.start,
                        "end": summary.end,
                        "total": summary.total,
                        "unique": round(summary.unique_sources, 1),
                        "top": [[k, int(c)] for k, c in summary.top_keys[:5]],
                        "alert": alert is not None,
                        "score": round(alert.score, 3) if alert else None,
                        "reason": alert.reason if alert else None,
                    }
                )
            ]

    from pyflink.datastream.functions import MapFunction

    class IncrementalEmitter(ProcessAllWindowFunction):
        """Emit a record from an aggregated :class:`SketchState`.

        Window memory is O(1): events are folded into the state by the
        ``SketchAggregate`` function instead of being buffered.
        """

        def __init__(self, top_k, z_threshold, warmup):
            self._top_k = top_k
            self._detector = AdaptiveDetector(warmup=warmup, z_threshold=z_threshold)

        def process(self, context, elements):
            state = None
            for state in elements:  # exactly one aggregated result
                pass
            window = context.window()
            summary = state.summary(
                index=int(window.start // 1000),
                start=window.start / 1000.0,
                end=window.end / 1000.0,
                top_k=self._top_k,
            )
            alert = self._detector.observe(summary)
            return [
                json.dumps(
                    {
                        "type": "window",
                        "start": summary.start,
                        "end": summary.end,
                        "total": summary.total,
                        "unique": round(summary.unique_sources, 1),
                        "top": [[k, int(c)] for k, c in summary.top_keys[:5]],
                        "alert": alert is not None,
                        "score": round(alert.score, 3) if alert else None,
                        "reason": alert.reason if alert else None,
                    }
                )
            ]

    class SketchAggregate(AggregateFunction):
        """Incrementally fold events into a fixed-size, mergeable SketchState."""

        def __init__(self, cms_width, cms_depth, hll_precision, hh_capacity, seed=0):
            self._cms_width = cms_width
            self._cms_depth = cms_depth
            self._hll_precision = hll_precision
            self._hh_capacity = hh_capacity
            self._seed = seed

        def create_accumulator(self):
            return SketchState(
                cms_width=self._cms_width,
                cms_depth=self._cms_depth,
                hll_precision=self._hll_precision,
                hh_capacity=self._hh_capacity,
                seed=self._seed,
            )

        def add(self, value, accumulator):
            accumulator.add(value[1])  # value = (ts, src_ip)
            return accumulator

        def get_result(self, accumulator):
            return accumulator

        def merge(self, accumulators):
            accumulators = list(accumulators)
            base = accumulators[0]
            for other in accumulators[1:]:
                base.merge(other)
            return base

    class JsonlWriter(MapFunction):
        def __init__(self, path):
            self._path = path

        def open(self, runtime_context):
            self._fh = open(self._path, "a", encoding="utf-8")

        def map(self, value):
            self._fh.write(value + "\n")
            self._fh.flush()
            return value

        def close(self):
            self._fh.close()

    windowed = parsed.window_all(
        TumblingEventTimeWindows.of(Time.seconds(args.window_size))
    )
    if args.incremental:
        result = windowed.aggregate(
            SketchAggregate(
                args.cms_width, args.cms_depth, args.hll_precision, args.hh_capacity
            ),
            IncrementalEmitter(10, args.z_threshold, args.warmup),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.STRING(),
        )
    else:
        result = windowed.process(
            AnomalyWindow(
                args.cms_width, args.cms_depth, args.hll_precision,
                args.hh_capacity, 10, args.z_threshold, args.warmup,
            ),
            output_type=Types.STRING(),
        )

    if args.out:
        result = result.map(JsonlWriter(args.out), output_type=Types.STRING())

    sink = build_sink(args)
    if sink is not None:
        result.sink_to(sink)
    else:
        result.print()

    env.execute("sketchstream-anomaly-detection")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
