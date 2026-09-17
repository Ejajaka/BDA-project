"""Publish network events to Kafka (JSON), optionally throttled to real time.

Prereqs (Python 3.11 venv recommended):
    pip install confluent-kafka
    docker compose up -d kafka

Examples
--------
    # replay a synthetic scenario as fast as possible
    python scripts/kafka_producer.py --scenario mixed --duration 120 --base-rate 2000

    # replay CIC-IDS2017 CSV at 5x real time
    python scripts/kafka_producer.py --cicids data/cicids/Wednesday-workingHours.pcap_ISCX.csv \
        --realtime --speed 5
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sketchstream.generator import build_scenario


def _event_to_json(e) -> str:
    return json.dumps(
        {
            "ts": e.ts,
            "src_ip": e.src_ip,
            "dst_ip": e.dst_ip,
            "dst_port": e.dst_port,
            "protocol": e.protocol,
            "packets": e.packets,
            "nbytes": e.nbytes,
            "label": e.label,
        }
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="network-events")
    parser.add_argument("--scenario", default="mixed",
                        choices=["sudden", "gradual", "heavy_hitter", "mixed", "none"])
    parser.add_argument("--cicids", default=None, help="path to a CIC-IDS2017 CSV")
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--base-rate", type=float, default=2000.0)
    parser.add_argument("--limit", type=int, default=0, help="max events (0 = all)")
    parser.add_argument("--realtime", action="store_true")
    parser.add_argument("--speed", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    try:
        from confluent_kafka import Producer
    except ImportError:
        print(
            "confluent-kafka is not installed. Use a Python 3.11 venv:\n"
            "  pip install -r requirements-streaming.txt",
            file=sys.stderr,
        )
        return 1

    if args.cicids:
        from sketchstream.io import load_cicids_events

        events = load_cicids_events(args.cicids, limit=args.limit or None)
    else:
        events, _ = build_scenario(
            args.scenario, duration=args.duration,
            base_rate=args.base_rate, seed=args.seed,
        )
        if args.limit:
            events = events[: args.limit]

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "queue.buffering.max.messages": 1_000_000,
            "linger.ms": 5,
        }
    )
    start_wall = time.perf_counter()
    for i, event in enumerate(events):
        while True:
            try:
                producer.produce(args.topic, value=_event_to_json(event))
                break
            except BufferError:
                producer.poll(0.5)
        if i % 100 == 0:
            producer.poll(0)
        if args.realtime and args.speed > 0:
            target = event.ts / args.speed
            elapsed = time.perf_counter() - start_wall
            if target > elapsed:
                time.sleep(target - elapsed)
    producer.flush()
    print(f"produced {len(events)} events to topic '{args.topic}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
