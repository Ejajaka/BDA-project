"""Run a scenario through the local pipeline and emit dashboard-ready JSONL.

Examples
--------
    # fast run, no animation
    python scripts/demo_stream.py --scenario mixed --out results/stream.jsonl

    # replay at 20x for a live-looking dashboard demo
    python scripts/demo_stream.py --scenario mixed --realtime --speed 20

Then, in another terminal:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sketchstream.config import DEFAULT_CONFIG_PATH, load_config
from sketchstream.detector import AdaptiveDetector, CusumDetector
from sketchstream.generator import build_scenario
from sketchstream.pipeline import dump_run, run_local_pipeline


def build_detector(name: str, detector_cfg: dict):
    if name == "adaptive":
        return AdaptiveDetector(**detector_cfg)
    if name == "cusum":
        return CusumDetector(**detector_cfg)
    raise ValueError(f"unknown detector: {name!r}")


def main(argv=None):
    # The config file provides defaults; explicit CLI flags (e.g. from
    # run_all.ps1) always override it.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    known, _ = pre.parse_known_args(argv)
    cfg = load_config(known.config)
    stream = cfg.get("stream", {}) or {}
    sketches = cfg.get("sketches", {}) or {}
    detector_cfg = cfg.get("detectors", {}) or {}

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=known.config,
                        help="parameter file (defaults come from here)")
    parser.add_argument("--scenario", default=stream.get("scenario", "mixed"),
                        choices=["sudden", "gradual", "heavy_hitter", "mixed", "none"])
    parser.add_argument("--duration", type=float, default=stream.get("duration", 120.0))
    parser.add_argument("--base-rate", type=float, default=stream.get("base_rate", 2000.0))
    parser.add_argument("--window-size", type=float, default=stream.get("window_size", 1.0))
    parser.add_argument("--mode", default=stream.get("window_mode", "tumbling"),
                        choices=["tumbling", "sliding"])
    parser.add_argument("--num-buckets", type=int, default=stream.get("slide_buckets", 3))
    parser.add_argument("--detector", default="adaptive", choices=["adaptive", "cusum"])
    parser.add_argument("--out", default="results/stream.jsonl")
    parser.add_argument("--realtime", action="store_true", help="animate at wall-clock speed")
    parser.add_argument("--speed", type=float, default=20.0, help="replay speedup factor")
    parser.add_argument("--seed", type=int, default=stream.get("seed", 42))
    args = parser.parse_args(argv)

    events, intervals = build_scenario(
        args.scenario, duration=args.duration, base_rate=args.base_rate, seed=args.seed
    )
    # Detector parameters: tuned demo defaults, overridden by the config file if
    # it specifies them. The warmup stays short so alerts appear during replay.
    demo_det = {
        "adaptive": dict(z_threshold=4.0),
        "cusum": dict(k=0.1, h=5.0),
    }
    cfg_det = dict(demo_det.get(args.detector, {}))
    cfg_det.update(detector_cfg.get(args.detector, {}) or {})
    cfg_det["warmup"] = 10
    detector = build_detector(args.detector, cfg_det)
    result = run_local_pipeline(
        events,
        detector=detector,
        window_size=args.window_size,
        mode=args.mode,
        num_buckets=args.num_buckets,
        cms_width=sketches.get("cms_width", 2000),
        cms_depth=sketches.get("cms_depth", 5),
        hll_precision=sketches.get("hll_precision", 12),
        hh_capacity=sketches.get("hh_capacity", 200),
        top_k=sketches.get("top_k", 10),
    )
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    dump_run(
        result,
        args.out,
        meta={
            "scenario": args.scenario,
            "window_size": args.window_size,
            "mode": args.mode,
            "n_events": result.n_events,
            "sketch_bytes": result.sketch_memory_bytes,
            "exact_bytes": result.exact_memory_bytes,
            "exact_unique": result.exact_unique,
            "intervals": intervals,
            "throughput": round(result.throughput, 1),
        },
        realtime=args.realtime,
        speed=args.speed,
    )
    print(
        f"wrote {args.out}: {len(result.summaries)} windows, "
        f"{len(result.alerts)} alerts, {result.n_events} events, "
        f"exact={result.exact_memory_bytes/1e6:.2f} MB vs "
        f"sketches={result.sketch_memory_bytes/1e3:.1f} KB"
    )


if __name__ == "__main__":
    main()
