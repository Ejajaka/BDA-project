"""Evaluate the system on a real dataset (CIC-IDS2017 or UNSW-NB15).

Produces the "real data" results: memory, sketch accuracy, detector ablation
(window-level + interval-level), and detection latency, writing CSV + figures.

Usage
-----
    # self-test on a synthetic CIC-IDS-format CSV (no download needed)
    python scripts/run_cicids_evaluation.py --demo

    # real CIC-IDS2017 (TrafficLabelling CSVs)
    python scripts/run_cicids_evaluation.py --cicids data/cicids/Wednesday-workingHours.pcap_ISCX.csv

    # all CSVs in a folder
    python scripts/run_cicids_evaluation.py --cicids data/cicids

    # UNSW-NB15 (full feature CSV with srcip/Stime)
    python scripts/run_cicids_evaluation.py --dataset unsw --unsw data/unsw/UNSW-NB15_1.csv

Note: for CIC-IDS use the *TrafficLabelling* release (has Source IP + Timestamp),
not MachineLearningCVE.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import os
import sys
from collections import Counter
from typing import Dict, List, Sequence

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    _HAVE_PLT = True
except Exception:  # pragma: no cover
    _HAVE_PLT = False

from sketchstream.detector import AdaptiveDetector, CusumDetector, FixedThresholdDetector
from sketchstream.events import NetworkEvent
from sketchstream.generator import build_scenario
from sketchstream.metrics import (
    attack_intervals,
    detection_latencies,
    evaluate_detection,
    interval_precision,
    interval_recall,
    relative_error,
)
from sketchstream.pipeline import run_local_pipeline
from sketchstream.sketches import CountMinSketch, HyperLogLog

_PROTO = {"TCP": 6, "UDP": 17, "ICMP": 1}


# --------------------------------------------------------------------------- #
def write_csv(path: str, header: Sequence[str], rows: Sequence[Sequence]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"  wrote {path}")


def save_fig(fig, path: str) -> None:
    if _HAVE_PLT:
        fig.tight_layout()
        fig.savefig(path, dpi=130)
        print(f"  wrote {path}")
        plt.close(fig)


def write_demo_cicids_csv(path: str, duration: float = 60.0, base_rate: float = 800.0) -> None:
    """Write synthetic traffic in CIC-IDS2017 'TrafficLabelling' column format."""
    events, _ = build_scenario("mixed", duration=duration, base_rate=base_rate, seed=7)
    base_time = _dt.datetime(2017, 7, 5, 9, 0, 0)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "Flow ID", " Source IP", " Source Port", " Destination IP",
            " Destination Port", " Protocol", " Timestamp", " Flow Duration",
            " Label",
        ])
        for i, e in enumerate(events):
            ts = base_time + _dt.timedelta(seconds=e.ts)
            writer.writerow([
                f"{e.src_ip}-{e.dst_ip}-{e.dst_port}-{i}",
                e.src_ip, 40000 + (i % 20000), e.dst_ip, e.dst_port,
                _PROTO.get(e.protocol, 6),
                ts.strftime("%d/%m/%Y %H:%M:%S"),
                1, e.label,
            ])
    print(f"  wrote demo CIC-IDS CSV: {path} ({len(events)} rows)")


def load_events(args) -> List[NetworkEvent]:
    if args.dataset == "unsw":
        from sketchstream.io import load_unsw_events

        return load_unsw_events(args.unsw, limit=args.limit or None)
    from sketchstream.io import load_cicids_events

    return load_cicids_events(args.cicids, limit=args.limit or None)


# --------------------------------------------------------------------------- #
def sketch_accuracy(events: List[NetworkEvent], cms_width: int, hll_precision: int):
    keys = [e.src_ip for e in events]
    truth = Counter(keys)
    cms = CountMinSketch(width=cms_width, depth=5)
    hll = HyperLogLog(precision=hll_precision)
    for k in keys:
        cms.add(k)
        hll.add(k)
    rel = [relative_error(cms.estimate(k), truth[k]) for k in truth]
    return {
        "distinct_keys": len(truth),
        "cms_mean_rel_error": float(np.mean(rel)),
        "cms_p95_rel_error": float(np.percentile(rel, 95)),
        "hll_estimate": hll.estimate(),
        "hll_rel_error": relative_error(hll.estimate(), len(truth)),
        "cms_bytes": cms.n_bytes,
        "hll_bytes": hll.n_bytes,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="cicids", choices=["cicids", "unsw"])
    parser.add_argument("--cicids", default="", help="CIC-IDS2017 CSV file or folder")
    parser.add_argument("--unsw", default="", help="UNSW-NB15 CSV file or folder")
    parser.add_argument("--demo", action="store_true",
                        help="self-test on a generated CIC-IDS-format CSV")
    parser.add_argument("--limit", type=int, default=300_000)
    parser.add_argument("--window-size", type=float, default=1.0)
    parser.add_argument("--cms-width", type=int, default=2000)
    parser.add_argument("--cms-depth", type=int, default=5)
    parser.add_argument("--hll-precision", type=int, default=12)
    parser.add_argument("--hh-capacity", type=int, default=200)
    parser.add_argument("--min-fraction", type=float, default=0.1)
    parser.add_argument("--min-events", type=int, default=5)
    parser.add_argument("--max-gap", type=int, default=0,
                        help="bridge benign gaps between attack windows")
    parser.add_argument("--out", default="results")
    parser.add_argument("--figs", default="figures")
    args = parser.parse_args(argv)

    os.makedirs(args.out, exist_ok=True)
    os.makedirs(args.figs, exist_ok=True)

    if args.demo:
        demo_path = os.path.join(args.out, "demo_cicids.csv")
        write_demo_cicids_csv(demo_path, duration=60.0, base_rate=800.0)
        args.cicids = demo_path
        args.dataset = "cicids"
    if args.dataset == "cicids" and not args.cicids:
        parser.error("--cicids is required (or use --demo)")
    if args.dataset == "unsw" and not args.unsw:
        parser.error("--unsw is required")

    print(f"[load] dataset={args.dataset}")
    events = load_events(args)
    if not events:
        print("no events loaded; check the path/columns.")
        return 1
    print(f"  loaded {len(events)} events, {len(set(e.src_ip for e in events))} distinct src IPs")

    truth = attack_intervals(
        events, args.window_size, args.min_fraction, args.min_events, args.max_gap
    )
    print(f"  ground-truth attack intervals: {len(truth)}")

    # ---- memory ----------------------------------------------------------- #
    result = run_local_pipeline(
        events,
        detector=AdaptiveDetector(warmup=10, z_threshold=4.0),
        window_size=args.window_size,
        cms_width=args.cms_width,
        cms_depth=args.cms_depth,
        hll_precision=args.hll_precision,
        hh_capacity=args.hh_capacity,
        checkpoint_every=max(len(events) // 10, 1),
    )
    print(
        f"  memory: exact={result.exact_memory_bytes/1e6:.2f} MB "
        f"({result.exact_unique} keys) vs sketches={result.sketch_memory_bytes/1e3:.1f} KB"
    )
    write_csv(
        os.path.join(args.out, "cicids_memory.csv"),
        ["events", "exact_hashmap_bytes", "sketch_bytes"],
        [[n, e, s] for n, e, s in result.memory_curve]
        + [[result.n_events, result.exact_memory_bytes, result.sketch_memory_bytes]],
    )

    # ---- sketch accuracy -------------------------------------------------- #
    acc = sketch_accuracy(events, args.cms_width, args.hll_precision)
    print(
        f"  CMS mean rel-error={acc['cms_mean_rel_error']:.3%}  "
        f"HLL rel-error={acc['hll_rel_error']:.3%} (n={acc['distinct_keys']})"
    )
    write_csv(
        os.path.join(args.out, "cicids_sketch_accuracy.csv"),
        list(acc.keys()),
        [list(acc.values())],
    )

    # ---- detector ablation (window + interval level) ---------------------- #
    base_rate = len(events) / max(events[-1].ts, 1e-9)
    detectors = {
        "fixed": FixedThresholdDetector(threshold=base_rate * args.window_size * 3),
        "adaptive_ewma": AdaptiveDetector(warmup=10, z_threshold=4.0),
        "adaptive_cusum": CusumDetector(warmup=10, k=0.1, h=5.0),
    }
    rows = []
    for name, det in detectors.items():
        run = run_local_pipeline(
            events, detector=det, window_size=args.window_size,
            cms_width=args.cms_width, cms_depth=args.cms_depth,
            hll_precision=args.hll_precision, hh_capacity=args.hh_capacity,
        )
        m = evaluate_detection(run.summaries, run.alerts, run.ground_truth)
        lat = [x for x in detection_latencies(run.alerts, truth) if x is not None]
        i_recall = interval_recall(run.alerts, truth)
        i_prec = interval_precision(run.alerts, truth)
        rows.append([
            name, m["tp"], m["fp"], m["fn"],
            round(m["precision"], 4), round(m["recall"], 4), round(m["f1"], 4),
            round(i_recall, 4), round(i_prec, 4),
            round(float(np.mean(lat)), 3) if lat else "",
            len(run.alerts),
        ])
        print(
            f"  {name:<14} P={m['precision']:.3f} R={m['recall']:.3f} F1={m['f1']:.3f} "
            f"| interval R={i_recall:.3f} P={i_prec:.3f} | alerts={len(run.alerts)}"
        )
    write_csv(
        os.path.join(args.out, "cicids_ablation.csv"),
        ["detector", "tp", "fp", "fn", "precision", "recall", "f1",
         "interval_recall", "interval_precision", "mean_latency_s", "alerts"],
        rows,
    )

    # ---- figures ---------------------------------------------------------- #
    if _HAVE_PLT:
        mc = result.memory_curve + [(result.n_events, result.exact_memory_bytes, result.sketch_memory_bytes)]
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot([r[0] for r in mc], [r[1] / 1e6 for r in mc], marker="o", label="Exact hashmap")
        ax.plot([r[0] for r in mc], [r[2] / 1e6 for r in mc], marker="s", label="Sketches")
        ax.set_xlabel("events processed")
        ax.set_ylabel("memory (MB)")
        ax.set_title(f"Memory: exact vs sketches ({args.dataset})")
        ax.legend()
        save_fig(fig, os.path.join(args.figs, "cicids_memory.png"))

        fig, ax = plt.subplots(figsize=(7, 4.5))
        names = [r[0] for r in rows]
        x = np.arange(len(names))
        ax.bar(x - 0.2, [r[5] for r in rows], width=0.4, label="window recall")
        ax.bar(x + 0.2, [r[7] for r in rows], width=0.4, label="interval recall")
        ax.set_xticks(x)
        ax.set_xticklabels(names)
        ax.set_ylim(0, 1.05)
        ax.set_ylabel("recall")
        ax.set_title(f"Detector recall ({args.dataset})")
        ax.legend()
        save_fig(fig, os.path.join(args.figs, "cicids_detector_recall.png"))

    print("done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
