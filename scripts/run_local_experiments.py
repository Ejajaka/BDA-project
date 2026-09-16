"""Run the offline experiments and write results (CSV) + figures (PNG).

Usage
-----
    python scripts/run_local_experiments.py --quick
    python scripts/run_local_experiments.py --experiments 1 2 3 4 5 6
    python scripts/run_local_experiments.py --config configs/default.yaml

Experiments
-----------
1  Exact hashmap vs sketches          -> memory vs #events
2  CMS size vs frequency error        -> memory/accuracy trade-off
3  HLL precision vs cardinality error -> memory/accuracy trade-off
4  Fixed vs adaptive threshold        -> false positives under traffic drift
5  Ablation (fixed / adaptive / cusum)-> precision, recall, F1, latency
6  Anomaly intensity sweep (2x..50x)  -> detection rate + latency
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import random
import sys
from collections import Counter
from typing import Dict, List, Sequence, Tuple

import numpy as np

# Make the project root importable when running this file directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:  # plotting is optional (headless CI / no matplotlib)
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    _HAVE_PLT = True
except Exception:  # pragma: no cover
    _HAVE_PLT = False

from sketchstream.detector import (
    AdaptiveDetector,
    CusumDetector,
    FixedThresholdDetector,
)
from sketchstream.events import NetworkEvent
from sketchstream.generator import AnomalySpec, SyntheticConfig, generate
from sketchstream.metrics import (
    detection_latencies,
    evaluate_detection,
    relative_error,
)
from sketchstream.pipeline import run_local_pipeline
from sketchstream.sketches import CountMinSketch, HyperLogLog


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def ensure_dirs(out_dir: str, fig_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(fig_dir, exist_ok=True)


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
    plt_close = getattr(plt, "close", None) if _HAVE_PLT else None
    if plt_close:
        plt_close(fig)


def make_events(n_events: int, base_rate: float, n_sources: int, seed: int):
    duration = n_events / base_rate
    config = SyntheticConfig(
        duration=duration, base_rate=base_rate, n_sources=n_sources, seed=seed
    )
    events, _ = generate(config)
    return events


def _window_topk_truth(events, size, k):
    truth: Dict[int, Counter] = {}
    for e in events:
        idx = int(e.ts // size)
        truth.setdefault(idx, Counter())[e.src_ip] += 1
    return {idx: c.most_common(k) for idx, c in truth.items()}


# --------------------------------------------------------------------------- #
# Experiment 1 - memory vs events
# --------------------------------------------------------------------------- #
def exp1_memory(out_dir, fig_dir, quick=False):
    print("[Exp1] exact hashmap vs sketches: memory vs #events")
    n_events = 60_000 if quick else 300_000
    base_rate, n_sources = 5000.0, 4000
    events = make_events(n_events, base_rate, n_sources, seed=1)
    checkpoint = max(n_events // 10, 1)

    result = run_local_pipeline(
        events,
        detector=AdaptiveDetector(warmup=10),
        window_size=1.0,
        cms_width=2000,
        cms_depth=5,
        hll_precision=12,
        checkpoint_every=checkpoint,
    )
    rows = [[n, exact_b, sketch_b] for n, exact_b, sketch_b in result.memory_curve]
    rows.append([result.n_events, result.exact_memory_bytes, result.sketch_memory_bytes])
    write_csv(
        os.path.join(out_dir, "exp1_memory.csv"),
        ["events", "exact_hashmap_bytes", "sketch_bytes"],
        rows,
    )
    print(
        f"  events={result.n_events}  exact={result.exact_memory_bytes/1e6:.2f} MB "
        f"({result.exact_unique} keys)  sketches={result.sketch_memory_bytes/1e3:.1f} KB "
        f"throughput={result.throughput:,.0f} ev/s"
    )
    if _HAVE_PLT:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        xs = [r[0] for r in rows]
        ax.plot(xs, [r[1] / 1e6 for r in rows], marker="o", label="Exact hashmap")
        ax.plot(xs, [r[2] / 1e6 for r in rows], marker="s", label="Sketches (CMS+HLL+HH)")
        ax.set_xlabel("events processed")
        ax.set_ylabel("memory (MB)")
        ax.set_title("Exp1: memory growth - exact vs probabilistic")
        ax.legend()
        save_fig(fig, os.path.join(fig_dir, "fig2_memory_vs_events.png"))
    return rows


# --------------------------------------------------------------------------- #
# Experiment 2 - CMS size vs frequency error
# --------------------------------------------------------------------------- #
def exp2_cms_error(out_dir, fig_dir, quick=False):
    print("[Exp2] CMS size vs frequency-estimation error")
    n_events = 40_000 if quick else 200_000
    base_rate, n_sources, seed = 5000.0, 3000, 2
    events = make_events(n_events, base_rate, n_sources, seed=seed)
    keys = [e.src_ip for e in events]
    truth = Counter(keys)

    widths = [64, 256, 1024, 4096, 16384] if not quick else [64, 512, 4096]
    rows = []
    for width in widths:
        cms = CountMinSketch(width=width, depth=5, seed=0)
        for key in keys:
            cms.add(key)
        rel = [relative_error(cms.estimate(k), truth[k]) for k in truth]
        add_err = [cms.estimate(k) - truth[k] for k in truth]
        rows.append(
            [
                width,
                5,
                cms.n_bytes,
                float(np.mean(rel)),
                float(np.percentile(rel, 95)),
                float(np.max(add_err)),
            ]
        )
    write_csv(
        os.path.join(out_dir, "exp2_cms_error.csv"),
        ["width", "depth", "bytes", "mean_rel_error", "p95_rel_error", "max_overcount"],
        rows,
    )
    for r in rows:
        print(f"  width={r[0]:>6}  bytes={r[2]:>8}  mean_err={r[3]:.3%}  p95={r[4]:.3%}")
    if _HAVE_PLT:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot([r[0] for r in rows], [r[3] * 100 for r in rows], marker="o")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("CMS width (cells per row)")
        ax.set_ylabel("mean relative error (%)")
        ax.set_title("Exp2: CMS accuracy vs size")
        save_fig(fig, os.path.join(fig_dir, "fig3_cms_error_vs_size.png"))
    return rows


# --------------------------------------------------------------------------- #
# Experiment 3 - HLL precision vs cardinality error
# --------------------------------------------------------------------------- #
def exp3_hll_error(out_dir, fig_dir, quick=False):
    print("[Exp3] HLL precision vs cardinality-estimation error")
    cardinalities = [1_000, 10_000, 100_000] if quick else [1_000, 10_000, 100_000, 1_000_000]
    precisions = [10, 12, 14] if quick else [10, 12, 14, 16]
    rows = []
    curves = {p: [] for p in precisions}
    for p in precisions:
        hll_bytes = (1 << p)
        for n in cardinalities:
            hll = HyperLogLog(precision=p, seed=0)
            for i in range(n):
                hll.add(i)
            err = relative_error(hll.estimate(), n)
            rows.append([p, (1 << p), hll_bytes, n, hll.estimate(), err])
            curves[p].append((n, err))
    write_csv(
        os.path.join(out_dir, "exp3_hll_error.csv"),
        ["precision", "registers", "bytes", "true_cardinality", "estimate", "rel_error"],
        rows,
    )
    for r in rows:
        print(f"  p={r[0]}  n={r[3]:>9}  est={r[4]:>10.0f}  rel_err={r[5]:.3%}")
    if _HAVE_PLT:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for p in precisions:
            xs = [n for n, _ in curves[p]]
            ys = [e * 100 for _, e in curves[p]]
            ax.plot(xs, ys, marker="o", label=f"precision={p} ({1 << p} regs)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("true cardinality")
        ax.set_ylabel("relative error (%)")
        ax.set_title("Exp3: HLL error vs cardinality")
        ax.legend()
        save_fig(fig, os.path.join(fig_dir, "fig4_hll_error.png"))
    return rows


# --------------------------------------------------------------------------- #
# Experiment 4 - fixed vs adaptive under traffic drift
# --------------------------------------------------------------------------- #
def exp4_fixed_vs_adaptive(out_dir, fig_dir, quick=False):
    print("[Exp4] fixed threshold vs adaptive under traffic drift")
    from sketchstream.events import WindowSummary

    n = 400
    spike_at = 340
    base, drift_to = 1000, 1800
    series = []
    for i in range(n):
        total = int(base + (drift_to - base) * min(i / 300.0, 1.0))
        if i == spike_at:
            total = 12000
        series.append(total)

    def summaries():
        for i, total in enumerate(series):
            yield WindowSummary(
                index=i, start=float(i), end=i + 1.0, total=total,
                unique_sources=total / 10,
                top_keys=[("10.0.0.9", total)] if i == spike_at else [("10.0.0.1", total // 5)],
            )

    fixed = FixedThresholdDetector(threshold=1400)
    adaptive = AdaptiveDetector(warmup=40, z_threshold=4.0)
    fixed_fp = adaptive_fp = 0
    fixed_det = adaptive_det = False
    for s in summaries():
        if fixed.observe(s) is not None:
            if abs(s.index - spike_at) <= 2:
                fixed_det = True
            else:
                fixed_fp += 1
        if adaptive.observe(s) is not None:
            if abs(s.index - spike_at) <= 2:
                adaptive_det = True
            else:
                adaptive_fp += 1
    rows = [
        ["fixed", fixed_fp, int(fixed_det)],
        ["adaptive", adaptive_fp, int(adaptive_det)],
    ]
    write_csv(
        os.path.join(out_dir, "exp4_threshold.csv"),
        ["detector", "false_positives", "spike_detected"],
        rows,
    )
    print(f"  fixed: FP={fixed_fp} detected={fixed_det} | adaptive: FP={adaptive_fp} detected={adaptive_det}")
    if _HAVE_PLT:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot(range(n), series, lw=1)
        ax.axhline(1400, color="crimson", ls="--", label="fixed threshold")
        ax.axvline(spike_at, color="green", ls=":", label="spike")
        ax.set_xlabel("window index")
        ax.set_ylabel("events / window")
        ax.set_title("Exp4: traffic drift + spike")
        ax.legend()
        save_fig(fig, os.path.join(fig_dir, "fig_traffic_drift.png"))
    return rows


# --------------------------------------------------------------------------- #
# Experiment 5 - ablation
# --------------------------------------------------------------------------- #
def _eval_run(events, intervals, detector, window_size, sketch_kwargs):
    result = run_local_pipeline(
        events, detector=detector, window_size=window_size, **sketch_kwargs
    )
    metrics = evaluate_detection(result.summaries, result.alerts, result.ground_truth)
    lat = [x for x in detection_latencies(result.alerts, intervals) if x is not None]
    metrics["mean_latency"] = float(np.mean(lat)) if lat else float("nan")
    metrics["alerts"] = len(result.alerts)
    return metrics


def exp5_ablation(out_dir, fig_dir, quick=False):
    print("[Exp5] ablation: fixed vs adaptive vs cusum")
    duration = 200.0 if quick else 400.0
    base_rate = 1500.0
    window_size = 1.0
    config = SyntheticConfig(
        duration=duration, base_rate=base_rate, seed=7,
        anomalies=[
            AnomalySpec(duration * 0.3, duration * 0.08, 12.0, target="192.168.0.66"),
            AnomalySpec(duration * 0.6, duration * 0.12, 6.0, mode="gradual"),
        ],
    )
    events, intervals = generate(config)
    sketch_kwargs = dict(cms_width=2000, cms_depth=5, hll_precision=12, hh_capacity=200)

    detectors = {
        "fixed": FixedThresholdDetector(threshold=base_rate * window_size * 3),
        "adaptive_ewma": AdaptiveDetector(warmup=30, z_threshold=4.0),
        "adaptive_cusum": CusumDetector(warmup=30, k=0.5, h=6.0),
    }
    rows = []
    for name, det in detectors.items():
        m = _eval_run(events, intervals, det, window_size, sketch_kwargs)
        rows.append([
            name, m["tp"], m["fp"], m["fn"],
            round(m["precision"], 4), round(m["recall"], 4), round(m["f1"], 4),
            round(m["mean_latency"], 3), m["alerts"],
        ])
        print(
            f"  {name:<14} P={m['precision']:.3f} R={m['recall']:.3f} "
            f"F1={m['f1']:.3f} FP={m['fp']} FN={m['fn']} lat={m['mean_latency']:.2f}s"
        )
    write_csv(
        os.path.join(out_dir, "exp5_ablation.csv"),
        ["detector", "tp", "fp", "fn", "precision", "recall", "f1", "mean_latency_s", "alerts"],
        rows,
    )
    if _HAVE_PLT:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        names = [r[0] for r in rows]
        ax.bar([n + " P" for n in names], [r[4] for r in rows], label="precision")
        ax.bar([n + " R" for n in names], [r[5] for r in rows], label="recall")
        ax.set_ylim(0, 1.05)
        ax.set_title("Exp5: detector precision / recall")
        ax.legend()
        save_fig(fig, os.path.join(fig_dir, "fig6_detector_quality.png"))
    return rows


# --------------------------------------------------------------------------- #
# Experiment 6 - anomaly intensity sweep
# --------------------------------------------------------------------------- #
def exp6_intensity(out_dir, fig_dir, quick=False):
    print("[Exp6] anomaly intensity sweep (detection rate + latency)")
    multipliers = [2, 5, 10, 50] if not quick else [5, 50]
    duration, base_rate, window_size = 200.0, 1500.0, 1.0
    rows = []
    for mult in multipliers:
        config = SyntheticConfig(
            duration=duration, base_rate=base_rate, seed=11,
            anomalies=[AnomalySpec(duration * 0.4, duration * 0.1, float(mult))],
        )
        events, intervals = generate(config)
        det = AdaptiveDetector(warmup=30, z_threshold=4.0)
        result = run_local_pipeline(
            events, detector=det, window_size=window_size,
            cms_width=2000, cms_depth=5, hll_precision=12, hh_capacity=200,
        )
        lat = [x for x in detection_latencies(result.alerts, intervals) if x is not None]
        detected = 1 if lat else 0
        rows.append([mult, detected, round(min(lat), 2) if lat else "", len(result.alerts)])
        print(f"  x{mult:<3} detected={bool(detected)} latency={rows[-1][2]}s alerts={len(result.alerts)}")
    write_csv(
        os.path.join(out_dir, "exp6_intensity.csv"),
        ["multiplier", "detected", "latency_s", "alerts"],
        rows,
    )
    if _HAVE_PLT:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        xs = [r[0] for r in rows]
        ys = [r[2] if r[2] != "" else 0 for r in rows]
        ax.plot(xs, ys, marker="o")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("anomaly intensity (x normal)")
        ax.set_ylabel("detection latency (s)")
        ax.set_title("Exp6: latency vs anomaly intensity")
        save_fig(fig, os.path.join(fig_dir, "fig7_latency_vs_intensity.png"))
    return rows


EXPERIMENTS = {
    "1": exp1_memory,
    "2": exp2_cms_error,
    "3": exp3_hll_error,
    "4": exp4_fixed_vs_adaptive,
    "5": exp5_ablation,
    "6": exp6_intensity,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiments", nargs="+", default=list(EXPERIMENTS), choices=list(EXPERIMENTS))
    parser.add_argument("--out", default="results", help="CSV output directory")
    parser.add_argument("--figs", default="figures", help="figure output directory")
    parser.add_argument("--quick", action="store_true", help="small sizes for a fast smoke run")
    parser.add_argument("--seed", type=int, default=0, help="global RNG seed")
    args = parser.parse_args(argv)

    random.seed(args.seed)
    np.random.seed(args.seed)
    ensure_dirs(args.out, args.figs)

    for key in args.experiments:
        EXPERIMENTS[key](args.out, args.figs, quick=args.quick)
    print("done.")


if __name__ == "__main__":
    main()
