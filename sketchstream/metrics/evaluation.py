"""Metrics for the sketches, the detectors and the end-to-end system."""
from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from ..detector.base import Alert
from ..events import NetworkEvent, WindowSummary


# --------------------------------------------------------------------------- #
# Estimation accuracy
# --------------------------------------------------------------------------- #
def relative_error(estimated: float, actual: float) -> float:
    """Relative error, safe when ``actual`` is zero."""
    if actual == 0:
        return 0.0 if estimated == 0 else float("inf")
    return abs(estimated - actual) / abs(actual)


# --------------------------------------------------------------------------- #
# Window-level ground truth
# --------------------------------------------------------------------------- #
def window_attack_counts(
    events: Sequence[NetworkEvent], size: float
) -> Dict[int, Tuple[int, int]]:
    """Return ``{window_index: (attack_events, total_events)}``."""
    counts: Dict[int, List[int]] = {}
    for e in events:
        idx = int(e.ts // size)
        slot = counts.setdefault(idx, [0, 0])
        slot[0] += 1 if e.is_attack else 0
        slot[1] += 1
    return {k: (v[0], v[1]) for k, v in counts.items()}


def ground_truth_windows(
    events: Sequence[NetworkEvent],
    size: float,
    min_fraction: float = 0.1,
    min_events: int = 5,
) -> Dict[int, bool]:
    """Label a window anomalous if attack traffic in it is significant.

    A window is positive when ``attack_events >= min_events`` AND
    ``attack_events / total_events >= min_fraction``. This avoids labelling
    windows with one stray attack packet as anomalies.
    """
    truth: Dict[int, bool] = {}
    for idx, (attack, total) in window_attack_counts(events, size).items():
        fraction = attack / total if total else 0.0
        truth[idx] = attack >= min_events and fraction >= min_fraction
    return truth


# --------------------------------------------------------------------------- #
# Detection quality
# --------------------------------------------------------------------------- #
def evaluate_detection(
    summaries: Sequence[WindowSummary],
    alerts: Sequence[Alert],
    truth: Mapping[int, bool],
) -> Dict[str, float]:
    """Window-level confusion matrix + precision/recall/F1."""
    predicted: Set[int] = {a.window_index for a in alerts}
    tp = fp = tn = fn = 0
    for s in summaries:
        actual = bool(truth.get(s.index, False))
        pred = s.index in predicted
        if actual and pred:
            tp += 1
        elif actual and not pred:
            fn += 1
        elif not actual and pred:
            fp += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": fpr,
    }


# --------------------------------------------------------------------------- #
# Latency
# --------------------------------------------------------------------------- #
def detection_latencies(
    alerts: Sequence[Alert],
    intervals: Iterable[Tuple[float, float]],
    slack: float = 0.0,
) -> List[Optional[float]]:
    """Latency from anomaly start to first alert after it, per interval.

    An alert fires at the *end* of the window that triggered it, so latency
    includes at most one window of delay. ``None`` means the anomaly was missed.
    """
    latencies: List[Optional[float]] = []
    for start, end in intervals:
        candidates = [
            a.window_end
            for a in alerts
            if start <= a.window_end <= end + slack
        ]
        latencies.append(min(candidates) - start if candidates else None)
    return latencies


# --------------------------------------------------------------------------- #
# Heavy hitters
# --------------------------------------------------------------------------- #
def topk_overlap(
    predicted: Sequence[Tuple[str, int]],
    actual: Sequence[Tuple[str, int]],
    k: int,
) -> float:
    """Jaccard-style overlap of the top-k key sets: |inter| / k."""
    pred_keys = {key for key, _ in predicted[:k]}
    actual_keys = {key for key, _ in actual[:k]}
    if k <= 0:
        return 0.0
    return len(pred_keys & actual_keys) / k


__all__ = [
    "relative_error",
    "window_attack_counts",
    "ground_truth_windows",
    "evaluate_detection",
    "detection_latencies",
    "topk_overlap",
]
