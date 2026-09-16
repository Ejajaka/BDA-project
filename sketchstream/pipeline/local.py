"""End-to-end local runner (single process).

This mirrors the Kafka/Flink pipeline but runs in-process, so it is usable for
development, unit tests and the offline experiments *without* any containers.
The streaming job in ``jobs/flink_job.py`` uses the same sketch/window/detector
classes, so results are comparable.

Both an exact hashmap baseline and the sketch pipeline are run over the same
events to compare memory and accuracy like-for-like.
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple

from ..counting import ExactCounter
from ..detector.base import Alert, Detector
from ..events import NetworkEvent, WindowSummary
from ..metrics import ground_truth_windows
from ..windows import SlidingWindowAggregator, TumblingWindowAggregator


@dataclass
class LocalRunResult:
    summaries: List[WindowSummary] = field(default_factory=list)
    alerts: List[Alert] = field(default_factory=list)
    n_events: int = 0
    processing_seconds: float = 0.0
    exact_memory_bytes: int = 0
    sketch_memory_bytes: int = 0
    exact_unique: int = 0
    exact_top: List[Tuple[str, int]] = field(default_factory=list)
    memory_curve: List[Tuple[int, int, int]] = field(default_factory=list)
    ground_truth: dict = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        return self.n_events / self.processing_seconds if self.processing_seconds else 0.0


def _make_aggregator(mode: str, window_size: float, **kwargs):
    if mode == "tumbling":
        return TumblingWindowAggregator(window_size, **kwargs)
    if mode == "sliding":
        kwargs.pop("num_buckets", None)
        return SlidingWindowAggregator(
            window_size, num_buckets=kwargs.pop("_num_buckets", 3), **kwargs
        )
    raise ValueError(f"unknown window mode: {mode!r}")


def run_local_pipeline(
    events: Sequence[NetworkEvent],
    detector: Detector,
    window_size: float = 1.0,
    mode: str = "tumbling",
    num_buckets: int = 3,
    cms_width: int = 2000,
    cms_depth: int = 5,
    hll_precision: int = 12,
    hh_capacity: int = 200,
    top_k: int = 10,
    seed: int = 0,
    key_fn: Optional[Callable[[NetworkEvent], str]] = None,
    checkpoint_every: int = 0,
    track_exact: bool = True,
) -> LocalRunResult:
    agg_kwargs = dict(
        cms_width=cms_width,
        cms_depth=cms_depth,
        hll_precision=hll_precision,
        hh_capacity=hh_capacity,
        seed=seed,
        top_k=top_k,
    )
    if key_fn is not None:
        agg_kwargs["key_fn"] = key_fn
    if mode == "sliding":
        agg_kwargs["_num_buckets"] = num_buckets
    aggregator = _make_aggregator(mode, window_size, **agg_kwargs)

    sketch_const = cms_width * cms_depth * 8 + (1 << hll_precision) + hh_capacity * 64
    exact = ExactCounter() if track_exact else None

    result = LocalRunResult()
    start = time.perf_counter()
    for i, event in enumerate(events, start=1):
        if exact is not None:
            exact.add(key_fn(event) if key_fn else event.src_ip)
        for summary in aggregator.add(event):
            result.summaries.append(summary)
            alert = detector.observe(summary)
            if alert is not None:
                result.alerts.append(alert)
        if checkpoint_every and exact is not None and i % checkpoint_every == 0:
            result.memory_curve.append(
                (i, exact.n_bytes, sketch_const)
            )

    for summary in aggregator.flush():
        result.summaries.append(summary)
        alert = detector.observe(summary)
        if alert is not None:
            result.alerts.append(alert)

    result.processing_seconds = time.perf_counter() - start

    result.n_events = len(events)
    if exact is not None:
        result.exact_memory_bytes = exact.n_bytes
        result.exact_unique = exact.unique()
        result.exact_top = exact.top_k(top_k)
    result.sketch_memory_bytes = sketch_const
    result.ground_truth = ground_truth_windows(events, window_size)
    return result


__all__ = ["LocalRunResult", "run_local_pipeline"]
