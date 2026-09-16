"""Event-time window aggregation over sketches.

Design note (important, and a core contribution of this project)
---------------------------------------------------------------
Neither Count-Min Sketch nor HyperLogLog supports *deletion*: you cannot
"remove" the events that fall out of a sliding window. The correct way to get
a sliding window is therefore:

    tumbling sub-buckets  +  merge on demand
        - CMS  : merge = element-wise ADD
        - HLL  : merge = element-wise MAX
        - HH   : approximate (candidate union, re-scored via merged CMS)

``TumblingWindowAggregator`` emits one :class:`WindowSummary` per bucket.
``SlidingWindowAggregator`` keeps the last ``num_buckets`` bucket summaries and
merges them into a rolling summary.
"""
from __future__ import annotations

from collections import deque
from typing import Callable, Deque, List, Optional

from .events import NetworkEvent, WindowSummary
from .sketches import CountMinSketch, HyperLogLog, SpaceSaving


def _default_key(event: NetworkEvent) -> str:
    return event.src_ip


class TumblingWindowAggregator:
    def __init__(
        self,
        size: float,
        cms_width: int = 2000,
        cms_depth: int = 5,
        hll_precision: int = 12,
        hh_capacity: int = 200,
        seed: int = 0,
        top_k: int = 10,
        key_fn: Optional[Callable[[NetworkEvent], str]] = None,
    ) -> None:
        if size <= 0:
            raise ValueError("window size must be positive")
        self.size = float(size)
        self.cms_width = int(cms_width)
        self.cms_depth = int(cms_depth)
        self.hll_precision = int(hll_precision)
        self.hh_capacity = int(hh_capacity)
        self.seed = int(seed)
        self.top_k = int(top_k)
        self.key_fn = key_fn or _default_key

        self._index: Optional[int] = None
        self._cms: Optional[CountMinSketch] = None
        self._hll: Optional[HyperLogLog] = None
        self._hh: Optional[SpaceSaving] = None
        self.total = 0

    # ------------------------------------------------------------------ #
    def _fresh(self, index: int) -> None:
        self._index = index
        self._cms = CountMinSketch(self.cms_width, self.cms_depth, self.seed)
        self._hll = HyperLogLog(self.hll_precision, self.seed)
        self._hh = SpaceSaving(self.hh_capacity)
        self.total = 0

    def _emit(self) -> WindowSummary:
        assert self._index is not None
        start = self._index * self.size
        return WindowSummary(
            index=self._index,
            start=start,
            end=start + self.size,
            total=self.total,
            unique_sources=self._hll.estimate() if self._hll else 0.0,
            top_keys=self._hh.top_k(self.top_k) if self._hh else [],
            cms=self._cms,
            hll=self._hll,
            heavy=self._hh,
        )

    def add(self, event: NetworkEvent) -> List[WindowSummary]:
        """Feed one event; return any windows that closed as a result."""
        idx = int(event.ts // self.size)
        emitted: List[WindowSummary] = []
        if self._index is None:
            self._fresh(idx)
        while idx > self._index:  # type: ignore[operator]
            emitted.append(self._emit())
            self._fresh(self._index + 1)  # type: ignore[operator]
        # Late (out-of-order) events are folded into the current window.
        key = self.key_fn(event)
        assert self._cms is not None and self._hll is not None and self._hh is not None
        self._cms.add(key)
        self._hll.add(key)
        self._hh.add(key)
        self.total += 1
        return emitted

    def flush(self) -> List[WindowSummary]:
        if self._index is None:
            return []
        summary = self._emit()
        self._index = None
        return [summary]


def merge_summaries(
    summaries: List[WindowSummary], top_k: int = 10
) -> Optional[WindowSummary]:
    """Merge bucket summaries into a single (sliding) window summary."""
    summaries = [s for s in summaries if s is not None]
    if not summaries:
        return None
    first = summaries[0]
    if first.cms is None or first.hll is None:
        # Merge on plain scalars only.
        return WindowSummary(
            index=first.index,
            start=first.start,
            end=summaries[-1].end,
            total=sum(s.total for s in summaries),
            unique_sources=sum(s.unique_sources for s in summaries),
            top_keys=[],
        )

    cms = CountMinSketch(first.cms.width, first.cms.depth, first.cms.seed)
    hll = HyperLogLog(first.hll.precision, first.hll.seed)
    total = 0
    candidates: set = set()
    for s in summaries:
        if s.cms is not None:
            cms.merge(s.cms)
        if s.hll is not None:
            hll.merge(s.hll)
        total += s.total
        candidates.update(k for k, _ in s.top_keys)

    tops = sorted(((k, cms.estimate(k)) for k in candidates), key=lambda kv: -kv[1])
    return WindowSummary(
        index=first.index,
        start=first.start,
        end=summaries[-1].end,
        total=total,
        unique_sources=hll.estimate(),
        top_keys=tops[:top_k],
        cms=cms,
        hll=hll,
        heavy=None,
    )


class SlidingWindowAggregator:
    """Sliding window = merge of the last ``num_buckets`` tumbling buckets."""

    def __init__(self, size: float, num_buckets: int = 3, **kwargs) -> None:
        if num_buckets < 1:
            raise ValueError("num_buckets must be >= 1")
        self.num_buckets = int(num_buckets)
        self.top_k = int(kwargs.get("top_k", 10))
        self._tumbling = TumblingWindowAggregator(size, **kwargs)
        self._buckets: Deque[WindowSummary] = deque(maxlen=self.num_buckets)

    def add(self, event: NetworkEvent) -> List[WindowSummary]:
        emitted: List[WindowSummary] = []
        for bucket in self._tumbling.add(event):
            self._buckets.append(bucket)
            merged = merge_summaries(list(self._buckets), top_k=self.top_k)
            if merged is not None:
                emitted.append(merged)
        return emitted

    def current(self) -> Optional[WindowSummary]:
        return merge_summaries(list(self._buckets), top_k=self.top_k)

    def flush(self) -> List[WindowSummary]:
        for bucket in self._tumbling.flush():
            self._buckets.append(bucket)
        merged = merge_summaries(list(self._buckets), top_k=self.top_k)
        return [merged] if merged is not None else []


__all__ = [
    "TumblingWindowAggregator",
    "SlidingWindowAggregator",
    "merge_summaries",
]
