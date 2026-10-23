# ---------------------------------------------------------------------------
# EVENT-TIME WINDOW AGGREGATION OVER SKETCHES
#
# CMS and HLL cannot delete, so a sliding window cannot be maintained by
# subtracting expired events. Instead we keep tumbling sub-buckets and merge
# them on demand:
#   CMS -> element-wise ADD
#   HLL -> element-wise MAX
#   HH  -> approximate candidate union, re-scored via the merged CMS
#
# TumblingWindowAggregator emits one WindowSummary per bucket.
# SlidingWindowAggregator keeps the last `num_buckets` summaries and merges
# them into a rolling summary.
#
# FLOWCHART
# ---------
#   Tumbling.add(event)
#          |
#          v
#   idx = int(event.ts // size)
#          |
#          v
#   first event? -> _fresh(idx)
#          |
#          v
#   while idx > current_index:
#       emit current bucket
#       _fresh(current_index + 1)
#          |
#          v
#   fold event into the CURRENT bucket (late events included):
#       cms.add(key), hll.add(key), hh.add(key), total += 1
#          |
#          v
#   return emitted buckets
#
#   Tumbling.flush() -> emit current bucket, then reset
#
#   merge_summaries(list)
#          |
#          v
#   drop None; empty? -> None
#          |
#          v
#   first summary has sketches?
#      no |                                yes |
#         v                                   v
#   scalar merge                       new cms + hll; merge each bucket
#   sum total, sum unique              candidates = union(top_keys)
#   top_keys = []                      tops = candidates sorted by cms.estimate
#                                      return summary (heavy=None)
#
#   Sliding.add(event)
#          |
#          v
#   for each bucket closed by tumbling:
#       push into deque(maxlen=num_buckets)
#       merged = merge_summaries(last N buckets)
#       emit merged if not None
# ---------------------------------------------------------------------------
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

    def _fresh(self, index: int) -> None:
        # FLOWCHART: "_fresh(index)" -> empty sketches for a new bucket.
        self._index = index
        self._cms = CountMinSketch(self.cms_width, self.cms_depth, self.seed)
        self._hll = HyperLogLog(self.hll_precision, self.seed)
        self._hh = SpaceSaving(self.hh_capacity)
        self.total = 0

    def _emit(self) -> WindowSummary:
        # FLOWCHART: "emit current bucket" -> WindowSummary for this index.
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
        # FLOWCHART: "idx = int(event.ts // size)".
        idx = int(event.ts // self.size)
        emitted: List[WindowSummary] = []
        if self._index is None:
            # FLOWCHART: "first event?" -> yes: start the bucket.
            self._fresh(idx)
        # FLOWCHART: "while idx > current_index" -> close skipped buckets.
        while idx > self._index:  # type: ignore[operator]
            emitted.append(self._emit())
            self._fresh(self._index + 1)  # type: ignore[operator]
        # FLOWCHART: "fold event into the CURRENT bucket"; late/out-of-order
        # events belong to an already-closed bucket, so they land here.
        key = self.key_fn(event)
        assert self._cms is not None and self._hll is not None and self._hh is not None
        self._cms.add(key)
        self._hll.add(key)
        self._hh.add(key)
        self.total += 1
        return emitted

    def flush(self) -> List[WindowSummary]:
        # FLOWCHART: "flush()" -> emit the open bucket, then reset.
        if self._index is None:
            return []
        summary = self._emit()
        self._index = None
        return [summary]


def merge_summaries(
    summaries: List[WindowSummary], top_k: int = 10
) -> Optional[WindowSummary]:
    # FLOWCHART: "merge_summaries(list)" -> drop None / handle empty.
    summaries = [s for s in summaries if s is not None]
    if not summaries:
        return None
    first = summaries[0]
    if first.cms is None or first.hll is None:
        # FLOWCHART: "first has sketches?" -> no: scalar merge only.
        return WindowSummary(
            index=first.index,
            start=first.start,
            end=summaries[-1].end,
            total=sum(s.total for s in summaries),
            unique_sources=sum(s.unique_sources for s in summaries),
            top_keys=[],
        )

    # FLOWCHART: "yes": create empty sketches and merge each bucket into them.
    cms = CountMinSketch(first.cms.width, first.cms.depth, first.cms.seed)
    hll = HyperLogLog(first.hll.precision, first.hll.seed)
    total = 0
    candidates: set = set()
    for s in summaries:
        if s.cms is not None:
            cms.merge(s.cms)   # ADD
        if s.hll is not None:
            hll.merge(s.hll)   # MAX
        total += s.total
        # FLOWCHART: "candidates = union(top_keys)".
        candidates.update(k for k, _ in s.top_keys)

    # FLOWCHART: "tops = candidates sorted by cms.estimate".
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
    def __init__(self, size: float, num_buckets: int = 3, **kwargs) -> None:
        if num_buckets < 1:
            raise ValueError("num_buckets must be >= 1")
        self.num_buckets = int(num_buckets)
        self.top_k = int(kwargs.get("top_k", 10))
        self._tumbling = TumblingWindowAggregator(size, **kwargs)
        # FLOWCHART: "deque(maxlen=num_buckets)" keeps the last N buckets.
        self._buckets: Deque[WindowSummary] = deque(maxlen=self.num_buckets)

    def add(self, event: NetworkEvent) -> List[WindowSummary]:
        # FLOWCHART: "for each bucket closed by tumbling".
        emitted: List[WindowSummary] = []
        for bucket in self._tumbling.add(event):
            # FLOWCHART: "push into deque(maxlen=num_buckets)".
            self._buckets.append(bucket)
            # FLOWCHART: "merged = merge_summaries(last N buckets)".
            merged = merge_summaries(list(self._buckets), top_k=self.top_k)
            if merged is not None:
                # FLOWCHART: "emit merged if not None".
                emitted.append(merged)
        return emitted

    def current(self) -> Optional[WindowSummary]:
        # FLOWCHART: merge the buckets currently in the deque.
        return merge_summaries(list(self._buckets), top_k=self.top_k)

    def flush(self) -> List[WindowSummary]:
        # FLOWCHART: "flush()" -> close the tumbling bucket and merge the rest.
        for bucket in self._tumbling.flush():
            self._buckets.append(bucket)
        merged = merge_summaries(list(self._buckets), top_k=self.top_k)
        return [merged] if merged is not None else []


__all__ = [
    "TumblingWindowAggregator",
    "SlidingWindowAggregator",
    "merge_summaries",
]
