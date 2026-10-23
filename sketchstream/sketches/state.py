# ---------------------------------------------------------------------------
# MERGEABLE, SERIALISABLE WINDOW SKETCH STATE
#
# Fixed-size core used by the incremental (Flink) path. Instead of buffering
# every event in a window (O(window) memory), one SketchState is updated per
# event in O(1). Parallel subtasks / checkpoints combine partial states with:
#   CountMinSketch -> element-wise ADD
#   HyperLogLog    -> element-wise MAX
#   SpaceSaving    -> approximate candidate merge
# Memory therefore stays bounded regardless of window size.
#
# FLOWCHART
# ---------
#   add(key, count)
#          |
#          v
#   cms.add(key, count)   # frequencies
#   hll.add(key)          # distinct sources
#   hh.add(key, count)    # heavy-hitter candidates
#   total += count
#
#   merge(other)
#          |
#          v
#   same dimensions?
#      no |                     yes |
#         v                        v
#      error                cms.merge(other.cms)   (ADD)
#                           hll.merge(other.hll)   (MAX)
#                           hh.merge(other.hh)     (approximate)
#                           total += other.total
#
#   summary(...) -> WindowSummary(total, unique=hll.estimate(),
#                                 top_keys=hh.top_k(), cms/hll/heavy refs)
#
#   to_dict()   -> dims + seed + total + cms table + hll registers + hh maps
#   from_dict() -> restore arrays, then rebuild the lazy heap from counts
# ---------------------------------------------------------------------------
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ..events import WindowSummary
from .count_min import CountMinSketch
from .heavy_hitters import SpaceSaving
from .hyperloglog import HyperLogLog


class SketchState:
    def __init__(
        self,
        cms_width: int = 2000,
        cms_depth: int = 5,
        hll_precision: int = 12,
        hh_capacity: int = 200,
        seed: int = 0,
    ) -> None:
        self.cms_width = int(cms_width)
        self.cms_depth = int(cms_depth)
        self.hll_precision = int(hll_precision)
        self.hh_capacity = int(hh_capacity)
        self.seed = int(seed)
        self.reset()

    def reset(self) -> None:
        # FLOWCHART: start with empty sketches and total = 0.
        self.cms = CountMinSketch(self.cms_width, self.cms_depth, self.seed)
        self.hll = HyperLogLog(self.hll_precision, self.seed)
        self.hh = SpaceSaving(self.hh_capacity)
        self.total = 0

    def add(self, key, count: int = 1) -> None:
        # FLOWCHART: "add(key, count)" -> fold one event into every sketch.
        self.cms.add(key, count)   # frequencies
        self.hll.add(key)          # distinct sources
        self.hh.add(key, count)    # heavy-hitter candidates
        self.total += count        # events in this window

    def merge(self, other: "SketchState") -> "SketchState":
        # FLOWCHART: "same dimensions?" -> no: reject the merge.
        if (self.cms_width, self.cms_depth, self.hll_precision) != (
            other.cms_width,
            other.cms_depth,
            other.hll_precision,
        ):
            raise ValueError("cannot merge SketchState of different dimensions")
        # FLOWCHART: "yes": merge each sketch with its own rule.
        self.cms.merge(other.cms)   # ADD
        self.hll.merge(other.hll)   # MAX
        self.hh.merge(other.hh)     # approximate
        self.total += other.total
        return self

    @classmethod
    def merged(cls, states: List["SketchState"]) -> "SketchState":
        # FLOWCHART: copy the first state, then merge the rest into it.
        if not states:
            raise ValueError("no states to merge")
        base = states[0].copy()
        for other in states[1:]:
            base.merge(other)
        return base

    def summary(
        self, index: int, start: float, end: float, top_k: int = 10
    ) -> WindowSummary:
        # FLOWCHART: "summary(...) -> WindowSummary".
        return WindowSummary(
            index=index,
            start=start,
            end=end,
            total=self.total,
            unique_sources=self.hll.estimate(),
            top_keys=self.hh.top_k(top_k),
            cms=self.cms,
            hll=self.hll,
            heavy=self.hh,
        )

    @property
    def n_bytes(self) -> int:
        return self.cms.n_bytes + self.hll.n_bytes + self.hh_capacity * 64

    def to_dict(self) -> Dict[str, Any]:
        # FLOWCHART: "to_dict()" -> dims + seed + total + sketch arrays/maps.
        return {
            "cms_width": self.cms_width,
            "cms_depth": self.cms_depth,
            "hll_precision": self.hll_precision,
            "hh_capacity": self.hh_capacity,
            "seed": self.seed,
            "total": self.total,
            "cms": self.cms.table.tolist(),
            "hll": self.hll.registers.tolist(),
            "hh_counts": dict(self.hh.counts),
            "hh_errors": dict(self.hh.errors),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SketchState":
        # FLOWCHART: "from_dict()" -> rebuild the state container.
        import numpy as np

        state = cls(
            cms_width=data["cms_width"],
            cms_depth=data["cms_depth"],
            hll_precision=data["hll_precision"],
            hh_capacity=data["hh_capacity"],
            seed=data.get("seed", 0),
        )
        # FLOWCHART: restore the saved arrays and heavy-hitter maps.
        state.cms.table = np.asarray(data["cms"], dtype=np.int64)
        state.hll.registers = np.asarray(data["hll"], dtype=np.uint8)
        state.hh.counts = {k: int(v) for k, v in data["hh_counts"].items()}
        state.hh.errors = {k: int(v) for k, v in data["hh_errors"].items()}
        state.total = int(data["total"])
        # FLOWCHART: rebuild the lazy heap from the restored counts.
        import heapq

        state.hh._heap = [(c, k) for k, c in state.hh.counts.items()]
        heapq.heapify(state.hh._heap)
        return state

    def copy(self) -> "SketchState":
        return SketchState.from_dict(self.to_dict())

    def top_k(self, k: int) -> List[Tuple[str, int]]:
        return self.hh.top_k(k)

    def __repr__(self) -> str:
        return (
            f"SketchState(total={self.total}, unique={self.hll.estimate():.0f}, "
            f"bytes={self.n_bytes})"
        )


__all__ = ["SketchState"]
