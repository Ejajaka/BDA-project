"""Exact hashmap counter: the memory-hungry baseline.

Provides ground-truth frequencies and exact cardinality so we can quantify the
accuracy/memory trade-off of the sketches.
"""
from __future__ import annotations

import sys
from typing import Dict, List, Tuple


def deep_getsizeof(obj, _seen: set | None = None) -> int:
    """Recursively estimate the memory footprint of a container (bytes).

    Repeated (interned) objects are counted only once.
    """
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return 0
    _seen.add(obj_id)

    size = sys.getsizeof(obj, 0)
    if isinstance(obj, dict):
        size += sum(
            deep_getsizeof(k, _seen) + deep_getsizeof(v, _seen)
            for k, v in obj.items()
        )
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(deep_getsizeof(item, _seen) for item in obj)
    return size


class ExactCounter:
    """Exact per-key frequency counter with measured memory."""

    def __init__(self) -> None:
        self.counts: Dict[str, int] = {}

    def add(self, key, count: int = 1) -> None:
        self.counts[key] = self.counts.get(key, 0) + count

    def update(self, key, count: int = 1) -> None:
        self.add(key, count)

    def estimate(self, key) -> int:
        return self.counts.get(key, 0)

    def unique(self) -> int:
        return len(self.counts)

    def total(self) -> int:
        return sum(self.counts.values())

    def top_k(self, k: int) -> List[Tuple[str, int]]:
        return sorted(self.counts.items(), key=lambda kv: kv[1], reverse=True)[:k]

    @property
    def n_bytes(self) -> int:
        return deep_getsizeof(self.counts)

    def clear(self) -> None:
        self.counts.clear()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"ExactCounter(unique={self.unique()}, bytes={self.n_bytes})"


__all__ = ["ExactCounter", "deep_getsizeof"]
