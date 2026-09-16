"""Space-Saving algorithm for heavy-hitter detection (Metwally et al., 2005).

Maintains at most ``capacity`` candidates and guarantees that any item whose
true frequency exceeds ``N / capacity`` is reported (the "frequent" guarantee).
For a tracked key the reported counter is an *upper bound*; ``error`` is the
maximum possible over-estimate, so ``counter - error`` is a lower bound.

A lazy min-heap keeps replacement O(log capacity) instead of a linear scan.
"""
from __future__ import annotations

import heapq
from typing import Dict, List, Optional, Tuple


class SpaceSaving:
    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = int(capacity)
        self.counts: Dict[str, int] = {}
        self.errors: Dict[str, int] = {}
        self._heap: List[Tuple[int, str]] = []

    # ------------------------------------------------------------------ #
    def add(self, key, count: int = 1) -> None:
        if count <= 0:
            raise ValueError("count must be positive")
        if key in self.counts:
            self.counts[key] += count
            heapq.heappush(self._heap, (self.counts[key], key))
            return
        if len(self.counts) < self.capacity:
            self.counts[key] = count
            self.errors[key] = 0
            heapq.heappush(self._heap, (count, key))
            return
        # Replace the element with the smallest counter.
        min_count, min_key = self._pop_min()
        del self.counts[min_key]
        del self.errors[min_key]
        self.counts[key] = min_count + count
        self.errors[key] = min_count
        heapq.heappush(self._heap, (self.counts[key], key))

    def update(self, key, count: int = 1) -> None:
        self.add(key, count)

    def _pop_min(self) -> Tuple[int, str]:
        while self._heap:
            count, key = self._heap[0]
            if self.counts.get(key) == count:
                heapq.heappop(self._heap)
                return count, key
            heapq.heappop(self._heap)  # stale entry
        raise RuntimeError("heap emptied unexpectedly")

    # ------------------------------------------------------------------ #
    def estimate(self, key) -> Optional[int]:
        """Upper-bound count, or ``None`` if the key is not tracked."""
        return self.counts.get(key)

    def lower_bound(self, key) -> Optional[int]:
        if key not in self.counts:
            return None
        return self.counts[key] - self.errors[key]

    def top_k(self, k: int) -> List[Tuple[str, int]]:
        items = sorted(self.counts.items(), key=lambda kv: kv[1], reverse=True)
        return items[:k]

    def merge(self, other: "SpaceSaving") -> "SpaceSaving":
        """Approximate merge by re-inserting the other's candidates.

        Space-Saving is not natively mergeable, so this is an approximation
        used only for sliding-window heavy-hitter candidates.
        """
        for key, count in other.counts.items():
            self.add(key, count)
        return self

    # ------------------------------------------------------------------ #
    @property
    def n_tracked(self) -> int:
        return len(self.counts)

    def clear(self) -> None:
        self.counts.clear()
        self.errors.clear()
        self._heap.clear()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"SpaceSaving(capacity={self.capacity}, tracked={self.n_tracked})"


__all__ = ["SpaceSaving"]
