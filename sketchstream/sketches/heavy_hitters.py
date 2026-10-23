# ---------------------------------------------------------------------------
# SPACE-SAVING HEAVY HITTERS (Metwally et al., 2005)
#
# Keeps at most `capacity` candidate keys (e.g. source IPs) and guarantees
# that any key whose true frequency exceeds N / capacity is reported. Each
# tracked key stores:
#   counts[key] = estimated frequency (an UPPER bound)
#   errors[key] = maximum over-estimate, so counts - errors is a LOWER bound
#
# A lazy min-heap finds the smallest counter in O(log capacity); stale heap
# entries are discarded when they reach the top. This avoids scanning all
# capacity counters every time a new key arrives.
#
# FLOWCHART
# ---------
#   add(key, count)
#          |
#          v
#   key already tracked?
#      yes |                          no |
#          v                             v
#   counts[key] += count          room left? (len < capacity)
#   push (new count, key)            yes |              no |
#          |                             v                v
#          |                     counts[key] = count   pop valid min (min_count, min_key)
#          |                     errors[key] = 0       delete min_key
#          |                     push (count, key)     counts[key] = min_count + count
#          |                                          errors[key] = min_count
#          |                                          push (count, key)
#          \_____________________________|______________/
#                                        v
#                                      done
#
#   _pop_min(): peek heap top (count, key)
#                   |
#                   v
#          counts[key] == count?
#             yes |          no |
#                 v             v
#          pop & return    pop stale entry, repeat
#
#   estimate(key)    -> counts[key]                 (upper bound, None if untracked)
#   lower_bound(key) -> counts[key] - errors[key]   (None if untracked)
#   top_k(k)         -> keys sorted by count, descending
#   merge(other)     -> add every (key, count) from other (approximate)
# ---------------------------------------------------------------------------
from __future__ import annotations

import heapq
from typing import Dict, List, Optional, Tuple


class SpaceSaving:
    def __init__(self, capacity: int) -> None:
        # FLOWCHART: capacity = number of counters we are allowed to keep.
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = int(capacity)
        self.counts: Dict[str, int] = {}
        self.errors: Dict[str, int] = {}
        # FLOWCHART: lazy min-heap of (count, key); may contain stale entries.
        self._heap: List[Tuple[int, str]] = []

    def add(self, key, count: int = 1) -> None:
        # FLOWCHART: "add(key, count)".
        if count <= 0:
            raise ValueError("count must be positive")
        if key in self.counts:
            # FLOWCHART: "key already tracked?" -> yes: increment and push.
            self.counts[key] += count
            heapq.heappush(self._heap, (self.counts[key], key))
            return
        if len(self.counts) < self.capacity:
            # FLOWCHART: "room left?" -> yes: insert with zero error.
            self.counts[key] = count
            self.errors[key] = 0
            heapq.heappush(self._heap, (count, key))
            return
        # FLOWCHART: "room left?" -> no: evict the valid minimum.
        min_count, min_key = self._pop_min()
        del self.counts[min_key]
        del self.errors[min_key]
        # FLOWCHART: new key inherits the evicted minimum's count + its own.
        self.counts[key] = min_count + count
        # FLOWCHART: the inherited amount is the worst-case over-estimate.
        self.errors[key] = min_count
        heapq.heappush(self._heap, (self.counts[key], key))

    def update(self, key, count: int = 1) -> None:
        self.add(key, count)

    def _pop_min(self) -> Tuple[int, str]:
        # FLOWCHART: "_pop_min(): peek the heap top".
        while self._heap:
            count, key = self._heap[0]
            if self.counts.get(key) == count:
                # FLOWCHART: "counts[key] == count?" -> yes: pop & return.
                heapq.heappop(self._heap)
                return count, key
            # FLOWCHART: stale entry -> discard it and repeat.
            heapq.heappop(self._heap)
        raise RuntimeError("heap emptied unexpectedly")

    def estimate(self, key) -> Optional[int]:
        # FLOWCHART: "estimate(key) -> counts[key]" (upper bound).
        return self.counts.get(key)

    def lower_bound(self, key) -> Optional[int]:
        # FLOWCHART: "lower_bound(key) -> counts[key] - errors[key]".
        if key not in self.counts:
            return None
        return self.counts[key] - self.errors[key]

    def top_k(self, k: int) -> List[Tuple[str, int]]:
        # FLOWCHART: "top_k(k) -> sort by count, descending".
        items = sorted(self.counts.items(), key=lambda kv: kv[1], reverse=True)
        return items[:k]

    def merge(self, other: "SpaceSaving") -> "SpaceSaving":
        # FLOWCHART: "merge(other) -> add every (key, count)" (approximate:
        # re-inserting candidates loses the original error bounds).
        for key, count in other.counts.items():
            self.add(key, count)
        return self

    @property
    def n_tracked(self) -> int:
        return len(self.counts)

    def clear(self) -> None:
        self.counts.clear()
        self.errors.clear()
        self._heap.clear()

    def __repr__(self) -> str:
        return f"SpaceSaving(capacity={self.capacity}, tracked={self.n_tracked})"


__all__ = ["SpaceSaving"]
