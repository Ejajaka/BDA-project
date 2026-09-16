"""Count-Min Sketch (Cormode & Muthukrishnan, 2003/2005).

Approximate frequency estimation over a stream in sub-linear, *fixed* memory.

Guarantee (with width = ceil(e / eps) and depth = ceil(ln(1/delta))):
    true_count <= estimate <= true_count + eps * N   with prob. >= 1 - delta
where N is the total number of streamed items.

The estimate never *under*-counts, because of hash collisions it can only
over-count. Taking the minimum across rows minimises that over-count.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np

from ..hashing import _MASK64, hash128


class CountMinSketch:
    def __init__(self, width: int, depth: int, seed: int = 0) -> None:
        if width <= 0 or depth <= 0:
            raise ValueError("width and depth must be positive")
        self.width = int(width)
        self.depth = int(depth)
        self.seed = int(seed)
        self.table = np.zeros((self.depth, self.width), dtype=np.int64)
        self._rows = np.arange(self.depth)

    # ------------------------------------------------------------------ #
    # construction helpers
    # ------------------------------------------------------------------ #
    @classmethod
    def from_error(cls, epsilon: float, delta: float, seed: int = 0) -> "CountMinSketch":
        """Size the sketch for additive error ``epsilon`` w.p. ``1 - delta``."""
        if not 0.0 < epsilon < 1.0:
            raise ValueError("epsilon must be in (0, 1)")
        if not 0.0 < delta < 1.0:
            raise ValueError("delta must be in (0, 1)")
        width = int(math.ceil(math.e / epsilon))
        depth = int(math.ceil(math.log(1.0 / delta)))
        return cls(width, depth, seed=seed)

    # ------------------------------------------------------------------ #
    # core operations
    # ------------------------------------------------------------------ #
    def _indices(self, key) -> np.ndarray:
        # Hash once, derive all `depth` indices via enhanced double hashing.
        h = hash128(key, self.seed)
        h1 = h & _MASK64
        h2 = ((h >> 64) | 1) & _MASK64
        return np.fromiter(
            ((h1 + i * h2) % self.width for i in range(self.depth)),
            dtype=np.int64,
            count=self.depth,
        )

    def add(self, key, count: int = 1, conservative: bool = False) -> None:
        """Insert ``count`` occurrences of ``key``.

        ``conservative=True`` enables the conservative-update variant, which
        reduces over-estimation (but makes the sketch non-linear / not
        mergeable by plain addition).
        """
        if count < 0:
            raise ValueError("Count-Min Sketch only supports non-negative updates")
        idx = self._indices(key)
        if conservative and count > 0:
            target = int(self.table[self._rows, idx].min()) + count
            below = self.table[self._rows, idx] < target
            self.table[self._rows[below], idx[below]] = target
        else:
            self.table[self._rows, idx] += count

    def estimate(self, key) -> int:
        """Return an upper-bound estimate of the true frequency of ``key``."""
        idx = self._indices(key)
        return int(self.table[self._rows, idx].min())

    def merge(self, other: "CountMinSketch") -> "CountMinSketch":
        """In-place union (only valid for non-conservative sketches)."""
        if (self.width, self.depth) != (other.width, other.depth):
            raise ValueError("cannot merge sketches of different dimensions")
        self.table += other.table
        return self

    # ------------------------------------------------------------------ #
    # diagnostics
    # ------------------------------------------------------------------ #
    @property
    def n_cells(self) -> int:
        return self.width * self.depth

    @property
    def n_bytes(self) -> int:
        return int(self.table.nbytes)

    def clear(self) -> None:
        self.table.fill(0)

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"CountMinSketch(width={self.width}, depth={self.depth}, "
            f"bytes={self.n_bytes})"
        )


def cms_memory_bytes(width: int, depth: int) -> int:
    """Theoretical memory of a CMS (int64 counters)."""
    return int(width) * int(depth) * 8


def cms_expected_error(width: int, depth: int, n: int) -> float:
    """Expected additive over-estimation error eps * N (upper bound)."""
    return (math.e / width) * n


__all__ = ["CountMinSketch", "cms_memory_bytes", "cms_expected_error", "Optional"]
