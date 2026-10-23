from __future__ import annotations

import math
from typing import Optional

import numpy as np

from ..hashing import _MASK64, hash128


class CountMinSketch:
    def __init__(self, width: int, depth: int, seed: int = 0) -> None:
        # FLOWCHART: "table = depth x width int64 zeros".
        if width <= 0 or depth <= 0:
            raise ValueError("width and depth must be positive")
        self.width = int(width)
        self.depth = int(depth)
        self.seed = int(seed)
        self.table = np.zeros((self.depth, self.width), dtype=np.int64)
        self._rows = np.arange(self.depth)

    @classmethod
    def from_error(cls, epsilon: float, delta: float, seed: int = 0) -> "CountMinSketch":
        # FLOWCHART: "from_error(epsilon, delta)".
        if not 0.0 < epsilon < 1.0:
            raise ValueError("epsilon must be in (0, 1)")
        if not 0.0 < delta < 1.0:
            raise ValueError("delta must be in (0, 1)")
        # FLOWCHART: "width = ceil(e / epsilon), depth = ceil(ln(1 / delta))".
        width = int(math.ceil(math.e / epsilon))
        depth = int(math.ceil(math.log(1.0 / delta)))
        return cls(width, depth, seed=seed)

    def _indices(self, key) -> np.ndarray:
        # FLOWCHART: "indices = _indices(key)".
        h = hash128(key, self.seed)
        # FLOWCHART: "h1 = low 64 bits".
        h1 = h & _MASK64
        # FLOWCHART: "h2 = high 64 bits, forced odd".
        h2 = ((h >> 64) | 1) & _MASK64
        # FLOWCHART: "idx_i = (h1 + i * h2) % width" for every row.
        return np.fromiter(
            ((h1 + i * h2) % self.width for i in range(self.depth)),
            dtype=np.int64,
            count=self.depth,
        )

    def add(self, key, count: int = 1, conservative: bool = False) -> None:
        # FLOWCHART: "add(key, count, conservative)".
        if count < 0:
            raise ValueError("Count-Min Sketch only supports non-negative updates")
        idx = self._indices(key)
        if conservative and count > 0:
            # FLOWCHART: "target = min(cells) + count".
            target = int(self.table[self._rows, idx].min()) + count
            # FLOWCHART: "raise only cells < target": cells already above the
            # new minimum are left alone, which reduces over-counting.
            below = self.table[self._rows, idx] < target
            self.table[self._rows[below], idx[below]] = target
        else:
            # FLOWCHART: "cells += count" (plain add).
            self.table[self._rows, idx] += count

    def estimate(self, key) -> int:
        # FLOWCHART: "estimate(key) -> min of the depth cells".
        idx = self._indices(key)
        return int(self.table[self._rows, idx].min())

    def merge(self, other: "CountMinSketch") -> "CountMinSketch":
        # FLOWCHART: "merge(other) -> table += other.table".
        if (self.width, self.depth) != (other.width, other.depth):
            raise ValueError("cannot merge sketches of different dimensions")
        # Adding tables is only valid without conservative updates, because
        # conservative update makes the sketch non-linear.
        self.table += other.table
        return self

    @property
    def n_cells(self) -> int:
        return self.width * self.depth

    @property
    def n_bytes(self) -> int:
        return int(self.table.nbytes)

    def clear(self) -> None:
        self.table.fill(0)

    def __repr__(self) -> str:
        return (
            f"CountMinSketch(width={self.width}, depth={self.depth}, "
            f"bytes={self.n_bytes})"
        )


def cms_memory_bytes(width: int, depth: int) -> int:
    return int(width) * int(depth) * 8


def cms_expected_error(width: int, depth: int, n: int) -> float:
    return (math.e / width) * n


__all__ = ["CountMinSketch", "cms_memory_bytes", "cms_expected_error", "Optional"]
