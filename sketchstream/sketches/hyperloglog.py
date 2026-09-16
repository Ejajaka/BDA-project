"""HyperLogLog (Flajolet et al., 2007) with HLL++-style small-range handling.

Estimates the number of *distinct* elements (cardinality) in a stream using
O(m log log n) bits, where m = 2^precision registers. Relative standard error
is approximately ``1.04 / sqrt(m)``.

We hash into 128 bits: the first ``precision`` bits select a register, the
remaining bits are used to compute the rank (position of the left-most 1).
Using 128-bit hashes makes the large-range correction unnecessary.
"""
from __future__ import annotations

import math

import numpy as np

from ..hashing import hash128


class HyperLogLog:
    def __init__(self, precision: int = 14, seed: int = 0) -> None:
        if not 4 <= precision <= 18:
            raise ValueError("precision must be in [4, 18] for uint8 registers")
        self.precision = int(precision)
        self.m = 1 << self.precision
        self.seed = int(seed)
        self.registers = np.zeros(self.m, dtype=np.uint8)
        self._alpha = self._alpha_for(self.m)
        self._max_bits = 128 - self.precision

    # ------------------------------------------------------------------ #
    @staticmethod
    def _alpha_for(m: int) -> float:
        if m == 16:
            return 0.673
        if m == 32:
            return 0.697
        if m == 64:
            return 0.709
        return 0.7213 / (1.0 + 1.079 / m)

    def _rho(self, w: int) -> int:
        """1 + number of leading zeros of ``w`` within the remaining bits."""
        if w == 0:
            return self._max_bits + 1
        return self._max_bits - w.bit_length() + 1

    # ------------------------------------------------------------------ #
    def add(self, key) -> None:
        h = hash128(key, self.seed)
        index = h & (self.m - 1)
        w = h >> self.precision
        rank = self._rho(w)
        if rank > self.registers[index]:
            self.registers[index] = rank

    def update(self, key) -> None:
        """Alias of :meth:`add` for readability in stream code."""
        self.add(key)

    def estimate(self) -> float:
        regs = self.registers.astype(np.float64)
        z = float(np.sum(np.exp2(-regs)))
        estimate = self._alpha * (self.m * self.m) / z
        # Small-range correction (linear counting) when many registers are empty.
        if estimate <= 2.5 * self.m:
            empty = int(np.count_nonzero(self.registers == 0))
            if empty > 0:
                estimate = self.m * math.log(self.m / empty)
        return estimate

    def merge(self, other: "HyperLogLog") -> "HyperLogLog":
        if self.precision != other.precision:
            raise ValueError("cannot merge HyperLogLogs of different precision")
        np.maximum(self.registers, other.registers, out=self.registers)
        return self

    # ------------------------------------------------------------------ #
    @property
    def n_bytes(self) -> int:
        return int(self.registers.nbytes)

    @property
    def standard_error(self) -> float:
        return 1.04 / math.sqrt(self.m)

    def clear(self) -> None:
        self.registers.fill(0)

    def __len__(self) -> int:
        return int(round(self.estimate()))

    def __bool__(self) -> bool:
        # Without this, an *empty* HLL (estimate 0) would be falsy.
        return True

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"HyperLogLog(precision={self.precision}, m={self.m}, "
            f"bytes={self.n_bytes}, est={self.estimate():.1f})"
        )


def hll_memory_bytes(precision: int) -> int:
    return (1 << int(precision)) * 1  # 1 byte per register


__all__ = ["HyperLogLog", "hll_memory_bytes"]
