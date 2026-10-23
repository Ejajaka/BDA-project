# ---------------------------------------------------------------------------
# HYPERLOGLOG (Flajolet et al., 2007) with HLL++ small-range handling
#
# Estimates the number of DISTINCT keys (cardinality) with m = 2^precision
# one-byte registers. Relative standard error is about 1.04 / sqrt(m).
#
# A 128-bit hash is split into:
#   - low `precision` bits -> which register to update
#   - remaining high bits  -> rank = 1 + number of leading zeros
# A register keeps the maximum rank seen. 128-bit hashes are enough that the
# large-range correction is unnecessary.
#
# FLOWCHART
# ---------
#   init(precision)
#          |
#          v
#   m = 2^precision; registers = m zeros; alpha = correction; max_bits = 128-precision
#
#   add(key)
#          |
#          v
#   h = hash128(key, seed)
#          |
#          v
#   index = low `precision` bits of h
#   w     = h >> precision (remaining bits)
#   rank  = 1 + leading zeros of w (capped)
#          |
#          v
#   rank > registers[index]?
#      yes |                  no |
#          v                     v
#   registers[index] = rank    ignore (a rarer pattern was already seen)
#          |
#          v
#        done
#
#   estimate()
#          |
#          v
#   z   = sum(2^-register)
#   raw = alpha * m^2 / z
#          |
#          v
#   raw <= 2.5 * m?  (small range)
#      yes |                       no |
#          v                          v
#   empty = count(registers == 0)   return raw
#   raw = m * ln(m / empty)         (linear counting)
#          |
#          v
#       return raw
#
#   merge(other) -> registers = max(registers, other.registers)
# ---------------------------------------------------------------------------
from __future__ import annotations

import math

import numpy as np

from ..hashing import hash128


class HyperLogLog:
    def __init__(self, precision: int = 14, seed: int = 0) -> None:
        # FLOWCHART: "m = 2^precision".
        if not 4 <= precision <= 18:
            raise ValueError("precision must be in [4, 18] for uint8 registers")
        self.precision = int(precision)
        self.m = 1 << self.precision
        self.seed = int(seed)
        # FLOWCHART: "registers = m zeros".
        self.registers = np.zeros(self.m, dtype=np.uint8)
        # FLOWCHART: "alpha = correction".
        self._alpha = self._alpha_for(self.m)
        # FLOWCHART: "max_bits = 128 - precision".
        self._max_bits = 128 - self.precision

    @staticmethod
    def _alpha_for(m: int) -> float:
        # FLOWCHART: "alpha = correction" constant for this m.
        if m == 16:
            return 0.673
        if m == 32:
            return 0.697
        if m == 64:
            return 0.709
        return 0.7213 / (1.0 + 1.079 / m)

    def _rho(self, w: int) -> int:
        # FLOWCHART: "rank = 1 + leading zeros of w (capped)".
        if w == 0:
            return self._max_bits + 1
        # bit_length() is the position of the highest set bit. Subtracting it
        # from max_bits counts the leading zeros; +1 makes the rank 1-based.
        return self._max_bits - w.bit_length() + 1

    def add(self, key) -> None:
        # FLOWCHART: "h = hash128(key, seed)".
        h = hash128(key, self.seed)
        # FLOWCHART: "index = low precision bits of h".
        index = h & (self.m - 1)
        # FLOWCHART: "w = h >> precision".
        w = h >> self.precision
        # FLOWCHART: "rank = 1 + leading zeros of w".
        rank = self._rho(w)
        if rank > self.registers[index]:
            # FLOWCHART: "rank > registers[index]?" -> yes: keep the larger rank.
            self.registers[index] = rank

    def update(self, key) -> None:
        self.add(key)

    def estimate(self) -> float:
        # FLOWCHART: "z = sum(2^-register)".
        regs = self.registers.astype(np.float64)
        z = float(np.sum(np.exp2(-regs)))
        # FLOWCHART: "raw = alpha * m^2 / z".
        estimate = self._alpha * (self.m * self.m) / z
        # FLOWCHART: "raw <= 2.5 * m?" -> yes: small-range correction.
        if estimate <= 2.5 * self.m:
            # FLOWCHART: "empty = count(registers == 0)".
            empty = int(np.count_nonzero(self.registers == 0))
            if empty > 0:
                # FLOWCHART: "raw = m * ln(m / empty)" (linear counting).
                estimate = self.m * math.log(self.m / empty)
        return estimate

    def merge(self, other: "HyperLogLog") -> "HyperLogLog":
        # FLOWCHART: "merge(other) -> registers = max(...)".
        if self.precision != other.precision:
            raise ValueError("cannot merge HyperLogLogs of different precision")
        # The union of two streams keeps the maximum rank per register.
        np.maximum(self.registers, other.registers, out=self.registers)
        return self

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
        return True

    def __repr__(self) -> str:
        return (
            f"HyperLogLog(precision={self.precision}, m={self.m}, "
            f"bytes={self.n_bytes}, est={self.estimate():.1f})"
        )


def hll_memory_bytes(precision: int) -> int:
    return (1 << int(precision)) * 1


__all__ = ["HyperLogLog", "hll_memory_bytes"]
