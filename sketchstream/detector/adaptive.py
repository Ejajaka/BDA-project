"""Adaptive anomaly detection (the project's main contribution).

Two online detectors that learn the *recent normal* traffic baseline instead of
using one fixed threshold:

1. :class:`AdaptiveDetector` - Exponentially Weighted Moving Average (EWMA) of
   the mean and variance, with a z-score decision rule. Well suited to sudden
   spikes.

2. :class:`CusumDetector` - one-sided CUSUM on the standardised deviation.
   Better at detecting *gradual* drifts that a z-score threshold misses.

Both work in log-space by default: Poisson-ish packet counts are naturally
multiplicative, so ``log1p`` makes the baseline variance well-behaved. A warmup
period seeds the baseline before any alert can fire.

Implementation note: the z-score of a sample is computed against the baseline
*before* that sample is folded in (otherwise the outlier inflates its own
mean/variance and hides itself - a classic bug).

Optionally, an alert can require corroboration by a heavy-hitter signal
(``require_hh`` / ``hh_share_threshold``), combining frequency-magnitude with a
concentration signal.
"""
from __future__ import annotations

import math
from typing import List, Optional, Tuple

import numpy as np

from ..events import WindowSummary
from .base import Alert, Detector


class _EwmaBaseline:
    """EWMA mean/variance tracker (West's / RiskMetrics recursion)."""

    def __init__(self, alpha: float = 0.2, warmup: int = 30) -> None:
        if not 0.0 < alpha < 1.0:
            raise ValueError("alpha must be in (0, 1)")
        if warmup < 2:
            raise ValueError("warmup must be >= 2")
        self.alpha = float(alpha)
        self.warmup = int(warmup)
        self.mean: Optional[float] = None
        self.var: float = 0.0
        self._buffer: List[float] = []

    @property
    def ready(self) -> bool:
        return self.mean is not None

    def update(self, x: float, min_std: float = 0.0) -> Tuple[bool, float, float]:
        """Fold ``x`` into the baseline.

        Returns ``(ready, z, std)`` where ``z`` / ``std`` describe ``x``
        relative to the baseline *before* ``x`` was incorporated. ``ready`` is
        False until the warmup period has completed.
        """
        if self.mean is None:
            self._buffer.append(x)
            if len(self._buffer) < self.warmup:
                return False, 0.0, 0.0
            arr = np.asarray(self._buffer, dtype=float)
            self.mean = float(arr.mean())
            self.var = float(arr.var())
            self._buffer = []
            return False, 0.0, 0.0

        std = max(math.sqrt(max(self.var, 0.0)), min_std)
        z = (x - self.mean) / std if std > 0 else 0.0

        prev = self.mean
        self.mean = self.alpha * x + (1.0 - self.alpha) * self.mean
        self.var = (1.0 - self.alpha) * (self.var + self.alpha * (x - prev) ** 2)
        return True, z, std


class AdaptiveDetector(Detector):
    name = "adaptive_ewma"

    def __init__(
        self,
        alpha: float = 0.2,
        z_threshold: float = 4.0,
        warmup: int = 30,
        min_std: float = 0.05,
        use_log: bool = True,
        hh_share_threshold: float = 0.0,
        require_hh: bool = False,
    ) -> None:
        self.alpha = float(alpha)
        self.z_threshold = float(z_threshold)
        self.use_log = bool(use_log)
        self.min_std = float(min_std)
        self.hh_share_threshold = float(hh_share_threshold)
        self.require_hh = bool(require_hh)
        self.baseline = _EwmaBaseline(alpha=alpha, warmup=warmup)
        self.n_seen = 0

    def _signal(self, summary: WindowSummary) -> float:
        value = float(summary.total)
        return math.log1p(value) if self.use_log else value

    def observe(self, summary: WindowSummary) -> Optional[Alert]:
        self.n_seen += 1
        x = self._signal(summary)
        ready, z, std = self.baseline.update(x, self.min_std)
        if not ready:
            return None
        top_share = summary.top_share()
        hh_ok = (not self.require_hh) or (top_share >= self.hh_share_threshold)
        if z >= self.z_threshold and hh_ok:
            return Alert(
                window_index=summary.index,
                window_end=summary.end,
                score=z,
                detector=self.name,
                reason=f"z={z:.2f} >= {self.z_threshold} (std={std:.3f})",
                details={
                    "total": summary.total,
                    "unique_sources": summary.unique_sources,
                    "top_share": top_share,
                    "mean": self.baseline.mean,
                    "std": std,
                    "z": z,
                },
            )
        return None


class CusumDetector(Detector):
    name = "adaptive_cusum"

    def __init__(
        self,
        k: float = 0.5,
        h: float = 6.0,
        alpha: float = 0.2,
        warmup: int = 30,
        min_std: float = 0.05,
        use_log: bool = True,
    ) -> None:
        self.k = float(k)
        self.h = float(h)
        self.use_log = bool(use_log)
        self.min_std = float(min_std)
        self.baseline = _EwmaBaseline(alpha=alpha, warmup=warmup)
        self.g_pos = 0.0
        self.g_neg = 0.0
        self.n_seen = 0

    def _signal(self, summary: WindowSummary) -> float:
        value = float(summary.total)
        return math.log1p(value) if self.use_log else value

    def observe(self, summary: WindowSummary) -> Optional[Alert]:
        self.n_seen += 1
        x = self._signal(summary)
        ready, z, std = self.baseline.update(x, self.min_std)
        if not ready:
            return None
        self.g_pos = max(0.0, self.g_pos + z - self.k)
        self.g_neg = max(0.0, self.g_neg - z - self.k)
        if self.g_pos >= self.h:
            score = self.g_pos
            self.g_pos = 0.0
            return Alert(
                window_index=summary.index,
                window_end=summary.end,
                score=score,
                detector=self.name,
                reason=f"CUSUM+ g={score:.2f} >= h={self.h}",
                details={"total": summary.total, "g": score, "std": std},
            )
        return None


__all__ = ["AdaptiveDetector", "CusumDetector", "_EwmaBaseline"]
