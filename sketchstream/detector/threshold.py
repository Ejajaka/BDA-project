"""Naive fixed-threshold detector (Baseline 2).

Alerts whenever a window's total (or rate) exceeds a hard-coded threshold.
This is the baseline the adaptive detector must beat under changing traffic.
"""
from __future__ import annotations

from typing import Optional

from ..events import WindowSummary
from .base import Alert, Detector


class FixedThresholdDetector(Detector):
    name = "fixed_threshold"

    def __init__(self, threshold: float, use_rate: bool = False) -> None:
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        self.threshold = float(threshold)
        self.use_rate = bool(use_rate)

    def _value(self, summary: WindowSummary) -> float:
        return summary.rate if self.use_rate else float(summary.total)

    def observe(self, summary: WindowSummary) -> Optional[Alert]:
        value = self._value(summary)
        if value > self.threshold:
            return Alert(
                window_index=summary.index,
                window_end=summary.end,
                score=value / self.threshold,
                detector=self.name,
                reason=f"total {value:.0f} > threshold {self.threshold:.0f}",
                details={"total": summary.total, "threshold": self.threshold},
            )
        return None


__all__ = ["FixedThresholdDetector"]
