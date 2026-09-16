"""Detector interface and alert type."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ..events import WindowSummary


@dataclass
class Alert:
    window_index: int
    window_end: float
    score: float
    detector: str
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)


class Detector:
    """Base class: consumes WindowSummary objects, emits Alerts."""

    name = "detector"

    def observe(self, summary: WindowSummary) -> Optional[Alert]:  # pragma: no cover
        raise NotImplementedError

    def reset(self) -> None:  # pragma: no cover - optional
        return None


__all__ = ["Alert", "Detector"]
