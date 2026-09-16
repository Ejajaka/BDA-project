"""Core data types shared across the pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

BENIGN = "BENIGN"


@dataclass(frozen=True)
class NetworkEvent:
    """A single network flow / log record.

    ``ts`` is an event-time timestamp in seconds (float). ``label`` carries the
    ground truth when available (``"BENIGN"`` or an attack name).
    """

    ts: float
    src_ip: str
    dst_ip: str = ""
    dst_port: int = 0
    protocol: str = ""
    packets: int = 1
    nbytes: int = 0
    label: str = BENIGN

    @property
    def is_attack(self) -> bool:
        return self.label != BENIGN

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "NetworkEvent":
        return cls(
            ts=float(d.get("ts", d.get("timestamp", 0.0))),
            src_ip=str(d.get("src_ip", d.get("Source IP", ""))),
            dst_ip=str(d.get("dst_ip", d.get("Destination IP", ""))),
            dst_port=int(d.get("dst_port", d.get("Destination Port", 0)) or 0),
            protocol=str(d.get("protocol", d.get("Protocol", ""))),
            packets=int(d.get("packets", 1) or 1),
            nbytes=int(d.get("nbytes", d.get("bytes", 0)) or 0),
            label=str(d.get("label", BENIGN)),
        )


@dataclass
class WindowSummary:
    """Aggregated statistics for one time window (tumbling or sliding)."""

    index: int
    start: float
    end: float
    total: int
    unique_sources: float
    top_keys: List[Tuple[str, int]] = field(default_factory=list)
    cms: Optional[Any] = None
    hll: Optional[Any] = None
    heavy: Optional[Any] = None

    @property
    def rate(self) -> float:
        span = max(self.end - self.start, 1e-9)
        return self.total / span

    def top_share(self) -> float:
        """Fraction of the window's traffic attributable to the top key."""
        if not self.top_keys or self.total <= 0:
            return 0.0
        return self.top_keys[0][1] / self.total


__all__ = ["NetworkEvent", "WindowSummary", "BENIGN"]
