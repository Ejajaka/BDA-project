"""Reproducible synthetic network-traffic generator.

Produces a time-ordered stream of :class:`NetworkEvent` with controllable
normal behaviour (Poisson arrivals + Zipf-distributed sources, i.e. natural
heavy hitters) and injected anomalies:

- ``sudden``       : rate multiplied instantly (DDoS-like)
- ``gradual``      : rate ramps up linearly over the interval
- ``target``       : concentrate the anomaly on one source IP (heavy hitter)

Sampling uses *thinning* so the time-varying arrival rate is exact.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional, Sequence, Tuple

from ..events import BENIGN, NetworkEvent


@dataclass
class AnomalySpec:
    start: float
    duration: float
    multiplier: float
    mode: str = "sudden"  # "sudden" | "gradual"
    target: Optional[str] = None  # source IP to concentrate traffic on
    label: str = "ANOMALY"

    def factor(self, t: float) -> float:
        """Rate multiplier at time ``t`` (1.0 when inactive)."""
        if not (self.start <= t < self.start + self.duration):
            return 1.0
        if self.mode == "gradual":
            progress = min(max((t - self.start) / self.duration, 0.0), 1.0)
            return 1.0 + (self.multiplier - 1.0) * progress
        return float(self.multiplier)

    def interval(self) -> Tuple[float, float]:
        return (self.start, self.start + self.duration)


@dataclass
class SyntheticConfig:
    duration: float = 600.0
    base_rate: float = 1000.0  # events / second during normal operation
    n_sources: int = 500
    zipf: float = 1.2
    seed: int = 42
    anomalies: List[AnomalySpec] = field(default_factory=list)
    dst_ports: Sequence[int] = (80, 443, 22, 53, 8080)
    protocols: Sequence[str] = ("TCP", "UDP")
    server_ip: str = "10.0.0.1"


def _source_pool(n: int) -> List[str]:
    return [f"10.0.{i // 256}.{i % 256}" for i in range(n)]


def generate(config: SyntheticConfig):
    """Return ``(events, anomaly_intervals)``.

    ``events`` are sorted by event time; ``anomaly_intervals`` is a list of
    ``(start, end)`` tuples describing ground-truth anomaly periods.
    """
    rng = random.Random(config.seed)
    sources = _source_pool(config.n_sources)
    weights = [1.0 / ((i + 1) ** config.zipf) for i in range(len(sources))]

    extra_max = sum(max(0.0, a.multiplier - 1.0) for a in config.anomalies)
    lam_max = max(config.base_rate * (1.0 + extra_max), 1e-9)

    events: List[NetworkEvent] = []
    t = 0.0
    while True:
        t += rng.expovariate(lam_max)
        if t >= config.duration:
            break

        active = [(a, a.factor(t) - 1.0) for a in config.anomalies]
        extra = sum(e for _, e in active if e > 0.0)
        lam = config.base_rate * (1.0 + extra)
        if rng.random() > lam / lam_max:  # thinning rejection
            continue

        if extra > 0.0 and rng.random() < extra / (1.0 + extra):
            # Attribute this event to one anomaly, weighted by its extra rate.
            positive = [(a, e) for a, e in active if e > 0.0]
            total = sum(e for _, e in positive)
            r = rng.random() * total
            chosen = positive[-1][0]
            for a, e in positive:
                r -= e
                if r <= 0.0:
                    chosen = a
                    break
            src = chosen.target or rng.choice(sources)
            label = chosen.label
        else:
            src = rng.choices(sources, weights=weights, k=1)[0]
            label = BENIGN

        events.append(
            NetworkEvent(
                ts=t,
                src_ip=src,
                dst_ip=config.server_ip,
                dst_port=rng.choice(config.dst_ports),
                protocol=rng.choice(config.protocols),
                packets=1,
                nbytes=rng.randint(64, 1500),
                label=label,
            )
        )

    intervals = [a.interval() for a in config.anomalies]
    return events, intervals


def build_scenario(
    name: str = "mixed",
    duration: float = 600.0,
    base_rate: float = 1000.0,
    seed: int = 42,
):
    """Convenience factory for common experiment scenarios."""
    name = name.lower()
    if name == "sudden":
        anomalies = [
            AnomalySpec(start=duration * 0.4, duration=duration * 0.1,
                        multiplier=10.0, mode="sudden", target="192.168.0.66")
        ]
    elif name == "gradual":
        anomalies = [
            AnomalySpec(start=duration * 0.4, duration=duration * 0.3,
                        multiplier=10.0, mode="gradual")
        ]
    elif name == "heavy_hitter":
        anomalies = [
            AnomalySpec(start=duration * 0.4, duration=duration * 0.2,
                        multiplier=20.0, mode="sudden", target="192.168.0.66")
        ]
    elif name == "mixed":
        anomalies = [
            AnomalySpec(start=duration * 0.25, duration=duration * 0.08,
                        multiplier=5.0, mode="sudden"),
            AnomalySpec(start=duration * 0.55, duration=duration * 0.15,
                        multiplier=15.0, mode="sudden", target="192.168.0.66"),
            AnomalySpec(start=duration * 0.8, duration=duration * 0.15,
                        multiplier=8.0, mode="gradual"),
        ]
    elif name == "none":
        anomalies = []
    else:
        raise ValueError(f"unknown scenario: {name!r}")

    config = SyntheticConfig(
        duration=duration, base_rate=base_rate, seed=seed, anomalies=anomalies
    )
    return generate(config)


__all__ = ["AnomalySpec", "SyntheticConfig", "generate", "build_scenario"]
