"""sketchstream: memory-efficient streaming analytics with probabilistic sketches.

A from-scratch, dependency-light toolkit for the project:
"Memory-Efficient Real-Time Network Anomaly Detection Using Probabilistic
Sketches in Distributed Data Streams".

Main building blocks
--------------------
- ``sketchstream.sketches``   : CountMinSketch, HyperLogLog, SpaceSaving
- ``sketchstream.counting``   : exact (hashmap) baseline for comparison
- ``sketchstream.windows``    : event-time tumbling + sliding window aggregation
- ``sketchstream.detector``   : fixed-threshold, adaptive (EWMA z-score) and CUSUM
- ``sketchstream.generator``  : reproducible synthetic traffic generation
- ``sketchstream.pipeline``   : local end-to-end runner
- ``sketchstream.metrics``    : error, detection quality and latency metrics
"""
from __future__ import annotations

__version__ = "0.1.0"

from .hashing import hash128, hash64, double_hashing
from .sketches import CountMinSketch, HyperLogLog, SpaceSaving
from .counting import ExactCounter

__all__ = [
    "__version__",
    "hash128",
    "hash64",
    "double_hashing",
    "CountMinSketch",
    "HyperLogLog",
    "SpaceSaving",
    "ExactCounter",
]
