"""Pipeline components."""
from __future__ import annotations

from .jsonl import dump_run, summary_to_record
from .local import LocalRunResult, run_local_pipeline

__all__ = [
    "LocalRunResult",
    "run_local_pipeline",
    "dump_run",
    "summary_to_record",
]
