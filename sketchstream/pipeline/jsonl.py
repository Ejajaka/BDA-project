"""JSONL streaming output, consumed by the dashboard and (optionally) Kafka.

Record schema (one JSON object per line):

    {"type": "meta",   ...}
    {"type": "window", "index": 4, "start": 4.0, "end": 5.0, "total": 1023,
     "unique": 312.0, "top": [["10.0.0.1", 120]], "top_share": 0.12,
     "alert": true, "score": 6.1, "reason": "...", "detector": "adaptive_ewma"}
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from ..events import WindowSummary


def summary_to_record(
    summary: WindowSummary, alert=None, extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    record: Dict[str, Any] = {
        "type": "window",
        "index": summary.index,
        "start": summary.start,
        "end": summary.end,
        "total": summary.total,
        "unique": round(float(summary.unique_sources), 1),
        "top": [[k, int(c)] for k, c in summary.top_keys[:5]],
        "top_share": round(summary.top_share(), 4),
        "alert": alert is not None,
        "score": round(float(alert.score), 3) if alert is not None else None,
        "reason": alert.reason if alert is not None else None,
        "detector": alert.detector if alert is not None else None,
    }
    if extra:
        record.update(extra)
    return record


def dump_run(
    result,
    path: str,
    meta: Optional[Dict[str, Any]] = None,
    realtime: bool = False,
    speed: float = 1.0,
) -> None:
    """Write a completed local run to JSONL.

    With ``realtime=True`` records are written at wall-clock speed scaled by
    ``speed`` (speed=10 replays 10x faster than the original stream), which is
    what makes the dashboard animate during a live demo.
    """
    alerts_by_index = {a.window_index: a for a in result.alerts}
    with open(path, "w", encoding="utf-8") as fh:
        if meta:
            fh.write(json.dumps({"type": "meta", **meta}) + "\n")
            fh.flush()
        prev_end: Optional[float] = None
        for summary in result.summaries:
            record = summary_to_record(summary, alerts_by_index.get(summary.index))
            fh.write(json.dumps(record) + "\n")
            fh.flush()
            if realtime:
                if prev_end is not None and speed > 0:
                    delay = (summary.end - prev_end) / speed
                    if delay > 0:
                        time.sleep(delay)
                prev_end = summary.end


__all__ = ["summary_to_record", "dump_run"]
