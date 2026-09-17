"""Loader for UNSW-NB15 flow files (optional cross-dataset validation).

Use the *full feature* CSVs that still contain ``srcip`` / ``dstip`` / ``Stime``
(e.g. ``UNSW-NB15_1.csv`` from the UNSW Canberra dataset page). If your file is
the ML training/testing set (which has no IPs or timestamps), the loader still
works but source IPs become empty and event time is synthesised from row order
(controlled by ``synthetic_interval``).

Label handling: ``attack_cat`` (e.g. "Generic", "Exploits", ...) is used when
present; otherwise the binary ``label`` column (0 = benign, 1 = attack).
"""
from __future__ import annotations

import glob
import math
import os
from typing import Iterator, List, Optional, Sequence

import pandas as pd

from ..events import BENIGN, NetworkEvent

_COLUMN_ALIASES = {
    "src_ip": ("srcip", "src ip", "source ip", "sourceip"),
    "dst_ip": ("dstip", "dst ip", "destination ip", "destinationip"),
    "dst_port": ("dsport", "dst port", "destination port"),
    "protocol": ("proto", "protocol"),
    "stime": ("stime", "start time", "timestamp"),
    "label": ("label",),
    "attack_cat": ("attack_cat", "attack category"),
    "bytes": ("sbytes", "bytes"),
    "packets": ("spkts", "packets"),
}

_BENIGN_TOKENS = {"benign", "0", "normal", ""}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _find(cols, aliases) -> Optional[str]:
    for alias in aliases:
        if alias in cols:
            return alias
    return None


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return int(f)
    except (TypeError, ValueError):
        return default


def _rows_to_events(
    df: pd.DataFrame, start_row: int, synthetic_interval: float, use_attack_labels: bool
) -> List[NetworkEvent]:
    df = _normalise_columns(df)
    cols = set(df.columns)
    src_col = _find(cols, _COLUMN_ALIASES["src_ip"])
    dst_col = _find(cols, _COLUMN_ALIASES["dst_ip"])
    dport_col = _find(cols, _COLUMN_ALIASES["dst_port"])
    proto_col = _find(cols, _COLUMN_ALIASES["protocol"])
    stime_col = _find(cols, _COLUMN_ALIASES["stime"])
    label_col = _find(cols, _COLUMN_ALIASES["label"])
    cat_col = _find(cols, _COLUMN_ALIASES["attack_cat"])
    bytes_col = _find(cols, _COLUMN_ALIASES["bytes"])
    pkts_col = _find(cols, _COLUMN_ALIASES["packets"])

    events: List[NetworkEvent] = []
    for offset, (_, row) in enumerate(df.iterrows()):
        if stime_col is not None:
            ts_val = pd.to_numeric(row.get(stime_col), errors="coerce")
            ts = float(ts_val) if pd.notna(ts_val) else (start_row + offset) * synthetic_interval
        else:
            ts = (start_row + offset) * synthetic_interval

        label = BENIGN
        if use_attack_labels:
            raw = None
            if cat_col is not None:
                raw = str(row.get(cat_col, "")).strip()
            if (raw is None or raw.lower() in _BENIGN_TOKENS) and label_col is not None:
                raw = str(row.get(label_col, "")).strip()
            if raw is not None and raw.lower() not in _BENIGN_TOKENS:
                label = raw

        events.append(
            NetworkEvent(
                ts=ts,
                src_ip=str(row.get(src_col, "")) if src_col else "",
                dst_ip=str(row.get(dst_col, "")) if dst_col else "",
                dst_port=_safe_int(row.get(dport_col)) if dport_col else 0,
                protocol=str(row.get(proto_col, "")).strip() if proto_col else "",
                packets=_safe_int(row.get(pkts_col), 1) if pkts_col else 1,
                nbytes=_safe_int(row.get(bytes_col), 0) if bytes_col else 0,
                label=label,
            )
        )
    return events


def iter_unsw_events(
    paths: Sequence[str] | str,
    chunksize: int = 200_000,
    limit: Optional[int] = None,
    synthetic_interval: float = 0.001,
    use_attack_labels: bool = True,
) -> Iterator[NetworkEvent]:
    if isinstance(paths, str):
        paths = [paths]
    emitted = 0
    row_cursor = 0
    for path in paths:
        for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
            for event in _rows_to_events(
                chunk, row_cursor, synthetic_interval, use_attack_labels
            ):
                row_cursor += 1
                yield event
                emitted += 1
                if limit is not None and emitted >= limit:
                    return


def load_unsw_events(
    paths: Sequence[str] | str,
    limit: Optional[int] = None,
    synthetic_interval: float = 0.001,
    use_attack_labels: bool = True,
) -> List[NetworkEvent]:
    if isinstance(paths, str) and os.path.isdir(paths):
        paths = sorted(glob.glob(os.path.join(paths, "*.csv")))
    elif isinstance(paths, str):
        paths = [paths]
    events = list(
        iter_unsw_events(
            paths,
            limit=limit,
            synthetic_interval=synthetic_interval,
            use_attack_labels=use_attack_labels,
        )
    )
    events.sort(key=lambda e: e.ts)
    return events


__all__ = ["load_unsw_events", "iter_unsw_events"]
