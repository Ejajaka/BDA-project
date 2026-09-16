"""Loader for CIC-IDS2017 "TrafficLabelling" CSV files.

Use the *TrafficLabelling* / *GeneratedLabelledFlows* release (the one that
still contains ``Source IP``, ``Destination IP`` and ``Timestamp``), NOT the
``MachineLearningCVE`` release (from which IPs and timestamps were removed).

Known data-quality issues handled here:
- column names differ in case / leading-trailing spaces across day files
- ``Timestamp`` may or may not include seconds ("dd/MM/yyyy HH:mm[:ss]")
- numeric columns contain ``NaN`` and ``Infinity``
- labels include benign (``BENIGN``) and several attack families

The function returns time-ordered ``NetworkEvent`` objects with ``ts`` as
seconds relative to the first record (so streams are easy to replay).
"""
from __future__ import annotations

import glob
import math
import os
from typing import Iterable, Iterator, List, Optional, Sequence

import pandas as pd

from ..events import BENIGN, NetworkEvent

# Canonical -> possible raw (lower-cased, stripped) column names.
_COLUMN_ALIASES = {
    "src_ip": ("source ip", "src ip", "sourceip"),
    "dst_ip": ("destination ip", "dst ip", "destinationip"),
    "src_port": ("source port", "src port"),
    "dst_port": ("destination port", "dst port"),
    "protocol": ("protocol",),
    "timestamp": ("timestamp",),
    "date": ("date", "flow date"),
    "time": ("time",),
    "label": ("label",),
    "bytes": ("flow bytes/s", "total length of fwd packets", "bytes"),
    "packets": ("total fwd packets", "packets", "flow packets/s"),
}

_PROTOCOL_MAP = {"6": "TCP", "17": "UDP", "1": "ICMP", "tcp": "TCP", "udp": "UDP"}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _find(cols: Iterable[str], aliases: Sequence[str]) -> Optional[str]:
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


def _parse_timestamps(df: pd.DataFrame) -> pd.Series:
    cols = set(df.columns)
    ts_col = _find(cols, _COLUMN_ALIASES["timestamp"])
    if ts_col is not None:
        ts = pd.to_datetime(df[ts_col], dayfirst=True, errors="coerce")
    else:
        date_col = _find(cols, _COLUMN_ALIASES["date"])
        time_col = _find(cols, _COLUMN_ALIASES["time"])
        if date_col is None or time_col is None:
            raise ValueError("could not locate timestamp/date/time columns")
        ts = pd.to_datetime(
            df[date_col].astype(str) + " " + df[time_col].astype(str),
            dayfirst=True,
            errors="coerce",
        )
    return ts


def _rows_to_events(df: pd.DataFrame, use_attack_labels: bool) -> List[NetworkEvent]:
    df = _normalise_columns(df)
    cols = set(df.columns)
    ts = _parse_timestamps(df)

    src_col = _find(cols, _COLUMN_ALIASES["src_ip"])
    dst_col = _find(cols, _COLUMN_ALIASES["dst_ip"])
    sport_col = _find(cols, _COLUMN_ALIASES["src_port"])
    dport_col = _find(cols, _COLUMN_ALIASES["dst_port"])
    proto_col = _find(cols, _COLUMN_ALIASES["protocol"])
    label_col = _find(cols, _COLUMN_ALIASES["label"])
    bytes_col = _find(cols, _COLUMN_ALIASES["bytes"])
    pkts_col = _find(cols, _COLUMN_ALIASES["packets"])

    valid = ts.notna()
    df = df[valid]
    ts = ts[valid]

    base = ts.min()
    events: List[NetworkEvent] = []
    for idx, row in df.iterrows():
        label = BENIGN
        if label_col is not None and use_attack_labels:
            raw = str(row.get(label_col, BENIGN)).strip()
            label = BENIGN if raw.upper().startswith("BENIGN") else raw
        proto = str(row.get(proto_col, "")).strip().lower() if proto_col else ""
        events.append(
            NetworkEvent(
                ts=(ts.loc[idx] - base).total_seconds(),
                src_ip=str(row.get(src_col, "")) if src_col else "",
                dst_ip=str(row.get(dst_col, "")) if dst_col else "",
                dst_port=_safe_int(row.get(dport_col)) if dport_col else 0,
                protocol=_PROTOCOL_MAP.get(proto, proto.upper()),
                packets=_safe_int(row.get(pkts_col), 1) if pkts_col else 1,
                nbytes=_safe_int(row.get(bytes_col), 0) if bytes_col else 0,
                label=label,
            )
        )
    events.sort(key=lambda e: e.ts)
    return events


def iter_cicids_events(
    paths: Sequence[str] | str,
    chunksize: int = 200_000,
    limit: Optional[int] = None,
    use_attack_labels: bool = True,
) -> Iterator[NetworkEvent]:
    """Stream events from one or more CIC-IDS2017 CSV files."""
    if isinstance(paths, str):
        paths = [paths]
    emitted = 0
    for path in paths:
        for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
            for event in _rows_to_events(chunk, use_attack_labels):
                yield event
                emitted += 1
                if limit is not None and emitted >= limit:
                    return


def load_cicids_events(
    paths: Sequence[str] | str,
    limit: Optional[int] = None,
    use_attack_labels: bool = True,
) -> List[NetworkEvent]:
    """Load (and merge) CIC-IDS2017 CSV file(s) into a list of events."""
    if isinstance(paths, str) and os.path.isdir(paths):
        paths = sorted(glob.glob(os.path.join(paths, "*.csv")))
    elif isinstance(paths, str):
        paths = [paths]
    events = list(
        iter_cicids_events(paths, limit=limit, use_attack_labels=use_attack_labels)
    )
    events.sort(key=lambda e: e.ts)
    return events


__all__ = ["load_cicids_events", "iter_cicids_events"]
