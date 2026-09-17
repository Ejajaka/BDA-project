"""Streamlit dashboard for the sketchstream anomaly detector.

It tails the JSONL produced by ``scripts/demo_stream.py`` (or by the Flink job)
and animates the live state:

    requests/sec | unique sources | top IP | memory | NORMAL/ANOMALY + heavy hitters

Run:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="sketchstream - network anomaly detection", layout="wide")


def read_all(path: str) -> Tuple[Dict, List[Dict]]:
    meta: Dict = {}
    rows: List[Dict] = []
    if not os.path.exists(path):
        return meta, rows
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("type") == "meta":
                meta = rec
            elif rec.get("type") == "window":
                rows.append(rec)
    return meta, rows


def main() -> None:
    st.title("sketchstream - real-time network anomaly detection")
    st.caption(
        "Count-Min Sketch + HyperLogLog + Space-Saving heavy hitters + adaptive "
        "detection, over event-time windows."
    )

    with st.sidebar:
        st.header("Controls")
        path = st.text_input("stream file", "results/stream.jsonl")
        speed = st.slider("replay speed (x real-time)", 1, 200, 20)
        replay = st.button("Replay live")
        st.markdown(
            "Generate a stream first:\n\n"
            "`python scripts/demo_stream.py --scenario mixed --realtime --speed 20`"
        )

    meta, rows = read_all(path)
    if not rows:
        st.warning(
            f"No window records found in `{path}`. Run scripts/demo_stream.py first."
        )
        return

    # ----- header metrics -------------------------------------------------- #
    c1, c2, c3, c4, c5 = st.columns(5)
    m_total = c1.empty()
    m_unique = c2.empty()
    m_top = c3.empty()
    m_status = c4.empty()
    m_mem = c5.empty()

    status_box = st.empty()
    st.subheader("Traffic rate (events / window)")
    chart_total = st.empty()
    st.subheader("Distinct sources (HyperLogLog estimate)")
    chart_unique = st.empty()
    st.subheader("Heavy hitters in the latest window")
    table_top = st.empty()

    def render(df: pd.DataFrame, current: Dict) -> None:
        m_total.metric("requests / window", f"{current['total']:,}")
        m_unique.metric("unique sources", f"{current['unique']:,.0f}")
        top = current.get("top") or []
        if top:
            m_top.metric(f"top IP ({current['top_share']:.0%})", top[0][0],
                         f"{top[0][1]:,}")
        else:
            m_top.metric("top IP", "-")
        if current.get("alert"):
            m_status.metric("status", "ANOMALY", f"z={current.get('score')}")
        else:
            m_status.metric("status", "NORMAL")
        if meta:
            exact = meta.get("exact_bytes", 0)
            sketch = meta.get("sketch_bytes", 0)
            m_mem.metric(
                "memory: sketches",
                f"{sketch/1e3:,.1f} KB",
                f"vs exact {exact/1e6:,.2f} MB",
                delta_color="off",
            )
        if current.get("alert"):
            status_box.error(
                f"ALERT @ window {current['index']} (t={current['end']:.0f}s): "
                f"{current.get('reason')}"
            )
        else:
            status_box.success(f"NORMAL @ window {current['index']} (t={current['end']:.0f}s)")

        idx = df.set_index("index")
        chart_total.line_chart(idx["total"])
        chart_unique.line_chart(idx["unique"])
        if top:
            table_top.dataframe(
                pd.DataFrame(top, columns=["source IP", "estimated count"]),
                width="stretch",
            )

    if not replay:
        df = pd.DataFrame(rows)
        render(df, rows[-1])
        return

    # ----- live replay ----------------------------------------------------- #
    history: List[Dict] = []
    prev_end = None
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("type") != "window":
                continue
            history.append(rec)
            render(pd.DataFrame(history), rec)
            if prev_end is not None and speed > 0:
                delay = max(0.0, (rec["end"] - prev_end) / speed)
                time.sleep(min(delay, 2.0))
            prev_end = rec["end"]


if __name__ == "__main__":
    main()
