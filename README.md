# sketchstream — Memory-Efficient Real-Time Network Anomaly Detection

> **Project 3**: *Memory-Efficient Real-Time Network Anomaly Detection Using Probabilistic Sketches in Distributed Data Streams.*

A Kafka + Flink streaming system that summarises huge network-event streams with
tiny probabilistic data structures (**Count-Min Sketch**, **HyperLogLog**,
**Space-Saving heavy hitters**), aggregates them over **event-time windows**, and
flags **DDoS-like spikes / heavy-hitter behaviour** with an **adaptive detector**
instead of a fixed threshold.

The novelty is a *unified streaming framework + rigorous empirical evaluation* —
we do **not** claim to have invented CMS, HLL, Kafka, Flink or anomaly detection.

---

## Rubric mapping

| Criterion | Where it is satisfied |
|---|---|
| **CO1 – Foundations (2)** | `sketchstream/hashing.py`, `sketchstream/sketches/` (CMS, HLL, Space-Saving implemented from scratch, with error bounds in docstrings) |
| **CO2/3/4 – New Big Data tech (12)** | `docker-compose.yml` (Kafka KRaft + Flink + 2 TaskManagers), `jobs/flink_job.py` (PyFlink), `scripts/kafka_producer.py`, Apache DataSketches-compatible sketch design |
| **CO5 – Design & problem solving (16)** | Sliding-window merge design (no-decrement issue), adaptive EWMA/CUSUM detector, ablation, memory/accuracy/latency trade-off analysis, reproducible experiment harness |

---

## Architecture

```
 CIC-IDS2017 CSV  /  synthetic generator
              |
              v
        (preprocessor)  scripts/kafka_producer.py
              |
              v
        Apache Kafka  topic: "network-events" (4 partitions)
              |
              v
        Apache Flink  (jobs/flink_job.py)  OR  local runner (sketchstream.pipeline)
              |
      +-------+--------+
      |                |
      v                v
  CountMinSketch   HyperLogLog        SpaceSaving (heavy hitters)
   (per-key freq)   (distinct IPs)    (top talkers)
      |                |
      +-------+--------+
              v
     Event-time tumbling / sliding window
              v
     Adaptive detector (EWMA z-score + CUSUM)
              v
        Alerts  ->  Kafka "alerts"  /  JSONL
              v
        Streamlit dashboard (dashboard/app.py)
```

---

## Repository layout

```
sketchstream/            core library (pure Python, no containers needed)
  hashing.py             reproducible 128-bit hashing + double hashing
  events.py              NetworkEvent, WindowSummary
  windows.py             tumbling + sliding aggregation (merge-based)
  sketches/              count_min.py, hyperloglog.py, heavy_hitters.py
  counting/exact.py      exact hashmap baseline (memory measurement)
  detector/              base.py, threshold.py, adaptive.py
  generator/synthetic.py reproducible traffic (spikes, gradual, heavy hitters)
  io/cicids.py           robust CIC-IDS2017 loader
  pipeline/local.py      end-to-end local runner
  pipeline/jsonl.py      dashboard-ready JSONL output
  metrics/evaluation.py  error, precision/recall/F1, latency, top-k overlap
jobs/flink_job.py        PyFlink streaming job (Kafka -> sketches -> alerts)
scripts/                 experiments, demo stream, Kafka producer
dashboard/app.py         Streamlit dashboard
tests/                   34 unit tests
configs/default.yaml     experiment parameters
docker-compose.yml       Kafka + Flink cluster
```

---

## Quickstart (no Docker required)

```powershell
pip install -r requirements.txt
python -m pytest -q                      # 34 tests
python scripts/run_local_experiments.py --quick
python scripts/demo_stream.py --scenario mixed --duration 90 --base-rate 2000
streamlit run dashboard/app.py           # then "Replay live"
```

Artifacts are written to `results/` (CSV) and `figures/` (PNG).

### Distributed path (Kafka + Flink)

```powershell
docker compose up -d
docker compose up -d --scale taskmanager=2
# Flink UI: http://localhost:8081

# producer (Python 3.11 venv recommended):
pip install -r requirements-streaming.txt
python scripts/kafka_producer.py --scenario mixed --realtime --speed 5

# submit the job (inside the Flink container):
docker exec -it sketchstream-jobmanager flink run -py /opt/jobs/flink_job.py
```

> **Python 3.13 caveat.** PyFlink does not support Python 3.13 yet. Create a
> Python 3.11 venv (`.venv311`) for `requirements-streaming.txt` and the Flink
> job. The entire `sketchstream` library and all experiments run on 3.13.

---

## Design decisions that earn the CO5 marks

1. **Sliding windows without deletion.** CMS and HLL cannot decrement, so a naive
   sliding window is *incorrect*. We keep tumbling sub-buckets and **merge** them
   (CMS = add, HLL = max); heavy hitters are re-scored from the merged CMS
   (`sketchstream/windows.py`).
2. **Adaptive baseline, not a magic number.** EWMA mean/variance in log-space with
   a z-score rule, plus a CUSUM variant for slow drifts. The z-score of a sample
   is computed against the baseline *before* absorbing it — otherwise the outlier
   hides itself (this exact bug is documented and tested).
3. **Heavy hitters need their own structure.** CMS estimates frequencies but does
   not *find* the top talkers; we use **Space-Saving** with a lazy min-heap.
4. **Honest memory story.** Sketches are fixed-size, exact hashmaps grow with
   cardinality. Crucially, sketches only win once cardinality is high — we report
   the crossover instead of hiding it.
5. **Distributed, not just parallel.** Kafka multi-partition + 2 Flink
   TaskManagers demonstrate a real distributed topology.

---

## Experiments (`scripts/run_local_experiments.py`)

| # | Experiment | Output |
|---|---|---|
| 1 | Exact hashmap vs sketches — memory vs #events | `exp1_memory.csv`, `fig2_memory_vs_events.png` |
| 2 | CMS width vs frequency-estimation error | `exp2_cms_error.csv`, `fig3_cms_error_vs_size.png` |
| 3 | HLL precision vs cardinality error | `exp3_hll_error.csv`, `fig4_hll_error.png` |
| 4 | Fixed vs adaptive threshold under traffic drift | `exp4_threshold.csv` |
| 5 | Ablation: fixed / adaptive-EWMA / adaptive-CUSUM | `exp5_ablation.csv`, `fig6_detector_quality.png` |
| 6 | Anomaly intensity sweep (2×…50×) — latency | `exp6_intensity.csv`, `fig7_latency_vs_intensity.png` |

### Early (synthetic) observations — re-run before quoting

- Memory: exact grows ~linearly with distinct keys; sketches stay constant
  (CMS 80 KB + HLL 4 KB + HH).
- CMS: mean relative error drops from ~thousands % (width 64) to <1 % (width 4096).
- HLL: precision 14 gives <1 % error at 10⁵–10⁶ distinct keys.
- Exp4: under a slow rate drift, the fixed threshold produced ~244 false
  positives while the adaptive detector produced **0**, both detecting the spike.
- **Metric caveat:** window-level recall under-counts sustained anomalies (only
  the onset window alerts before the baseline adapts). For the paper, report
  *interval-level* detection (an interval is detected if any window inside it
  alerts) alongside window-level P/R/F1.

---

## Datasets

- **Primary:** CIC-IDS2017 "TrafficLabelling" / "GeneratedLabelledFlows" CSVs
  (must contain `Source IP`, `Destination IP`, `Timestamp`). Place in
  `data/cicids/`. *Do not* use the `MachineLearningCVE` release — it strips IPs
  and timestamps.
- **Synthetic:** `sketchstream.generator` for controlled, reproducible runs
  (spikes, gradual ramps, heavy hitters), with fixed seeds.
- **Optional:** UNSW-NB15 for cross-dataset validation (Phase 7 only).

---

## Roadmap (aligned with the project prioritisation)

- [x] **Phase 1** — preprocessing + exact counting
- [x] **Phase 3** — CMS + HLL
- [x] **Phase 4** — windows + adaptive detection + heavy hitters
- [x] **Phase 5** — dashboard
- [x] **Phase 6** — experiments (exact vs approx, fixed vs adaptive, ablation)
- [ ] **Phase 2** — run the Kafka→Flink job end-to-end on the Docker cluster and
      capture throughput/latency from the Flink metrics UI
- [ ] Interval-level detection metric + real CIC-IDS2017 evaluation numbers
- [ ] (Optional) UNSW-NB15 cross-dataset validation

---

## Notes & limitations

- The Flink job currently aggregates each window with a `ProcessAllWindowFunction`
  (buffers the window). This is simple and correct for 1 s windows; a
  production-grade version would keep incremental, mergeable sketch **state**
  (e.g. Flink `AggregateFunction` over CMS/HLL).
- `min_std` in the adaptive detector guards against a near-zero baseline variance;
  tune it per stream rate.
- Memory is measured in-process (`deep_getsizeof`) for like-for-like comparison;
  JVM-side memory should be read from Flink metrics when running distributed.
