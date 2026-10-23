# Recreation Manual — *sketchstream*
### Memory-Efficient Real-Time Network Anomaly Detection Using Probabilistic Sketches in Distributed Data Streams

This manual documents **everything built so far**, step by step, so that any reader
can (a) reproduce the results and (b) rebuild the system from scratch.

> **State of this document:** reflects the current `master` branch of
> `Ejajaka/BDA-project`. Core system, experiments, dashboard, real CIC-IDS2017
> evaluation and the distributed Kafka + Flink run are all verified. Evidence is
> in `results/` and `figures/`; UNSW-NB15 remains optional.

---

## Table of contents

1. [What you get at the end](#1-what-you-get-at-the-end)
2. [Prerequisites](#2-prerequisites)
3. [Quick start (reproduce results in 5 commands)](#3-quick-start-reproduce-results-in-5-commands)
4. [Repository layout — every file explained](#4-repository-layout--every-file-explained)
5. [The project in one page](#5-the-project-in-one-page)
6. [Part A — Reproduce the results](#part-a--reproduce-the-results)
7. [Part B — Rebuild from scratch, component by component](#part-b--rebuild-from-scratch-component-by-component)
8. [Part C — Distributed deployment (Kafka + Flink)](#part-c--distributed-deployment-kafka--flink)
9. [Results observed so far](#9-results-observed-so-far)
10. [Design decisions, findings and bugs fixed](#10-design-decisions-findings-and-bugs-fixed)
11. [Troubleshooting](#11-troubleshooting)
12. [Roadmap — what is NOT done yet](#12-roadmap--what-is-not-done-yet)
13. [Glossary & references](#13-glossary--references)

---

## 1. What you get at the end

A working **real-time network anomaly detection system**:

- Ingests a continuous stream of network events (source IP, timestamp, port, …)
- Summarises it in **fixed, tiny memory** using three from-scratch probabilistic
  data structures: **Count-Min Sketch** (per-key frequency), **HyperLogLog**
  (distinct sources), **Space-Saving** (heavy hitters)
- Aggregates events into **event-time windows** (tumbling or sliding)
- **Raises alerts** using an **adaptive** threshold (EWMA z-score / CUSUM) instead
  of a hard-coded value
- Ships with a **live dashboard**, an **experiment harness** producing CSV + PNG
  evidence, and a **Kafka → Flink** distributed deployment

Everything is pure Python (runs with no containers); the distributed stack uses
Docker.

---

## 2. Prerequisites

Verified on this machine:

| Tool | Version used | Needed for |
|---|---|---|
| Python | 3.13.14 | core library, tests, experiments, dashboard |
| pip | 26.1.2 | installs |
| Java (Temurin) | 17.0.20 | Flink (via Docker image) |
| Docker | 28.5.1 (+ Compose v2.40) | Kafka + Flink |
| Git | 2.48.1 | version control |
| GitHub CLI (`gh`) | 2.100.0 | repo creation |

Minimum for the core path: **Python 3.10+**. The Flink job alone needs **Python
3.11** (PyFlink does not support 3.13 yet) — see [Part C](#part-c--distributed-deployment-kafka--flink).

---

## 3. Quick start (reproduce results in 5 commands)

Open PowerShell in the project root and run:

```powershell
# 1. Install core dependencies
pip install -r requirements.txt

# 2. Run the unit tests                -> expect: 34 passed
python -m pytest -q

# 3. Run the experiments (fast mode)   -> writes results\*.csv + figures\*.png
python scripts/run_local_experiments.py --quick

# 4. Generate a demo stream            -> writes results\stream.jsonl
python scripts/demo_stream.py --scenario mixed --duration 90 --base-rate 2000

# 5. Start the dashboard (browser opens; click "Replay live")
streamlit run dashboard/app.py
```

That is the whole core system, end to end.

---

## 4. Repository layout — every file explained

```
BDA-project/
├── README.md                       Project overview + rubric mapping
├── run_all.bat / run_all.ps1       One-command runners (start here)
├── requirements.txt                Core deps: numpy, pandas, matplotlib, pyyaml, pytest, streamlit
├── requirements-streaming.txt      Kafka/Flink deps: apache-flink==1.20.*, confluent-kafka (Python 3.11)
├── pytest.ini                      pytest config (tests/ dir)
├── conftest.py                     makes repo root importable under pytest
├── .gitignore                      ignores data/, results/, figures/, jars/, caches
│
├── docs/
│   ├── MANUAL.md                   (this file) full recreation guide
│   └── project-proposal.txt        original project proposal text
├── configs/
│   └── default.yaml                documented parameter set (stream/sketches/detectors)
│
├── deploy/
│   ├── Dockerfile.flink            Flink image + Python/PyFlink (for the cluster run)
│   └── docker-compose.yml          Kafka (KRaft) + Flink JobManager + TaskManager(s)
│
├── sketchstream/                   *** THE CORE LIBRARY (pure Python) ***
│   ├── __init__.py                 public exports + version
│   ├── hashing.py                  deterministic 128-bit hashing + enhanced double hashing
│   ├── events.py                   NetworkEvent, WindowSummary dataclasses
│   ├── windows.py                  TumblingWindowAggregator, SlidingWindowAggregator, merge_summaries
│   ├── sketches/
│   │   ├── __init__.py
│   │   ├── count_min.py            CountMinSketch (+ from_error sizing)
│   │   ├── hyperloglog.py          HyperLogLog (+ small-range correction)
│   │   ├── heavy_hitters.py        SpaceSaving (+ lazy min-heap)
│   │   └── state.py                SketchState: mergeable/serialisable incremental state
│   ├── counting/
│   │   ├── __init__.py
│   │   └── exact.py                ExactCounter baseline + deep_getsizeof
│   ├── detector/
│   │   ├── __init__.py
│   │   ├── base.py                 Alert dataclass + Detector interface
│   │   ├── threshold.py            FixedThresholdDetector
│   │   └── adaptive.py             AdaptiveDetector (EWMA z-score) + CusumDetector
│   ├── generator/
│   │   ├── __init__.py
│   │   └── synthetic.py            AnomalySpec, SyntheticConfig, generate, build_scenario
│   ├── io/
│   │   ├── __init__.py
│   │   ├── cicids.py               CIC-IDS2017 CSV loader (robust to messy headers)
│   │   └── unsw.py                 UNSW-NB15 loader (optional cross-dataset)
│   ├── metrics/
│   │   ├── __init__.py
│   │   └── evaluation.py           error, ground truth, P/R/F1, latency, top-k overlap
│   └── pipeline/
│       ├── __init__.py
│       ├── local.py                run_local_pipeline (exact + sketches side by side)
│       └── jsonl.py                dump_run -> dashboard-ready JSONL
│
├── flink/
│   └── flink_job.py                PyFlink job: Kafka -> sketches -> alerts
├── scripts/
│   ├── run_local_experiments.py    Experiments 1-6 (CSV + figures)
│   ├── run_cicids_evaluation.py    Dataset evaluation (--demo, CIC-IDS2017, UNSW-NB15)
│   ├── demo_stream.py              Generate JSONL for the dashboard
│   └── kafka_producer.py           Publish events to Kafka (synthetic or CIC-IDS)
├── dashboard/
│   └── app.py                      Streamlit live dashboard
└── tests/
    ├── test_count_min.py           CMS correctness, error bound, merge, conservative update
    ├── test_hyperloglog.py         HLL accuracy, duplicates, merge, determinism
    ├── test_heavy_hitters.py       Space-Saving frequent guarantee, capacity, bounds
    ├── test_windows.py             Tumbling counts, sliding merge, summary merging
    ├── test_detector.py            Fixed/adaptive/CUSUM behaviour incl. drift
    ├── test_generator.py           Rate correctness, anomaly labelling, determinism
    ├── test_state.py               Mergeable SketchState: merge, serialisation
    └── test_metrics.py             Confusion matrix, latency, top-k overlap
```

Generated (not committed — see `.gitignore`): `results/` (CSV + JSONL),
`figures/` (PNG), `data/` (datasets) and `jars/` (Kafka connector JAR, fetched
on demand by `run_all`).

---

## 5. The project in one page

### Problem
Network-monitoring systems produce an unbounded stream of events. Counting each
key exactly with a hashmap consumes memory proportional to the number of
**distinct** keys, which explodes for large traffic. At the same time, monitoring
needs to detect abnormal behaviour — DDoS-like spikes, heavy-hitter IPs, sudden
changes in the number of distinct sources.

### Approach
Summarise the stream with fixed-size probabilistic structures and detect anomalies
from those summaries, in real time:

- **Count-Min Sketch** → "how often did this IP appear?" (approximate, upper-bounded)
- **HyperLogLog** → "how many distinct IPs appeared?" (approximate cardinality)
- **Space-Saving** → "which IPs dominate the traffic?" (heavy hitters)
- **Event-time windows** → analyse recent traffic, not all history
- **Adaptive detector** → learn the recent normal baseline; alert on deviation

### Research question
*Can CMS and HLL provide memory-efficient real-time traffic analytics while
retaining acceptable estimation accuracy and enabling low-latency anomaly
detection?*

### Novelty (what is ours)
A **unified streaming framework** combining frequency + cardinality + temporal +
heavy-hitter signals with an **adaptive** threshold, plus a rigorous empirical
evaluation (memory, error, throughput, precision/recall/F1, latency). We do **not**
claim to have invented CMS, HLL, Kafka, Flink or anomaly detection.

---

# Part A — Reproduce the results

## A.1 Install and test

```powershell
pip install -r requirements.txt
python -m pytest -q
```

Expected: `34 passed`. These tests prove the sketches are correct:

| Test file | What it proves |
|---|---|
| `test_count_min.py` | estimate never under-counts; error within bound; merge == single sketch; conservative ≤ standard |
| `test_hyperloglog.py` | <10% error at n=100, <6% at n=100k, duplicates don't inflate, merge is union, seed-deterministic |
| `test_heavy_hitters.py` | frequent item survives; capacity respected; frequent-guarantee; never under-estimates tracked keys |
| `test_windows.py` | window counts, sliding merge of last k buckets, CMS/HLL merge in summaries |
| `test_detector.py` | fixed threshold fires; adaptive silent in warmup; detects 10× spike; fewer FPs than fixed under drift; CUSUM catches ramps |
| `test_generator.py` | event count matches base rate; anomaly raises rate + is labelled; determinism |
| `test_metrics.py` | confusion matrix, latency, top-k overlap, ground-truth windows |

## A.2 Run the experiments

```powershell
python scripts/run_local_experiments.py --quick   # fast
python scripts/run_local_experiments.py           # full sizes
python scripts/run_local_experiments.py --experiments 1 4 5   # subset
```

Outputs:

| Experiment | File(s) | What it shows |
|---|---|---|
| 1 | `results/exp1_memory.csv`, `figures/fig2_memory_vs_events.png` | exact hashmap memory grows with #events/keys; sketches stay flat |
| 2 | `results/exp2_cms_error.csv`, `figures/fig3_cms_error_vs_size.png` | CMS width vs frequency error |
| 3 | `results/exp3_hll_error.csv`, `figures/fig4_hll_error.png` | HLL precision vs cardinality error |
| 4 | `results/exp4_threshold.csv`, `figures/fig_traffic_drift.png` | fixed vs adaptive FPs under traffic drift |
| 5 | `results/exp5_ablation.csv`, `figures/fig6_detector_quality.png` | detector precision / recall / F1 |
| 6 | `results/exp6_intensity.csv`, `figures/fig7_latency_vs_intensity.png` | detection latency vs anomaly intensity |

See [Section 9](#9-results-observed-so-far) for the values observed on this machine.

## A.3 Run the live demo + dashboard

Terminal 1 (generate a real-time-animated stream):

```powershell
python scripts/demo_stream.py --scenario mixed --duration 90 --base-rate 2000 --realtime --speed 20
```

Terminal 2 (dashboard):

```powershell
streamlit run dashboard/app.py
```

In the browser click **“Replay live”** and watch requests/window, unique sources,
top IP, memory, and the NORMAL → ANOMALY switch.

## A.4 Git / GitHub (how it was published)

```powershell
git init
git add -A
git commit -m "Initial commit: sketchstream - streaming sketches + adaptive anomaly detection"
gh repo create BDA-project --private --source=. --remote=origin --push
gh api -X PUT "repos/Ejajaka/BDA-project/collaborators/redpanda1109" -f permission=push
```

> GitHub does not allow spaces in repo names; “BDA project” → `BDA-project`.

---

# Part B — Rebuild from scratch, component by component

Follow these in order. Each step states the file to create and the idea/formula.
The numbers in parentheses are the actual filenames in this repo.

## B.0 Concepts you must understand first

- **Stream / event**: a record with an event-time timestamp.
- **Cardinality**: number of *distinct* items (e.g., distinct source IPs).
- **Frequency**: how many times a key occurred.
- **Probabilistic sketch**: a data structure that answers a query approximately
  using sub-linear, fixed memory (with a bounded error probability).
- **Window**: a time slice (e.g., 1 second) over which we aggregate.
- **Heavy hitter**: a key responsible for a large share of the total.

## B.1 Deterministic hashing (`sketchstream/hashing.py`)

Why: Python's built-in `hash()` is salted per process, so sketches would differ
between runs/nodes. We need reproducible hashing.

- `hash128(key, seed)` → 128-bit int. Uses `xxhash` if installed, else
  `hashlib.blake2b(key, digest_size=16, salt=…)`.
- `double_hashing(key, i, width, seed)` → enhanced double hashing (Kirsch–Mitzenmacher):
  `h1 = hash mod 2^64`, `h2 = (hash >> 64) | 1` (forced odd), index `= (h1 + i*h2) mod width`.

## B.2 Count-Min Sketch (`sketchstream/sketches/count_min.py`)

Idea: a `depth × width` table of counters and `depth` hash functions. To add a key,
increment one cell per row. To estimate, take the **minimum** across rows.

- Sizing: `width = ceil(e / ε)`, `depth = ceil(ln(1/δ))` (`from_error`).
- Guarantee: `true ≤ estimate ≤ true + ε·N` with probability ≥ `1 − δ` (`N` = stream size).
- `add(key, count=1, conservative=False)`; `estimate(key)`; `merge(other)` (table addition).
- Implementation detail: hash the key **once** and derive all `depth` indices
  (never re-hash per row — that was a performance bug).

**Recreate:** create the table with NumPy `zeros((depth, width), int64)`; store
`self._rows = arange(depth)`; `add` does `table[rows, idx] += count`.

## B.3 HyperLogLog (`sketchstream/sketches/hyperloglog.py`)

Idea: estimate the number of distinct elements from the maximum number of leading
zeros observed in hashed values, combining `m = 2^p` registers with a **harmonic
mean**.

- For each key: hash to 128 bits; first `p` bits select a register; the rest give
  `ρ` = position of the left-most 1. Store `max` in the register.
- Estimate: `E = α_m · m² / Σ 2^(−M[j])`.
- Small-range correction (linear counting) when `E ≤ 2.5m`:
  `E = m · ln(m / V)` where `V` = number of empty registers.
- Relative standard error ≈ `1.04 / sqrt(m)`.
- `merge(other)`: element-wise **max** of registers.
- Implementation detail: define `__bool__` so an empty HLL (estimate 0) is not
  treated as “false” (fixed a bug where `assert self._hll` failed).

## B.4 Space-Saving heavy hitters (`sketchstream/sketches/heavy_hitters.py`)

Idea: keep at most `capacity` candidate keys with (count, error). If full and a new
key arrives, evict the smallest-counter entry and replace it with the new key whose
count = min + 1, error = min.

- Guarantee: any key with frequency > `N / capacity` is reported.
- `top_k(k)`; `estimate` is an upper bound; `lower_bound = count − error`.
- Implementation: a **lazy min-heap** gives O(log capacity) replacement instead of
  a linear scan.
- `merge` is approximate (not natively mergeable) — used only for sliding candidates.

## B.5 Exact baseline (`sketchstream/counting/exact.py`)

- `ExactCounter`: a dict of exact counts; `unique()`, `top_k()`, and `n_bytes`
  using a recursive `deep_getsizeof` (counts interned objects once).
- Used to compare memory/accuracy/throughput against the sketches.

## B.6 Events and windows (`sketchstream/events.py`, `sketchstream/windows.py`)

- `NetworkEvent(ts, src_ip, dst_ip, dst_port, protocol, packets, nbytes, label)`.
- `WindowSummary(index, start, end, total, unique_sources, top_keys, cms, hll, heavy)`
  with `rate` and `top_share()`.
- `TumblingWindowAggregator(size, …)`: on each event compute
  `index = int(ts // size)`; when the index advances, emit the current window and
  start a new one. Maintains a CMS, HLL and Space-Saving **per window**.
- `SlidingWindowAggregator(size, num_buckets)`: keeps the last `num_buckets`
  tumbling summaries and merges them.

**Key design insight (CO5):** CMS/HLL cannot *delete*, so a sliding window cannot
simply drop old events. The correct approach is **tumbling sub-buckets + merge**:
CMS merges by addition, HLL by max, heavy hitters are re-scored from the merged CMS.

## B.7 Detectors (`sketchstream/detector/`)

- `FixedThresholdDetector(threshold)` — alerts when `total > threshold`. The baseline.
- `AdaptiveDetector(alpha, z_threshold, warmup, min_std, use_log, …)`:
  - Work in log-space (`log1p(total)`) because counts are multiplicative.
  - Maintain EWMA mean and variance (RiskMetrics recursion):
    `mean ← αx + (1−α)·mean`
    `var ← (1−α)·(var + α·(x − mean_prev)²)`
  - z-score = `(x − mean) / max(sqrt(var), min_std)`.
  - Alert when `z ≥ z_threshold` after a `warmup` period.
  - **Critical detail:** compute the z-score against the baseline **before**
    folding in the current sample. Otherwise the outlier inflates its own mean/var
    and hides itself (documented bug + test).
- `CusumDetector(k, h, …)` — one-sided CUSUM on the standardised deviation:
  `g⁺ ← max(0, g⁺ + z − k)`; alert when `g⁺ ≥ h`. Better for **gradual** drifts.
  Use a **small `k`** (e.g. 0.1) to catch slow ramps.

## B.8 Synthetic generator (`sketchstream/generator/synthetic.py`)

- `AnomalySpec(start, duration, multiplier, mode, target, label)` with
  `factor(t)`: 1.0 outside the interval; `multiplier` (sudden) or a linear ramp
  (gradual) inside.
- `SyntheticConfig(duration, base_rate, n_sources, zipf, seed, anomalies, …)`.
- `generate(config)` uses **thinning**: draw candidates at `λ_max`, accept with
  probability `λ(t)/λ_max`, so the time-varying rate is exact. Sources follow a
  **Zipf** distribution (natural heavy hitters); anomaly events go to `target`
  when specified.
- `build_scenario(name)` presets: `sudden`, `gradual`, `heavy_hitter`, `mixed`, `none`.

## B.9 CIC-IDS2017 loader (`sketchstream/io/cicids.py`)

- Use the **TrafficLabelling / GeneratedLabelledFlows** CSVs (they contain
  `Source IP`, `Destination IP`, `Timestamp`). NOT the `MachineLearningCVE` release.
- Handles: inconsistent/space-padded headers, `dd/MM/yyyy HH:mm[:ss]` timestamps,
  NaN/Infinity, protocol numbers (6→TCP, 17→UDP, 1→ICMP).
- `load_cicids_events(paths, limit=…)` → time-ordered events with `ts` relative to
  the first record.

## B.10 Metrics (`sketchstream/metrics/evaluation.py`)

- `relative_error(est, actual)`.
- `window_attack_counts(events, size)` and `ground_truth_windows(...)`: a window is
  anomalous if attack events ≥ `min_events` **and** their fraction ≥ `min_fraction`.
- `evaluate_detection(summaries, alerts, truth)` → TP/FP/TN/FN, precision, recall,
  F1, FPR.
- `detection_latencies(alerts, intervals)` → first alert after each anomaly start.
- `topk_overlap(pred, actual, k)` — used for the heavy-hitter evaluation
  (`cicids_heavyhitters.csv` and `exp1_topk_overlap.csv`).

## B.11 Local pipeline (`sketchstream/pipeline/local.py`)

- `run_local_pipeline(events, detector, window_size, mode, cms_width, cms_depth,
  hll_precision, hh_capacity, checkpoint_every, …)`.
- Feeds events through the aggregator; on each closed window calls
  `detector.observe(summary)`; runs an `ExactCounter` in parallel; samples a memory
  curve; returns `LocalRunResult` (summaries, alerts, memory, throughput, ground truth).

## B.12 JSONL output (`sketchstream/pipeline/jsonl.py`)

- `dump_run(result, path, meta, realtime, speed)` writes one JSON object per window
  (`total`, `unique`, `top`, `alert`, `score`, …). With `realtime=True` it sleeps to
  match wall-clock time (scaled by `speed`) — this is what animates the dashboard.

## B.13 Tests (`tests/`)

Recreate the seven test files described in [A.1](#a1-install-and-test). Run
`python -m pytest -q` until all pass. Tests were written **before** trusting the
numbers they validate.

---

# Part C — Distributed deployment (Kafka + Flink)

> PyFlink does not support Python 3.13. Use a Python **3.11** virtualenv for this
> part only. The core library and all experiments run on 3.13.

## C.1 Start Kafka + Flink

```powershell
docker compose -f deploy/docker-compose.yml build                        # Flink+Python image
docker compose -f deploy/docker-compose.yml up -d --scale taskmanager=2  # 1 JM + 2 TMs
```

- Flink UI: **http://localhost:8082** (8081 is taken by another app on this
  machine — change the port mapping in `deploy/docker-compose.yml` if needed)
- Kafka (host): `localhost:9092`; Kafka (in-cluster): `kafka:29092`

`deploy/docker-compose.yml` runs Kafka in **KRaft** mode (official `apache/kafka`
image; Bitnami's images were deprecated) and one Flink JobManager plus two
TaskManagers built from `deploy/Dockerfile.flink` (adds Python 3 + PyFlink + the
project + the Kafka connector JAR in `/opt/flink/lib`).

> Simplest: run `run_all.bat -Distributed`, which performs C.1–C.4 for you.

## C.2 Install streaming deps (Python 3.11)

```powershell
# if the 'py' launcher is unavailable, use the install path:
#   & "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m venv .venv311
py -3.11 -m venv .venv311
.venv311\Scripts\activate
pip install -r requirements-streaming.txt
```

## C.3 Produce events to Kafka

```powershell
# synthetic
python scripts/kafka_producer.py --scenario mixed --realtime --speed 5

# or real data
python scripts/kafka_producer.py --cicids "data/cicids/Wednesday-workingHours.pcap_ISCX.csv"
```

Events are JSON on topic `network-events` (4 partitions).

## C.4 Run the Flink job (verified)

```powershell
# submit to the Docker cluster (the connector jar is baked into the image)
docker exec sketchstream-jobmanager bash -lc "flink run -d -py /opt/flink/flink_job.py \
  --bootstrap kafka:29092 --from-earliest --out /tmp/flink_stream.jsonl \
  --window-size 1 --parallelism 2"

# read output from the TaskManager that ran the window operator
docker exec sketchstream-taskmanager-1 bash -lc "cat /tmp/flink_stream.jsonl"

# list / cancel
docker exec sketchstream-jobmanager bash -lc "flink list -r"
docker exec sketchstream-jobmanager bash -lc "flink cancel <jobid>"
```

> Local `python flink/flink_job.py --local` also works on **Linux**, but PyFlink's
> Python harness is unreliable on Windows — prefer the in-cluster run.

Pipeline inside the job: Kafka → parse `(ts, src_ip)` → event-time watermarks →
1-second tumbling windows → `ProcessAllWindowFunction` maintains CMS + HLL +
Space-Saving, builds a `WindowSummary`, runs the same `AdaptiveDetector`, and emits
JSON records (to Kafka topic `alerts` and/or `--out` JSONL).

## C.5 Configuration knobs

`--bootstrap`, `--topic`, `--alerts-topic`, `--window-size`, `--parallelism`,
`--cms-width`, `--cms-depth`, `--hll-precision`, `--hh-capacity`, `--z-threshold`,
`--warmup`, `--out`. The Kafka connector JAR must be on the classpath
(`--jar path/to/flink-connector-kafka-…jar`).

---

## 9. Results observed so far

> Synthetic data, `--quick` mode, on the development machine. Re-run to reproduce;
> numbers vary with seeds/sizes. Do not quote these in the paper without re-running.

**Experiment 1 — memory:** 59,746 events, 3,025 distinct keys →
exact **0.26 MB** vs sketches **96.9 KB**; throughput ≈ **49,300 events/s**.

**Experiment 2 — CMS error (depth 5):**

| width | bytes | mean rel. error | p95 |
|---|---|---|---|
| 64 | 2,560 | 7912% | 20200% |
| 512 | 20,480 | 308% | 1000% |
| 4096 | 163,840 | 0.95% | 0% |

**Experiment 3 — HLL error:** precision 14 gives 0.20% / 0.48% / 0.89% at
1k / 10k / 100k distinct; precision 10 gives ~2–8%.

**Experiment 4 — fixed vs adaptive under drift:** fixed threshold → **244 false
positives**; adaptive → **0**; both detected the spike.

**Experiment 5 — ablation:** fixed P=1.00 R=0.795 F1=0.886;
adaptive-EWMA P=1.00 R=0.051; CUSUM(k=0.5) P=1.00 R=0.103.
*(The low recall is a metric artefact — see findings; use interval-level detection.)*

**Experiment 6 — intensity:** 5× and 50× spikes detected with ~1 s latency.

**Live demo (`mixed`, 90 s, base 2000):** 712,181 events, 501 distinct IPs,
exact 0.05 MB vs sketches 97 KB (sketches *lose* at this cardinality — expected),
2 alerts for 3 anomalies. With `CusumDetector(k=0.1, h=6)` all three are detected
(t = 23, 27, 50, 55, 82 s).

---

## 10. Design decisions, findings and bugs fixed

1. **Sliding windows cannot delete.** CMS/HLL have no decrement → use tumbling
   sub-buckets and merge (CMS add, HLL max).
2. **Z-score must use the *prior* baseline.** Computing it after updating the EWMA
   lets a spike hide itself. Fixed and covered by a test.
3. **Heavy hitters need their own structure.** CMS measures frequency but cannot
   *find* top talkers → Space-Saving.
4. **Adaptive ≠ always better.** Fixed thresholds produce false positives under
   traffic drift (244 vs 0 in Exp4); adaptive is robust. But no single detector wins:
   EWMA suits sudden spikes, low-`k` CUSUM suits gradual ramps.
5. **Sketches are not free.** With few distinct keys the exact hashmap is smaller
   (0.05 MB vs 0.097 MB at 501 keys). Report the **crossover**.
6. **Window-level recall under-counts sustained anomalies** (only the onset window
   alerts before the baseline adapts). Report **interval-level** detection too.
7. **Bugs found and fixed during development:** (a) HLL `__len__` made an empty HLL
   falsy; (b) z-score computed after the EWMA update; (c) CMS re-hashed per row
   (fixed: hash once, derive indices); (d) window assert on HLL truthiness.

---

## 11. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `ModuleNotFoundError: sketchstream` running a script | Scripts add the repo root to `sys.path`; run from the project root. Tests use `conftest.py`. |
| PowerShell shows a red “NativeCommandError” for `git`/`java`/`pip` output | PowerShell treats stderr as errors; the command usually succeeded. Check the actual output. |
| `apache-flink` install fails | PyFlink needs Python ≤3.11. Use a `.venv311` environment. |
| `docker compose` warns about `deploy.replicas` | Also run `docker compose -f deploy/docker-compose.yml up -d --scale taskmanager=2`. |
| Flink job can’t find Kafka connector | Add the matching `flink-connector-kafka` JAR via `--jar`. |
| Dashboard says “No window records found” | Run `scripts/demo_stream.py` first; check the file path in the sidebar. |
| Experiment output looks empty | Ensure `results/` and `figures/` exist (the script creates them). |

---

## 12. Roadmap — status

- [x] Run Kafka → Flink end-to-end on Docker (verified; alert fired).
- [x] Add a dedicated `scripts/run_cicids_evaluation.py` (with `--demo` self-test).
- [x] Add **interval-level** detection metrics to `metrics/evaluation.py`.
- [x] Add mergeable, serialisable sketch state (`sketches/state.py`).
- [x] Wire `SketchState` into a Flink `AggregateFunction` (`--incremental`), so
      window memory is O(1) instead of O(window) — verified on the cluster.
- [x] UNSW-NB15 loader (`io/unsw.py`).
- [x] Run `run_cicids_evaluation.py` on **real CIC-IDS2017** (Friday DDoS day).
- [ ] UNSW-NB15 real numbers — public ML splits lack IPs/timestamps and the raw
      mirrors are normalised; needs the official raw CSVs.
- [ ] IEEE-style paper write-up using the figures.

---

## 13. Glossary & references

| Term | Meaning |
|---|---|
| CMS | Count-Min Sketch — approximate frequency estimation |
| HLL | HyperLogLog — approximate distinct counting |
| Space-Saving | Heavy-hitter algorithm |
| EWMA | Exponentially Weighted Moving Average |
| CUSUM | Cumulative Sum change-detection |
| DDoS | Distributed Denial of Service |
| KRaft | Kafka's ZooKeeper-free consensus mode |

**Foundational papers**
- Cormode & Muthukrishnan, *An Improved Data Stream Summary: The Count-Min Sketch and its Applications* (2005).
- Flajolet et al., *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm* (2007).
- Metwally, Agrawal & El Abbadi, *Efficient Computation of Frequent and Top-k Elements in Data Streams* (Space-Saving, 2005).
- Heule, Nunkesser & Hall, *HyperLogLog in Practice* (HLL++, 2013).

**Dataset**
- CIC-IDS2017 — Canadian Institute for Cybersecurity, University of New Brunswick:
  https://www.unb.ca/cic/datasets/ids-2017.html (use the TrafficLabelling CSVs).

---

## 14. Update log — distributed run verified

The Kafka → Flink path was run end-to-end on a real Docker cluster (1 JobManager
+ 2 TaskManagers, 8 slots). A PyFlink job consumed `network-events` from Kafka,
maintained CMS + HLL + Space-Saving over 1-second event-time windows, and fired an
adaptive alert on an injected heavy-hitter spike:

```json
{"type":"window","start":3.0,"end":4.0,"total":1243,"unique":47.3,
 "top":[["192.168.0.66",1106],["10.0.0.0",36]],
 "alert":true,"score":40.821,"reason":"z=40.82 >= 3.0 (std=0.050)"}
```

Evidence saved to `results/flink_stream.jsonl`, `results/flink_alerts.jsonl`,
`results/flink_job_overview.json`.

### Incremental state verified
The `--incremental` path (`SketchAggregate` + `IncrementalEmitter` using
`SketchState.merge`) produced **identical** results to the buffered path on the
cluster (`total=1243`, `192.168.0.66=1106`, `z=40.82`), but folds each event into
a fixed-size state instead of buffering the window:
```
flink run -d -py /opt/flink/flink_job.py --bootstrap kafka:29092 --from-earliest \
  --incremental --out /tmp/flink_incremental.jsonl --window-size 1 --parallelism 2 \
  --warmup 2 --z-threshold 3.0
```
Evidence: `results/flink_incremental.jsonl`.

### Real CIC-IDS2017 evaluated
`scripts/run_cicids_evaluation.py` was run on the real **Friday-WorkingHours-
Afternoon-DDos** file (225,745 events, 2,067 source IPs, 21 attack intervals):
memory 0.18 MB exact vs 96.9 KB sketches, CMS mean error 9.8 %, HLL error 0.25 %,
**interval recall 1.00** for all detectors. Evidence:
`results/cicids_memory.csv`, `results/cicids_sketch_accuracy.csv`,
`results/cicids_ablation.csv`.

### Dashboard verified
`dashboard/app.py` serves (HTTP 200, health `ok`) and runs with **no exceptions**
under Streamlit's `AppTest` (5 metrics rendered).

### Bugs found and fixed while getting the distributed job running (CO5 material)

1. **Wrong connector import path.** Flink 1.20 PyFlink uses
   `pyflink.datastream.connectors.kafka` (not `pyflink.connector.kafka`), with
   `KafkaOffsetsInitializer`.
2. **Timestamp assigner must be an object.** A plain lambda fails; subclass
   `pyflink.common.watermark_strategy.TimestampAssigner` and implement
   `extract_timestamp`.
3. **`ProcessAllWindowFunction.process()` returns an iterable** — it does *not*
   take an `out` argument (PyFlink differs from Java Flink). This caused
   `missing 1 required positional argument: 'out'`.
4. **Idle-partition watermark stall.** A small Kafka topic leaves some partitions
   empty, so `windowAll`'s watermark never advances and no windows fire. Fix:
   `.with_idleness(Duration.of_seconds(5))`.
5. **Windows + PyFlink.** PyFlink has no 3.13 wheel: use Python 3.11, and run the
   job inside the Linux container because the Windows harness is unreliable.
6. **Infra:** Bitnami Kafka images were deprecated → use `apache/kafka`; Docker
   Desktop can wedge under load → `wsl --shutdown` + relaunch fixes it.

---

*End of manual. Keep this file updated whenever the system changes.*
