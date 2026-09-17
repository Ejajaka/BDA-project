from sketchstream.detector import Alert
from sketchstream.events import NetworkEvent, WindowSummary
from sketchstream.metrics import (
    attack_intervals,
    detected_intervals,
    detection_latencies,
    evaluate_detection,
    ground_truth_windows,
    interval_precision,
    interval_recall,
    relative_error,
    topk_overlap,
    window_attack_counts,
)


def test_relative_error():
    assert relative_error(100, 100) == 0.0
    assert relative_error(110, 100) == 0.1
    assert relative_error(0, 0) == 0.0


def test_window_ground_truth():
    events = [
        NetworkEvent(ts=0.1, src_ip="a", label="BENIGN"),
        NetworkEvent(ts=0.2, src_ip="b", label="BENIGN"),
        NetworkEvent(ts=1.1, src_ip="c", label="DDoS"),
        NetworkEvent(ts=1.2, src_ip="c", label="DDoS"),
        NetworkEvent(ts=1.3, src_ip="c", label="DDoS"),
        NetworkEvent(ts=1.4, src_ip="c", label="DDoS"),
        NetworkEvent(ts=1.5, src_ip="c", label="DDoS"),
        NetworkEvent(ts=1.6, src_ip="c", label="DDoS"),
    ]
    counts = window_attack_counts(events, size=1.0)
    assert counts[0] == (0, 2)
    assert counts[1] == (6, 6)
    truth = ground_truth_windows(events, size=1.0, min_fraction=0.5, min_events=3)
    assert truth[0] is False
    assert truth[1] is True


def test_evaluate_detection_confusion():
    summaries = [WindowSummary(index=i, start=i, end=i + 1, total=0, unique_sources=0)
                 for i in range(4)]
    alerts = [
        Alert(window_index=1, window_end=2.0, score=5.0, detector="d", reason=""),
        Alert(window_index=2, window_end=3.0, score=5.0, detector="d", reason=""),
    ]
    truth = {0: False, 1: True, 2: True, 3: False}
    # predict 1 (TP), 2 (TP); miss none. Add an FP by predicting window 3.
    alerts.append(Alert(window_index=3, window_end=4.0, score=5.0, detector="d", reason=""))
    result = evaluate_detection(summaries, alerts, truth)
    assert result["tp"] == 2
    assert result["fp"] == 1
    assert result["fn"] == 0
    assert abs(result["precision"] - 2 / 3) < 1e-9
    assert result["recall"] == 1.0


def test_detection_latency():
    alerts = [
        Alert(window_index=5, window_end=6.0, score=5.0, detector="d", reason=""),
    ]
    latencies = detection_latencies(alerts, [(5.5, 9.0)])
    assert latencies[0] is not None and abs(latencies[0] - 0.5) < 1e-9
    assert detection_latencies(alerts, [(0.0, 1.0)])[0] is None


def test_topk_overlap():
    pred = [("a", 10), ("b", 9), ("c", 8)]
    actual = [("a", 10), ("b", 9), ("d", 7)]
    assert topk_overlap(pred, actual, k=3) == 2 / 3


def test_attack_intervals_merge_runs():
    events = []
    # windows 1 and 2 are attacks, window 4 too (gap at 3)
    for w in (1, 2, 4):
        for _ in range(10):
            events.append(NetworkEvent(ts=w + 0.5, src_ip="x", label="DDoS"))
    intervals = attack_intervals(events, size=1.0, min_fraction=0.5, min_events=5)
    assert intervals == [(1.0, 3.0), (4.0, 5.0)]
    bridged = attack_intervals(
        events, size=1.0, min_fraction=0.5, min_events=5, max_gap_windows=1
    )
    assert bridged == [(1.0, 5.0)]


def test_interval_recall_and_precision():
    alerts = [
        Alert(window_index=2, window_end=3.0, score=5.0, detector="d", reason=""),
        Alert(window_index=9, window_end=10.0, score=5.0, detector="d", reason=""),
    ]
    intervals = [(1.0, 4.0), (7.0, 8.0)]  # second interval missed
    assert detected_intervals(alerts, intervals) == [True, False]
    assert interval_recall(alerts, intervals) == 0.5
    # one of two alerts is inside an interval -> precision 0.5
    assert interval_precision(alerts, intervals) == 0.5

