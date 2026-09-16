from sketchstream.detector import (
    AdaptiveDetector,
    CusumDetector,
    FixedThresholdDetector,
)
from sketchstream.events import NetworkEvent, WindowSummary


def _summary(total, index=0, top_share=0.5):
    top = int(total * top_share)
    return WindowSummary(
        index=index,
        start=float(index),
        end=float(index) + 1.0,
        total=total,
        unique_sources=total / 10,
        top_keys=[("10.0.0.1", top)] if top else [],
    )


def test_fixed_threshold_triggers():
    det = FixedThresholdDetector(threshold=1000)
    assert det.observe(_summary(500)) is None
    assert det.observe(_summary(5000)) is not None


def test_adaptive_no_alert_during_warmup():
    det = AdaptiveDetector(warmup=20)
    for i in range(20):
        assert det.observe(_summary(1000, index=i)) is None


def test_adaptive_detects_spike_but_not_normal():
    det = AdaptiveDetector(warmup=50, z_threshold=4.0)
    for i in range(200):
        det.observe(_summary(1000, index=i))
    # a normal window should not alert
    assert det.observe(_summary(1050, index=200)) is None
    # a 10x spike must alert
    assert det.observe(_summary(10_000, index=201)) is not None


def test_adaptive_fewer_false_positives_than_fixed_under_drift():
    fixed = FixedThresholdDetector(threshold=1200)
    adaptive = AdaptiveDetector(warmup=30, z_threshold=5.0)
    fixed_fp = adaptive_fp = 0
    # normal traffic slowly drifts upward from 1000 to 1600/s
    for i in range(300):
        total = 1000 + int(600 * i / 300)
        if fixed.observe(_summary(total, index=i)) is not None:
            fixed_fp += 1
        if adaptive.observe(_summary(total, index=i)) is not None:
            adaptive_fp += 1
    assert adaptive_fp <= fixed_fp


def test_adaptive_requires_heavy_hitter_when_configured():
    det = AdaptiveDetector(
        warmup=30, z_threshold=4.0, require_hh=True, hh_share_threshold=0.5
    )
    for i in range(60):
        det.observe(_summary(1000, index=i, top_share=0.1))
    # big spike but spread out (low concentration) -> suppressed
    assert det.observe(_summary(10_000, index=100, top_share=0.05)) is None


def test_cusum_detects_gradual_ramp():
    det = CusumDetector(warmup=40, k=0.5, h=6.0)
    alerted = False
    for i in range(200):
        total = 1000
        if i >= 100:
            total = 1000 + (i - 100) * 60  # gradual increase
        if det.observe(_summary(total, index=i)) is not None:
            alerted = True
    assert alerted
