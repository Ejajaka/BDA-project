from sketchstream.events import NetworkEvent, WindowSummary
from sketchstream.windows import (
    SlidingWindowAggregator,
    TumblingWindowAggregator,
    merge_summaries,
)


def _events(n, size=1.0):
    return [
        NetworkEvent(ts=i * (size / n), src_ip=f"10.0.0.{i % 7}")
        for i in range(n)
    ]


def test_tumbling_emits_correct_number_of_windows():
    agg = TumblingWindowAggregator(size=1.0)
    emitted = []
    for e in _events(300, size=3.0):
        emitted.extend(agg.add(e))
    emitted.extend(agg.flush())
    assert len(emitted) == 3


def test_tumbling_totals_match_input():
    agg = TumblingWindowAggregator(size=1.0)
    total = 0
    for e in _events(500, size=4.0):
        for s in agg.add(e):
            total += s.total
    for s in agg.flush():
        total += s.total
    assert total == 500


def test_sliding_merges_last_buckets():
    agg = SlidingWindowAggregator(size=1.0, num_buckets=3)
    summaries = []
    for e in _events(500, size=6.0):
        summaries.extend(agg.add(e))
    # a fully-populated sliding window sums three tumbling buckets
    full = [s for s in summaries if s.end - s.start == 3.0]
    assert full, "expected at least one full 3-bucket sliding window"
    assert full[-1].total > 0


def test_merge_summaries_adds_totals_and_merges_sketches():
    agg = TumblingWindowAggregator(size=1.0)
    buckets = []
    for e in _events(200, size=2.0):
        buckets.extend(agg.add(e))
    buckets.extend(agg.flush())
    merged = merge_summaries(buckets)
    assert merged is not None
    assert merged.total == sum(b.total for b in buckets)
    # CMS merged estimate >= any individual bucket's estimate
    key = buckets[0].top_keys[0][0] if buckets[0].top_keys else None
    if key is not None:
        assert merged.cms.estimate(key) >= buckets[0].cms.estimate(key)


def test_windowsummary_rate_and_share():
    s = WindowSummary(
        index=0, start=0.0, end=2.0, total=100,
        unique_sources=10, top_keys=[("a", 60), ("b", 40)],
    )
    assert s.rate == 50.0
    assert abs(s.top_share() - 0.6) < 1e-9
