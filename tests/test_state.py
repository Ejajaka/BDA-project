import random

from sketchstream.sketches import SketchState


def _feed(state, keys):
    for k in keys:
        state.add(k)


def test_merge_matches_single_state():
    keys = [f"k-{i % 500}" for i in range(20_000)]
    single = SketchState(cms_width=300, cms_depth=4, hll_precision=12, hh_capacity=100)
    _feed(single, keys)

    a = SketchState(cms_width=300, cms_depth=4, hll_precision=12, hh_capacity=100)
    b = SketchState(cms_width=300, cms_depth=4, hll_precision=12, hh_capacity=100)
    _feed(a, keys[:10_000])
    _feed(b, keys[10_000:])
    a.merge(b)

    assert a.total == single.total
    for key in ("k-0", "k-1", "k-2"):
        assert a.cms.estimate(key) == single.cms.estimate(key)
    assert abs(a.hll.estimate() - single.hll.estimate()) / single.hll.estimate() < 0.02


def test_merged_classmethod():
    parts = []
    for chunk in range(3):
        s = SketchState(cms_width=200, cms_depth=4, hll_precision=10, hh_capacity=50)
        _feed(s, [f"x-{i % 100}" for i in range(1000)])
        parts.append(s)
    merged = SketchState.merged(parts)
    assert merged.total == 3000


def test_serialisation_roundtrip():
    s = SketchState(cms_width=200, cms_depth=4, hll_precision=10, hh_capacity=50)
    _feed(s, [f"y-{i % 300}" for i in range(5000)])
    clone = SketchState.from_dict(s.to_dict())
    assert clone.total == s.total
    assert clone.cms.estimate("y-1") == s.cms.estimate("y-1")
    assert clone.hll.estimate() == s.hll.estimate()
    assert clone.top_k(3) == s.top_k(3)


def test_summary_fields():
    s = SketchState(cms_width=200, cms_depth=4, hll_precision=12, hh_capacity=50)
    _feed(s, ["a"] * 100 + ["b"] * 10 + ["c"] * 5)
    summary = s.summary(index=3, start=3.0, end=4.0, top_k=2)
    assert summary.total == 115
    assert summary.index == 3
    assert summary.top_keys[0][0] == "a"
    # HLL is approximate; allow a small overshoot around the true value of 3
    assert 0 < summary.unique_sources < 4


def test_fixed_memory_regardless_of_events():
    s = SketchState()
    before = s.n_bytes
    _feed(s, [f"k-{random.randrange(1_000_000)}" for _ in range(50_000)])
    assert s.n_bytes == before
