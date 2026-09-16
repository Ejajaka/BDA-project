import random

from sketchstream.sketches import HyperLogLog


def test_small_cardinality_is_accurate():
    hll = HyperLogLog(precision=14)
    n = 100
    for i in range(n):
        hll.add(f"u-{i}")
    assert abs(hll.estimate() - n) / n < 0.10


def test_large_cardinality_within_expected_error():
    hll = HyperLogLog(precision=14)
    n = 100_000
    for i in range(n):
        hll.add(i)
    rel = abs(hll.estimate() - n) / n
    # theoretical std error is 1.04/sqrt(m) ~= 0.8%; allow 6 sigma
    assert rel < 0.06, rel


def test_duplicates_do_not_change_much():
    hll = HyperLogLog(precision=12)
    for i in range(5000):
        hll.add(f"k-{i}")
    before = hll.estimate()
    for _ in range(10):
        for i in range(5000):
            hll.add(f"k-{i}")
    after = hll.estimate()
    assert abs(after - before) / before < 0.02


def test_merge_is_union():
    a = HyperLogLog(precision=12)
    b = HyperLogLog(precision=12)
    for i in range(3000):
        a.add(f"a-{i}")
    for i in range(3000):
        b.add(f"b-{i}")
    a.merge(b)
    assert abs(a.estimate() - 6000) / 6000 < 0.10


def test_deterministic_with_seed():
    a = HyperLogLog(precision=12, seed=7)
    b = HyperLogLog(precision=12, seed=7)
    rng = random.Random(0)
    data = [rng.randrange(10_000) for _ in range(20_000)]
    for x in data:
        a.add(x)
    for x in data:
        b.add(x)
    assert a.estimate() == b.estimate()
