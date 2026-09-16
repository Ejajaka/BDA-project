import random
from collections import Counter

from sketchstream.sketches import SpaceSaving


def test_frequent_items_survive():
    ss = SpaceSaving(capacity=20)
    # one very frequent key plus many rare keys
    for _ in range(5000):
        ss.add("heavy")
    rng = random.Random(0)
    for _ in range(20_000):
        ss.add(f"rare-{rng.randrange(5000)}")
    top = ss.top_k(1)
    assert top[0][0] == "heavy"


def test_respects_capacity():
    ss = SpaceSaving(capacity=50)
    for i in range(10_000):
        ss.add(f"k-{i}")
    assert ss.n_tracked <= 50


def test_frequent_guarantee():
    capacity = 100
    n = 50_000
    rng = random.Random(1)
    data = [f"k-{rng.randrange(2000)}" for _ in range(n)]
    # inject a guaranteed-frequent item (frequency > n / capacity)
    data.extend(["star"] * 2000)
    ss = SpaceSaving(capacity=capacity)
    for k in data:
        ss.add(k)
    truth = Counter(data)
    threshold = n / capacity
    for key, count in truth.items():
        if count > threshold:
            assert ss.estimate(key) is not None, key
            est = ss.estimate(key)
            assert est is not None and est >= count


def test_never_underestimates_tracked():
    ss = SpaceSaving(capacity=200)
    data = [f"k-{i % 300}" for i in range(20_000)]
    for k in data:
        ss.add(k)
    truth = Counter(data)
    for key in ss.counts:
        assert ss.estimate(key) >= truth[key]
        assert ss.lower_bound(key) <= truth[key]
