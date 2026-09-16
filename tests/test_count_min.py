import math
import random

from sketchstream.sketches import CountMinSketch


def _stream(n, n_keys, seed=0):
    rng = random.Random(seed)
    return [f"key-{rng.randrange(n_keys)}" for _ in range(n)]


def test_estimate_never_underestimates():
    cms = CountMinSketch(width=200, depth=5)
    data = _stream(5000, 300)
    for key in data:
        cms.add(key)
    truth = {}
    for key in data:
        truth[key] = truth.get(key, 0) + 1
    for key, count in truth.items():
        assert cms.estimate(key) >= count


def test_error_within_bound():
    n = 10_000
    width, depth = 500, 5
    cms = CountMinSketch(width=width, depth=depth)
    data = _stream(n, 1000, seed=1)
    for key in data:
        cms.add(key)
    eps = math.e / width
    bound = 3 * eps * n  # generous (3x the expected worst case)
    for key in set(data):
        truth = data.count(key)
        assert cms.estimate(key) - truth <= bound


def test_merge_matches_single_sketch():
    data_a = _stream(3000, 400, seed=2)
    data_b = _stream(3000, 400, seed=3)
    cms_a = CountMinSketch(300, 4)
    cms_b = CountMinSketch(300, 4)
    for k in data_a:
        cms_a.add(k)
    for k in data_b:
        cms_b.add(k)
    cms_a.merge(cms_b)
    combined = CountMinSketch(300, 4)
    for k in data_a + data_b:
        combined.add(k)
    for key in set(data_a + data_b):
        assert cms_a.estimate(key) == combined.estimate(key)


def test_conservative_update_is_at_most_standard():
    data = _stream(4000, 500, seed=4)
    standard = CountMinSketch(200, 5)
    conservative = CountMinSketch(200, 5)
    for k in data:
        standard.add(k)
        conservative.add(k, conservative=True)
    for key in set(data):
        assert conservative.estimate(key) <= standard.estimate(key)


def test_from_error_dimensions():
    cms = CountMinSketch.from_error(epsilon=0.01, delta=0.01)
    assert cms.width == math.ceil(math.e / 0.01)
    assert cms.depth == math.ceil(math.log(1 / 0.01))
