from sketchstream.generator import AnomalySpec, SyntheticConfig, generate


def test_event_count_matches_base_rate():
    config = SyntheticConfig(duration=60.0, base_rate=1000.0, seed=0)
    events, intervals = generate(config)
    assert intervals == []
    # Poisson with lambda=60000: allow ~5% deviation
    assert 0.90 * 60_000 <= len(events) <= 1.10 * 60_000


def test_anomaly_raises_rate_and_is_labelled():
    config = SyntheticConfig(
        duration=60.0,
        base_rate=500.0,
        seed=1,
        anomalies=[
            AnomalySpec(start=20.0, duration=10.0, multiplier=20.0, target="1.2.3.4")
        ],
    )
    events, intervals = generate(config)
    assert intervals == [(20.0, 30.0)]
    during = [e for e in events if 20.0 <= e.ts < 30.0]
    outside = [e for e in events if e.ts < 20.0 or e.ts >= 30.0]
    # much higher rate during anomaly per unit time
    assert len(during) / 10.0 > 3 * (len(outside) / 50.0)
    # anomaly traffic concentrated on the target and labelled
    target_attacks = [e for e in during if e.is_attack and e.src_ip == "1.2.3.4"]
    assert target_attacks


def test_gradual_anomaly_factor_increases():
    a = AnomalySpec(start=0.0, duration=10.0, multiplier=10.0, mode="gradual")
    assert a.factor(-1) == 1.0
    assert a.factor(5.0) > a.factor(1.0) > 1.0
    assert a.factor(10.0) == 1.0  # outside interval


def test_deterministic_generation():
    config = SyntheticConfig(duration=30.0, base_rate=500.0, seed=99)
    a, _ = generate(config)
    b, _ = generate(config)
    assert len(a) == len(b)
    assert all(x.ts == y.ts and x.src_ip == y.src_ip for x, y in zip(a, b))
