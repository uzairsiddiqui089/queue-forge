import random

from queueforge.domain.retry import RetryPolicy


def test_delay_is_between_zero_and_the_ceiling() -> None:
    policy = RetryPolicy(base_delay=2, max_delay=300)
    rng = random.Random(1)
    for attempt, ceiling in [(1, 2), (2, 4), (3, 8), (4, 16), (5, 32)]:
        delays = [policy.delay_for(attempt, rng) for _ in range(500)]
        assert all(0 <= d <= ceiling for d in delays)
        assert max(delays) > ceiling * 0.9  # the full range is really used (jitter)
        assert min(delays) < ceiling * 0.1


def test_ceiling_is_capped_at_max_delay() -> None:
    policy = RetryPolicy(base_delay=2, max_delay=300)
    rng = random.Random(2)
    assert all(policy.delay_for(30, rng) <= 300 for _ in range(500))


def test_zero_delay_policy_for_tests() -> None:
    assert RetryPolicy(base_delay=0, max_delay=0).delay_for(3) == 0
