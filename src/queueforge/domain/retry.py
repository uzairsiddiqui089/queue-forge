"""Retry policy: how many tries a job gets and how long to wait between them."""

import random
from dataclasses import dataclass

# Every new job gets this many attempts in total (the first try counts as attempt 1).
DEFAULT_MAX_ATTEMPTS = 5


@dataclass(frozen=True)
class RetryPolicy:
    base_delay: float = 2.0
    max_delay: float = 300.0

    def delay_for(self, attempt: int, rng: random.Random | None = None) -> float:
        """Seconds to wait after attempt number `attempt` failed.

        Exponential backoff: the ceiling doubles with each attempt (2, 4, 8, ... up
        to max_delay). Full jitter: we then pick a random time between 0 and that
        ceiling. Without jitter, every job that failed together would retry at the
        same instant and hammer the system again (a "thundering herd").
        """
        ceiling = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        return (rng or random).uniform(0, ceiling)
