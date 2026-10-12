"""Per-API-key rate limiting, backed by Redis.

Redis is used here and ONLY here. It holds short-lived counters; it never holds
jobs or any state we cannot afford to lose. If Redis is down or slow we "fail open"
(allow the request): rate limiting protects the service, it is not needed for
correctness, and a Redis outage must not take the whole API down with it.

Method: fixed window. Each key gets a counter per calendar minute. Simple and
cheap; the trade-off is that a client can send up to 2x the limit around a minute
boundary (the end of one window plus the start of the next).
"""

import logging
import time
from dataclasses import dataclass
from typing import Protocol

import redis

log = logging.getLogger("queueforge.ratelimit")


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int


class RateLimiter(Protocol):
    def check(self, key: str) -> RateLimitResult: ...


class RedisRateLimiter:
    def __init__(self, client: redis.Redis, limit: int, window_seconds: int = 60) -> None:
        self._redis = client
        self._limit = limit
        self._window = window_seconds

    @classmethod
    def from_url(cls, url: str, limit: int, window_seconds: int = 60) -> "RedisRateLimiter":
        # Short timeouts: a slow Redis must not make every API request slow.
        client = redis.Redis.from_url(
            url, decode_responses=True, socket_timeout=0.2, socket_connect_timeout=0.2
        )
        return cls(client, limit, window_seconds)

    def check(self, key: str) -> RateLimitResult:
        now = time.time()
        window_id = int(now // self._window)
        retry_after = max(1, int(self._window - (now % self._window)))
        redis_key = f"qf:rl:{key}:{window_id}"
        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.incr(redis_key)
            pipe.expire(redis_key, self._window, nx=True)  # set the TTL once, atomically
            count = int(pipe.execute()[0])
        except redis.RedisError:
            log.warning("rate limiter unavailable, allowing request")
            return RateLimitResult(True, self._limit, self._limit, 0)
        return RateLimitResult(
            allowed=count <= self._limit,
            limit=self._limit,
            remaining=max(0, self._limit - count),
            retry_after_seconds=retry_after,
        )
