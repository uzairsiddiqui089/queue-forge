"""Rate limiting with a real Redis, and the API's behaviour when Redis is down."""

import time
import uuid
from collections.abc import Callable

import redis
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from queueforge.api.app import create_app
from queueforge.services.ratelimit import RedisRateLimiter

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
ECHO = {"task_type": "echo", "payload": {"message": "hi"}}


def limited_api(engine: Engine, client: "redis.Redis", limit: int) -> TestClient:
    return TestClient(create_app(engine=engine, rate_limiter=RedisRateLimiter(client, limit=limit)))


def test_requests_over_the_limit_get_429(
    engine: Engine, redis_client: "redis.Redis", make_client: MakeClient
) -> None:
    api = limited_api(engine, redis_client, limit=3)
    _, key = make_client("alice")
    headers = {"X-API-Key": key}

    remaining = []
    for _ in range(3):
        response = api.post("/v1/jobs", json=ECHO, headers=headers)
        assert response.status_code == 201
        remaining.append(response.headers["X-RateLimit-Remaining"])
    assert remaining == ["2", "1", "0"]

    blocked = api.post("/v1/jobs", json=ECHO, headers=headers)
    assert blocked.status_code == 429
    assert int(blocked.headers["Retry-After"]) >= 1
    assert blocked.headers["X-RateLimit-Limit"] == "3"
    assert blocked.headers["X-RateLimit-Remaining"] == "0"
    assert blocked.json() == {"detail": "Rate limit exceeded"}


def test_each_client_has_its_own_limit(
    engine: Engine, redis_client: "redis.Redis", make_client: MakeClient
) -> None:
    api = limited_api(engine, redis_client, limit=1)
    _, alice = make_client("alice")
    _, bob = make_client("bob")
    assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": alice}).status_code == 201
    assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": alice}).status_code == 429
    assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": bob}).status_code == 201


def test_the_limit_resets_in_the_next_window(redis_client: "redis.Redis") -> None:
    limiter = RedisRateLimiter(redis_client, limit=1, window_seconds=1)
    time.sleep(1.0 - (time.time() % 1.0) + 0.05)  # start just after a window boundary
    assert limiter.check("c").allowed is True
    assert limiter.check("c").allowed is False
    time.sleep(1.1)
    assert limiter.check("c").allowed is True


def test_api_keeps_working_when_redis_is_down(engine: Engine, make_client: MakeClient) -> None:
    dead = RedisRateLimiter.from_url("redis://127.0.0.1:1/0", limit=1)
    api = TestClient(create_app(engine=engine, rate_limiter=dead))
    _, key = make_client("alice")
    for _ in range(3):  # would be blocked after 1 if Redis worked
        assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key}).status_code == 201


def test_unauthenticated_requests_do_not_touch_the_limiter(
    engine: Engine, redis_client: "redis.Redis"
) -> None:
    api = limited_api(engine, redis_client, limit=1)
    response = api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": "qf_wrong"})
    assert response.status_code == 401
    assert "X-RateLimit-Remaining" not in response.headers
    assert redis_client.dbsize() == 0
