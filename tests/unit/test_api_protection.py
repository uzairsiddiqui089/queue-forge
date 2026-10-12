"""Body size limit, generic 500s and rate-limiter failure behaviour (no database needed)."""

from collections.abc import Iterator

from fastapi.testclient import TestClient

from queueforge.api.app import create_app
from queueforge.db.engine import make_engine
from queueforge.services.ratelimit import RedisRateLimiter

UNREACHABLE_DB = "postgresql+psycopg://nobody:not-a-real-password@127.0.0.1:1/none"
SMALL_BODY = '{"task_type":"echo","payload":{"message":"hi"}}'


def make_app(max_body_bytes: int = 100) -> TestClient:
    app = create_app(engine=make_engine(UNREACHABLE_DB), max_body_bytes=max_body_bytes)
    return TestClient(app, raise_server_exceptions=False)


def test_oversized_body_is_rejected_by_content_length() -> None:
    response = make_app().post("/v1/jobs", content=b"x" * 500)
    assert response.status_code == 413
    assert response.json() == {"detail": "Request body too large"}


def test_oversized_chunked_body_without_length_is_rejected() -> None:
    def chunks() -> Iterator[bytes]:
        for _ in range(10):
            yield b"x" * 50  # 500 bytes in total, but no Content-Length header

    response = make_app().post("/v1/jobs", content=chunks())
    assert response.status_code == 413


def test_body_under_the_limit_reaches_the_endpoint() -> None:
    response = make_app(max_body_bytes=1000).post(
        "/v1/jobs", content=SMALL_BODY, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 401  # got past the size check; no API key supplied


def test_unhandled_error_returns_generic_500_without_details() -> None:
    app = create_app(engine=make_engine(UNREACHABLE_DB))

    @app.get("/boom")
    def boom() -> None:
        raise RuntimeError("internal detail: postgresql://user:SECRET@db/x")

    response = TestClient(app, raise_server_exceptions=False).get("/boom")
    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
    assert "SECRET" not in response.text


def test_rate_limiter_fails_open_when_redis_is_down() -> None:
    limiter = RedisRateLimiter.from_url("redis://127.0.0.1:1/0", limit=1)
    for _ in range(3):  # far over the limit, but with no Redis nothing is counted
        assert limiter.check("some-client").allowed is True


def test_api_docs_can_be_disabled_for_production() -> None:
    app = create_app(engine=make_engine(UNREACHABLE_DB), docs_enabled=False)
    client = TestClient(app, raise_server_exceptions=False)
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404, path


def test_api_docs_are_available_in_development() -> None:
    client = TestClient(create_app(engine=make_engine(UNREACHABLE_DB)))
    assert client.get("/docs").status_code == 200


def test_database_down_gives_503_with_retry_after_not_500() -> None:
    # Authenticating needs the database, which is unreachable here.
    response = make_app(max_body_bytes=1000).post(
        "/v1/jobs",
        content=SMALL_BODY,
        headers={"Content-Type": "application/json", "X-API-Key": "qf_anything"},
    )
    assert response.status_code == 503
    assert response.json() == {"detail": "Service temporarily unavailable"}
    assert response.headers["Retry-After"] == "5"
