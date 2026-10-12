"""Health endpoint behaviour that needs no running database."""

from fastapi.testclient import TestClient

from queueforge.api.app import create_app
from queueforge.db.engine import make_engine

# Port 1 is reserved and nothing listens there, so connections are refused at once.
UNREACHABLE_URL = "postgresql+psycopg://nobody:not-a-real-password@127.0.0.1:1/none"


def make_client() -> TestClient:
    return TestClient(create_app(engine=make_engine(UNREACHABLE_URL)))


def test_healthz_does_not_depend_on_the_database() -> None:
    response = make_client().get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_returns_503_when_database_is_down() -> None:
    response = make_client().get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"
