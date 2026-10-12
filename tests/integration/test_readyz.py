"""Readiness against a real PostgreSQL. Tests run against Postgres, never SQLite,
because later milestones depend on Postgres-specific behaviour (SKIP LOCKED)."""

import os

import pytest
from fastapi.testclient import TestClient

from queueforge.api.app import create_app
from queueforge.db.engine import make_engine

pytestmark = pytest.mark.integration

DATABASE_URL = os.environ.get("DATABASE_URL")


@pytest.mark.skipif(DATABASE_URL is None, reason="DATABASE_URL is not set")
def test_readyz_returns_200_with_a_live_database() -> None:
    assert DATABASE_URL is not None
    client = TestClient(create_app(engine=make_engine(DATABASE_URL)))
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
