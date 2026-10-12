"""Readiness against a real PostgreSQL."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from queueforge.api.app import create_app

pytestmark = pytest.mark.integration


def test_readyz_returns_200_with_a_live_database(engine: Engine) -> None:
    response = TestClient(create_app(engine=engine)).get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
