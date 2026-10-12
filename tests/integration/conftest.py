"""Fixtures for tests that need a real PostgreSQL.

Safety: these tests wipe tables, so they only run against a database whose name
ends in "_test" (TEST_DATABASE_URL), never the development database.
"""

import os
import uuid
from collections.abc import Callable, Iterator

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, make_url, text
from sqlalchemy.orm import Session

from queueforge.api.app import create_app
from queueforge.db.engine import make_engine
from queueforge.db.models import ApiClient
from queueforge.services.auth import generate_api_key, hash_api_key

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    if TEST_DATABASE_URL is None:
        pytest.skip("TEST_DATABASE_URL is not set")
    database_name = make_url(TEST_DATABASE_URL).database or ""
    if not database_name.endswith("_test"):
        pytest.fail(
            f"Refusing to run: test database name must end in '_test', got {database_name!r}"
        )

    # Build the schema with the real migrations, so the tests also check them.
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", TEST_DATABASE_URL.replace("%", "%%"))
    command.upgrade(config, "head")

    test_engine = make_engine(TEST_DATABASE_URL)
    yield test_engine
    test_engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE notifications, job_attempts, idempotency_keys, jobs, api_clients CASCADE"
            )
        )


@pytest.fixture
def api(engine: Engine) -> TestClient:
    return TestClient(create_app(engine=engine))


@pytest.fixture
def make_client(engine: Engine) -> Callable[[str], tuple[uuid.UUID, str]]:
    """Create an API client; returns (client_id, raw_api_key)."""

    def _make(name: str) -> tuple[uuid.UUID, str]:
        key = generate_api_key()
        with Session(engine) as session:
            client = ApiClient(name=name, key_hash=hash_api_key(key))
            session.add(client)
            session.commit()
            return client.id, key

    return _make
