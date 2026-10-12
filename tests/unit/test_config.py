"""Settings are validated at startup: bad combinations must stop the process early."""

import pytest
from pydantic import ValidationError

from queueforge.config import Settings

GOOD_URL = "postgresql+psycopg://svc:a-long-random-password-123@db:5432/queueforge"


def make(**overrides: object) -> Settings:
    return Settings(_env_file=None, database_url=GOOD_URL, **overrides)  # type: ignore[call-arg, arg-type]


def test_defaults_are_valid() -> None:
    settings = make()
    assert settings.environment == "development"
    assert settings.worker_heartbeat_interval_seconds * 3 <= settings.worker_lease_seconds


def test_heartbeat_must_be_well_below_the_lease() -> None:
    with pytest.raises(ValidationError, match="third"):
        make(worker_lease_seconds=30, worker_heartbeat_interval_seconds=15)
    make(worker_lease_seconds=30, worker_heartbeat_interval_seconds=10)  # exactly a third is fine


@pytest.mark.parametrize(
    "overrides",
    [
        {"worker_lease_seconds": 0},
        {"worker_poll_interval_seconds": 0},
        {"rate_limit_per_minute": 0},
        {"max_request_body_bytes": 10},
        {"db_pool_size": 0},
        {"db_pool_timeout_seconds": 0},
        {"max_queued_jobs_per_client": 0},
        {"retry_base_delay_seconds": -1},
    ],
)
def test_nonsensical_values_are_rejected(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        make(**overrides)


def test_retry_max_delay_must_not_be_below_base() -> None:
    with pytest.raises(ValidationError, match="RETRY_MAX_DELAY"):
        make(retry_base_delay_seconds=10, retry_max_delay_seconds=5)


def test_per_client_queue_cap_cannot_exceed_the_global_cap() -> None:
    with pytest.raises(ValidationError, match="MAX_QUEUED_JOBS_PER_CLIENT"):
        make(max_queued_jobs_per_client=500, max_queued_jobs_total=100)


@pytest.mark.parametrize("password", ["change-me", "password", "short", "Postgres", "queueforge"])
def test_production_rejects_weak_or_placeholder_database_passwords(password: str) -> None:
    url = f"postgresql+psycopg://svc:{password}@db:5432/queueforge"
    with pytest.raises(ValidationError, match="password"):
        Settings(_env_file=None, database_url=url, environment="production")  # type: ignore[call-arg]


def test_production_accepts_a_strong_password() -> None:
    assert make(environment="production").environment == "production"


def test_development_still_allows_the_example_password() -> None:
    url = "postgresql+psycopg://queueforge:change-me@127.0.0.1:5432/queueforge"
    assert Settings(_env_file=None, database_url=url).environment == "development"  # type: ignore[call-arg]
