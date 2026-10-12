"""Application configuration, read from environment variables.

There are deliberately no defaults for credentials: the database URL must be
supplied by the environment (see `.env.example`), never hard-coded here.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # e.g. postgresql+psycopg://user:password@host:5432/dbname
    database_url: str = Field(min_length=1)
    log_level: str = "INFO"

    # Worker tuning (not secrets).
    worker_poll_interval_seconds: float = 1.0
    # A claimed job is leased for this long; the worker renews it with a heartbeat while it
    # works, and the sweeper re-queues jobs whose lease expired (the worker is presumed dead).
    worker_lease_seconds: int = 30
    worker_heartbeat_interval_seconds: float = 10.0  # must be well below the lease
    worker_sweep_interval_seconds: float = 10.0

    # Retry delay = random value between 0 and min(max, base * 2^(attempt-1)) ("full jitter").
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 300.0


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # value comes from the environment
