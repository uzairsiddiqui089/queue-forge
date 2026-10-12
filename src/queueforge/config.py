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
    # Milestone 3 uses a fixed lease. Heartbeats and expiry recovery arrive in Milestone 4.
    worker_lease_seconds: int = 30


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # value comes from the environment
