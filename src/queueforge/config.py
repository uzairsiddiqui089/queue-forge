"""Application configuration, read from environment variables.

There are deliberately no defaults for credentials: the database URL must be
supplied by the environment (see `.env.example`), never hard-coded here.

Settings are validated when the app or a worker starts. A bad combination stops the
process immediately with a clear message, instead of misbehaving later.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import make_url

# Passwords that must never be used in production (case-insensitive).
WEAK_PASSWORDS = frozenset(
    {"change-me", "changeme", "password", "postgres", "queueforge", "secret", "admin", "test"}
)
MIN_PRODUCTION_PASSWORD_LENGTH = 12


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # "production" turns on stricter checks and hides the interactive API docs.
    environment: Literal["development", "production"] = "development"

    # e.g. postgresql+psycopg://user:password@host:5432/dbname
    database_url: str = Field(min_length=1)
    log_level: str = "INFO"

    # API protection (REDIS_URL may contain a password in production).
    redis_url: str | None = None  # rate limiting only; if unset, rate limiting is off
    rate_limit_per_minute: int = Field(120, ge=1)  # per API key
    max_request_body_bytes: int = Field(65_536, ge=1024)
    # Backpressure: how many jobs may wait in the queue. Over the limit, submits are refused.
    max_queued_jobs_per_client: int = Field(10_000, ge=1)
    max_queued_jobs_total: int = Field(100_000, ge=1)
    # How long the "is the whole queue full?" answer may be reused. Counting a deep queue on
    # every submit is expensive (measured 44-99 ms at 100k queued); 0 disables the cache.
    queue_depth_cache_seconds: float = Field(1.0, ge=0)
    # How long /metrics results may be reused (the big aggregates scan whole tables). 0 = never.
    metrics_cache_seconds: float = Field(10.0, ge=0)

    # Database connections. Budget: (API replicas x (pool + overflow)) + (workers x ~3)
    # must stay below PostgreSQL's max_connections (100 by default).
    db_pool_size: int = Field(10, ge=1)
    db_max_overflow: int = Field(10, ge=0)
    db_pool_timeout_seconds: float = Field(10.0, gt=0)  # waiting for a free connection
    db_statement_timeout_seconds: float = Field(30.0, gt=0)
    db_lock_timeout_seconds: float = Field(5.0, gt=0)
    db_idle_in_transaction_timeout_seconds: float = Field(60.0, gt=0)

    # Worker tuning.
    worker_poll_interval_seconds: float = Field(1.0, gt=0)
    # A claimed job is leased for this long; the worker renews it with a heartbeat while it
    # works, and the sweeper re-queues jobs whose lease expired (the worker is presumed dead).
    worker_lease_seconds: int = Field(30, ge=1)  # the heartbeat rule below is the real safeguard
    worker_heartbeat_interval_seconds: float = Field(10.0, gt=0)  # must be well below the lease
    worker_sweep_interval_seconds: float = Field(10.0, gt=0)
    # On shutdown: how long to let the current job finish before handing it back.
    worker_shutdown_drain_seconds: float = Field(25.0, ge=0)
    # If this many timed-out handler threads are still stuck, the worker exits so it is restarted.
    worker_max_abandoned_threads: int = Field(3, ge=1)
    # The worker touches this file regularly; the container healthcheck reads its age.
    worker_liveness_file: str = "/tmp/queueforge-worker-alive"  # noqa: S108

    # Retention: how long FINISHED jobs are kept before the maintenance process deletes them
    # (with their attempts, idempotency keys and notifications). Queued/running jobs are never
    # deleted. A deleted job answers 404 from then on.
    retention_succeeded_days: int = Field(7, ge=1)
    retention_failed_days: int = Field(30, ge=1)
    retention_dead_letter_days: int = Field(30, ge=1)
    purge_batch_size: int = Field(1000, ge=1, le=10_000)  # jobs per transaction
    purge_max_batches_per_cycle: int = Field(50, ge=1)  # bounds one cycle's work
    maintenance_interval_seconds: float = Field(60.0, gt=0)
    maintenance_liveness_file: str = "/tmp/queueforge-maintenance-alive"  # noqa: S108

    # Fair claiming: workers serve the client with the fewest running jobs first, so one
    # client's big backlog does not delay everyone else. Turn off for oldest-first overall.
    fair_claiming: bool = True
    # At most this many clients with waiting jobs are compared per claim (keeps the claim cheap).
    # With more active clients than this, fairness is approximate: see docs/architecture.md.
    fair_claim_max_clients: int = Field(64, ge=1)

    # Retry delay = random value between 0 and min(max, base * 2^(attempt-1)) ("full jitter").
    retry_base_delay_seconds: float = Field(2.0, ge=0)
    retry_max_delay_seconds: float = Field(300.0, ge=0)

    @model_validator(mode="after")
    def check_consistency(self) -> "Settings":
        if self.worker_heartbeat_interval_seconds * 3 > self.worker_lease_seconds:
            raise ValueError(
                "WORKER_HEARTBEAT_INTERVAL_SECONDS must be at most a third of "
                "WORKER_LEASE_SECONDS, or jobs will lose their lease while still running"
            )
        if self.retry_max_delay_seconds < self.retry_base_delay_seconds:
            raise ValueError("RETRY_MAX_DELAY_SECONDS must not be below RETRY_BASE_DELAY_SECONDS")
        if self.max_queued_jobs_per_client > self.max_queued_jobs_total:
            raise ValueError("MAX_QUEUED_JOBS_PER_CLIENT must not exceed MAX_QUEUED_JOBS_TOTAL")
        if self.environment == "production":
            password = make_url(self.database_url).password or ""
            if password.lower() in WEAK_PASSWORDS or len(password) < MIN_PRODUCTION_PASSWORD_LENGTH:
                raise ValueError(
                    "In production the database password must not be a placeholder and must be "
                    f"at least {MIN_PRODUCTION_PASSWORD_LENGTH} characters long"
                )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # value comes from the environment
