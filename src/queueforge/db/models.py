"""Database tables. PostgreSQL is the source of truth for all job state.

Timestamps use the database clock (`now()`), never the clock of an API or
worker machine, so comparisons stay consistent across processes.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Index, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from queueforge.db.base import Base
from queueforge.domain.states import AttemptOutcome, JobStatus

_STATUS_LIST = ", ".join(f"'{s.value}'" for s in JobStatus)
_OUTCOME_LIST = ", ".join(f"'{o.value}'" for o in AttemptOutcome)


class ApiClient(Base):
    __tablename__ = "api_clients"
    __table_args__ = (CheckConstraint("role IN ('client', 'admin')", name="ck_api_clients_role"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    # SHA-256 of the API key. The key itself is never stored.
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    role: Mapped[str] = mapped_column(String(20), default="client", server_default="client")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    disabled_at: Mapped[datetime | None] = mapped_column(default=None)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(f"status IN ({_STATUS_LIST})", name="ck_jobs_status"),
        # Workers (Milestone 3) look for the next queued job by run_at.
        Index("ix_jobs_queued_run_at", "run_at", postgresql_where=text("status = 'queued'")),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    client_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("api_clients.id"), index=True)
    task_type: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)

    status: Mapped[str] = mapped_column(
        String(20), default=JobStatus.QUEUED.value, server_default=JobStatus.QUEUED.value
    )
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(default=5, server_default="5")
    run_at: Mapped[datetime] = mapped_column(server_default=func.now())

    # Lease fields are used by workers from Milestone 3 onwards.
    lease_owner: Mapped[str | None] = mapped_column(String(100), default=None)
    lease_token: Mapped[uuid.UUID | None] = mapped_column(default=None)
    lease_expires_at: Mapped[datetime | None] = mapped_column(default=None)

    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    last_error: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(default=None)


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    # The primary key is what makes concurrent duplicates safe: only one insert wins.
    client_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("api_clients.id"), primary_key=True)
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id"))
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    expires_at: Mapped[datetime]


class JobAttempt(Base):
    """One row per execution try of a job (created when a worker claims it)."""

    __tablename__ = "job_attempts"
    __table_args__ = (
        UniqueConstraint("job_id", "attempt_number", name="uq_job_attempts_job_attempt"),
        CheckConstraint(
            f"outcome IS NULL OR outcome IN ({_OUTCOME_LIST})", name="ck_job_attempts_outcome"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id"), index=True)
    attempt_number: Mapped[int]
    worker_id: Mapped[str] = mapped_column(String(100))
    started_at: Mapped[datetime] = mapped_column(server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(default=None)
    duration_ms: Mapped[int | None] = mapped_column(default=None)
    outcome: Mapped[str | None] = mapped_column(String(30), default=None)  # None while running
    error_type: Mapped[str | None] = mapped_column(String(100), default=None)
    error_message: Mapped[str | None] = mapped_column(String(500), default=None)  # sanitized


class Notification(Base):
    """Side effect of the `simulated_notification` task.

    The primary key on job_id is what makes that task safe to run twice: a second
    run for the same job inserts nothing. This is how a handler stays correct
    under at-least-once delivery.
    """

    __tablename__ = "notifications"

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id"), primary_key=True)
    recipient: Mapped[str] = mapped_column(String(200))
    message: Mapped[str] = mapped_column(String(1000))
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
