"""Claiming jobs and recording their outcome. This is the heart of the system.

Rules enforced here:
- Claiming is ONE short transaction, committed BEFORE the handler runs.
- Two workers can never claim the same job: `FOR UPDATE SKIP LOCKED` makes a
  worker skip rows another worker has locked, instead of waiting or double-taking.
- The attempt counter goes up at claim time, so a job that kills its worker
  still uses up an attempt.
- Results are written with a "fence": the UPDATE only matches if the job is still
  `running` AND still holds the lease token this worker was given. A worker that
  lost its lease writes 0 rows and its late result is discarded.
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import Integer, func, select, update
from sqlalchemy.orm import Session

from queueforge.db.models import Job, JobAttempt
from queueforge.domain.states import AttemptOutcome, JobStatus, can_transition


@dataclass(frozen=True)
class ClaimedJob:
    id: uuid.UUID
    task_type: str
    payload: dict[str, Any]
    attempt_number: int
    lease_token: uuid.UUID


def claim_next_job(session: Session, worker_id: str, lease_seconds: int) -> ClaimedJob | None:
    """Claim the oldest runnable job, or return None if there is none."""
    assert can_transition(JobStatus.QUEUED, JobStatus.RUNNING)

    job_id = session.scalar(
        select(Job.id)
        .where(Job.status == JobStatus.QUEUED.value, Job.run_at <= func.now())
        .order_by(Job.run_at, Job.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if job_id is None:
        session.rollback()  # nothing to do; end the transaction
        return None

    token = uuid.uuid4()
    row = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.QUEUED.value)
        .values(
            status=JobStatus.RUNNING.value,
            attempts=Job.attempts + 1,
            lease_token=token,
            lease_owner=worker_id,
            lease_expires_at=func.now() + timedelta(seconds=lease_seconds),
        )
        .returning(Job.attempts, Job.task_type, Job.payload)
        .execution_options(synchronize_session=False)
    ).one_or_none()
    if row is None:  # cannot happen while we hold the row lock, but never assume
        session.rollback()
        return None

    session.add(JobAttempt(job_id=job_id, attempt_number=row.attempts, worker_id=worker_id))
    session.commit()
    return ClaimedJob(
        id=job_id,
        task_type=row.task_type,
        payload=row.payload,
        attempt_number=row.attempts,
        lease_token=token,
    )


def finish_job(
    session: Session,
    claimed: ClaimedJob,
    *,
    status: JobStatus,
    result: dict[str, Any] | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> bool:
    """Record the outcome. Returns False if the lease was lost (the write is discarded)."""
    assert can_transition(JobStatus.RUNNING, status)
    outcome = (
        AttemptOutcome.SUCCEEDED
        if status == JobStatus.SUCCEEDED
        else AttemptOutcome.PERMANENT_ERROR
    )
    last_error = {"type": error_type, "message": error_message} if error_type is not None else None

    updated = session.execute(
        update(Job)
        .where(
            Job.id == claimed.id,
            Job.status == JobStatus.RUNNING.value,
            Job.lease_token == claimed.lease_token,  # the fence
        )
        .values(
            status=status.value,
            result=result,
            last_error=last_error,
            completed_at=func.now(),
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
        )
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).first()
    if updated is None:
        session.rollback()
        return False

    session.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == claimed.id,
            JobAttempt.attempt_number == claimed.attempt_number,
        )
        .values(
            finished_at=func.now(),
            duration_ms=(func.extract("epoch", func.now() - JobAttempt.started_at) * 1000).cast(
                Integer
            ),
            outcome=outcome.value,
            error_type=error_type,
            error_message=error_message,
        )
        .execution_options(synchronize_session=False)
    )
    session.commit()  # job update and attempt update succeed or fail together
    return True
