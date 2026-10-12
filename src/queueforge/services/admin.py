"""Operations for admins: inspect dead-lettered jobs and put them back in the queue."""

import uuid
from enum import StrEnum

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from queueforge.db.models import Job, JobAttempt
from queueforge.domain.retry import DEFAULT_MAX_ATTEMPTS
from queueforge.domain.states import JobStatus, can_transition


class RequeueResult(StrEnum):
    REQUEUED = "requeued"
    NOT_FOUND = "not_found"
    NOT_DEAD_LETTERED = "not_dead_lettered"


def list_dead_lettered(session: Session, limit: int, offset: int) -> list[Job]:
    stmt = (
        select(Job)
        .where(Job.status == JobStatus.DEAD_LETTERED.value)
        .order_by(Job.completed_at.desc(), Job.id)
        .limit(limit)
        .offset(offset)
    )
    return list(session.scalars(stmt))


def get_any_job(session: Session, job_id: uuid.UUID) -> Job | None:
    """Admins can read any client's job."""
    return session.get(Job, job_id)


def list_attempts(session: Session, job_id: uuid.UUID) -> list[JobAttempt]:
    stmt = select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.attempt_number)
    return list(session.scalars(stmt))


def requeue_dead_lettered(session: Session, job_id: uuid.UUID) -> RequeueResult:
    """Give a dead-lettered job a fresh set of attempts.

    We do NOT reset `attempts` to 0. The attempt history in `job_attempts` must stay
    unique and truthful, so instead we raise `max_attempts` to allow another full
    round of tries on top of those already used.
    """
    assert can_transition(JobStatus.DEAD_LETTERED, JobStatus.QUEUED)
    updated = session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.DEAD_LETTERED.value)  # conditional
        .values(
            status=JobStatus.QUEUED.value,
            run_at=func.now(),
            max_attempts=Job.attempts + DEFAULT_MAX_ATTEMPTS,
            completed_at=None,
        )
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).first()
    if updated is not None:
        session.commit()
        return RequeueResult.REQUEUED
    session.rollback()
    exists = session.scalar(select(Job.id).where(Job.id == job_id))
    return RequeueResult.NOT_DEAD_LETTERED if exists else RequeueResult.NOT_FOUND
