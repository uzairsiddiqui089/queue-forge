"""Claiming jobs, recording outcomes, renewing leases and recovering dead workers.

Rules enforced here:
- Claiming is ONE short transaction, committed BEFORE the handler runs.
- Two workers can never claim the same job: `FOR UPDATE SKIP LOCKED` makes a
  worker skip rows another worker has locked, instead of waiting or double-taking.
- The attempt counter goes up at claim time, so a job that kills its worker
  still uses up an attempt and cannot loop forever.
- Every write by a worker is "fenced": the UPDATE only matches if the job is still
  `running` AND still holds the lease token this worker was given. A worker that
  lost its lease writes 0 rows and its late result is discarded.
- All times come from the database clock (`now()`), never from a worker's clock.
"""

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import Integer, func, select, text, update
from sqlalchemy.orm import Session

from queueforge.db.models import Job, JobAttempt
from queueforge.domain.retry import RetryPolicy
from queueforge.domain.states import AttemptOutcome, JobStatus, can_transition


@dataclass(frozen=True)
class ClaimedJob:
    id: uuid.UUID
    task_type: str
    payload: dict[str, Any]
    attempt_number: int
    max_attempts: int
    lease_token: uuid.UUID


# Fair claiming, step 1: choose WHICH CLIENT to serve next.
#
# The first CTE lists the clients that have runnable queued jobs, one index lookup per client
# (a "loose index scan": jump from one client_id to the next instead of reading every job),
# looking at no more than :max_clients of them. The second counts each client's running jobs
# (only a handful of rows exist in the `running` state). The client with the FEWEST running
# jobs wins; ties go to the client whose oldest runnable job has waited longest.
_PICK_FAIR_CLIENT_SQL = text(
    """
    WITH RECURSIVE clients(client_id, depth) AS (
        (SELECT client_id, 1 FROM jobs
         WHERE status = 'queued' AND run_at <= now()
         ORDER BY client_id LIMIT 1)
        UNION ALL
        SELECT (SELECT j.client_id FROM jobs j
                WHERE j.status = 'queued' AND j.run_at <= now() AND j.client_id > c.client_id
                ORDER BY j.client_id LIMIT 1),
               c.depth + 1
        FROM clients c
        WHERE c.client_id IS NOT NULL AND c.depth < :max_clients
    ),
    running AS (
        SELECT client_id, count(*) AS n FROM jobs WHERE status = 'running' GROUP BY client_id
    )
    SELECT c.client_id
    FROM clients c
    LEFT JOIN running r ON r.client_id = c.client_id
    CROSS JOIN LATERAL (
        SELECT j.run_at FROM jobs j
        WHERE j.client_id = c.client_id AND j.status = 'queued' AND j.run_at <= now()
        ORDER BY j.run_at LIMIT 1
    ) oldest
    WHERE c.client_id IS NOT NULL
    ORDER BY coalesce(r.n, 0), oldest.run_at
    LIMIT 1
    """
)


def _pick_job_id(session: Session, *, fair: bool, max_clients: int) -> uuid.UUID | None:
    """Find (and row-lock) the next job to run.

    Fair mode serves the client with the fewest running jobs first, so a small client is not
    stuck behind another client's backlog. It never leaves a worker idle while any runnable job
    exists: if the chosen client's jobs are all locked by other workers, or fair mode finds
    nothing, it falls back to the plain "oldest runnable job overall" rule.
    """
    if fair:
        client_id = session.scalar(_PICK_FAIR_CLIENT_SQL, {"max_clients": max_clients})
        if client_id is not None:
            job_id = session.scalar(
                select(Job.id)
                .where(
                    Job.client_id == client_id,
                    Job.status == JobStatus.QUEUED.value,
                    Job.run_at <= func.now(),
                )
                .order_by(Job.run_at, Job.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if job_id is not None:
                return job_id
    return session.scalar(
        select(Job.id)
        .where(Job.status == JobStatus.QUEUED.value, Job.run_at <= func.now())
        .order_by(Job.run_at, Job.created_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )


def claim_next_job(
    session: Session,
    worker_id: str,
    lease_seconds: int,
    *,
    fair: bool = True,
    max_clients: int = 64,
) -> ClaimedJob | None:
    """Claim the next runnable job, or return None if there is none.

    With `fair=True` (the default) clients take turns by how many jobs each already has running;
    with `fair=False` the oldest runnable job overall goes first."""
    assert can_transition(JobStatus.QUEUED, JobStatus.RUNNING)

    job_id = _pick_job_id(session, fair=fair, max_clients=max_clients)
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
        .returning(Job.attempts, Job.max_attempts, Job.task_type, Job.payload)
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
        max_attempts=row.max_attempts,
        lease_token=token,
    )


def _close_attempt(
    session: Session,
    job_id: uuid.UUID,
    attempt_number: int,
    outcome: AttemptOutcome,
    error_type: str | None,
    error_message: str | None,
) -> None:
    session.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt_number == attempt_number,
            JobAttempt.outcome.is_(None),  # never overwrite an attempt that already ended
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


def finish_job(
    session: Session,
    claimed: ClaimedJob,
    *,
    status: JobStatus,
    result: dict[str, Any] | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> bool:
    """Record a FINAL outcome (`succeeded` or `failed`).

    Returns False if the lease was lost (the write is discarded).
    """
    assert status in (JobStatus.SUCCEEDED, JobStatus.FAILED)
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

    _close_attempt(session, claimed.id, claimed.attempt_number, outcome, error_type, error_message)
    session.commit()  # job update and attempt update succeed or fail together
    return True


def _retry_values(
    attempts: int, max_attempts: int, delay_seconds: float, last_error: dict[str, str]
) -> dict[str, Any]:
    """Column values for a job that failed but may be tried again.

    Out of attempts -> `dead_lettered` (final). Otherwise -> back to `queued`, to be
    claimed again once `run_at` (now + backoff delay) has passed.
    """
    common: dict[str, Any] = {
        "last_error": last_error,
        "lease_owner": None,
        "lease_token": None,
        "lease_expires_at": None,
    }
    if attempts >= max_attempts:
        assert can_transition(JobStatus.RUNNING, JobStatus.DEAD_LETTERED)
        return {**common, "status": JobStatus.DEAD_LETTERED.value, "completed_at": func.now()}
    assert can_transition(JobStatus.RUNNING, JobStatus.QUEUED)
    return {
        **common,
        "status": JobStatus.QUEUED.value,
        "run_at": func.now() + timedelta(seconds=delay_seconds),
    }


def fail_retryable(
    session: Session,
    claimed: ClaimedJob,
    *,
    error_type: str,
    error_message: str,
    delay_seconds: float,
) -> JobStatus | None:
    """Record a TEMPORARY failure: retry later, or dead-letter if attempts are used up.

    Returns the job's new status, or None if the lease was lost (write discarded).
    """
    values = _retry_values(
        claimed.attempt_number,
        claimed.max_attempts,
        delay_seconds,
        {"type": error_type, "message": error_message},
    )
    updated = session.execute(
        update(Job)
        .where(
            Job.id == claimed.id,
            Job.status == JobStatus.RUNNING.value,
            Job.lease_token == claimed.lease_token,  # the fence
        )
        .values(**values)
        .returning(Job.status)
        .execution_options(synchronize_session=False)
    ).first()
    if updated is None:
        session.rollback()
        return None

    _close_attempt(
        session,
        claimed.id,
        claimed.attempt_number,
        AttemptOutcome.RETRYABLE_ERROR,
        error_type,
        error_message,
    )
    session.commit()
    return JobStatus(updated.status)


def extend_lease(
    session: Session, job_id: uuid.UUID, lease_token: uuid.UUID, lease_seconds: int
) -> bool:
    """Heartbeat: push the lease expiry forward. False means the lease was lost."""
    updated = session.execute(
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.RUNNING.value,
            Job.lease_token == lease_token,
        )
        .values(lease_expires_at=func.now() + timedelta(seconds=lease_seconds))
        .returning(Job.id)
        .execution_options(synchronize_session=False)
    ).first()
    session.commit()
    return updated is not None


def sweep_expired_leases(session: Session, policy: RetryPolicy, batch_size: int = 50) -> int:
    """Recover jobs whose worker stopped renewing its lease (crashed, killed, or stuck).

    Each expired job is re-queued with a backoff delay, or dead-lettered if it has
    used all its attempts. Returns how many jobs were recovered.

    Safe to run on every worker at once: `SKIP LOCKED` hands each expired job to
    exactly one sweeper, and the UPDATE re-checks that it is still running and
    still expired.

    NOTE: if the "dead" worker was only slow, the job may run again while the first
    run is still going. That duplicate execution is why handlers must be idempotent.
    The old worker's late result is rejected by the lease-token fence.
    """
    expired = session.execute(
        select(Job.id, Job.attempts, Job.max_attempts)
        .where(Job.status == JobStatus.RUNNING.value, Job.lease_expires_at < func.now())
        .order_by(Job.lease_expires_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    ).all()

    recovered = 0
    for job_id, attempts, max_attempts in expired:
        values = _retry_values(
            attempts,
            max_attempts,
            policy.delay_for(attempts),
            {"type": "LeaseExpired", "message": "The worker stopped renewing its lease"},
        )
        updated = session.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.lease_expires_at < func.now(),
            )
            .values(**values)
            .returning(Job.id)
            .execution_options(synchronize_session=False)
        ).first()
        if updated is not None:
            _close_attempt(
                session,
                job_id,
                attempts,
                AttemptOutcome.LEASE_EXPIRED,
                "LeaseExpired",
                "The worker stopped renewing its lease",
            )
            recovered += 1
    session.commit()
    return recovered


def release_job(session: Session, claimed: ClaimedJob) -> bool:
    """Hand a running job back to the queue because its worker is shutting down.

    The attempt is closed as `released` and `max_attempts` goes up by one, so the
    interrupted attempt does not use up one of the job's tries (`attempts` itself is
    never reduced, because the attempt history must stay unique and truthful).
    Fenced like every other worker write. Returns False if the lease was already lost.
    """
    assert can_transition(JobStatus.RUNNING, JobStatus.QUEUED)
    updated = session.execute(
        update(Job)
        .where(
            Job.id == claimed.id,
            Job.status == JobStatus.RUNNING.value,
            Job.lease_token == claimed.lease_token,  # the fence
        )
        .values(
            status=JobStatus.QUEUED.value,
            run_at=func.now(),
            max_attempts=Job.max_attempts + 1,
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
    _close_attempt(session, claimed.id, claimed.attempt_number, AttemptOutcome.RELEASED, None, None)
    session.commit()
    return True
