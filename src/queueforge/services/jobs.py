"""Job submission and lookup.

Transaction boundary: a job and its idempotency record are written in ONE
transaction. The caller is only told "created" after that transaction commits,
so a client never holds a job ID for a job that was not saved.
"""

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from queueforge.db.models import IdempotencyKey, Job
from queueforge.domain.states import JobStatus

IDEMPOTENCY_RETENTION = timedelta(hours=24)


class IdempotencyConflict(Exception):
    """The same Idempotency-Key was reused with a different request body."""


@dataclass(frozen=True)
class QueueLimits:
    """Backpressure: the most jobs allowed to wait in the queue."""

    per_client: int = 10_000
    total: int = 100_000
    retry_after_seconds: int = 30
    # How long the answer to "is the whole queue full?" may be reused (seconds). Counting a
    # deep queue on every submit cost 44-99 ms at 100,000 queued jobs (docs/benchmarking.md).
    # The global cap is already a soft limit, so a short staleness is acceptable. 0 = always
    # count (exact for a single process; used by the tests).
    global_check_cache_seconds: float = 0.0


_global_full_cache: tuple[float, bool] = (0.0, False)  # (time checked, was the queue full)


class QueueFull(Exception):
    """The queue is at its limit. `scope` is "client" (that client's own cap) or "global"."""

    def __init__(self, scope: str, retry_after_seconds: int) -> None:
        super().__init__(scope)
        self.scope = scope
        self.retry_after_seconds = retry_after_seconds


def _global_queue_full(session: Session, limits: QueueLimits) -> bool:
    """Is the whole queue at its cap? The answer may be reused for a short time (see
    QueueLimits.global_check_cache_seconds). A benign race: two threads may both recount."""
    global _global_full_cache
    ttl = limits.global_check_cache_seconds
    if ttl > 0:
        checked_at, was_full = _global_full_cache
        if time.monotonic() - checked_at < ttl:
            return was_full
    full = _queued_at_least(session, limits.total, None)
    if ttl > 0:
        _global_full_cache = (time.monotonic(), full)
    return full


def _queued_at_least(session: Session, limit: int, client_id: uuid.UUID | None) -> bool:
    """True if at least `limit` jobs are waiting (for one client, or overall).

    Counts only up to `limit` rows, so the cost of the check is bounded no matter how
    big the queue grows. On its own the check is not safe under concurrency (two submits
    can both see "one below the cap"), so the per-client check runs under an advisory lock
    (see submit_job). The GLOBAL cap is not locked: it is a soft limit and can be exceeded
    slightly by simultaneous submits from different clients.
    """
    waiting = select(Job.id).where(Job.status == JobStatus.QUEUED.value)
    if client_id is not None:
        waiting = waiting.where(Job.client_id == client_id)
    count = session.scalar(select(func.count()).select_from(waiting.limit(limit).subquery()))
    return (count or 0) >= limit


def request_hash(task_type: str, payload: dict[str, Any]) -> str:
    # Canonical JSON (sorted keys, no spaces) so equal requests hash equally.
    canonical = json.dumps(
        {"task_type": task_type, "payload": payload}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _find_live_key(session: Session, client_id: uuid.UUID, key: str) -> IdempotencyKey | None:
    # "Live" means not expired, judged by the database clock.
    stmt = select(IdempotencyKey).where(
        IdempotencyKey.client_id == client_id,
        IdempotencyKey.key == key,
        IdempotencyKey.expires_at > func.now(),
    )
    return session.scalars(stmt).one_or_none()


def _replay(session: Session, existing: IdempotencyKey, req_hash: str) -> tuple[Job, bool]:
    if existing.request_hash != req_hash:
        raise IdempotencyConflict
    job = session.get(Job, existing.job_id)
    assert job is not None  # guaranteed by the foreign key
    return job, False


def submit_job(
    session: Session,
    client_id: uuid.UUID,
    task_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None,
    limits: QueueLimits | None = None,
) -> tuple[Job, bool]:
    """Save a job. Returns (job, created). created=False means an earlier
    submission with the same idempotency key was returned instead.

    Raises QueueFull if `limits` is given and the queue is over its limit. A replay of an
    earlier submission is never refused: it creates nothing."""
    req_hash = request_hash(task_type, payload)

    if idempotency_key is not None:
        existing = _find_live_key(session, client_id, idempotency_key)
        if existing is not None:
            return _replay(session, existing, req_hash)
        # An expired record for this key is treated as if it never existed.
        session.execute(
            delete(IdempotencyKey).where(
                IdempotencyKey.client_id == client_id,
                IdempotencyKey.key == idempotency_key,
                IdempotencyKey.expires_at <= func.now(),
            )
        )

    if limits is not None:
        # The global cap is a soft limit, so it is checked FIRST and without the lock below.
        # (Counting a deep queue takes tens of milliseconds; doing it while holding the
        # client's lock made that client's concurrent submits queue up behind each other and
        # time out. Measured in docs/benchmarking.md.)
        if _global_queue_full(session, limits):
            session.rollback()
            raise QueueFull("global", limits.retry_after_seconds)
        # Make the per-client cap exact. Without this, two simultaneous submits could both
        # see "499 of 500" and both insert. The lock is held until this transaction ends
        # (commit or rollback), so one client's check-and-insert steps run one at a time.
        # Other clients are not affected. The lock wait is bounded by lock_timeout.
        session.execute(
            select(func.pg_advisory_xact_lock(func.hashtextextended(f"queue-cap:{client_id}", 0)))
        )
        if _queued_at_least(session, limits.per_client, client_id):
            session.rollback()
            raise QueueFull("client", limits.retry_after_seconds)

    job = Job(client_id=client_id, task_type=task_type, payload=payload)
    session.add(job)
    session.flush()  # assigns defaults without committing

    if idempotency_key is not None:
        expires_at = session.scalar(select(func.now() + IDEMPOTENCY_RETENTION))
        assert expires_at is not None
        session.add(
            IdempotencyKey(
                client_id=client_id,
                key=idempotency_key,
                request_hash=req_hash,
                job_id=job.id,
                expires_at=expires_at,
            )
        )

    try:
        session.commit()
    except IntegrityError:
        # Two requests with the same key raced; the primary key picked a winner
        # and this request lost. Undo our job, then return the winner's job.
        session.rollback()
        if idempotency_key is None:
            raise
        winner = _find_live_key(session, client_id, idempotency_key)
        if winner is None:
            raise
        return _replay(session, winner, req_hash)

    return job, True


def get_job(session: Session, client_id: uuid.UUID, job_id: uuid.UUID) -> Job | None:
    """Fetch a job only if it belongs to this client (others look like "not found")."""
    stmt = select(Job).where(Job.id == job_id, Job.client_id == client_id)
    return session.scalars(stmt).one_or_none()
