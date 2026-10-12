"""Job submission and lookup.

Transaction boundary: a job and its idempotency record are written in ONE
transaction. The caller is only told "created" after that transaction commits,
so a client never holds a job ID for a job that was not saved.
"""

import hashlib
import json
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from queueforge.db.models import IdempotencyKey, Job

IDEMPOTENCY_RETENTION = timedelta(hours=24)


class IdempotencyConflict(Exception):
    """The same Idempotency-Key was reused with a different request body."""


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
) -> tuple[Job, bool]:
    """Save a job. Returns (job, created). created=False means an earlier
    submission with the same idempotency key was returned instead."""
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
