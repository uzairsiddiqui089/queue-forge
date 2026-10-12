"""The only code a worker will ever run: a fixed allow-list of task handlers.

Handlers run OUTSIDE any database transaction held by the worker. A handler must
be safe to run more than once for the same job, because the system is
at-least-once (a crash can cause a re-run).
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from queueforge.db.models import Notification
from queueforge.tasks.schemas import (
    EchoPayload,
    FlakyPayload,
    NotificationPayload,
    SleepPayload,
)


class TaskError(Exception):
    """A deliberate, expected failure. Its message is written by us and is safe to store."""


@dataclass(frozen=True)
class HandlerContext:
    job_id: Any  # uuid.UUID
    attempt_number: int
    payload: BaseModel
    session_factory: sessionmaker[Session]


def handle_echo(ctx: HandlerContext) -> dict[str, Any]:
    assert isinstance(ctx.payload, EchoPayload)
    return {"echo": ctx.payload.message}


def handle_sleep(ctx: HandlerContext) -> dict[str, Any]:
    assert isinstance(ctx.payload, SleepPayload)
    time.sleep(ctx.payload.seconds)
    return {"slept_seconds": ctx.payload.seconds}


def handle_flaky(ctx: HandlerContext) -> dict[str, Any]:
    """Fails on its first `fail_times` attempts, then succeeds (for retry tests)."""
    assert isinstance(ctx.payload, FlakyPayload)
    if ctx.attempt_number <= ctx.payload.fail_times:
        raise TaskError(f"deliberate failure on attempt {ctx.attempt_number}")
    return {"succeeded_on_attempt": ctx.attempt_number}


def handle_notification(ctx: HandlerContext) -> dict[str, Any]:
    """Idempotent side effect: at most one notification row per job, however often it runs."""
    assert isinstance(ctx.payload, NotificationPayload)
    stmt = (
        insert(Notification)
        .values(
            job_id=ctx.job_id,
            recipient=ctx.payload.recipient,
            message=ctx.payload.message,
        )
        .on_conflict_do_nothing(index_elements=["job_id"])
        .returning(Notification.job_id)
    )
    with ctx.session_factory() as session:
        inserted = session.execute(stmt).first() is not None
        session.commit()
    return {"recipient": ctx.payload.recipient, "already_sent": not inserted}


HANDLERS: dict[str, Callable[[HandlerContext], dict[str, Any]]] = {
    "echo": handle_echo,
    "sleep": handle_sleep,
    "flaky": handle_flaky,
    "simulated_notification": handle_notification,
}
