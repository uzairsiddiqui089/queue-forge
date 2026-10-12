"""Job lifecycle states and the rules for moving between them.

This table is the single source of truth. Every database state change must be
a legal transition here, and is also written as a conditional UPDATE
(`WHERE status = :expected`) so a stale or illegal change matches 0 rows.
"""

from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


class AttemptOutcome(StrEnum):
    """How one execution attempt ended. An attempt that is still running has no outcome."""

    SUCCEEDED = "succeeded"
    RETRYABLE_ERROR = "retryable_error"  # used from Milestone 4
    PERMANENT_ERROR = "permanent_error"
    LEASE_EXPIRED = "lease_expired"  # used from Milestone 4


ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.QUEUED,  # retry or lease expired, attempts remain
            JobStatus.DEAD_LETTERED,  # attempts exhausted
        }
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.DEAD_LETTERED: frozenset(),
}

TERMINAL_STATUSES = frozenset(s for s, targets in ALLOWED_TRANSITIONS.items() if not targets)


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]
