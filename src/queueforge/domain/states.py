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
    # The worker was shut down mid-job and handed the job back. Does not use up an attempt.
    RELEASED = "released"


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
    # Only an admin can move a dead-lettered job, and only back to the queue.
    JobStatus.DEAD_LETTERED: frozenset({JobStatus.QUEUED}),
}

# Final for the system itself. (A dead-lettered job can still be re-queued by an admin.)
TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED})


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]
