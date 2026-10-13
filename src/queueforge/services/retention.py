"""Retention: delete old finished jobs, and the rows that hang off them, in small batches.

What is deleted, and when:
- `succeeded`, `failed` and `dead_lettered` jobs once they finished more than the configured
  number of days ago (a different number for each status). Their attempt records, idempotency
  keys and notification rows go in the same statement, so nothing is left pointing at a
  missing job.
- idempotency keys whose 24-hour window has passed (they are useless after it).
- worker presence rows that have not been seen for a day.

What is NEVER deleted: jobs that are still `queued` or `running`, however old.

Safety properties:
- Each batch is one short transaction (default 1,000 jobs), so a purge never holds a long lock
  or makes a huge transaction.
- Rows are chosen with `FOR UPDATE SKIP LOCKED`: a job someone else is touching right now
  (for example an admin re-queueing a dead-lettered job) is skipped and picked up next cycle,
  and several purgers can run at once without clashing.
- Oldest first, so a backlog is worked off in order.
"""

from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from queueforge.domain.states import JobStatus

WORKER_ROWS_RETENTION_DAYS = 1


@dataclass(frozen=True)
class RetentionPolicy:
    succeeded_days: int = 7
    failed_days: int = 30
    dead_letter_days: int = 30
    batch_size: int = 1000

    def days_for(self, status: JobStatus) -> int:
        return {
            JobStatus.SUCCEEDED: self.succeeded_days,
            JobStatus.FAILED: self.failed_days,
            JobStatus.DEAD_LETTERED: self.dead_letter_days,
        }[status]


# Order matters: the common, biggest category first.
PURGED_STATUSES = (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTERED)

# One statement deletes a batch of jobs AND their dependants. PostgreSQL checks foreign keys
# at the end of the statement, so deleting parent and child rows together is fine.
_DELETE_JOBS_BATCH = text(
    """
    WITH doomed AS (
        SELECT id FROM jobs
        WHERE status = :status AND completed_at < now() - make_interval(days => :days)
        ORDER BY completed_at
        LIMIT :batch
        FOR UPDATE SKIP LOCKED
    ),
    attempts AS (DELETE FROM job_attempts WHERE job_id IN (SELECT id FROM doomed) RETURNING 1),
    keys AS (DELETE FROM idempotency_keys WHERE job_id IN (SELECT id FROM doomed) RETURNING 1),
    notes AS (DELETE FROM notifications WHERE job_id IN (SELECT id FROM doomed) RETURNING 1),
    gone AS (DELETE FROM jobs WHERE id IN (SELECT id FROM doomed) RETURNING 1)
    SELECT (SELECT count(*) FROM gone), (SELECT count(*) FROM attempts),
           (SELECT count(*) FROM keys), (SELECT count(*) FROM notes)
    """
)

_DELETE_EXPIRED_KEYS_BATCH = text(
    """
    WITH doomed AS (
        SELECT client_id, key FROM idempotency_keys
        WHERE expires_at < now()
        ORDER BY expires_at
        LIMIT :batch
        FOR UPDATE SKIP LOCKED
    ),
    gone AS (
        DELETE FROM idempotency_keys k USING doomed d
        WHERE k.client_id = d.client_id AND k.key = d.key
        RETURNING 1
    )
    SELECT count(*) FROM gone
    """
)

_DELETE_STALE_WORKERS = text(
    """
    WITH gone AS (
        DELETE FROM workers
        WHERE last_seen_at < now() - make_interval(days => :days)
        RETURNING 1
    )
    SELECT count(*) FROM gone
    """
)

_COUNT_ELIGIBLE = text(
    """
    SELECT count(*) FROM jobs
    WHERE status = :status AND completed_at < now() - make_interval(days => :days)
    """
)


@dataclass
class PurgeResult:
    jobs: int = 0
    attempts: int = 0
    idempotency_keys: int = 0
    notifications: int = 0
    workers: int = 0
    batches: int = 0
    more_to_do: bool = False  # stopped at the batch limit while more was waiting

    @property
    def total_rows(self) -> int:
        return self.jobs + self.attempts + self.idempotency_keys + self.notifications + self.workers


def count_eligible(session: Session, policy: RetentionPolicy) -> dict[str, int]:
    """How many jobs a purge would delete right now, per status (a read-only dry run)."""
    counts = {}
    for status in PURGED_STATUSES:
        value = session.execute(
            _COUNT_ELIGIBLE, {"status": status.value, "days": policy.days_for(status)}
        ).scalar_one()
        counts[status.value] = int(value)
    session.rollback()
    return counts


def run_purge_cycle(
    session_factory: Callable[[], Session], policy: RetentionPolicy, max_batches: int
) -> PurgeResult:
    """Delete up to `max_batches` batches of old rows. Reports whether more is waiting."""
    result = PurgeResult()

    for status in PURGED_STATUSES:
        while True:
            if result.batches >= max_batches:
                result.more_to_do = True
                return result
            with session_factory() as session:
                row = session.execute(
                    _DELETE_JOBS_BATCH,
                    {
                        "status": status.value,
                        "days": policy.days_for(status),
                        "batch": policy.batch_size,
                    },
                ).one()
                session.commit()
            result.batches += 1
            result.jobs += int(row[0])
            result.attempts += int(row[1])
            result.idempotency_keys += int(row[2])
            result.notifications += int(row[3])
            if int(row[0]) < policy.batch_size:
                break  # this status is caught up

    while True:
        if result.batches >= max_batches:
            result.more_to_do = True
            return result
        with session_factory() as session:
            expired = int(
                session.execute(
                    _DELETE_EXPIRED_KEYS_BATCH, {"batch": policy.batch_size}
                ).scalar_one()
            )
            session.commit()
        result.batches += 1
        result.idempotency_keys += expired
        if expired < policy.batch_size:
            break

    with session_factory() as session:
        result.workers = int(
            session.execute(
                _DELETE_STALE_WORKERS, {"days": WORKER_ROWS_RETENTION_DAYS}
            ).scalar_one()
        )
        session.commit()
    return result
