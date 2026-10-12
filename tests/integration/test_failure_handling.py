"""Retries, dead-lettering, lease expiry and recovery, against a real PostgreSQL."""

import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import Engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from queueforge.db.models import Job, JobAttempt
from queueforge.domain.retry import RetryPolicy
from queueforge.domain.states import JobStatus
from queueforge.services import jobs as job_service
from queueforge.tasks import handlers
from queueforge.worker.claims import (
    claim_next_job,
    fail_retryable,
    finish_job,
    sweep_expired_leases,
)
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
NO_DELAY = RetryPolicy(base_delay=0, max_delay=0)  # retries are runnable immediately


@pytest.fixture
def sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def client_id(make_client: MakeClient) -> uuid.UUID:
    return make_client("alice")[0]


def submit(
    sessions: sessionmaker[Session],
    client_id: uuid.UUID,
    task_type: str,
    payload: dict[str, object],
) -> uuid.UUID:
    with sessions() as session:
        job, _ = job_service.submit_job(session, client_id, task_type, payload, None)
        return job.id


def load_job(sessions: sessionmaker[Session], job_id: uuid.UUID) -> Job:
    with sessions() as session:
        job = session.get(Job, job_id)
        assert job is not None
        return job


def outcomes(sessions: sessionmaker[Session], job_id: uuid.UUID) -> list[str | None]:
    with sessions() as session:
        stmt = (
            select(JobAttempt.outcome)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number)
        )
        return list(session.scalars(stmt))


def expire_lease(engine: Engine, job_id: uuid.UUID | None = None) -> None:
    """Pretend the worker holding the job stopped heartbeating a while ago."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE jobs SET lease_expires_at = now() - interval '1 second' "
                "WHERE status = 'running' AND (CAST(:id AS uuid) IS NULL OR id = :id)"
            ),
            {"id": job_id},
        )


# --- retries -----------------------------------------------------------------


def test_retry_then_succeed(sessions: sessionmaker[Session], client_id: uuid.UUID) -> None:
    job_id = submit(sessions, client_id, "flaky", {"fail_times": 2})
    worker = Worker(sessions, "w1", retry_policy=NO_DELAY)

    worker.run_once()
    assert load_job(sessions, job_id).status == "queued"
    worker.run_once()
    assert load_job(sessions, job_id).status == "queued"
    worker.run_once()

    job = load_job(sessions, job_id)
    assert job.status == "succeeded" and job.attempts == 3
    assert job.result == {"succeeded_on_attempt": 3}
    assert outcomes(sessions, job_id) == ["retryable_error", "retryable_error", "succeeded"]


def test_a_failed_job_waits_for_its_backoff_delay(
    sessions: sessionmaker[Session], client_id: uuid.UUID, engine: Engine
) -> None:
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        claimed = claim_next_job(session, "w1", 30)
    assert claimed is not None
    with sessions() as session:
        status = fail_retryable(
            session, claimed, error_type="RetryableError", error_message="x", delay_seconds=60
        )
    assert status == JobStatus.QUEUED
    with engine.connect() as conn:
        waiting = conn.execute(
            text("SELECT run_at > now() + interval '50 seconds' FROM jobs WHERE id = :id"),
            {"id": job_id},
        ).scalar()
    assert waiting is True
    with sessions() as session:
        assert claim_next_job(session, "w2", 30) is None  # not runnable yet


def test_retries_exhausted_dead_letters_the_job(
    sessions: sessionmaker[Session], client_id: uuid.UUID
) -> None:
    job_id = submit(sessions, client_id, "flaky", {"fail_times": 10})  # always fails
    worker = Worker(sessions, "w1", retry_policy=NO_DELAY)
    for _ in range(5):  # max_attempts is 5
        assert worker.run_once() is True

    job = load_job(sessions, job_id)
    assert job.status == "dead_lettered"
    assert job.attempts == 5
    assert job.completed_at is not None
    assert job.last_error is not None and job.last_error["type"] == "RetryableError"
    assert outcomes(sessions, job_id) == ["retryable_error"] * 5
    assert worker.run_once() is False  # and it is never picked up again


def test_permanent_error_fails_immediately_without_retry(
    sessions: sessionmaker[Session], client_id: uuid.UUID, monkeypatch: pytest.MonkeyPatch
) -> None:
    def invalid(ctx: handlers.HandlerContext) -> dict[str, object]:
        raise handlers.TaskError("this input can never work")

    monkeypatch.setitem(handlers.HANDLERS, "echo", invalid)
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    worker = Worker(sessions, "w1", retry_policy=NO_DELAY)
    worker.run_once()

    job = load_job(sessions, job_id)
    assert job.status == "failed" and job.attempts == 1
    assert outcomes(sessions, job_id) == ["permanent_error"]
    assert worker.run_once() is False


def test_unknown_exception_is_retried_without_leaking_its_message(
    sessions: sessionmaker[Session], client_id: uuid.UUID, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(ctx: handlers.HandlerContext) -> dict[str, object]:
        raise RuntimeError("postgresql://user:SECRET-PW@db/x")

    monkeypatch.setitem(handlers.HANDLERS, "echo", boom)
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    Worker(sessions, "w1", retry_policy=NO_DELAY).run_once()

    job = load_job(sessions, job_id)
    assert job.status == "queued"  # unknown errors are treated as temporary
    assert "SECRET-PW" not in str(job.last_error)


def test_poison_job_is_eventually_dead_lettered(
    sessions: sessionmaker[Session], client_id: uuid.UUID, engine: Engine
) -> None:
    """A job that kills its worker every single time must not loop forever."""
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    for attempt in range(1, 6):
        with sessions() as session:
            claimed = claim_next_job(session, f"doomed-{attempt}", 30)
        assert claimed is not None and claimed.attempt_number == attempt
        expire_lease(engine)  # the worker "died": no result, no heartbeat
        with sessions() as session:
            assert sweep_expired_leases(session, NO_DELAY) == 1

    job = load_job(sessions, job_id)
    assert job.status == "dead_lettered" and job.attempts == 5
    assert job.last_error is not None and job.last_error["type"] == "LeaseExpired"
    assert outcomes(sessions, job_id) == ["lease_expired"] * 5
    with sessions() as session:
        assert claim_next_job(session, "w", 30) is None


# --- lease expiry and recovery ----------------------------------------------------


def test_sweeper_leaves_live_leases_alone(
    sessions: sessionmaker[Session], client_id: uuid.UUID
) -> None:
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        claim_next_job(session, "w1", 30)
    with sessions() as session:
        assert sweep_expired_leases(session, NO_DELAY) == 0
    assert load_job(sessions, job_id).status == "running"


def test_abandoned_job_is_requeued_and_runs_again(
    sessions: sessionmaker[Session], client_id: uuid.UUID, engine: Engine
) -> None:
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        claim_next_job(session, "crashed-worker", 30)
    expire_lease(engine)
    with sessions() as session:
        assert sweep_expired_leases(session, NO_DELAY) == 1

    job = load_job(sessions, job_id)
    assert job.status == "queued" and job.lease_token is None

    assert Worker(sessions, "w2", retry_policy=NO_DELAY).run_once() is True
    job = load_job(sessions, job_id)
    assert job.status == "succeeded" and job.attempts == 2
    assert outcomes(sessions, job_id) == ["lease_expired", "succeeded"]


def test_stale_worker_cannot_overwrite_the_recovered_job(
    sessions: sessionmaker[Session], client_id: uuid.UUID, engine: Engine
) -> None:
    """The scenario behind duplicate execution: worker A is slow, not dead."""
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        a = claim_next_job(session, "worker-a", 30)
    assert a is not None
    expire_lease(engine)
    with sessions() as session:
        sweep_expired_leases(session, NO_DELAY)
    with sessions() as session:
        b = claim_next_job(session, "worker-b", 30)
    assert b is not None and b.lease_token != a.lease_token

    with sessions() as session:  # A wakes up and tries to report its (late) result
        assert finish_job(session, a, status=JobStatus.SUCCEEDED, result={"from": "a"}) is False
    with sessions() as session:
        assert finish_job(session, b, status=JobStatus.SUCCEEDED, result={"from": "b"}) is True

    job = load_job(sessions, job_id)
    assert job.result == {"from": "b"}
    assert outcomes(sessions, job_id) == ["lease_expired", "succeeded"]


def test_concurrent_sweepers_recover_each_job_exactly_once(
    sessions: sessionmaker[Session], client_id: uuid.UUID, engine: Engine
) -> None:
    ids = [submit(sessions, client_id, "echo", {"message": str(i)}) for i in range(20)]
    for _ in ids:
        with sessions() as session:
            claim_next_job(session, "w", 30)
    expire_lease(engine)

    def sweep() -> int:
        with sessions() as session:
            return sweep_expired_leases(session, NO_DELAY, batch_size=3)

    totals: list[int] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for _ in range(8):
            totals.extend(pool.map(lambda _: sweep(), range(4)))

    assert sum(totals) == 20
    with sessions() as session:
        assert session.scalar(select(func.count()).where(Job.status == "queued")) == 20
        per_job = session.execute(
            select(JobAttempt.job_id, func.count()).group_by(JobAttempt.job_id)
        ).all()
        assert all(n == 1 for _, n in per_job)  # each expiry recorded once


def test_heartbeat_keeps_a_long_job_from_being_stolen(
    sessions: sessionmaker[Session], client_id: uuid.UUID
) -> None:
    # The lease is 1 s but the job takes 2 s: only the heartbeat keeps it alive.
    job_id = submit(sessions, client_id, "sleep", {"seconds": 2})
    worker = Worker(sessions, "w1", lease_seconds=1, heartbeat_interval=0.2, retry_policy=NO_DELAY)
    runner = threading.Thread(target=worker.run_once)
    runner.start()

    stolen = 0
    while runner.is_alive():  # a rival sweeps the whole time
        with sessions() as session:
            stolen += sweep_expired_leases(session, NO_DELAY)
        runner.join(timeout=0.1)

    assert stolen == 0
    job = load_job(sessions, job_id)
    assert job.status == "succeeded" and job.attempts == 1
