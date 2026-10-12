"""Worker behaviour against a real PostgreSQL."""

import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from queueforge.db.models import Job, JobAttempt, Notification
from queueforge.domain.states import JobStatus
from queueforge.services import jobs as job_service
from queueforge.tasks import handlers
from queueforge.worker.claims import claim_next_job, finish_job
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]


@pytest.fixture
def sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


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


def attempts_of(sessions: sessionmaker[Session], job_id: uuid.UUID) -> list[JobAttempt]:
    with sessions() as session:
        return list(session.scalars(select(JobAttempt).where(JobAttempt.job_id == job_id)))


# --- claiming ----------------------------------------------------------------


def test_claim_returns_none_when_queue_is_empty(sessions: sessionmaker[Session]) -> None:
    with sessions() as session:
        assert claim_next_job(session, "w1", 30) is None


def test_claim_marks_job_running_and_records_attempt(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client_id, _ = make_client("alice")
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        claimed = claim_next_job(session, "w1", 30)
    assert claimed is not None and claimed.id == job_id
    job = load_job(sessions, job_id)
    assert job.status == "running"
    assert job.attempts == 1
    assert job.lease_owner == "w1"
    assert job.lease_token == claimed.lease_token
    assert job.lease_expires_at is not None
    [attempt] = attempts_of(sessions, job_id)
    assert attempt.attempt_number == 1 and attempt.outcome is None


def test_a_claimed_job_is_not_claimed_again(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client_id, _ = make_client("alice")
    submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        assert claim_next_job(session, "w1", 30) is not None
    with sessions() as session:
        assert claim_next_job(session, "w2", 30) is None


def test_job_scheduled_in_the_future_is_not_claimed(
    sessions: sessionmaker[Session], make_client: MakeClient, engine: Engine
) -> None:
    client_id, _ = make_client("alice")
    submit(sessions, client_id, "echo", {"message": "x"})
    with engine.begin() as conn:
        conn.execute(text("UPDATE jobs SET run_at = now() + interval '1 hour'"))
    with sessions() as session:
        assert claim_next_job(session, "w1", 30) is None


# --- running tasks end to end ---------------------------------------------------


def test_worker_runs_a_job_submitted_through_the_api(
    api: TestClient, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    _, key = make_client("alice")
    headers = {"X-API-Key": key}
    created = api.post(
        "/v1/jobs", json={"task_type": "echo", "payload": {"message": "hi"}}, headers=headers
    ).json()

    assert Worker(sessions, "w1").run_once() is True

    job = api.get(f"/v1/jobs/{created['id']}", headers=headers).json()
    assert job["status"] == "succeeded"
    assert job["result"] == {"echo": "hi"}
    assert job["attempts"] == 1
    assert job["completed_at"] is not None
    [attempt] = attempts_of(sessions, uuid.UUID(created["id"]))
    assert attempt.outcome == "succeeded"
    assert attempt.worker_id == "w1"
    assert attempt.finished_at is not None and attempt.duration_ms is not None


def test_worker_returns_false_when_idle(sessions: sessionmaker[Session]) -> None:
    assert Worker(sessions, "w1").run_once() is False


def test_sleep_task(sessions: sessionmaker[Session], make_client: MakeClient) -> None:
    client_id, _ = make_client("alice")
    job_id = submit(sessions, client_id, "sleep", {"seconds": 0.05})
    Worker(sessions, "w1").run_once()
    job = load_job(sessions, job_id)
    assert job.status == "succeeded" and job.result == {"slept_seconds": 0.05}


def test_flaky_task_fails_then_would_succeed_later(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    # No retries until Milestone 4, so a failing attempt is final for now.
    client_id, _ = make_client("alice")
    failing = submit(sessions, client_id, "flaky", {"fail_times": 1})
    Worker(sessions, "w1").run_once()
    job = load_job(sessions, failing)
    assert job.status == "failed"
    assert job.last_error == {"type": "TaskError", "message": "deliberate failure on attempt 1"}
    [attempt] = attempts_of(sessions, failing)
    assert attempt.outcome == "permanent_error" and attempt.error_type == "TaskError"

    succeeding = submit(sessions, client_id, "flaky", {"fail_times": 0})
    Worker(sessions, "w1").run_once()
    assert load_job(sessions, succeeding).status == "succeeded"


def test_notification_task_is_idempotent(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client_id, _ = make_client("alice")
    payload = {"recipient": "bob@example.com", "message": "hello"}
    job_id = submit(sessions, client_id, "simulated_notification", payload)
    Worker(sessions, "w1").run_once()
    assert load_job(sessions, job_id).result == {
        "recipient": "bob@example.com",
        "already_sent": False,
    }

    # Simulate a duplicate execution of the same job (as after a crash).
    ctx = handlers.HandlerContext(job_id, 2, handlers.NotificationPayload(**payload), sessions)
    assert handlers.handle_notification(ctx)["already_sent"] is True
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(Notification)) == 1


def test_unexpected_error_is_stored_without_leaking_its_message(
    sessions: sessionmaker[Session],
    make_client: MakeClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(ctx: handlers.HandlerContext) -> dict[str, object]:
        raise RuntimeError("connection string postgresql://user:SECRET-PW@db/x")

    monkeypatch.setitem(handlers.HANDLERS, "echo", boom)
    client_id, _ = make_client("alice")
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    Worker(sessions, "w1").run_once()

    job = load_job(sessions, job_id)
    assert job.status == "failed"
    assert job.last_error is not None and job.last_error["type"] == "UnexpectedError"
    assert "SECRET-PW" not in str(job.last_error)
    [attempt] = attempts_of(sessions, job_id)
    assert "SECRET-PW" not in (attempt.error_message or "")


# --- fencing: a worker that lost its lease cannot overwrite the job ---------------


def test_stale_worker_result_is_discarded(
    sessions: sessionmaker[Session], make_client: MakeClient, engine: Engine
) -> None:
    client_id, _ = make_client("alice")
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    with sessions() as session:
        claimed = claim_next_job(session, "slow-worker", 30)
    assert claimed is not None

    # Someone else takes over the lease (what recovery will do in Milestone 4).
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE jobs SET lease_token = :t, lease_owner = 'other' WHERE id = :id"),
            {"t": uuid.uuid4(), "id": job_id},
        )

    with sessions() as session:
        accepted = finish_job(session, claimed, status=JobStatus.SUCCEEDED, result={"late": True})
    assert accepted is False

    job = load_job(sessions, job_id)
    assert job.status == "running" and job.result is None  # untouched
    [attempt] = attempts_of(sessions, job_id)
    assert attempt.outcome is None


# --- competing workers ---------------------------------------------------------------


def test_competing_workers_never_double_process_a_job(
    sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client_id, _ = make_client("alice")
    total = 200
    ids = {submit(sessions, client_id, "echo", {"message": str(i)}) for i in range(total)}
    assert len(ids) == total

    def drain(worker_id: str) -> int:
        worker = Worker(sessions, worker_id)
        processed = 0
        while worker.run_once():
            processed += 1
        return processed

    with ThreadPoolExecutor(max_workers=4) as pool:
        counts = list(pool.map(drain, [f"w{i}" for i in range(4)]))

    assert sum(counts) == total  # every job processed exactly once in total
    with sessions() as session:
        jobs = list(session.scalars(select(Job)))
        assert len(jobs) == total
        assert all(j.status == "succeeded" and j.attempts == 1 for j in jobs)
        # One attempt row per job: no job was ever claimed twice.
        per_job = session.execute(
            select(JobAttempt.job_id, func.count()).group_by(JobAttempt.job_id)
        ).all()
        assert len(per_job) == total
        assert all(n == 1 for _, n in per_job)
