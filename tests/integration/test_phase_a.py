"""Phase A: failure injection for the production-critical fixes.

Each test breaks something on purpose (a hung handler, a shutdown signal, a failing
database, a flooded queue, an exhausted connection pool) and checks the system copes.
"""

import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from queueforge.api.app import create_app
from queueforge.db.engine import make_engine
from queueforge.db.models import Job, JobAttempt
from queueforge.domain.retry import RetryPolicy
from queueforge.services import jobs as job_service
from queueforge.services.jobs import QueueLimits
from queueforge.tasks import handlers
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
NO_DELAY = RetryPolicy(base_delay=0, max_delay=0)
ECHO = {"task_type": "echo", "payload": {"message": "hi"}}


@pytest.fixture
def sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def client_id(make_client: MakeClient) -> uuid.UUID:
    return make_client("alice")[0]


@pytest.fixture
def unblock() -> Iterator[threading.Event]:
    """Lets a deliberately stuck handler finish when the test is over."""
    event = threading.Event()
    yield event
    event.set()


def submit(
    sessions: sessionmaker[Session], client_id: uuid.UUID, task: str, payload: dict[str, Any]
) -> uuid.UUID:
    with sessions() as session:
        job, _ = job_service.submit_job(session, client_id, task, payload, None)
        return job.id


def load_job(sessions: sessionmaker[Session], job_id: uuid.UUID) -> Job:
    with sessions() as session:
        job = session.get(Job, job_id)
        assert job is not None
        return job


def outcomes(sessions: sessionmaker[Session], job_id: uuid.UUID) -> list[str | None]:
    with sessions() as session:
        rows = (
            session.query(JobAttempt).filter_by(job_id=job_id).order_by(JobAttempt.attempt_number)
        )
        return [a.outcome for a in rows]


def wait_until(condition: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    pytest.fail(f"timed out waiting for: {what}")


# --- R1: a hung handler is cut off ---------------------------------------------------


def test_hung_handler_is_timed_out_and_the_job_is_retried(
    sessions: sessionmaker[Session],
    client_id: uuid.UUID,
    unblock: threading.Event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def hangs_on_first_attempt(ctx: handlers.HandlerContext) -> dict[str, object]:
        if ctx.attempt_number == 1:
            unblock.wait(30)  # never returns during the test
        return {"echo": "ok"}

    monkeypatch.setitem(handlers.HANDLERS, "echo", hangs_on_first_attempt)
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    worker = Worker(sessions, "w1", retry_policy=NO_DELAY, handler_timeouts={"echo": 0.3})

    started = time.monotonic()
    assert worker.run_once() is True
    assert time.monotonic() - started < 3  # gave up after ~0.3 s instead of waiting forever

    job = load_job(sessions, job_id)
    assert job.status == "queued" and job.lease_token is None  # not stuck running
    assert job.last_error == {
        "type": "HandlerTimeout",
        "message": "The task exceeded its time limit",
    }

    assert worker.run_once() is True  # the worker is free; the retry succeeds
    assert load_job(sessions, job_id).status == "succeeded"
    assert outcomes(sessions, job_id) == ["retryable_error", "succeeded"]


def test_worker_exits_when_too_many_handlers_are_stuck(
    sessions: sessionmaker[Session],
    client_id: uuid.UUID,
    unblock: threading.Event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(handlers.HANDLERS, "echo", lambda ctx: unblock.wait(30) or {})
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    worker = Worker(
        sessions,
        "w1",
        retry_policy=NO_DELAY,
        handler_timeouts={"echo": 0.2},
        max_abandoned_threads=2,
        poll_interval=0.05,
    )
    runner = threading.Thread(target=worker.run_forever)
    runner.start()
    runner.join(timeout=15)

    assert not runner.is_alive()  # it stopped itself instead of piling up stuck threads
    assert worker.exit_requested is True  # so a supervisor knows to restart it
    assert load_job(sessions, job_id).status == "queued"  # and the job is not lost


# --- R3: graceful shutdown --------------------------------------------------------------


def test_shutdown_lets_a_short_job_finish_and_starts_nothing_new(
    sessions: sessionmaker[Session], client_id: uuid.UUID
) -> None:
    stop = threading.Event()
    worker = Worker(sessions, "w1", stop_event=stop, drain_seconds=5)
    job_id = submit(sessions, client_id, "sleep", {"seconds": 0.6})
    other_id = submit(sessions, client_id, "echo", {"message": "later"})

    runner = threading.Thread(target=worker.run_once)
    runner.start()
    time.sleep(0.2)
    stop.set()  # SIGTERM arrives mid-job
    runner.join(timeout=10)

    assert load_job(sessions, job_id).status == "succeeded"  # drained, not interrupted
    assert outcomes(sessions, job_id) == ["succeeded"]
    assert worker.run_once() is False  # shutting down: no new work is claimed
    assert load_job(sessions, other_id).status == "queued"


def test_shutdown_hands_back_a_long_job_without_using_up_an_attempt(
    sessions: sessionmaker[Session],
    client_id: uuid.UUID,
    unblock: threading.Event,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(handlers.HANDLERS, "echo", lambda ctx: unblock.wait(30) or {})
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    stop = threading.Event()
    worker = Worker(sessions, "w1", stop_event=stop, drain_seconds=0.3)

    runner = threading.Thread(target=worker.run_once)
    runner.start()
    wait_until(lambda: load_job(sessions, job_id).status == "running", 5, "job to start")
    stop.set()
    runner.join(timeout=10)
    assert not runner.is_alive()

    job = load_job(sessions, job_id)
    assert job.status == "queued" and job.lease_token is None
    assert job.attempts == 1 and job.max_attempts == 6  # the interrupted try is not charged
    assert outcomes(sessions, job_id) == ["released"]  # and it is not recorded as a failure

    unblock.set()
    healthy = Worker(sessions, "w2")  # another worker simply picks it up
    assert healthy.run_once() is True
    assert load_job(sessions, job_id).status == "succeeded"
    assert outcomes(sessions, job_id) == ["released", "succeeded"]


# --- R2 / R4: the worker survives errors and does not flood the log --------------------


class FlakySessions:
    """A session factory whose first `failures` calls fail like a database outage."""

    def __init__(self, real: sessionmaker[Session], failures: int) -> None:
        self._real = real
        self.remaining = failures

    def __call__(self) -> Session:
        if self.remaining > 0:
            self.remaining -= 1
            raise OperationalError("SELECT 1", {}, Exception("connection refused"))
        return self._real()


def test_worker_survives_a_database_outage_and_logs_little(
    sessions: sessionmaker[Session],
    client_id: uuid.UUID,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    flaky = FlakySessions(sessions, failures=40)
    stop = threading.Event()
    worker = Worker(
        flaky,  # type: ignore[arg-type]
        "w1",
        stop_event=stop,
        poll_interval=0.01,
        max_error_backoff=0.05,
        sweep_interval=0.01,
    )
    caplog.set_level(logging.INFO, logger="queueforge.worker")
    runner = threading.Thread(target=worker.run_forever)
    runner.start()
    try:
        wait_until(lambda: load_job(sessions, job_id).status == "succeeded", 15, "recovery")
    finally:
        stop.set()
        runner.join(timeout=10)

    messages = [r.getMessage() for r in caplog.records]
    assert messages.count("worker loop error; backing off") == 1  # the full trace, once
    assert messages.count("worker recovered") == 1
    noisy = [m for m in messages if m.startswith("worker loop")]
    assert len(noisy) <= 8  # dozens of failures, a handful of log lines


def test_unexpected_exception_in_the_loop_does_not_kill_the_worker(
    sessions: sessionmaker[Session],
    client_id: uuid.UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import queueforge.worker.runner as runner_module

    real_claim = runner_module.claim_next_job
    calls = {"n": 0}

    def sometimes_explodes(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] <= 3:
            raise ValueError("a bug that is not a database error")
        return real_claim(*args, **kwargs)

    monkeypatch.setattr(runner_module, "claim_next_job", sometimes_explodes)
    job_id = submit(sessions, client_id, "echo", {"message": "x"})
    stop = threading.Event()
    worker = Worker(sessions, "w1", stop_event=stop, poll_interval=0.01, max_error_backoff=0.05)
    runner = threading.Thread(target=worker.run_forever)
    runner.start()
    try:
        wait_until(lambda: load_job(sessions, job_id).status == "succeeded", 10, "job to finish")
        assert runner.is_alive()  # still running after three unexpected exceptions
    finally:
        stop.set()
        runner.join(timeout=10)


# --- R6: backpressure -----------------------------------------------------------------------


def limited_api(engine: Engine, per_client: int, total: int) -> TestClient:
    limits = QueueLimits(per_client=per_client, total=total, retry_after_seconds=7)
    return TestClient(create_app(engine=engine, queue_limits=limits))


def test_one_client_cannot_fill_the_queue(engine: Engine, make_client: MakeClient) -> None:
    api = limited_api(engine, per_client=3, total=100)
    _, alice = make_client("alice")
    _, bob = make_client("bob")
    first = api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": alice, "Idempotency-Key": "k1"})
    assert first.status_code == 201
    for _ in range(2):
        assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": alice}).status_code == 201

    blocked = api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": alice})
    assert blocked.status_code == 429
    assert blocked.headers["Retry-After"] == "7"
    assert blocked.json() == {"detail": "Too many queued jobs"}

    assert (
        api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": bob}).status_code == 201
    )  # others fine
    replay = api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": alice, "Idempotency-Key": "k1"})
    assert replay.status_code == 200  # a replay creates nothing, so it is never refused
    assert replay.json()["id"] == first.json()["id"]


def test_a_full_queue_refuses_everyone_with_503(engine: Engine, make_client: MakeClient) -> None:
    api = limited_api(engine, per_client=100, total=3)
    _, alice = make_client("alice")
    _, bob = make_client("bob")
    for key in (alice, alice, bob):
        assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key}).status_code == 201
    for key in (alice, bob):
        refused = api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key})
        assert refused.status_code == 503
        assert refused.headers["Retry-After"] == "7"


def test_submits_are_accepted_again_once_the_queue_drains(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    api = limited_api(engine, per_client=2, total=100)
    _, key = make_client("alice")
    for _ in range(2):
        api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key})
    assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key}).status_code == 429

    worker = Worker(sessions, "w1")
    while worker.run_once():
        pass
    assert api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key}).status_code == 201


# --- R7 / R8: connection pool and database timeouts ----------------------------------------


def test_pool_exhaustion_gives_503_not_500(engine: Engine, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    tiny = make_engine(
        engine.url.render_as_string(hide_password=False),
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.3,
    )
    api = TestClient(create_app(engine=tiny), raise_server_exceptions=False)
    hogging = tiny.connect()  # the only connection is busy
    try:
        response = api.post("/v1/jobs", json=ECHO, headers={"X-API-Key": key})
    finally:
        hogging.close()
        tiny.dispose()
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"


def test_database_timeouts_are_applied_to_every_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        assert conn.execute(text("SHOW statement_timeout")).scalar_one() == "30s"
        assert conn.execute(text("SHOW lock_timeout")).scalar_one() == "5s"
        assert conn.execute(text("SHOW idle_in_transaction_session_timeout")).scalar_one() == "1min"


def test_a_runaway_query_is_cancelled_by_the_statement_timeout(engine: Engine) -> None:
    quick = make_engine(
        engine.url.render_as_string(hide_password=False), statement_timeout_seconds=0.3
    )
    try:
        started = time.monotonic()
        with pytest.raises(OperationalError), quick.connect() as conn:
            conn.execute(text("SELECT pg_sleep(10)"))
        assert time.monotonic() - started < 3
    finally:
        quick.dispose()


# --- O1: you can see whether workers are alive -----------------------------------------------


def workers_alive(api: TestClient, admin_key: str) -> float:
    body = api.get("/metrics", headers={"X-API-Key": admin_key}).text
    for family in text_string_to_metric_families(body):
        if family.name == "queueforge_workers_alive":
            return float(family.samples[0].value)
    raise AssertionError("queueforge_workers_alive is missing from /metrics")


def test_workers_alive_metric_and_liveness_file(
    api: TestClient,
    engine: Engine,
    sessions: sessionmaker[Session],
    make_admin: MakeClient,
    tmp_path: Path,
) -> None:
    _, admin_key = make_admin("ops")
    assert workers_alive(api, admin_key) == 0  # no workers yet: this is the alert condition

    alive_file = tmp_path / "alive"
    stop = threading.Event()
    worker = Worker(
        sessions, "w1", stop_event=stop, poll_interval=0.05, liveness_file=str(alive_file)
    )
    runner = threading.Thread(target=worker.run_forever)
    runner.start()
    try:
        wait_until(lambda: workers_alive(api, admin_key) == 1, 10, "worker to appear")
        assert alive_file.exists() and time.time() - float(alive_file.read_text()) < 10

        with engine.begin() as conn:  # pretend it went silent a minute ago
            conn.execute(text("UPDATE workers SET last_seen_at = now() - interval '60 seconds'"))
        assert workers_alive(api, admin_key) == 0
    finally:
        stop.set()
        runner.join(timeout=10)

    with engine.connect() as conn:  # a clean shutdown is recorded
        assert conn.execute(text("SELECT stopped_at IS NOT NULL FROM workers")).scalar_one() is True
