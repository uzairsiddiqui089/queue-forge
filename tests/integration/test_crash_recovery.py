"""The real thing: kill a worker process in the middle of a job and watch recovery.

This starts actual `python -m queueforge.worker` processes and kills one with a hard
kill (SIGKILL on Linux/macOS, TerminateProcess on Windows). Nothing in the worker
gets a chance to clean up, exactly like a crash or a power cut.
"""

import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest
from sqlalchemy import Engine, select
from sqlalchemy.orm import sessionmaker

from queueforge.db.models import Job, JobAttempt
from queueforge.services import jobs as job_service

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


def wait_for(
    condition: Callable[[], bool],
    timeout: float,
    what: str,
    diagnostics: Callable[[], str] | None = None,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.1)
    extra = ("\n--- worker output ---\n" + diagnostics()) if diagnostics else ""
    pytest.fail(f"timed out waiting for: {what}{extra}")


@pytest.mark.skipif(TEST_DATABASE_URL is None, reason="TEST_DATABASE_URL is not set")
def test_job_survives_a_worker_being_killed_mid_run(
    engine: Engine, make_client: Callable[[str], tuple[uuid.UUID, str]], tmp_path: Path
) -> None:
    assert TEST_DATABASE_URL is not None
    env = {
        **os.environ,
        "DATABASE_URL": TEST_DATABASE_URL,
        # Short timers so the test is quick; the logic is identical to production values.
        "WORKER_LEASE_SECONDS": "2",
        "WORKER_HEARTBEAT_INTERVAL_SECONDS": "0.5",
        "WORKER_POLL_INTERVAL_SECONDS": "0.2",
        "WORKER_SWEEP_INTERVAL_SECONDS": "0.5",
        "RETRY_BASE_DELAY_SECONDS": "0.1",
        "RETRY_MAX_DELAY_SECONDS": "0.1",
    }
    sessions = sessionmaker(engine, expire_on_commit=False)
    client_id, _ = make_client("alice")
    with sessions() as session:
        job, _ = job_service.submit_job(session, client_id, "sleep", {"seconds": 3}, None)
        job_id = job.id

    def status() -> str:
        with sessions() as session:
            current = session.scalar(select(Job.status).where(Job.id == job_id))
            assert current is not None
            return current

    log_file = (tmp_path / "workers.log").open("wb")  # kept, so a failure can show why

    def worker_output() -> str:
        log_file.flush()
        return (tmp_path / "workers.log").read_text(errors="replace")[-3000:]

    def start_worker() -> "subprocess.Popen[bytes]":
        return subprocess.Popen(
            [sys.executable, "-m", "queueforge.worker"],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    first = start_worker()
    second: subprocess.Popen[bytes] | None = None
    try:
        wait_for(lambda: status() == "running", 30, "first worker to start the job", worker_output)
        first.kill()  # hard kill in the middle of the job
        first.wait()
        assert status() == "running"  # nobody has noticed yet

        second = start_worker()  # a fresh worker; its sweeper must rescue the job
        wait_for(
            lambda: status() == "succeeded",
            40,
            "the job to be recovered and finished",
            worker_output,
        )
    finally:
        for proc in (first, second):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait()

    with sessions() as session:
        done = session.get(Job, job_id)
        assert done is not None and done.attempts == 2
        attempts = list(
            session.scalars(
                select(JobAttempt)
                .where(JobAttempt.job_id == job_id)
                .order_by(JobAttempt.attempt_number)
            )
        )
    assert [a.outcome for a in attempts] == ["lease_expired", "succeeded"]
    assert attempts[0].worker_id != attempts[1].worker_id  # a different worker finished it
