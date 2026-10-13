"""Retention: old finished jobs and what hangs off them are deleted, nothing else."""

import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from queueforge.maintenance.runner import Maintenance
from queueforge.services import jobs as job_service
from queueforge.services.retention import RetentionPolicy, count_eligible, run_purge_cycle
from tests.integration.test_metrics import scrape

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
POLICY = RetentionPolicy(succeeded_days=7, failed_days=30, dead_letter_days=30, batch_size=1000)


@pytest.fixture
def sessions(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False)


def add_job(
    engine: Engine,
    client_id: uuid.UUID,
    status: str,
    age_days: float,
    *,
    attempt: bool = True,
    idempotency_key: bool = False,
    notification: bool = False,
) -> uuid.UUID:
    """Insert a job that finished `age_days` ago, with the rows that depend on it."""
    job_id = uuid.uuid4()
    finished = status in ("succeeded", "failed", "dead_lettered")
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO jobs (id, client_id, task_type, payload, status, attempts, "
                "created_at, run_at, completed_at) VALUES (:id, :c, 'echo', "
                '\'{"message":"x"}\', :status, 1, now() - make_interval(secs => :s), now(), '
                "CASE WHEN :finished THEN now() - make_interval(secs => :s) END)"
            ),
            {
                "id": job_id,
                "c": client_id,
                "status": status,
                "s": age_days * 86400,
                "finished": finished,
            },
        )
        if attempt:
            conn.execute(
                text(
                    "INSERT INTO job_attempts (job_id, attempt_number, worker_id, outcome) "
                    "VALUES (:id, 1, 'w', 'succeeded')"
                ),
                {"id": job_id},
            )
        if idempotency_key:
            conn.execute(
                text(
                    "INSERT INTO idempotency_keys "
                    "(client_id, key, request_hash, job_id, expires_at) "
                    "VALUES (:c, :k, 'h', :id, now() + interval '1 hour')"
                ),
                {"c": client_id, "k": f"key-{job_id}", "id": job_id},
            )
        if notification:
            conn.execute(
                text(
                    "INSERT INTO notifications (job_id, recipient, message) VALUES (:id, 'r', 'm')"
                ),
                {"id": job_id},
            )
    return job_id


def count(engine: Engine, table: str, where: str = "true") -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT count(*) FROM {table} WHERE {where}")).scalar_one())


def exists(engine: Engine, job_id: uuid.UUID) -> bool:
    with engine.connect() as conn:
        return (
            conn.execute(
                text("SELECT count(*) FROM jobs WHERE id = :id"), {"id": job_id}
            ).scalar_one()
            == 1
        )


def test_old_jobs_and_everything_attached_to_them_are_deleted(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    old = add_job(engine, client, "succeeded", 10, idempotency_key=True, notification=True)
    young = add_job(engine, client, "succeeded", 1, idempotency_key=True, notification=True)

    result = run_purge_cycle(sessions, POLICY, max_batches=10)

    assert result.jobs == 1 and result.attempts == 1
    assert result.idempotency_keys == 1 and result.notifications == 1
    assert not exists(engine, old) and exists(engine, young)
    # Nothing left pointing at the deleted job, and the young job's rows are untouched.
    assert count(engine, "job_attempts", f"job_id = '{old}'") == 0
    assert count(engine, "idempotency_keys", f"job_id = '{old}'") == 0
    assert count(engine, "notifications", f"job_id = '{old}'") == 0
    assert count(engine, "job_attempts", f"job_id = '{young}'") == 1
    assert count(engine, "idempotency_keys", f"job_id = '{young}'") == 1


def test_each_status_has_its_own_retention_period(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    at_10_days = {
        s: add_job(engine, client, s, 10) for s in ("succeeded", "failed", "dead_lettered")
    }
    at_40_days = {
        s: add_job(engine, client, s, 40) for s in ("succeeded", "failed", "dead_lettered")
    }

    run_purge_cycle(sessions, POLICY, max_batches=10)

    assert not exists(engine, at_10_days["succeeded"])  # 7-day limit passed
    assert exists(engine, at_10_days["failed"])  # 30-day limit not reached
    assert exists(engine, at_10_days["dead_lettered"])
    assert not any(exists(engine, j) for j in at_40_days.values())  # everything past 30 days


def test_jobs_that_are_not_finished_are_never_deleted_however_old(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    waiting = add_job(engine, client, "queued", 400, attempt=False)
    running = add_job(engine, client, "running", 400)

    result = run_purge_cycle(sessions, POLICY, max_batches=10)

    assert result.jobs == 0
    assert exists(engine, waiting) and exists(engine, running)


def test_work_is_done_in_batches_and_one_cycle_is_bounded(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    for _ in range(25):
        add_job(engine, client, "succeeded", 10)
    small = RetentionPolicy(batch_size=10)

    first = run_purge_cycle(sessions, small, max_batches=2)
    assert first.jobs == 20 and first.more_to_do is True  # stopped at the limit with work left
    assert count(engine, "jobs") == 5

    second = run_purge_cycle(sessions, small, max_batches=10)
    assert second.jobs == 5 and second.more_to_do is False
    assert count(engine, "jobs") == 0 and count(engine, "job_attempts") == 0


def test_two_purgers_running_together_delete_every_job_exactly_once(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    for _ in range(150):
        add_job(engine, client, "succeeded", 10)
    small = RetentionPolicy(batch_size=7)

    def purge_everything(_: int) -> int:
        deleted = 0
        while True:
            result = run_purge_cycle(sessions, small, max_batches=5)
            deleted += result.jobs
            if not result.more_to_do:
                return deleted

    with ThreadPoolExecutor(max_workers=3) as pool:
        totals = list(pool.map(purge_everything, range(3)))

    assert sum(totals) == 150  # each job deleted by exactly one purger
    assert count(engine, "jobs") == 0 and count(engine, "job_attempts") == 0


def test_a_job_someone_else_is_changing_right_now_is_skipped_not_waited_for(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    dead = add_job(engine, client, "dead_lettered", 40)  # old enough to purge
    other = add_job(engine, client, "succeeded", 10)

    with sessions() as admin:  # e.g. an admin re-queueing the dead-lettered job
        admin.execute(text("SELECT id FROM jobs WHERE id = :id FOR UPDATE"), {"id": dead})
        started = time.monotonic()
        result = run_purge_cycle(sessions, POLICY, max_batches=5)
        assert time.monotonic() - started < 2.0  # did not wait for the lock
        assert exists(engine, dead) and not exists(engine, other)
        assert result.jobs == 1
        admin.rollback()

    assert run_purge_cycle(sessions, POLICY, max_batches=5).jobs == 1  # picked up next time
    assert not exists(engine, dead)


def test_expired_idempotency_keys_go_and_live_ones_stay(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    with sessions() as session:
        job, _ = job_service.submit_job(session, client, "echo", {"message": "x"}, "live-key")
        job2, _ = job_service.submit_job(session, client, "echo", {"message": "y"}, "dead-key")
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE idempotency_keys SET expires_at = now() - interval '1 minute' "
                "WHERE key = 'dead-key'"
            )
        )

    result = run_purge_cycle(sessions, POLICY, max_batches=5)

    assert result.idempotency_keys == 1
    assert count(engine, "idempotency_keys", "key = 'live-key'") == 1
    assert count(engine, "idempotency_keys", "key = 'dead-key'") == 0
    assert exists(engine, job.id) and exists(engine, job2.id)  # queued jobs are untouched


def test_stale_worker_rows_are_removed(engine: Engine, sessions: sessionmaker[Session]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO workers (id, last_seen_at) VALUES ('old', now() - interval '3 days')")
        )
        conn.execute(text("INSERT INTO workers (id, last_seen_at) VALUES ('fresh', now())"))
    result = run_purge_cycle(sessions, POLICY, max_batches=5)
    assert result.workers == 1
    assert count(engine, "workers", "id = 'fresh'") == 1


def test_dry_run_counts_what_would_go_and_deletes_nothing(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    for _ in range(3):
        add_job(engine, client, "succeeded", 10)
    add_job(engine, client, "failed", 10)  # not old enough for its 30-day limit
    with sessions() as session:
        counts = count_eligible(session, POLICY)
    assert counts == {"succeeded": 3, "failed": 0, "dead_lettered": 0}
    assert count(engine, "jobs") == 4


def test_the_loop_survives_errors_and_still_purges(
    engine: Engine, sessions: sessionmaker[Session], make_client: MakeClient
) -> None:
    client = make_client("alice")[0]
    old = add_job(engine, client, "succeeded", 10)
    calls = {"n": 0}

    def flaky_sessions() -> Session:
        calls["n"] += 1
        if calls["n"] <= 3:
            raise RuntimeError("the database is having a moment")
        return sessions()

    stop = threading.Event()
    maintenance = Maintenance(
        flaky_sessions, POLICY, interval_seconds=0.05, max_error_backoff=0.05, stop_event=stop
    )
    runner = threading.Thread(target=maintenance.run_forever)
    runner.start()
    try:
        deadline = time.monotonic() + 15
        while exists(engine, old) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not exists(engine, old)  # purged after the failures
        assert runner.is_alive()  # and the loop never died
    finally:
        stop.set()
        runner.join(timeout=10)


def test_the_oldest_finished_job_age_metric_shows_when_purging_has_stopped(
    engine: Engine,
    sessions: sessionmaker[Session],
    make_client: MakeClient,
    make_admin: MakeClient,
    api: TestClient,
) -> None:
    client = make_client("alice")[0]
    _, admin_key = make_admin("ops")
    add_job(engine, client, "succeeded", 10)

    ten_days = 10 * 86400
    before = scrape(api, admin_key)["queueforge_oldest_finished_job_age_seconds{}"]
    assert ten_days <= before < ten_days + 300  # the stuck-purger signal an alert would use

    run_purge_cycle(sessions, POLICY, max_batches=5)
    assert scrape(api, admin_key)["queueforge_oldest_finished_job_age_seconds{}"] == 0


def test_retention_indexes_exist(engine: Engine) -> None:
    with engine.connect() as conn:
        names = {r[0] for r in conn.execute(text("SELECT indexname FROM pg_indexes"))}
    assert {
        "ix_jobs_finished_status_completed",
        "ix_idempotency_keys_expires_at",
        "ix_idempotency_keys_job_id",
    } <= names
