"""The /metrics endpoint reports queue state straight from the database."""

import time
import uuid
from collections.abc import Callable

from fastapi.testclient import TestClient
from prometheus_client.parser import text_string_to_metric_families
from sqlalchemy import Engine, text
from sqlalchemy.orm import sessionmaker

from queueforge.domain.retry import RetryPolicy
from queueforge.services import jobs as job_service
from queueforge.worker.claims import claim_next_job
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]


def scrape(api: TestClient, admin_key: str) -> dict[str, float]:
    response = api.get("/metrics", headers={"X-API-Key": admin_key})
    assert response.status_code == 200
    values: dict[str, float] = {}
    for family in text_string_to_metric_families(response.text):
        for sample in family.samples:
            if sample.name.endswith("_created"):
                continue
            label = ",".join(f"{k}={v}" for k, v in sorted(sample.labels.items()))
            values[f"{sample.name}{{{label}}}"] = sample.value
    return values


def run_sql(engine: Engine, statement: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(statement))


def test_metrics_reflect_the_real_queue(
    api: TestClient, engine: Engine, make_client: MakeClient, make_admin: MakeClient
) -> None:
    sessions = sessionmaker(engine, expire_on_commit=False)
    client_id, _ = make_client("alice")
    _, admin_key = make_admin("ops")

    def submit(task: str, payload: dict[str, object]) -> None:
        with sessions() as session:
            job_service.submit_job(session, client_id, task, payload, None)

    assert scrape(api, admin_key)["queueforge_jobs{status=queued}"] == 0

    submit("echo", {"message": "a"})
    Worker(sessions, "w1").run_once()  # job 1 succeeds
    submit("echo", {"message": "b"})  # job 2 and 3 stay queued
    submit("echo", {"message": "c"})
    submit("flaky", {"fail_times": 1})  # job 4 is claimed by a worker that then "dies"
    run_sql(engine, "UPDATE jobs SET run_at = now() + interval '1 hour' WHERE task_type = 'echo'")
    with sessions() as session:
        claim_next_job(session, "doomed", 30)
    run_sql(
        engine,
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE status = 'running'",
    )
    run_sql(
        engine, "UPDATE jobs SET run_at = now() - interval '30 seconds' WHERE status = 'queued'"
    )

    m = scrape(api, admin_key)
    assert m["queueforge_jobs{status=succeeded}"] == 1
    assert m["queueforge_jobs{status=queued}"] == 2
    assert m["queueforge_jobs{status=running}"] == 1
    assert m["queueforge_jobs{status=dead_lettered}"] == 0
    assert m["queueforge_expired_leases{}"] == 1  # a worker died holding a job
    assert m["queueforge_oldest_queued_job_age_seconds{}"] >= 29
    assert m["queueforge_oldest_running_job_age_seconds{}"] >= 0
    assert m["queueforge_job_attempts_total{outcome=succeeded}"] == 1

    assert scrape(api, admin_key)  # scraping twice works (no duplicate registration)


def test_dead_letter_and_failure_counts(
    api: TestClient, engine: Engine, make_client: MakeClient, make_admin: MakeClient
) -> None:
    sessions = sessionmaker(engine, expire_on_commit=False)
    client_id, _ = make_client("alice")
    _, admin_key = make_admin("ops")
    with sessions() as session:
        job_service.submit_job(session, client_id, "flaky", {"fail_times": 10}, None)
    worker = Worker(sessions, "w1", retry_policy=RetryPolicy(0, 0))
    for _ in range(5):
        worker.run_once()

    m = scrape(api, admin_key)
    assert m["queueforge_jobs{status=dead_lettered}"] == 1
    assert m["queueforge_job_attempts_total{outcome=retryable_error}"] == 5


def test_metrics_are_reused_for_the_cache_interval_and_refresh_after_it(
    engine: Engine, make_client: MakeClient, make_admin: MakeClient
) -> None:
    from queueforge.api.app import create_app

    sessions = sessionmaker(engine, expire_on_commit=False)
    client_id, _ = make_client("alice")
    _, admin_key = make_admin("ops")
    api = TestClient(create_app(engine=engine, metrics_cache_seconds=0.6))

    def queued() -> float:
        return scrape(api, admin_key)["queueforge_jobs{status=queued}"]

    assert queued() == 0
    with sessions() as session:
        job_service.submit_job(session, client_id, "echo", {"message": "a"}, None)
    assert queued() == 0  # still the cached reading: the database was not asked again
    time.sleep(0.8)
    assert queued() == 1  # the interval passed, so the new job is now visible


def test_new_indexes_are_used_by_the_sweeper_and_dead_letter_queries(engine: Engine) -> None:
    with engine.connect() as conn:
        names = {
            r[0]
            for r in conn.execute(text("SELECT indexname FROM pg_indexes WHERE tablename = 'jobs'"))
        }
    assert {"ix_jobs_running_lease", "ix_jobs_dead_lettered_completed"} <= names
