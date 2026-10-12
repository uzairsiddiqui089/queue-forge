"""Admin endpoints: dead-letter inspection and re-queue."""

import uuid
from collections.abc import Callable

from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from queueforge.domain.retry import RetryPolicy
from queueforge.services import jobs as job_service
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
NO_DELAY = RetryPolicy(base_delay=0, max_delay=0)


def auth(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


def dead_letter_a_job(
    sessions: sessionmaker[Session], client_id: uuid.UUID, fail_times: int = 6
) -> uuid.UUID:
    """Submit a flaky job and run it until it has used all 5 attempts."""
    with sessions() as session:
        job, _ = job_service.submit_job(
            session, client_id, "flaky", {"fail_times": fail_times}, None
        )
        job_id = job.id
    worker = Worker(sessions, "w1", retry_policy=NO_DELAY)
    for _ in range(5):
        worker.run_once()
    return job_id


def test_only_admins_can_use_admin_endpoints(
    api: TestClient, make_client: MakeClient, make_admin: MakeClient
) -> None:
    _, user_key = make_client("alice")
    some_id = uuid.uuid4()
    calls = [
        ("GET", "/v1/admin/dead-letter"),
        ("GET", f"/v1/admin/jobs/{some_id}"),
        ("POST", f"/v1/admin/jobs/{some_id}/requeue"),
        ("GET", "/metrics"),
    ]
    for method, path in calls:
        assert api.request(method, path).status_code == 401, path
        assert api.request(method, path, headers=auth(user_key)).status_code == 403, path


def test_dead_letter_list_shows_failed_jobs_of_all_clients(
    api: TestClient, engine: Engine, make_client: MakeClient, make_admin: MakeClient
) -> None:
    sessions = sessionmaker(engine, expire_on_commit=False)
    alice_id, _ = make_client("alice")
    bob_id, _ = make_client("bob")
    _, admin_key = make_admin("ops")
    dead_a = dead_letter_a_job(sessions, alice_id)
    dead_b = dead_letter_a_job(sessions, bob_id)
    with sessions() as session:  # a healthy job that must NOT be listed
        job_service.submit_job(session, alice_id, "echo", {"message": "fine"}, None)

    body = api.get("/v1/admin/dead-letter", headers=auth(admin_key)).json()
    assert {j["id"] for j in body} == {str(dead_a), str(dead_b)}
    assert all(j["status"] == "dead_lettered" and j["last_error"] for j in body)
    assert {j["client_id"] for j in body} == {str(alice_id), str(bob_id)}


def test_admin_can_see_the_attempt_history(
    api: TestClient, engine: Engine, make_client: MakeClient, make_admin: MakeClient
) -> None:
    sessions = sessionmaker(engine, expire_on_commit=False)
    client_id, _ = make_client("alice")
    _, admin_key = make_admin("ops")
    job_id = dead_letter_a_job(sessions, client_id)

    detail = api.get(f"/v1/admin/jobs/{job_id}", headers=auth(admin_key)).json()
    assert [a["attempt_number"] for a in detail["attempt_history"]] == [1, 2, 3, 4, 5]
    assert all(a["outcome"] == "retryable_error" for a in detail["attempt_history"])
    assert api.get(f"/v1/admin/jobs/{uuid.uuid4()}", headers=auth(admin_key)).status_code == 404


def test_requeue_gives_a_dead_lettered_job_fresh_attempts(
    api: TestClient, engine: Engine, make_client: MakeClient, make_admin: MakeClient
) -> None:
    sessions = sessionmaker(engine, expire_on_commit=False)
    client_id, user_key = make_client("alice")
    _, admin_key = make_admin("ops")
    job_id = dead_letter_a_job(sessions, client_id, fail_times=6)  # needs a 7th attempt to pass

    response = api.post(f"/v1/admin/jobs/{job_id}/requeue", headers=auth(admin_key))
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "queued"
    assert body["attempts"] == 5 and body["max_attempts"] == 10  # history kept, 5 more tries

    worker = Worker(sessions, "w2", retry_policy=NO_DELAY)
    worker.run_once()  # attempt 6: still fails
    worker.run_once()  # attempt 7: succeeds
    final = api.get(f"/v1/jobs/{job_id}", headers=auth(user_key)).json()
    assert final["status"] == "succeeded" and final["attempts"] == 7

    history = api.get(f"/v1/admin/jobs/{job_id}", headers=auth(admin_key)).json()
    assert [a["attempt_number"] for a in history["attempt_history"]] == list(range(1, 8))


def test_requeue_is_refused_for_jobs_that_are_not_dead_lettered(
    api: TestClient, make_client: MakeClient, make_admin: MakeClient
) -> None:
    _, user_key = make_client("alice")
    _, admin_key = make_admin("ops")
    job = api.post(
        "/v1/jobs",
        json={"task_type": "echo", "payload": {"message": "x"}},
        headers=auth(user_key),
    ).json()
    refused = api.post(f"/v1/admin/jobs/{job['id']}/requeue", headers=auth(admin_key))
    assert refused.status_code == 409
    missing = api.post(f"/v1/admin/jobs/{uuid.uuid4()}/requeue", headers=auth(admin_key))
    assert missing.status_code == 404
