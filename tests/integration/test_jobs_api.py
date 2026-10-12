"""Job API behaviour against a real database."""

import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select, text
from sqlalchemy.orm import Session

from queueforge.db.models import Job
from queueforge.services import jobs as job_service

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
ECHO = {"task_type": "echo", "payload": {"message": "hello"}}


def auth(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


# --- submit and retrieve ---------------------------------------------------


def test_submit_then_retrieve(api: TestClient, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    created = api.post("/v1/jobs", json=ECHO, headers=auth(key))
    assert created.status_code == 201
    body = created.json()
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["payload"] == {"message": "hello"}

    fetched = api.get(f"/v1/jobs/{body['id']}", headers=auth(key))
    assert fetched.status_code == 200
    assert fetched.json() == body


def test_job_is_really_stored_in_the_database(
    api: TestClient, make_client: MakeClient, engine: Engine
) -> None:
    _, key = make_client("alice")
    job_id = api.post("/v1/jobs", json=ECHO, headers=auth(key)).json()["id"]
    with Session(engine) as session:
        job = session.get(Job, uuid.UUID(job_id))
        assert job is not None
        assert job.status == "queued"


# --- validation (422) ------------------------------------------------------


def test_unknown_task_type_is_rejected(api: TestClient, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    bad = {"task_type": "run_shell", "payload": {"cmd": "rm -rf /"}}
    assert api.post("/v1/jobs", json=bad, headers=auth(key)).status_code == 422


def test_invalid_payloads_are_rejected(api: TestClient, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    bad_bodies = [
        {"task_type": "sleep", "payload": {"seconds": 999}},  # over the 30 s limit
        {"task_type": "sleep", "payload": {"seconds": -1}},
        {"task_type": "echo", "payload": {"message": "x" * 1001}},  # too long
        {"task_type": "echo", "payload": {"message": "hi", "extra": 1}},  # unknown field
        {"task_type": "echo", "payload": {}},  # missing field
    ]
    for body in bad_bodies:
        assert api.post("/v1/jobs", json=body, headers=auth(key)).status_code == 422, body


def test_malformed_job_id_is_422(api: TestClient, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    assert api.get("/v1/jobs/not-a-uuid", headers=auth(key)).status_code == 422


# --- authentication and ownership -------------------------------------------


def test_missing_or_wrong_key_is_401(api: TestClient, make_client: MakeClient) -> None:
    make_client("alice")
    assert api.post("/v1/jobs", json=ECHO).status_code == 401
    assert api.post("/v1/jobs", json=ECHO, headers=auth("qf_wrong")).status_code == 401
    assert api.get(f"/v1/jobs/{uuid.uuid4()}").status_code == 401


def test_disabled_client_is_rejected(
    api: TestClient, make_client: MakeClient, engine: Engine
) -> None:
    _, key = make_client("alice")
    with engine.begin() as conn:
        conn.execute(text("UPDATE api_clients SET disabled_at = now()"))
    assert api.post("/v1/jobs", json=ECHO, headers=auth(key)).status_code == 401


def test_other_clients_job_looks_like_not_found(api: TestClient, make_client: MakeClient) -> None:
    _, alice_key = make_client("alice")
    _, bob_key = make_client("bob")
    job_id = api.post("/v1/jobs", json=ECHO, headers=auth(alice_key)).json()["id"]
    assert api.get(f"/v1/jobs/{job_id}", headers=auth(bob_key)).status_code == 404
    assert api.get(f"/v1/jobs/{uuid.uuid4()}", headers=auth(alice_key)).status_code == 404


# --- idempotency -------------------------------------------------------------


def test_same_key_same_body_returns_original_job(api: TestClient, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    headers = {**auth(key), "Idempotency-Key": "order-1"}
    first = api.post("/v1/jobs", json=ECHO, headers=headers)
    second = api.post("/v1/jobs", json=ECHO, headers=headers)
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


def test_same_key_different_body_is_409(api: TestClient, make_client: MakeClient) -> None:
    _, key = make_client("alice")
    headers = {**auth(key), "Idempotency-Key": "order-1"}
    api.post("/v1/jobs", json=ECHO, headers=headers)
    other = {"task_type": "echo", "payload": {"message": "different"}}
    assert api.post("/v1/jobs", json=other, headers=headers).status_code == 409


def test_same_key_for_different_clients_is_independent(
    api: TestClient, make_client: MakeClient
) -> None:
    _, alice_key = make_client("alice")
    _, bob_key = make_client("bob")
    a = api.post("/v1/jobs", json=ECHO, headers={**auth(alice_key), "Idempotency-Key": "k"})
    b = api.post("/v1/jobs", json=ECHO, headers={**auth(bob_key), "Idempotency-Key": "k"})
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]


def test_expired_key_is_treated_as_new(
    api: TestClient, make_client: MakeClient, engine: Engine
) -> None:
    _, key = make_client("alice")
    headers = {**auth(key), "Idempotency-Key": "order-1"}
    first = api.post("/v1/jobs", json=ECHO, headers=headers)
    with engine.begin() as conn:
        conn.execute(text("UPDATE idempotency_keys SET expires_at = now() - interval '1 second'"))
    second = api.post("/v1/jobs", json=ECHO, headers=headers)
    assert second.status_code == 201
    assert second.json()["id"] != first.json()["id"]


def test_concurrent_duplicates_create_exactly_one_job(
    engine: Engine, make_client: MakeClient
) -> None:
    client_id, _ = make_client("alice")

    def submit() -> tuple[uuid.UUID, bool]:
        with Session(engine) as session:
            job, created = job_service.submit_job(
                session, client_id, "echo", {"message": "race"}, "same-key"
            )
            return job.id, created

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: submit(), range(8)))

    assert len({job_id for job_id, _ in results}) == 1  # everyone got the same job
    assert sum(created for _, created in results) == 1  # exactly one request created it
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1
