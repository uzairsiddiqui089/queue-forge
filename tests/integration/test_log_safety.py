"""Logs must never contain job payloads or API keys."""

import io
import json
import logging
import uuid
from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker

from queueforge.logging import ALLOWED_FIELDS, JsonFormatter
from queueforge.worker.runner import Worker

MakeClient = Callable[[str], tuple[uuid.UUID, str]]
PAYLOAD_MARKER = "PAYLOAD-MARKER-9f3a71"
WRONG_KEY = "qf_WRONG-KEY-MARKER-55c0"
STANDARD = {"ts", "level", "logger", "message", "exception"}


@pytest.fixture
def log_output() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    # Capture everything QueueForge itself logs (loggers named "queueforge.*"). We do not
    # capture the test client's own "httpx" logger, which prints full URLs: that is test
    # tooling, and in production uvicorn's access log (which would do the same) is off.
    ours = logging.getLogger("queueforge")
    previous_level = ours.level
    ours.addHandler(handler)
    ours.setLevel(logging.DEBUG)
    yield stream
    ours.removeHandler(handler)
    ours.setLevel(previous_level)


def test_logs_contain_no_payloads_or_api_keys(
    api: TestClient,
    engine: Engine,
    make_client: MakeClient,
    log_output: io.StringIO,
) -> None:
    sessions = sessionmaker(engine, expire_on_commit=False)
    _, key = make_client("alice")
    headers = {"X-API-Key": key}

    created = api.post(
        "/v1/jobs",
        json={"task_type": "echo", "payload": {"message": PAYLOAD_MARKER}},
        headers={**headers, "Idempotency-Key": "k-1"},
    )
    assert created.status_code == 201
    api.post(  # a failed login with a wrong key
        "/v1/jobs",
        json={"task_type": "echo", "payload": {"message": "x"}},
        headers={"X-API-Key": WRONG_KEY},
    )
    Worker(sessions, "w1").run_once()  # runs the job that holds the marker
    job_url = f"/v1/jobs/{created.json()['id']}?debug={PAYLOAD_MARKER}"  # marker in the query
    api.get(job_url, headers=headers)

    output = log_output.getvalue()
    assert output, "expected some log output"
    assert PAYLOAD_MARKER not in output
    assert key not in output
    assert WRONG_KEY not in output

    lines = [json.loads(line) for line in output.splitlines()]  # every line is valid JSON
    for entry in lines:
        assert set(entry) <= STANDARD | ALLOWED_FIELDS
    messages = {e["message"] for e in lines}
    assert {"request", "job claimed", "job recorded"} <= messages
    assert any(e.get("job_id") == created.json()["id"] for e in lines)  # identifiers ARE logged
