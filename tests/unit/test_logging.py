import json
import logging
import sys

from queueforge.logging import ALLOWED_FIELDS, JsonFormatter


def render(**extra: object) -> dict[str, object]:
    record = logging.LogRecord(
        "queueforge.test", logging.INFO, __file__, 1, "something happened", None, None
    )
    for key, value in extra.items():
        setattr(record, key, value)
    result: dict[str, object] = json.loads(JsonFormatter().format(record))
    return result


def test_output_is_json_with_standard_fields() -> None:
    entry = render(job_id="abc", worker_id="w1")
    assert entry["message"] == "something happened"
    assert entry["level"] == "INFO"
    assert entry["job_id"] == "abc" and entry["worker_id"] == "w1"
    assert "ts" in entry


def test_fields_outside_the_allow_list_are_dropped() -> None:
    entry = render(job_id="abc", payload={"card": "4111"}, api_key="qf_secret", password="hunter2")
    assert entry["job_id"] == "abc"
    text = json.dumps(entry)
    for leaked in ("4111", "qf_secret", "hunter2", "payload", "api_key", "password"):
        assert leaked not in text


def test_allow_list_has_no_sensitive_names() -> None:
    sensitive = {"payload", "body", "api_key", "key", "password", "headers", "query"}
    assert not sensitive & ALLOWED_FIELDS


def test_stack_trace_is_included_for_exceptions() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", None, sys.exc_info())
    entry = json.loads(JsonFormatter().format(record))
    assert "ValueError: boom" in entry["exception"]
