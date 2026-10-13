"""Structured (JSON) logging with an allow-list of fields.

Only the fields in ALLOWED_FIELDS can ever appear in a log line. Anything else a
caller passes (a payload, an API key, a request body) is silently dropped, so
sensitive data cannot leak by accident. Messages themselves must be fixed strings.
"""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

ALLOWED_FIELDS = frozenset(
    {
        "job_id",
        "task_type",
        "client_id",
        "worker_id",
        "attempt",
        "max_attempts",
        "status",
        "new_status",
        "method",
        "path",
        "status_code",
        "duration_ms",
        "recovered",
        "failures",
        "purged_jobs",
        "purged_attempts",
        "purged_idempotency_keys",
        "purged_workers",
        "batches",
        "more_to_do",
        "limit",
    }
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in ALLOWED_FIELDS:
            if field in record.__dict__:
                entry[field] = record.__dict__[field]
        if record.exc_info:
            # Stack traces go to the log only, never to an API response. Note that an
            # exception's own text can contain anything, so protect access to the logs.
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Send all logs to stdout as JSON. Safe to call more than once."""
    root = logging.getLogger()
    for handler in root.handlers:
        if getattr(handler, "_queueforge", False):
            root.setLevel(level)
            return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._queueforge = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)
