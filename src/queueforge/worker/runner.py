"""The worker loop: claim a job, run its handler, record the result."""

import json
import logging
import threading
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from queueforge.domain.states import JobStatus
from queueforge.tasks.handlers import HANDLERS, HandlerContext, TaskError
from queueforge.tasks.schemas import PAYLOAD_MODELS
from queueforge.worker.claims import ClaimedJob, claim_next_job, finish_job

log = logging.getLogger("queueforge.worker")

MAX_RESULT_BYTES = 10_000
GENERIC_ERROR = "The task failed with an unexpected error"


class Worker:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        worker_id: str,
        *,
        lease_seconds: int = 30,
        poll_interval: float = 1.0,
    ) -> None:
        self._sessions = session_factory
        self.worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._poll_interval = poll_interval

    def run_once(self) -> bool:
        """Process at most one job. Returns True if a job was claimed."""
        with self._sessions() as session:
            claimed = claim_next_job(session, self.worker_id, self._lease_seconds)
        if claimed is None:
            return False

        # Log identifiers only. Never the payload.
        log.info(
            "claimed job_id=%s task_type=%s attempt=%d worker_id=%s",
            claimed.id,
            claimed.task_type,
            claimed.attempt_number,
            self.worker_id,
        )
        # The handler runs here, with no database transaction open.
        status, result, error_type, error_message = self._execute(claimed)

        with self._sessions() as session:
            recorded = finish_job(
                session,
                claimed,
                status=status,
                result=result,
                error_type=error_type,
                error_message=error_message,
            )
        if recorded:
            log.info("finished job_id=%s status=%s", claimed.id, status.value)
        else:
            log.warning("lease lost, result discarded job_id=%s", claimed.id)
        return True

    def _execute(
        self, claimed: ClaimedJob
    ) -> tuple[JobStatus, dict[str, Any] | None, str | None, str | None]:
        try:
            handler = HANDLERS[claimed.task_type]
            payload = PAYLOAD_MODELS[claimed.task_type].model_validate(claimed.payload)
            ctx = HandlerContext(claimed.id, claimed.attempt_number, payload, self._sessions)
            result = handler(ctx)
            if len(json.dumps(result)) > MAX_RESULT_BYTES:
                raise TaskError("Result is too large")
            return JobStatus.SUCCEEDED, result, None, None
        except TaskError as exc:
            # Expected failure; the message was written by us, so it is safe to store.
            return JobStatus.FAILED, None, "TaskError", str(exc)[:500]
        except Exception:
            # Unknown failure. The full trace goes to the log only; what we store
            # is generic, because the real message could contain secrets.
            log.exception("unexpected error job_id=%s task_type=%s", claimed.id, claimed.task_type)
            return JobStatus.FAILED, None, "UnexpectedError", GENERIC_ERROR

    def run_forever(self, stop: threading.Event) -> None:
        log.info("worker started worker_id=%s", self.worker_id)
        while not stop.is_set():
            try:
                if self.run_once():
                    continue  # more work may be waiting: skip the pause
            except SQLAlchemyError:
                log.exception("database error; will retry")
            stop.wait(self._poll_interval)
        log.info("worker stopped worker_id=%s", self.worker_id)
