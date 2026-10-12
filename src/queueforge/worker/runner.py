"""The worker loop: claim a job, run its handler, record the result.

A worker does three things:
1. Poll loop: claim and run jobs.
2. Heartbeat: while a handler runs, keep renewing the job's lease.
3. Sweeper: now and then, recover jobs whose lease expired (their worker died).
"""

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from queueforge.domain.retry import RetryPolicy
from queueforge.domain.states import JobStatus
from queueforge.tasks.handlers import HANDLERS, HandlerContext, RetryableError, TaskError
from queueforge.tasks.schemas import PAYLOAD_MODELS
from queueforge.worker.claims import (
    ClaimedJob,
    claim_next_job,
    extend_lease,
    fail_retryable,
    finish_job,
    sweep_expired_leases,
)

log = logging.getLogger("queueforge.worker")

MAX_RESULT_BYTES = 10_000
GENERIC_ERROR = "The task failed with an unexpected error"


@dataclass(frozen=True)
class Outcome:
    kind: str  # "success", "permanent" or "retryable"
    result: dict[str, Any] | None = None
    error_type: str | None = None
    error_message: str | None = None


class Worker:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        worker_id: str,
        *,
        lease_seconds: int = 30,
        poll_interval: float = 1.0,
        heartbeat_interval: float = 10.0,
        sweep_interval: float = 10.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._sessions = session_factory
        self.worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._sweep_interval = sweep_interval
        self._retry = retry_policy or RetryPolicy()
        self._last_sweep = float("-inf")

    def _fields(self, claimed: ClaimedJob) -> dict[str, Any]:
        """The only job details we ever log: identifiers, never the payload."""
        return {
            "job_id": str(claimed.id),
            "task_type": claimed.task_type,
            "attempt": claimed.attempt_number,
            "max_attempts": claimed.max_attempts,
            "worker_id": self.worker_id,
        }

    # --- one job ---------------------------------------------------------------

    def run_once(self) -> bool:
        """Process at most one job. Returns True if a job was claimed."""
        with self._sessions() as session:
            claimed = claim_next_job(session, self.worker_id, self._lease_seconds)
        if claimed is None:
            return False

        log.info("job claimed", extra=self._fields(claimed))
        # The handler runs here, with no database transaction open.
        outcome = self._execute_with_heartbeat(claimed)
        self._record(claimed, outcome)
        return True

    def _execute_with_heartbeat(self, claimed: ClaimedJob) -> Outcome:
        stop = threading.Event()
        beat = threading.Thread(target=self._heartbeat, args=(claimed, stop), daemon=True)
        beat.start()
        try:
            return self._execute(claimed)
        finally:
            stop.set()
            beat.join()

    def _heartbeat(self, claimed: ClaimedJob, stop: threading.Event) -> None:
        while not stop.wait(self._heartbeat_interval):
            try:
                with self._sessions() as session:
                    still_ours = extend_lease(
                        session, claimed.id, claimed.lease_token, self._lease_seconds
                    )
            except SQLAlchemyError:
                log.exception("heartbeat failed; will try again", extra=self._fields(claimed))
                continue
            if not still_ours:
                # We cannot stop the handler, but the fence will discard its result.
                log.warning("lease lost", extra=self._fields(claimed))
                return

    def _execute(self, claimed: ClaimedJob) -> Outcome:
        try:
            handler = HANDLERS[claimed.task_type]
            payload = PAYLOAD_MODELS[claimed.task_type].model_validate(claimed.payload)
            ctx = HandlerContext(claimed.id, claimed.attempt_number, payload, self._sessions)
            result = handler(ctx)
            if len(json.dumps(result)) > MAX_RESULT_BYTES:
                raise TaskError("Result is too large")
            return Outcome("success", result=result)
        except TaskError as exc:
            # Permanent: retrying cannot help. Our own message, safe to store.
            return Outcome("permanent", error_type="TaskError", error_message=str(exc)[:500])
        except RetryableError as exc:
            return Outcome("retryable", error_type="RetryableError", error_message=str(exc)[:500])
        except Exception:
            # Unknown failure: treated as temporary. That is safe because retries are
            # limited (max_attempts), so a bug cannot loop forever. The full trace goes
            # to the log only; what we store is generic, because the real message
            # could contain secrets.
            log.exception("unexpected error in handler", extra=self._fields(claimed))
            return Outcome("retryable", error_type="UnexpectedError", error_message=GENERIC_ERROR)

    def _record(self, claimed: ClaimedJob, outcome: Outcome) -> None:
        with self._sessions() as session:
            if outcome.kind == "success":
                ok = finish_job(session, claimed, status=JobStatus.SUCCEEDED, result=outcome.result)
                new_status: JobStatus | None = JobStatus.SUCCEEDED if ok else None
            elif outcome.kind == "permanent":
                ok = finish_job(
                    session,
                    claimed,
                    status=JobStatus.FAILED,
                    error_type=outcome.error_type,
                    error_message=outcome.error_message,
                )
                new_status = JobStatus.FAILED if ok else None
            else:
                assert outcome.error_type is not None and outcome.error_message is not None
                new_status = fail_retryable(
                    session,
                    claimed,
                    error_type=outcome.error_type,
                    error_message=outcome.error_message,
                    delay_seconds=self._retry.delay_for(claimed.attempt_number),
                )
        if new_status is None:
            log.warning("lease lost, result discarded", extra=self._fields(claimed))
        else:
            log.info(
                "job recorded", extra={**self._fields(claimed), "new_status": new_status.value}
            )

    # --- recovery ----------------------------------------------------------------

    def sweep_once(self) -> int:
        with self._sessions() as session:
            recovered = sweep_expired_leases(session, self._retry)
        if recovered:
            log.warning(
                "recovered jobs with expired leases",
                extra={"recovered": recovered, "worker_id": self.worker_id},
            )
        return recovered

    def _sweep_if_due(self) -> None:
        now = time.monotonic()
        if now - self._last_sweep >= self._sweep_interval:
            self._last_sweep = now
            self.sweep_once()

    # --- main loop -----------------------------------------------------------------

    def run_forever(self, stop: threading.Event) -> None:
        log.info("worker started", extra={"worker_id": self.worker_id})
        while not stop.is_set():
            try:
                self._sweep_if_due()
                if self.run_once():
                    continue  # more work may be waiting: skip the pause
            except SQLAlchemyError:
                log.exception("database error; will retry")
            stop.wait(self._poll_interval)
        log.info("worker stopped", extra={"worker_id": self.worker_id})
