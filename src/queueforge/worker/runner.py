"""The worker loop: claim a job, run its handler, record the result.

A worker does three things:
1. Poll loop: claim and run jobs.
2. Heartbeat: while a handler runs, keep renewing the job's lease.
3. Sweeper: now and then, recover jobs whose lease expired (their worker died).

Failure handling inside the worker:
- Each handler runs in its own thread with a TIME LIMIT. If it runs too long, the attempt
  is recorded as a retryable failure and the worker moves on. (Python cannot kill a thread,
  so the stuck thread is "abandoned". If too many pile up, the worker exits so that its
  supervisor restarts it with a clean process.)
- Any unexpected error in the loop is caught: the worker backs off (growing, jittered
  delay) and carries on, instead of dying or spamming the log.
- On shutdown (SIGTERM) the worker stops claiming, gives the current job a limited time
  to finish, and otherwise hands it back to the queue without using up an attempt.
"""

import contextlib
import json
import logging
import random
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from queueforge.domain.retry import RetryPolicy
from queueforge.domain.states import JobStatus
from queueforge.tasks.handlers import (
    DEFAULT_HANDLER_TIMEOUT_SECONDS,
    HANDLER_TIMEOUT_SECONDS,
    HANDLERS,
    HandlerContext,
    RetryableError,
    TaskError,
)
from queueforge.tasks.schemas import PAYLOAD_MODELS
from queueforge.worker.claims import (
    ClaimedJob,
    claim_next_job,
    extend_lease,
    fail_retryable,
    finish_job,
    release_job,
    sweep_expired_leases,
)
from queueforge.worker.presence import mark_worker_stopped, touch_worker

log = logging.getLogger("queueforge.worker")

MAX_RESULT_BYTES = 10_000
GENERIC_ERROR = "The task failed with an unexpected error"
TIMEOUT_ERROR = "The task exceeded its time limit"
TICK_SECONDS = 0.1  # how often the worker checks on a running handler
HOUSEKEEPING_SECONDS = 5.0  # how often presence and the liveness file are refreshed


@dataclass(frozen=True)
class Outcome:
    kind: str  # "success", "permanent", "retryable" or "released"
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
        stop_event: threading.Event | None = None,
        drain_seconds: float = 25.0,
        handler_timeouts: Mapping[str, float] | None = None,
        max_abandoned_threads: int = 3,
        max_error_backoff: float = 30.0,
        liveness_file: str | None = None,
    ) -> None:
        self._sessions = session_factory
        self.worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._sweep_interval = sweep_interval
        self._retry = retry_policy or RetryPolicy()
        self._stop = stop_event or threading.Event()
        self._drain_seconds = drain_seconds
        self._timeouts = dict(
            HANDLER_TIMEOUT_SECONDS if handler_timeouts is None else handler_timeouts
        )
        self._max_abandoned = max_abandoned_threads
        self._max_error_backoff = max_error_backoff
        self._liveness_file = liveness_file
        self._last_sweep = float("-inf")
        self._last_housekeeping = float("-inf")
        self._abandoned: list[threading.Thread] = []
        self.exit_requested = False  # True if the worker stopped itself because it is unhealthy

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
        if self._stop.is_set():
            return False  # shutting down: never start new work
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
            return self._execute_with_limits(claimed)
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

    def _execute_with_limits(self, claimed: ClaimedJob) -> Outcome:
        """Run the handler in its own thread, enforcing the time limit and the shutdown drain."""
        box: dict[str, Outcome] = {}
        finished = threading.Event()

        def run() -> None:
            try:
                box["outcome"] = self._execute(claimed)
            finally:
                finished.set()

        thread = threading.Thread(target=run, daemon=True, name=f"handler-{claimed.id}")
        thread.start()

        limit = self._timeouts.get(claimed.task_type, DEFAULT_HANDLER_TIMEOUT_SECONDS)
        deadline = time.monotonic() + limit
        drain_deadline: float | None = None
        while not finished.wait(TICK_SECONDS):
            self._housekeeping()  # stay visible as alive while a long job runs
            now = time.monotonic()
            if self._stop.is_set() and drain_deadline is None:
                drain_deadline = now + self._drain_seconds
                log.info("shutdown requested; letting the job finish", extra=self._fields(claimed))
            if drain_deadline is not None and now >= drain_deadline:
                self._abandon(thread, claimed)
                return Outcome("released")
            if now >= deadline:
                self._abandon(thread, claimed)
                log.warning("handler exceeded its time limit", extra=self._fields(claimed))
                return Outcome(
                    "retryable", error_type="HandlerTimeout", error_message=TIMEOUT_ERROR
                )
        return box["outcome"]

    def _abandon(self, thread: threading.Thread, claimed: ClaimedJob) -> None:
        """Stop waiting for a handler thread that is still running.

        If too many stuck threads accumulate, ask the whole process to restart: a fresh
        process is the only way to really get rid of them.
        """
        self._abandoned = [t for t in self._abandoned if t.is_alive()]
        self._abandoned.append(thread)
        if len(self._abandoned) >= self._max_abandoned:
            log.error("too many stuck handlers; worker will exit", extra=self._fields(claimed))
            self.exit_requested = True
            self._stop.set()

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
            new_status: JobStatus | None
            if outcome.kind == "success":
                ok = finish_job(session, claimed, status=JobStatus.SUCCEEDED, result=outcome.result)
                new_status = JobStatus.SUCCEEDED if ok else None
            elif outcome.kind == "permanent":
                ok = finish_job(
                    session,
                    claimed,
                    status=JobStatus.FAILED,
                    error_type=outcome.error_type,
                    error_message=outcome.error_message,
                )
                new_status = JobStatus.FAILED if ok else None
            elif outcome.kind == "released":
                new_status = JobStatus.QUEUED if release_job(session, claimed) else None
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

    # --- recovery and housekeeping ----------------------------------------------------

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

    def _housekeeping(self) -> None:
        """Refresh the liveness file and the worker's presence row (rate limited)."""
        now = time.monotonic()
        if now - self._last_housekeeping < HOUSEKEEPING_SECONDS:
            return
        self._last_housekeeping = now
        if self._liveness_file:
            # A missing liveness file only makes the healthcheck fail; never crash for it.
            with contextlib.suppress(OSError):
                Path(self._liveness_file).write_text(str(time.time()))
        # The main loop reports database trouble; presence is best effort.
        with contextlib.suppress(SQLAlchemyError), self._sessions() as session:
            touch_worker(session, self.worker_id)

    # --- main loop ------------------------------------------------------------------

    def _backoff_seconds(self, failures: int) -> float:
        """Growing wait after repeated errors, with jitter, capped."""
        ceiling = min(self._max_error_backoff, self._poll_interval * 2**failures)
        return random.uniform(ceiling / 2, ceiling)

    def _log_loop_error(self, failures: int) -> None:
        extra = {"worker_id": self.worker_id, "failures": failures}
        if failures == 1:
            log.exception("worker loop error; backing off", extra=extra)  # full trace once
        elif failures & (failures - 1) == 0:  # 2, 4, 8, 16 ...: keep the volume low
            log.warning("worker loop still failing", extra=extra)

    def run_forever(self) -> None:
        log.info("worker started", extra={"worker_id": self.worker_id})
        failures = 0
        while not self._stop.is_set():
            try:
                self._housekeeping()
                self._sweep_if_due()
                worked = self.run_once()
            except Exception:
                # Never let one bad iteration kill the worker. Back off and try again.
                failures += 1
                self._log_loop_error(failures)
                self._stop.wait(self._backoff_seconds(failures))
                continue
            if failures:
                log.info(
                    "worker recovered",
                    extra={"worker_id": self.worker_id, "failures": failures},
                )
                failures = 0
            if not worked:
                self._stop.wait(self._poll_interval)
        with contextlib.suppress(SQLAlchemyError), self._sessions() as session:
            mark_worker_stopped(session, self.worker_id)
        log.info("worker stopped", extra={"worker_id": self.worker_id})
