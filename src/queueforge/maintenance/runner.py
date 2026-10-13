"""The maintenance loop: runs the retention purge on a schedule.

It follows the worker's safety habits: an unexpected error backs off and retries instead of
killing the process (a dead purger means old data piles up unnoticed), it stops cleanly on
SIGTERM, it writes a liveness file for the container healthcheck, and it logs counts only
(never job contents).
"""

import contextlib
import logging
import random
import threading
import time
from collections.abc import Callable
from pathlib import Path

from sqlalchemy.orm import Session

from queueforge.services.retention import PurgeResult, RetentionPolicy, run_purge_cycle

log = logging.getLogger("queueforge.maintenance")

CATCH_UP_PAUSE_SECONDS = 1.0  # when a cycle hit its limit, go again soon instead of waiting


class Maintenance:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        policy: RetentionPolicy,
        *,
        interval_seconds: float = 60.0,
        max_batches_per_cycle: int = 50,
        stop_event: threading.Event | None = None,
        max_error_backoff: float = 60.0,
        liveness_file: str | None = None,
    ) -> None:
        self._sessions = session_factory
        self._policy = policy
        self._interval = interval_seconds
        self._max_batches = max_batches_per_cycle
        self._stop = stop_event or threading.Event()
        self._max_error_backoff = max_error_backoff
        self._liveness_file = liveness_file

    def run_cycle(self) -> PurgeResult:
        started = time.monotonic()
        result = run_purge_cycle(self._sessions, self._policy, self._max_batches)
        log.info(
            "purge cycle",
            extra={
                "purged_jobs": result.jobs,
                "purged_attempts": result.attempts,
                "purged_idempotency_keys": result.idempotency_keys,
                "purged_workers": result.workers,
                "batches": result.batches,
                "more_to_do": result.more_to_do,
                "duration_ms": round((time.monotonic() - started) * 1000, 1),
            },
        )
        return result

    def _touch_liveness(self) -> None:
        if self._liveness_file:
            with contextlib.suppress(OSError):
                Path(self._liveness_file).write_text(str(time.time()))

    def _backoff_seconds(self, failures: int) -> float:
        ceiling = min(self._max_error_backoff, self._interval * 2**failures)
        return random.uniform(ceiling / 2, ceiling)

    def run_forever(self) -> None:
        log.info("maintenance started")
        failures = 0
        while not self._stop.is_set():
            self._touch_liveness()
            try:
                result = self.run_cycle()
            except Exception:
                # Never let one bad cycle kill the purger. Back off and try again.
                failures += 1
                if failures == 1:
                    log.exception("purge cycle failed; backing off", extra={"failures": failures})
                elif failures & (failures - 1) == 0:
                    log.warning("purge still failing", extra={"failures": failures})
                self._stop.wait(self._backoff_seconds(failures))
                continue
            if failures:
                log.info("maintenance recovered", extra={"failures": failures})
                failures = 0
            self._stop.wait(CATCH_UP_PAUSE_SECONDS if result.more_to_do else self._interval)
        log.info("maintenance stopped")
