"""Run a worker: `python -m queueforge.worker`."""

import os
import signal
import socket
import sys
import threading
from types import FrameType

from sqlalchemy.orm import sessionmaker

from queueforge.config import get_settings
from queueforge.db.engine import engine_from_settings
from queueforge.domain.retry import RetryPolicy
from queueforge.logging import configure_logging
from queueforge.worker.runner import Worker

EXIT_UNHEALTHY = 3  # the worker stopped itself; a supervisor should restart it


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)

    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        # Stop claiming new jobs; the worker finishes (or hands back) the current one.
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    engine = engine_from_settings(settings)
    worker = Worker(
        sessionmaker(engine, expire_on_commit=False),
        worker_id=f"{socket.gethostname()}-{os.getpid()}",
        lease_seconds=settings.worker_lease_seconds,
        poll_interval=settings.worker_poll_interval_seconds,
        heartbeat_interval=settings.worker_heartbeat_interval_seconds,
        sweep_interval=settings.worker_sweep_interval_seconds,
        retry_policy=RetryPolicy(
            base_delay=settings.retry_base_delay_seconds,
            max_delay=settings.retry_max_delay_seconds,
        ),
        stop_event=stop,
        drain_seconds=settings.worker_shutdown_drain_seconds,
        max_abandoned_threads=settings.worker_max_abandoned_threads,
        liveness_file=settings.worker_liveness_file,
        fair_claiming=settings.fair_claiming,
        fair_claim_max_clients=settings.fair_claim_max_clients,
    )
    worker.run_forever()
    return EXIT_UNHEALTHY if worker.exit_requested else 0


if __name__ == "__main__":
    sys.exit(main())
