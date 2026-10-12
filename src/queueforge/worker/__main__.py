"""Run a worker: `python -m queueforge.worker`."""

import logging
import os
import signal
import socket
import threading
from types import FrameType

from sqlalchemy.orm import sessionmaker

from queueforge.config import get_settings
from queueforge.db.engine import make_engine
from queueforge.worker.runner import Worker


def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        # Finish the current job, then exit. Never abandon a job mid-way on purpose.
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    engine = make_engine(settings.database_url)
    worker = Worker(
        sessionmaker(engine, expire_on_commit=False),
        worker_id=f"{socket.gethostname()}-{os.getpid()}",
        lease_seconds=settings.worker_lease_seconds,
        poll_interval=settings.worker_poll_interval_seconds,
    )
    worker.run_forever(stop)


if __name__ == "__main__":
    main()
