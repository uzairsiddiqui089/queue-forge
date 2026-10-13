"""Run the maintenance process: `python -m queueforge.maintenance [--once] [--dry-run]`.

(no flags)  loop forever, purging old data every MAINTENANCE_INTERVAL_SECONDS
--once      run purge cycles until nothing more is eligible, then exit (for cron or manual use)
--dry-run   only COUNT what a purge would delete, delete nothing
"""

import argparse
import signal
import sys
import threading
from types import FrameType

from sqlalchemy.orm import sessionmaker

from queueforge.config import get_settings
from queueforge.db.engine import engine_from_settings
from queueforge.logging import configure_logging
from queueforge.maintenance.runner import Maintenance
from queueforge.services.retention import RetentionPolicy, count_eligible


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    policy = RetentionPolicy(
        succeeded_days=settings.retention_succeeded_days,
        failed_days=settings.retention_failed_days,
        dead_letter_days=settings.retention_dead_letter_days,
        batch_size=settings.purge_batch_size,
    )
    sessions = sessionmaker(engine_from_settings(settings), expire_on_commit=False)

    if args.dry_run:
        with sessions() as session:
            counts = count_eligible(session, policy)
        print(f"Would delete (jobs older than their retention): {counts}")
        return 0

    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    maintenance = Maintenance(
        sessions,
        policy,
        interval_seconds=settings.maintenance_interval_seconds,
        max_batches_per_cycle=settings.purge_max_batches_per_cycle,
        stop_event=stop,
        liveness_file=settings.maintenance_liveness_file,
    )
    if args.once:
        while not stop.is_set():
            if not maintenance.run_cycle().more_to_do:
                break
        return 0
    maintenance.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
