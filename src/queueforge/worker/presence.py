"""Worker presence: lets operators see which worker processes are alive."""

from sqlalchemy import func, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from queueforge.db.models import WorkerRecord

# A worker not seen for this long is counted as gone (see the workers_alive metric).
PRESENCE_STALE_SECONDS = 30


def touch_worker(session: Session, worker_id: str) -> None:
    """Register the worker, or refresh its last-seen time."""
    stmt = insert(WorkerRecord).values(id=worker_id)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"], set_={"last_seen_at": func.now(), "stopped_at": None}
    )
    session.execute(stmt)
    session.commit()


def mark_worker_stopped(session: Session, worker_id: str) -> None:
    session.execute(
        update(WorkerRecord).where(WorkerRecord.id == worker_id).values(stopped_at=func.now())
    )
    session.commit()
