"""Prometheus metrics, computed from the database each time /metrics is scraped.

Workers are separate processes, so instead of each exposing its own numbers we read
the truth straight from PostgreSQL. That also means the figures survive restarts.
"""

from collections.abc import Iterable

from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from sqlalchemy import Engine, text

from queueforge.api.deps import AdminClient
from queueforge.domain.states import AttemptOutcome, JobStatus
from queueforge.worker.presence import PRESENCE_STALE_SECONDS

router = APIRouter(tags=["metrics"])


class DatabaseCollector:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def collect(self) -> Iterable[Metric]:
        with self._engine.connect() as conn:
            by_status: dict[str, int] = {
                row[0]: row[1]
                for row in conn.execute(text("SELECT status, count(*) FROM jobs GROUP BY status"))
            }
            oldest_queued = conn.execute(
                text(
                    "SELECT coalesce(extract(epoch FROM now() - min(run_at)), 0) "
                    "FROM jobs WHERE status = 'queued' AND run_at <= now()"
                )
            ).scalar_one()
            oldest_running = conn.execute(
                text(
                    "SELECT coalesce(extract(epoch FROM now() - min(a.started_at)), 0) "
                    "FROM jobs j JOIN job_attempts a "
                    "ON a.job_id = j.id AND a.attempt_number = j.attempts AND a.outcome IS NULL "
                    "WHERE j.status = 'running'"
                )
            ).scalar_one()
            expired = conn.execute(
                text(
                    "SELECT count(*) FROM jobs "
                    "WHERE status = 'running' AND lease_expires_at < now()"
                )
            ).scalar_one()
            workers_alive = conn.execute(
                text(
                    "SELECT count(*) FROM workers WHERE stopped_at IS NULL "
                    f"AND last_seen_at > now() - interval '{PRESENCE_STALE_SECONDS} seconds'"
                )
            ).scalar_one()
            by_outcome: dict[str, int] = {
                row[0]: row[1]
                for row in conn.execute(
                    text("SELECT outcome, count(*) FROM job_attempts GROUP BY outcome")
                )
            }

        jobs = GaugeMetricFamily(
            "queueforge_jobs", "Jobs currently in each status", labels=["status"]
        )
        for s in JobStatus:
            jobs.add_metric([s.value], float(by_status.get(s.value, 0)))
        yield jobs

        yield GaugeMetricFamily(
            "queueforge_oldest_queued_job_age_seconds",
            "Age of the oldest job that is ready to run but not yet claimed (queue lag)",
            value=float(oldest_queued),
        )
        yield GaugeMetricFamily(
            "queueforge_oldest_running_job_age_seconds",
            "How long the longest-running current attempt has been going",
            value=float(oldest_running),
        )
        yield GaugeMetricFamily(
            "queueforge_expired_leases",
            "Running jobs whose lease has expired and not yet been swept (workers may be down)",
            value=float(expired),
        )
        yield GaugeMetricFamily(
            "queueforge_workers_alive",
            "Worker processes seen in the last 30 seconds. ALERT when this is 0: nothing is "
            "running jobs or recovering crashed ones",
            value=float(workers_alive),
        )
        attempts = CounterMetricFamily(
            "queueforge_job_attempts", "Finished attempts by outcome", labels=["outcome"]
        )
        for o in AttemptOutcome:
            attempts.add_metric([o.value], float(by_outcome.get(o.value, 0)))
        yield attempts


def make_registry(engine: Engine) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(DatabaseCollector(engine))
    return registry


@router.get("/metrics")
def metrics(request: Request, _admin: AdminClient) -> Response:
    payload = generate_latest(request.app.state.metrics_registry)
    return Response(payload, media_type=CONTENT_TYPE_LATEST)
