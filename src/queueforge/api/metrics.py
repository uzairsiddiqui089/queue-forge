"""Prometheus metrics, computed from the database when /metrics is scraped.

Workers are separate processes, so instead of each exposing its own numbers we read
the truth straight from PostgreSQL. That also means the figures survive restarts.

Cost: two of the queries (jobs by status, attempts by outcome) scan whole tables. With
1,000,000 finished jobs a scrape took 1-4 s (docs/benchmarking.md). So the result is reused
for `metrics_cache_seconds` (default 10): however often it is scraped, the database does
this work at most once per interval per API process. The figures can therefore be up to
that many seconds old. Retention (planned) will keep the tables small enough to avoid
needing the cache at all.
"""

import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass

from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from sqlalchemy import Engine, text

from queueforge.api.deps import AdminClient
from queueforge.domain.states import AttemptOutcome, JobStatus
from queueforge.worker.presence import PRESENCE_STALE_SECONDS

router = APIRouter(tags=["metrics"])


@dataclass(frozen=True)
class Reading:
    by_status: dict[str, int]
    oldest_queued: float
    oldest_running: float
    expired: int
    workers_alive: int
    by_outcome: dict[str, int]


class DatabaseCollector:
    def __init__(self, engine: Engine, cache_seconds: float = 0.0) -> None:
        self._engine = engine
        self._cache_seconds = cache_seconds
        self._lock = threading.Lock()
        self._cached: tuple[float, Reading] | None = None

    def _read(self) -> Reading:
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
        return Reading(
            by_status,
            float(oldest_queued),
            float(oldest_running),
            expired,
            workers_alive,
            by_outcome,
        )

    def _reading(self) -> Reading:
        if self._cache_seconds <= 0:
            return self._read()
        with self._lock:  # one refresh at a time; concurrent scrapes wait and then share it
            now = time.monotonic()
            if self._cached is None or now - self._cached[0] >= self._cache_seconds:
                self._cached = (now, self._read())
            return self._cached[1]

    def collect(self) -> Iterable[Metric]:
        r = self._reading()

        jobs = GaugeMetricFamily(
            "queueforge_jobs", "Jobs currently in each status", labels=["status"]
        )
        for s in JobStatus:
            jobs.add_metric([s.value], float(r.by_status.get(s.value, 0)))
        yield jobs

        yield GaugeMetricFamily(
            "queueforge_oldest_queued_job_age_seconds",
            "Age of the oldest job that is ready to run but not yet claimed (queue lag)",
            value=r.oldest_queued,
        )
        yield GaugeMetricFamily(
            "queueforge_oldest_running_job_age_seconds",
            "How long the longest-running current attempt has been going",
            value=r.oldest_running,
        )
        yield GaugeMetricFamily(
            "queueforge_expired_leases",
            "Running jobs whose lease has expired and not yet been swept (workers may be down)",
            value=float(r.expired),
        )
        yield GaugeMetricFamily(
            "queueforge_workers_alive",
            "Worker processes seen in the last 30 seconds. ALERT when this is 0: nothing is "
            "running jobs or recovering crashed ones",
            value=float(r.workers_alive),
        )
        attempts = CounterMetricFamily(
            "queueforge_job_attempts", "Finished attempts by outcome", labels=["outcome"]
        )
        for o in AttemptOutcome:
            attempts.add_metric([o.value], float(r.by_outcome.get(o.value, 0)))
        yield attempts


def make_registry(engine: Engine, cache_seconds: float = 0.0) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(DatabaseCollector(engine, cache_seconds))
    return registry


@router.get("/metrics")
def metrics(request: Request, _admin: AdminClient) -> Response:
    payload = generate_latest(request.app.state.metrics_registry)
    return Response(payload, media_type=CONTENT_TYPE_LATEST)
