"""QueueForge benchmark harness.

Runs real workloads against a real, isolated stack (its own Docker Compose project with
its own database) and measures job timing from DATABASE timestamps, not from the load
generator's clock:

- queue wait   = first attempt's `started_at` minus the job's `created_at`
- run time     = the attempt's `duration_ms`
- end-to-end   = `completed_at` minus `created_at`

Only measured values are written to the result files (docs/benchmark-results/*.json).
See docs/benchmarking.md for how to run it and for the results.

Usage (from the repo root, with the database credentials from .env exported):
    set -a; . ./.env; set +a
    uv run python scripts/benchmark/bench.py up --workers 4 --build   # first time / after code changes
    uv run python scripts/benchmark/bench.py burst --workers 4 --jobs 2000 --repeat 3
    uv run python scripts/benchmark/bench.py down
"""

import argparse
import datetime as dt
import decimal
import hashlib
import json
import os
import platform
import random
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import psycopg

ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "docs" / "benchmark-results"
API = os.environ.get("BENCH_API", "http://127.0.0.1:18000")
DB_HOST_PORT = os.environ.get("BENCH_DB_PORT", "15432")

# The complete application settings used by every benchmark run. They are written out here
# on purpose, so results never depend on whatever happens to be in your shell or `.env`
# (a stray `.env` value once silently re-enabled a 120/min limit during harness testing).
# To change one for an experiment, set BENCH_<NAME> in the environment.
#
# Everything equals the production default EXCEPT the rate limit: the default of 120
# requests per minute per key would cap the load generator. The limiter itself stays
# switched on, so the Redis check still runs on every request.
BENCH_ENV_DEFAULTS = {
    "RATE_LIMIT_PER_MINUTE": "100000000",  # <- the one deliberate deviation from the default
    "MAX_QUEUED_JOBS_PER_CLIENT": "10000",
    "MAX_QUEUED_JOBS_TOTAL": "100000",
    "MAX_REQUEST_BODY_BYTES": "65536",
    "FAIR_CLAIMING": "true",
    "FAIR_CLAIM_MAX_CLIENTS": "64",
    "QUEUE_DEPTH_CACHE_SECONDS": "1",
    "METRICS_CACHE_SECONDS": "10",
    "WORKER_LEASE_SECONDS": "30",
    "WORKER_HEARTBEAT_INTERVAL_SECONDS": "10",
    "WORKER_SWEEP_INTERVAL_SECONDS": "10",
    "WORKER_SHUTDOWN_DRAIN_SECONDS": "25",
    "RETRY_BASE_DELAY_SECONDS": "2",
    "RETRY_MAX_DELAY_SECONDS": "300",
    "LOG_LEVEL": "INFO",
    "ENVIRONMENT": "development",
}

DOCKER = shutil.which("docker") or r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"


# --- helpers ------------------------------------------------------------------------------


def pct(values: list[float], q: float) -> float | None:
    """q-th percentile (0-100) with linear interpolation. None if there are no values."""
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q / 100
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def summarize(values: list[float]) -> dict[str, float | int | None]:
    return {
        "n": len(values),
        "p50": pct(values, 50),
        "p95": pct(values, 95),
        "p99": pct(values, 99),
        "max": max(values) if values else None,
        "mean": sum(values) / len(values) if values else None,
    }


def db_dsn() -> str:
    try:
        user, password, name = (
            os.environ[k] for k in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
        )
    except KeyError as missing:
        sys.exit(f"Missing {missing}. Run: set -a; . ./.env; set +a")
    return f"postgresql://{user}:{password}@127.0.0.1:{DB_HOST_PORT}/{name}"


def purge_sessions() -> Any:
    """A SQLAlchemy session factory for the benchmark database (the real purge code uses it)."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    user, password, name = (
        os.environ[k] for k in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
    )
    url = f"postgresql+psycopg://{user}:{password}@127.0.0.1:{DB_HOST_PORT}/{name}"
    return sessionmaker(create_engine(url, pool_size=2), expire_on_commit=False)


class BackgroundPurger:
    """Runs the real purge back to back while a scenario runs: the WORST case for interference.
    (The shipped maintenance process is much gentler: one bounded cycle a minute.)"""

    def __init__(self, days: int, batch_size: int) -> None:
        from queueforge.services.retention import RetentionPolicy

        self._policy = RetentionPolicy(
            succeeded_days=days, failed_days=days, dead_letter_days=days, batch_size=batch_size
        )
        self._sessions = purge_sessions()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.rows = 0
        self.jobs = 0

    def _run(self) -> None:
        from queueforge.services.retention import run_purge_cycle

        while not self._stop.is_set():
            result = run_purge_cycle(self._sessions, self._policy, max_batches=5)
            self.rows += result.total_rows
            self.jobs += result.jobs
            if not result.more_to_do and result.total_rows == 0:
                self._stop.wait(0.5)

    def __enter__(self) -> "BackgroundPurger":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=60)


def connect() -> psycopg.Connection[Any]:
    return psycopg.connect(db_dsn(), autocommit=True)


def compose_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {**os.environ}
    for key, value in effective_settings().items():
        env[key] = value  # explicit, so neither the shell nor .env can change a benchmark
    env.update(extra or {})
    return env


STACK_ENV: dict[str, str] = {}  # per-scenario overrides actually applied to the running stack


def effective_settings() -> dict[str, str]:
    base = {k: os.environ.get(f"BENCH_{k}", v) for k, v in BENCH_ENV_DEFAULTS.items()}
    return {**base, **STACK_ENV}


def compose(*args: str, env: dict[str, str] | None = None, capture: bool = False) -> str:
    cmd = [
        DOCKER, "compose", "-p", "qfbench",
        "-f", str(ROOT / "docker-compose.yml"),
        "-f", str(ROOT / "scripts" / "benchmark" / "docker-compose.bench.yml"),
        *args,
    ]  # fmt: skip
    result = subprocess.run(
        cmd, cwd=ROOT, env=compose_env(env), capture_output=capture, text=True, check=False
    )
    if result.returncode != 0:
        sys.exit(f"compose {' '.join(args)} failed:\n{result.stderr or result.stdout}")
    return result.stdout if capture else ""


def wait_until(
    condition: Callable[[], bool], timeout: float, what: str, every: float = 1.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(every)
    sys.exit(f"timed out waiting for: {what}")


def workers_alive(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        "SELECT count(*) FROM workers WHERE stopped_at IS NULL "
        "AND last_seen_at > now() - interval '15 seconds'"
    ).fetchone()
    return int(row[0]) if row else 0


def ensure_stack(workers: int, env: dict[str, str] | None = None, build: bool = False) -> None:
    """Bring the benchmark stack up with exactly `workers` live worker processes."""
    STACK_ENV.clear()
    STACK_ENV.update(
        env or {}
    )  # recorded in every result file, so a result shows its real settings
    args = ["up", "-d", "--scale", f"worker={workers}"]
    args.append("--build" if build else "--no-build")
    compose(*args, env=env)
    wait_until(lambda: api_healthy(), 90, "the API to become healthy", every=1.0)
    with connect() as conn:
        wait_until(lambda: workers_alive(conn) == workers, 120, f"{workers} workers alive", 1.0)


def clean_start(
    workers: int,
    env: dict[str, str] | None = None,
    *,
    keep_history: bool = False,
    wipe: bool = True,
) -> None:
    """Start a scenario with an empty queue, THEN bring up the workers.

    The order matters. The data is cleared while no worker is running. Otherwise workers
    can pick up stray queued jobs left behind by an earlier scenario (submit-only never
    starts workers, so its jobs stay queued) while the cleanup is deleting those same
    rows. That race made the cleanup fail with a foreign-key error during harness testing.
    """
    ensure_stack(0, env=env)
    if wipe:
        with connect() as conn:
            reset_data(conn, keep_history)
    ensure_stack(workers, env=env)


def api_healthy() -> bool:
    try:
        return httpx.get(f"{API}/healthz", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


def make_client(
    conn: psycopg.Connection[Any], name: str, role: str = "client"
) -> tuple[uuid.UUID, str]:
    key = "qf_" + secrets.token_urlsafe(32)
    client_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO api_clients (id, name, key_hash, role) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (name) DO UPDATE SET key_hash = EXCLUDED.key_hash RETURNING id",
        (client_id, name, hashlib.sha256(key.encode()).hexdigest(), role),
    )
    row = conn.execute("SELECT id FROM api_clients WHERE name = %s", (name,)).fetchone()
    assert row is not None
    return row[0], key


def reset_data(conn: psycopg.Connection[Any], keep_history: bool = False) -> None:
    """Empty the queue tables. With keep_history, only the benchmark clients' own rows go;
    the preloaded finished-job history (client named 'history') stays."""
    if not keep_history:
        conn.execute("TRUNCATE jobs, job_attempts, idempotency_keys, notifications CASCADE")
        conn.execute("ANALYZE jobs, job_attempts")
        return
    own = "SELECT id FROM jobs WHERE client_id IN (SELECT id FROM api_clients WHERE name <> 'history')"
    conn.execute(f"DELETE FROM job_attempts WHERE job_id IN ({own})")
    conn.execute(f"DELETE FROM idempotency_keys WHERE job_id IN ({own})")
    conn.execute(
        "DELETE FROM jobs WHERE client_id IN (SELECT id FROM api_clients WHERE name <> 'history')"
    )


# --- load generation ------------------------------------------------------------------------


@dataclass
class Sent:
    status: int  # HTTP status, or -1 if the request itself failed
    latency: float  # seconds
    job_id: str | None
    at: float  # wall clock time the request was sent


_local = threading.local()


def http() -> httpx.Client:
    if not hasattr(_local, "client"):
        _local.client = httpx.Client(base_url=API, timeout=60)
    client: httpx.Client = _local.client
    return client


def submit(key: str, task_type: str, payload: dict[str, Any]) -> Sent:
    sent_at = time.time()
    started = time.perf_counter()
    try:
        response = http().post(
            "/v1/jobs",
            json={"task_type": task_type, "payload": payload},
            headers={"X-API-Key": key},
        )
        status = response.status_code
        job_id = response.json().get("id") if status == 201 else None
    except httpx.HTTPError:
        status, job_id = -1, None
    return Sent(status, time.perf_counter() - started, job_id, sent_at)


def closed_loop(n: int, concurrency: int, make_request: Callable[[int], Sent]) -> list[Sent]:
    """Send n requests as fast as `concurrency` threads allow."""
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        return list(pool.map(make_request, range(n)))


def open_loop(
    rate: float, seconds: float, concurrency: int, make_request: Callable[[int], Sent]
) -> list[Sent]:
    """Send requests at a fixed arrival rate (per second), regardless of how fast they finish."""
    total = int(rate * seconds)
    start = time.monotonic()
    futures = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for i in range(total):
            delay = start + i / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            futures.append(pool.submit(make_request, i))
        return [f.result() for f in futures]


def submit_stats(sent: list[Sent]) -> dict[str, Any]:
    ok = [s for s in sent if s.status == 201]
    window = (max(s.at + s.latency for s in sent) - min(s.at for s in sent)) if sent else 0.0
    return {
        "requests": len(sent),
        "status_counts": dict(Counter(str(s.status) for s in sent)),
        "accepted": len(ok),
        "window_seconds": window,
        "accepted_per_second": len(ok) / window if window else None,
        "latency_seconds_accepted": summarize([s.latency for s in ok]),
        "latency_seconds_all": summarize([s.latency for s in sent]),
    }


# --- measuring from the database ---------------------------------------------------------------


def wait_drain(conn: psycopg.Connection[Any], ids: list[uuid.UUID], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT count(*) FROM jobs WHERE id = ANY(%s) AND status IN ('queued', 'running')",
            (ids,),
        ).fetchone()
        if row and row[0] == 0:
            return True
        time.sleep(1.0)
    return False


def job_metrics(conn: psycopg.Connection[Any], ids: list[uuid.UUID]) -> dict[str, Any]:
    """Timing and outcome figures for exactly these jobs, from database timestamps."""
    row = conn.execute(
        """
        WITH j AS (
            SELECT id, status, attempts, created_at, completed_at FROM jobs WHERE id = ANY(%(ids)s)
        ), a AS (
            SELECT job_id, started_at, duration_ms FROM job_attempts
            WHERE job_id = ANY(%(ids)s) AND attempt_number = 1
        )
        SELECT count(*),
               count(*) FILTER (WHERE j.status = 'succeeded'),
               count(*) FILTER (WHERE j.status = 'failed'),
               count(*) FILTER (WHERE j.status = 'dead_lettered'),
               count(*) FILTER (WHERE j.status IN ('queued', 'running')),
               coalesce(sum(j.attempts) - count(*), 0),
               min(j.created_at), max(j.completed_at),
               percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP
                   (ORDER BY extract(epoch FROM a.started_at - j.created_at)),
               max(extract(epoch FROM a.started_at - j.created_at)),
               percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY a.duration_ms / 1000.0),
               percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP
                   (ORDER BY extract(epoch FROM j.completed_at - j.created_at)),
               max(extract(epoch FROM j.completed_at - j.created_at)),
               extract(epoch FROM max(j.completed_at) - min(j.created_at))
        FROM j LEFT JOIN a ON a.job_id = j.id
        """,
        {"ids": ids},
    ).fetchone()
    assert row is not None
    (jobs, ok, failed, dead, unfinished, extra_attempts, _first, _last, wait, wait_max, run, e2e,
     e2e_max, span) = row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]  # fmt: skip

    def trio(values: list[float] | None, maximum: float | None = None) -> dict[str, float | None]:
        v = values or [None, None, None]
        return {"p50": v[0], "p95": v[1], "p99": v[2], "max": maximum}

    return {
        "jobs": jobs,
        "succeeded": ok,
        "failed": failed,
        "dead_lettered": dead,
        "unfinished": unfinished,
        "extra_attempts": int(extra_attempts),  # attempts beyond the first: retries or re-runs
        "error_rate": (failed + dead) / jobs if jobs else None,
        "completion_window_seconds": float(span) if span is not None else None,
        "completion_per_second": (ok / float(span)) if span else None,
        "queue_wait_seconds": trio(wait, wait_max),
        "run_seconds": trio(run),
        "end_to_end_seconds": trio(e2e, e2e_max),
    }


class StatsSampler:
    """Samples `docker stats` for the benchmark containers while a scenario runs."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._samples: list[tuple[str, float, float]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "StatsSampler":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=15)

    def _run(self) -> None:
        while not self._stop.is_set():
            out = subprocess.run(
                [DOCKER, "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"],
                capture_output=True, text=True, check=False,
            )  # fmt: skip
            for line in out.stdout.splitlines():
                name, cpu, mem = (line.split("|") + ["", "", ""])[:3]
                if not name.startswith("qfbench-"):
                    continue
                service = re.sub(r"^qfbench-|-\d+$", "", name)
                try:
                    cpu_v = float(cpu.strip("%"))
                    mem_v = float(re.match(r"([\d.]+)", mem).group(1))  # type: ignore[union-attr]
                    if "GiB" in mem.split("/")[0]:
                        mem_v *= 1024
                except (ValueError, AttributeError):
                    continue
                self._samples.append((service, cpu_v, mem_v))

    def summary(self) -> dict[str, Any]:
        by_service: dict[str, list[tuple[float, float]]] = {}
        for service, cpu, mem in self._samples:
            by_service.setdefault(service, []).append((cpu, mem))
        return {
            service: {
                "samples": len(rows),
                "cpu_percent_mean_per_container": sum(c for c, _ in rows) / len(rows),
                "cpu_percent_max_per_container": max(c for c, _ in rows),
                "memory_mib_max_per_container": max(m for _, m in rows),
            }
            for service, rows in by_service.items()
        }


def environment(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    def run(*cmd: str) -> str:
        try:
            return subprocess.run(
                cmd, capture_output=True, text=True, check=False, cwd=ROOT
            ).stdout.strip()
        except OSError:
            return "unknown"

    host: dict[str, str] = {"os": platform.platform(), "python": sys.version.split()[0]}
    if platform.system() == "Windows":
        host["cpu"] = run(
            "powershell", "-NoProfile", "-Command",
            "(Get-CimInstance Win32_Processor | ForEach-Object { $_.Name + ' (' + $_.NumberOfCores"
            " + ' cores, ' + $_.NumberOfLogicalProcessors + ' threads)' }) -join '; '",
        )  # fmt: skip
        host["ram_gb"] = run(
            "powershell", "-NoProfile", "-Command",
            "[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB,1)",
        )  # fmt: skip
    pg_settings = {
        name: conn.execute("SELECT current_setting(%s)", (name,)).fetchone()[0]  # type: ignore[index]
        for name in (
            "shared_buffers", "work_mem", "max_connections", "fsync", "synchronous_commit",
            "wal_level", "random_page_cost", "effective_cache_size",
        )
    }  # fmt: skip
    return {
        "date": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "git_commit": run("git", "rev-parse", "--short", "HEAD"),
        "git_dirty": bool(run("git", "status", "--porcelain")),
        "host": host,
        "docker": run(
            DOCKER,
            "info",
            "--format",
            "ncpu={{.NCPU}} memory_bytes={{.MemTotal}} server={{.ServerVersion}}",
        ),
        "postgres_version": conn.execute("SELECT version()").fetchone()[0],  # type: ignore[index]
        "postgres_settings": pg_settings,
        "application_settings": effective_settings(),
        "api_url": API,
    }


def _json_default(value: Any) -> Any:
    """PostgreSQL returns exact decimals for some timing figures; store them as numbers."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    return str(value)


def save(scenario: str, label: str, payload: dict[str, Any], conn: psycopg.Connection[Any]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    name = f"{stamp}_{scenario}" + (f"_{label}" if label else "") + ".json"
    path = RESULTS_DIR / name
    payload = {"scenario": scenario, "label": label, "environment": environment(conn), **payload}
    path.write_text(json.dumps(payload, indent=2, default=_json_default))
    print(f"saved {path.relative_to(ROOT)}")
    return path


def headline(m: dict[str, Any]) -> str:
    wait, e2e = m["queue_wait_seconds"], m["end_to_end_seconds"]

    def f(x: float | None) -> str:
        return "n/a" if x is None else f"{x:.3f}"

    cps = m["completion_per_second"]
    return (
        f"jobs={m['jobs']} ok={m['succeeded']} err={m['failed'] + m['dead_lettered']} "
        f"unfinished={m['unfinished']} extra_attempts={m['extra_attempts']} "
        f"completion/s={'n/a' if cps is None else f'{cps:.1f}'} "
        f"wait p50/p95/p99={f(wait['p50'])}/{f(wait['p95'])}/{f(wait['p99'])}s "
        f"e2e p50/p95/p99={f(e2e['p50'])}/{f(e2e['p95'])}/{f(e2e['p99'])}s"
    )


# --- scenarios -----------------------------------------------------------------------------------


def run_batch(
    conn: psycopg.Connection[Any],
    key: str,
    sent: list[Sent],
    drain_timeout: float,
    sampler: StatsSampler | None = None,
) -> dict[str, Any]:
    ids = [uuid.UUID(s.job_id) for s in sent if s.job_id]
    drained = wait_drain(conn, ids, drain_timeout)
    result = {
        "submit": submit_stats(sent),
        "drained_within_timeout": drained,
        "jobs": job_metrics(conn, ids),
    }
    if sampler is not None:
        result["containers"] = sampler.summary()
    return result


def payload_for(kind: str, rng: random.Random) -> tuple[str, dict[str, Any]]:
    if kind == "echo":
        return "echo", {"message": "benchmark"}
    seconds = rng.choice([0.05, 0.1, 0.15, 0.2])  # "mixed": mean 0.125 s
    return "sleep", {"seconds": seconds}


def scenario_burst(args: argparse.Namespace) -> None:
    """Submit N jobs as fast as possible; measure how fast the workers drain them."""
    clean_start(args.workers, wipe=not args.keep_data)
    with connect() as conn:
        _, key = make_client(conn, "bench")
        runs = []
        for i in range(args.repeat):
            if not args.keep_data:
                reset_data(conn)
            rng = random.Random(1000 + i)
            requests = [payload_for(args.kind, rng) for _ in range(args.jobs)]
            with StatsSampler() as sampler:
                sent = closed_loop(
                    args.jobs, args.concurrency, lambda n, reqs=requests: submit(key, *reqs[n])
                )
                run = run_batch(conn, key, sent, args.drain_timeout, sampler)
            print(f"  run {i + 1}/{args.repeat}: {headline(run['jobs'])}; "
                  f"submit/s={run['submit']['accepted_per_second']:.1f}")  # fmt: skip
            runs.append(run)
        save("burst", args.label, {"parameters": vars(args), "runs": runs}, conn)


def scenario_steady(args: argparse.Namespace) -> None:
    """Send jobs at a fixed arrival rate for a while; measure queueing delay."""
    clean_start(args.workers)
    with connect() as conn:
        _, key = make_client(conn, "bench")
        results = []
        for rate in args.rates:
            reset_data(conn)
            rng = random.Random(7)
            with StatsSampler() as sampler:
                sent = open_loop(
                    rate, args.seconds, args.concurrency,
                    lambda n, r=rng: submit(key, *payload_for(args.kind, r)),
                )  # fmt: skip
                run = run_batch(conn, key, sent, args.drain_timeout, sampler)
            run["arrival_rate_per_second"] = rate
            print(f"  rate {rate}/s: {headline(run['jobs'])}")
            results.append(run)
        save("steady", args.label, {"parameters": vars(args), "runs": results}, conn)


def scenario_idle(args: argparse.Namespace) -> None:
    """One job at a time into an idle system: how long until a worker notices it?"""
    clean_start(args.workers)
    with connect() as conn:
        _, key = make_client(conn, "bench")
        reset_data(conn)
        sent = []
        rng = random.Random(3)
        for _ in range(args.jobs):
            sent.append(submit(key, "echo", {"message": "idle"}))
            time.sleep(1.0 + rng.random() * 0.7)  # not locked to the workers' poll rhythm
        run = run_batch(conn, key, sent, 60)
        print(f"  {headline(run['jobs'])}")
        save("idle-latency", args.label, {"parameters": vars(args), "runs": [run]}, conn)


def scenario_submit_only(args: argparse.Namespace) -> None:
    """API cost only: no workers, so nothing competes with the API and database."""
    with connect() as conn:
        ensure_stack(0)
        _, key = make_client(conn, "bench")
        reset_data(conn, args.keep_history)
        if args.deep_queue:
            # A big waiting queue belonging to ANOTHER client: no worker is running, so it
            # stays queued, and every submit has to check the global queue cap against it.
            deep_client, _ = make_client(conn, "bench-deep")
            insert_queued(conn, deep_client, args.deep_queue, "echo")
            conn.execute("ANALYZE jobs")
        sent = closed_loop(
            args.jobs, args.concurrency, lambda n: submit(key, "echo", {"message": "api-only"})
        )
        stats = submit_stats(sent)
        lat = stats["latency_seconds_accepted"]
        rate = stats["accepted_per_second"] or 0.0
        p50, p95, p99 = (
            lat[k] if lat[k] is not None else float("nan") for k in ("p50", "p95", "p99")
        )
        print(f"  accepted/s={rate:.1f} status={stats['status_counts']} "
              f"latency p50/p95/p99={p50:.3f}/{p95:.3f}/{p99:.3f}s")  # fmt: skip
        save("submit-only", args.label, {"parameters": vars(args), "submit": stats}, conn)
        if args.deep_queue:
            reset_data(conn, args.keep_history)  # remove the synthetic queue again


def scenario_fairness(args: argparse.Namespace) -> None:
    """Client A queues a big backlog; how long does client B's small job wait behind it?"""
    clean_start(args.workers)
    with connect() as conn:
        client_a, _ = make_client(conn, "bench-a")
        _, key_b = make_client(conn, "bench-b")
        out: dict[str, Any] = {}
        for label, backlog in (("baseline_b_alone", 0), ("b_behind_a_backlog", args.backlog)):
            reset_data(conn)
            # A's backlog goes straight into the database (the API is too slow on this machine
            # to build a backlog while workers keep up). B then submits through the real API.
            ids_a = insert_queued(conn, client_a, backlog, "mixed") if backlog else []
            conn.execute("ANALYZE jobs")
            queued_at_b_start = conn.execute(
                "SELECT count(*) FROM jobs WHERE status = 'queued'"
            ).fetchone()[0]  # type: ignore[index]
            sent_b = []
            for _ in range(args.b_jobs):
                sent_b.append(submit(key_b, "echo", {"message": "b"}))
                time.sleep(0.5)
            ids_b = [uuid.UUID(s.job_id) for s in sent_b if s.job_id]
            wait_drain(conn, ids_b, args.drain_timeout)
            out[label] = {
                "backlog_inserted_for_a": len(ids_a),
                "queued_jobs_when_b_started": queued_at_b_start,
                "b": job_metrics(conn, ids_b),
            }
            if ids_a:
                wait_drain(conn, ids_a, args.drain_timeout)
            print(
                f"  {label}: queued when B started={queued_at_b_start}; B {headline(out[label]['b'])}"
            )
        save("fairness", args.label, {"parameters": vars(args), "results": out}, conn)


def insert_queued(
    conn: psycopg.Connection[Any],
    client_id: uuid.UUID | list[uuid.UUID],
    n: int,
    kind: str,
) -> list[uuid.UUID]:
    """Put n ready-to-run jobs straight into the queue (bypassing the API), return their ids.

    Used to measure what the workers and the database can do on their own, because on this
    machine the API cannot create a backlog fast enough. "mixed" cycles through sleep
    durations of 0.05, 0.1, 0.15 and 0.2 s (mean 0.125 s).
    """
    if kind == "echo":
        task, payload = "'echo'", "jsonb_build_object('message', 'drain')"
    else:
        task = "'sleep'"
        payload = "jsonb_build_object('seconds', (ARRAY[0.05, 0.1, 0.15, 0.2])[1 + (g %% 4)])"
    clients = client_id if isinstance(client_id, list) else [client_id]
    rows = conn.execute(
        f"INSERT INTO jobs (id, client_id, task_type, payload, status, run_at, created_at) "
        f"SELECT gen_random_uuid(), (%s::uuid[])[1 + (g %% %s)], {task}, {payload}, 'queued', "
        f"now(), now() FROM generate_series(1, %s) g RETURNING id",
        (clients, len(clients), n),
    ).fetchall()
    return [r[0] for r in rows]


def scenario_drain(args: argparse.Namespace) -> None:
    """Workers' own capacity: a full queue appears at once, how fast is it emptied?"""
    clean_start(args.workers, keep_history=args.keep_history)
    with connect() as conn:
        if args.clients > 1:
            client_id = [make_client(conn, f"bench-{i}")[0] for i in range(args.clients)]
        else:
            client_id, _ = make_client(conn, "bench")
        runs = []
        if args.without_new_indexes:
            # Controlled A/B: same database, same data, the two Phase B indexes dropped
            # for this run only (they are re-created at the end, below).
            for name in NEW_INDEXES:
                conn.execute(f"DROP INDEX IF EXISTS {name}")
        if args.vacuum_first:
            # Remove dead rows first, so leftover bloat from earlier runs cannot distort this one.
            conn.execute("VACUUM (ANALYZE) jobs, job_attempts")
        tables_before = table_stats(conn)
        for i in range(args.repeat):
            reset_data(conn, args.keep_history)
            purger = (
                BackgroundPurger(args.purge_days, args.purge_batch)
                if args.purge_concurrently
                else None
            )
            with StatsSampler() as sampler:
                if purger:
                    purger.__enter__()
                ids = insert_queued(conn, client_id, args.jobs, args.kind)
                conn.execute("ANALYZE jobs")
                drained = wait_drain(conn, ids, args.drain_timeout)
                containers = sampler.summary()
                if purger:
                    purger.__exit__(None, None, None)
            metrics = job_metrics(conn, ids)
            window = conn.execute(
                "SELECT extract(epoch FROM max(finished_at) - min(started_at)) "
                "FROM job_attempts WHERE job_id = ANY(%s)",
                (ids,),
            ).fetchone()[0]  # type: ignore[index]
            per_worker = dict(
                conn.execute(
                    "SELECT worker_id, count(*) FROM job_attempts WHERE job_id = ANY(%s) GROUP BY 1",
                    (ids,),
                ).fetchall()
            )
            run = {
                "drained_within_timeout": drained,
                "drain_seconds_first_start_to_last_finish": float(window),
                "drain_jobs_per_second": args.jobs / float(window) if window else None,
                "jobs_per_worker": sorted(per_worker.values()),
                "purged_during_run": {"jobs": purger.jobs, "rows": purger.rows} if purger else None,
                "jobs": metrics,
                "containers": containers,
            }
            print(
                f"  run {i + 1}/{args.repeat}: {run['drain_jobs_per_second']:.1f} jobs/s "
                f"({args.jobs} jobs in {window:.1f}s; per worker {run['jobs_per_worker']}) "
                f"err={metrics['failed'] + metrics['dead_lettered']} "
                f"extra_attempts={metrics['extra_attempts']}"
            )
            runs.append(run)
        if args.without_new_indexes:
            for ddl in NEW_INDEXES.values():
                conn.execute(ddl)
        save(
            "drain", args.label,
            {"parameters": vars(args), "tables_before": tables_before, "runs": runs}, conn,
        )  # fmt: skip


def scenario_overload(args: argparse.Namespace) -> None:
    """Send far more than the system can finish; check limits engage, nothing is lost, it recovers."""
    env = {
        "MAX_QUEUED_JOBS_PER_CLIENT": str(args.per_client_cap),
        "MAX_QUEUED_JOBS_TOTAL": str(args.total_cap),
    }
    clean_start(args.workers, env=env)
    with connect() as conn:
        clients = [make_client(conn, f"bench-{i}")[1] for i in range(args.clients)]
        reset_data(conn)
        started = time.time()
        results: list[Sent] = []
        lock = threading.Lock()

        def pump(key: str) -> None:
            part = open_loop(
                args.rate_per_client, args.seconds, args.concurrency,
                lambda n: submit(key, "sleep", {"seconds": 0.1}),
            )  # fmt: skip
            with lock:
                results.extend(part)

        with StatsSampler() as sampler:
            threads = [threading.Thread(target=pump, args=(k,)) for k in clients]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            ids = [uuid.UUID(s.job_id) for s in results if s.job_id]
            drain_started = time.time()
            drained = wait_drain(conn, ids, args.drain_timeout)
            drain_seconds = time.time() - drain_started
            containers = sampler.summary()

        bins: dict[int, Counter[str]] = {}
        for s in results:
            bins.setdefault(int((s.at - started) // 5) * 5, Counter())[str(s.status)] += 1
        by_status = {
            status: summarize([s.latency for s in results if str(s.status) == status])
            for status in sorted({str(s.status) for s in results})
        }
        metrics = job_metrics(conn, ids)
        print(f"  statuses={dict(Counter(str(s.status) for s in results))} drained={drained} "
              f"drain_seconds={drain_seconds:.1f}")  # fmt: skip
        print(f"  accepted jobs: {headline(metrics)}")
        # After the load stops, are new submits accepted again?
        after = submit(clients[0], "echo", {"message": "after"})
        print(f"  submit after recovery: HTTP {after.status}")
        save(
            "overload", args.label,
            {
                "parameters": vars(args),
                "status_counts": dict(Counter(str(s.status) for s in results)),
                "latency_by_status": by_status,
                "status_by_5s_bin": {str(k): dict(v) for k, v in sorted(bins.items())},
                "drained": drained,
                "drain_seconds_after_load_stopped": drain_seconds,
                "accepted_jobs": metrics,
                "submit_after_recovery_status": after.status,
                "containers": containers,
            },
            conn,
        )  # fmt: skip


def table_stats(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT relname, n_live_tup, n_dead_tup, pg_total_relation_size(relid), "
        "pg_relation_size(relid) FROM pg_stat_user_tables WHERE relname IN ('jobs', 'job_attempts')"
    ).fetchall()
    return {
        r[0]: {"live_rows": r[1], "dead_rows": r[2], "total_bytes": r[3], "table_bytes": r[4]}
        for r in rows
    }


def scenario_soak(args: argparse.Namespace) -> None:
    """Steady load for a while; watch for drift in latency, errors, memory and table bloat."""
    clean_start(args.workers)
    with connect() as conn:
        _, key = make_client(conn, "bench")
        reset_data(conn)
        conn.execute("SELECT pg_stat_reset()")
        series: list[dict[str, Any]] = []
        stop = threading.Event()
        started = time.time()

        def watch() -> None:
            while not stop.wait(args.sample_seconds):
                since = dt.datetime.fromtimestamp(time.time() - args.sample_seconds, dt.UTC)
                with connect() as c:
                    window = c.execute(
                        "SELECT count(*), percentile_cont(0.95) WITHIN GROUP "
                        "(ORDER BY extract(epoch FROM a.started_at - j.created_at)) "
                        "FROM jobs j JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = 1 "
                        "WHERE j.completed_at >= %s",
                        (since,),
                    ).fetchone()
                    depth = c.execute(
                        "SELECT count(*) FROM jobs WHERE status = 'queued'"
                    ).fetchone()
                    series.append({
                        "elapsed_seconds": round(time.time() - started),
                        "completed_in_window": window[0] if window else None,  # type: ignore[index]
                        "queue_wait_p95_seconds_in_window": window[1] if window else None,  # type: ignore[index]
                        "queued_now": depth[0] if depth else None,
                        "tables": table_stats(c),
                    })  # fmt: skip

        rng = random.Random(11)
        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        with StatsSampler() as sampler:
            sent = open_loop(
                args.rate, args.seconds, args.concurrency,
                lambda n: submit(key, *payload_for("mixed", rng)),
            )  # fmt: skip
            run = run_batch(conn, key, sent, args.drain_timeout, sampler)
        stop.set()
        watcher.join(timeout=30)
        print(f"  {headline(run['jobs'])}")
        save(
            "soak", args.label, {"parameters": vars(args), "run": run, "time_series": series}, conn
        )


# --- history size and query plans -------------------------------------------------------------------


def scenario_preload(args: argparse.Namespace) -> None:
    """Fill the database with finished jobs, like a system that has been running for a while."""
    with connect() as conn:
        client_id, _ = make_client(conn, "history")
        reset_data(conn)
        batch = 100_000
        done = 0
        started = time.time()
        while done < args.rows:
            n = min(batch, args.rows - done)
            conn.execute(
                """
                WITH ins AS (
                    INSERT INTO jobs (id, client_id, task_type, payload, status, attempts,
                                      max_attempts, run_at, result, last_error,
                                      created_at, updated_at, completed_at)
                    SELECT gen_random_uuid(), %(c)s, 'echo',
                           jsonb_build_object('message', 'history-' || g),
                           CASE WHEN g %% 100 = 0 THEN 'dead_lettered' ELSE 'succeeded' END,
                           CASE WHEN g %% 100 = 0 THEN 5 ELSE 1 END, 5,
                           t, CASE WHEN g %% 100 = 0 THEN NULL ELSE jsonb_build_object('echo', 'x') END,
                           CASE WHEN g %% 100 = 0
                                THEN jsonb_build_object('type', 'RetryableError', 'message', 'history')
                                END,
                           t, t, t + interval '1 second'
                    FROM generate_series(%(offset)s + 1, %(offset)s + %(n)s) g,
                         LATERAL (SELECT now() - (g * interval '2 seconds') - interval '1 day') x(t)
                    RETURNING id, created_at
                )
                INSERT INTO job_attempts (job_id, attempt_number, worker_id, started_at,
                                          finished_at, duration_ms, outcome)
                SELECT id, 1, 'history', created_at + interval '100 milliseconds',
                       created_at + interval '110 milliseconds', 10, 'succeeded' FROM ins
                """,
                {"c": client_id, "offset": done, "n": n},
            )
            done += n
            print(f"  {done}/{args.rows} rows ({time.time() - started:.0f}s)", flush=True)
        conn.execute("ANALYZE jobs, job_attempts")
        stats = table_stats(conn)
        print(f"  {stats}")
        save("preload", args.label, {"parameters": vars(args), "tables": stats,
                                     "seconds": time.time() - started}, conn)  # fmt: skip


PLAN_QUERIES: dict[str, tuple[str, tuple[Any, ...]]] = {
    "claim_next_job": (
        "SELECT id FROM jobs WHERE status = 'queued' AND run_at <= now() "
        "ORDER BY run_at, created_at LIMIT 1 FOR UPDATE SKIP LOCKED", ()),
    "sweeper_find_expired": (
        "SELECT id, attempts, max_attempts FROM jobs WHERE status = 'running' "
        "AND lease_expires_at < now() ORDER BY lease_expires_at LIMIT 50 FOR UPDATE SKIP LOCKED", ()),
    "per_client_queue_count": (
        "SELECT count(*) FROM (SELECT id FROM jobs WHERE status = 'queued' AND client_id = %s "
        "LIMIT 10000) t", ("{client}",)),
    "global_queue_count": (
        "SELECT count(*) FROM (SELECT id FROM jobs WHERE status = 'queued' LIMIT 100000) t", ()),
    "dead_letter_listing": (
        "SELECT * FROM jobs WHERE status = 'dead_lettered' ORDER BY completed_at DESC, id "
        "LIMIT 50", ()),
    "metrics_jobs_by_status": ("SELECT status, count(*) FROM jobs GROUP BY status", ()),
    "metrics_oldest_queued": (
        "SELECT coalesce(extract(epoch FROM now() - min(run_at)), 0) FROM jobs "
        "WHERE status = 'queued' AND run_at <= now()", ()),
    "metrics_oldest_running": (
        "SELECT coalesce(extract(epoch FROM now() - min(a.started_at)), 0) FROM jobs j "
        "JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = j.attempts "
        "AND a.outcome IS NULL WHERE j.status = 'running'", ()),
    "metrics_expired_leases": (
        "SELECT count(*) FROM jobs WHERE status = 'running' AND lease_expires_at < now()", ()),
    "metrics_attempts_by_outcome": (
        "SELECT outcome, count(*) FROM job_attempts GROUP BY outcome", ()),
    "get_job_by_id": ("SELECT * FROM jobs WHERE id = %s", ("{job}",)),
    "attempt_history_for_job": (
        "SELECT * FROM job_attempts WHERE job_id = %s ORDER BY attempt_number", ("{job}",)),
}  # fmt: skip

# The two indexes added by migration 0004, so a drain run can be repeated without them.
NEW_INDEXES = {
    "ix_jobs_running_lease": "CREATE INDEX IF NOT EXISTS ix_jobs_running_lease ON jobs "
    "(lease_expires_at) WHERE status = 'running'",
    "ix_jobs_dead_lettered_completed": "CREATE INDEX IF NOT EXISTS ix_jobs_dead_lettered_completed "
    "ON jobs (completed_at DESC, id) WHERE status = 'dead_lettered'",
}

CANDIDATE_INDEXES = [
    "CREATE INDEX cand_running_lease ON jobs (lease_expires_at) WHERE status = 'running'",
    "CREATE INDEX cand_dead_completed ON jobs (completed_at DESC, id) WHERE status = 'dead_lettered'",
]


def explain(conn: psycopg.Connection[Any], sql: str, params: tuple[Any, ...]) -> dict[str, Any]:
    conn.execute("BEGIN")
    try:
        rows = conn.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params).fetchall()
    finally:
        conn.execute("ROLLBACK")
    lines = [r[0] for r in rows]
    exec_ms = next(
        (
            float(m.group(1))
            for line in lines
            if (m := re.search(r"Execution Time: ([\d.]+) ms", line))
        ),
        None,
    )
    node = lines[0].strip() if lines else ""
    return {"execution_ms": exec_ms, "top_node": node, "plan": lines}


def scenario_plans(args: argparse.Namespace) -> None:
    """Query timings and plans with a big history, plus what candidate indexes would change.

    Runs with NO workers: otherwise they would start processing the synthetic queued jobs
    created here and change what is being measured."""
    ensure_stack(0)
    with connect() as conn:
        client_id, key = make_client(conn, "history")
        _, plan_client_key = make_client(conn, "plans")
        admin_id, admin_key = make_client(conn, "bench-admin", role="admin")
        plans_client = conn.execute("SELECT id FROM api_clients WHERE name = 'plans'").fetchone()[0]  # type: ignore[index]
        # A realistic live queue and some running jobs whose workers died (expired leases).
        conn.execute(
            "DELETE FROM jobs WHERE status IN ('queued', 'running') AND client_id = %s",
            (plans_client,),
        )
        conn.execute(
            "INSERT INTO jobs (id, client_id, task_type, payload, status, run_at, created_at) "
            "SELECT gen_random_uuid(), %s, 'echo', '{\"message\":\"q\"}', 'queued', "
            "now() - (g * interval '1 millisecond'), now() FROM generate_series(1, %s) g",
            (plans_client, args.queued),
        )
        conn.execute(
            "INSERT INTO jobs (id, client_id, task_type, payload, status, attempts, lease_owner, "
            "lease_token, lease_expires_at, run_at, created_at) "
            "SELECT gen_random_uuid(), %s, 'echo', '{\"message\":\"r\"}', 'running', 1, 'dead-worker', "
            "gen_random_uuid(), now() - interval '1 minute', now(), now() FROM generate_series(1, 50)",
            (plans_client,),
        )
        conn.execute("ANALYZE jobs")
        job_id = conn.execute("SELECT id FROM jobs ORDER BY created_at LIMIT 1").fetchone()[0]  # type: ignore[index]
        subst = {"{client}": plans_client, "{job}": job_id}

        def run_all() -> dict[str, Any]:
            out = {}
            for name, (sql, params) in PLAN_QUERIES.items():
                real = tuple(subst.get(p, p) if isinstance(p, str) else p for p in params)
                runs = [explain(conn, sql, real) for _ in range(args.repeat)]
                best = min(runs, key=lambda r: r["execution_ms"] or 1e9)
                out[name] = {
                    "execution_ms_each_run": [r["execution_ms"] for r in runs],
                    "top_node": best["top_node"],
                    "plan": best["plan"],
                }
                print(
                    f"  {name}: {[round(r['execution_ms'] or 0, 2) for r in runs]} ms  <- {best['top_node'][:70]}"
                )
            return out

        print("current schema:")
        before = run_all()
        print("with candidate indexes (created in a transaction, rolled back afterwards):")
        conn.execute("BEGIN")
        try:
            for ddl in CANDIDATE_INDEXES:
                conn.execute(ddl)
            conn.execute("ANALYZE jobs")
            conn.execute("COMMIT")  # EXPLAIN uses its own transactions; drop them again below
            after = run_all()
        finally:
            conn.execute("DROP INDEX IF EXISTS cand_running_lease")
            conn.execute("DROP INDEX IF EXISTS cand_dead_completed")
            conn.execute("ANALYZE jobs")

        # The real HTTP cost of a /metrics scrape at this size.
        scrape = []
        for _ in range(args.repeat):
            t0 = time.perf_counter()
            r = httpx.get(f"{API}/metrics", headers={"X-API-Key": admin_key}, timeout=120)
            scrape.append(time.perf_counter() - t0)
            assert r.status_code == 200, r.status_code
        print(f"  GET /metrics seconds: {[round(s, 3) for s in scrape]}")
        save("plans", args.label, {
            "parameters": vars(args), "tables": table_stats(conn),
            "candidate_indexes": CANDIDATE_INDEXES,
            "current_schema": before, "with_candidate_indexes": after,
            "metrics_scrape_seconds": scrape,
        }, conn)  # fmt: skip
        _ = (client_id, key, plan_client_key, admin_id)
        # Leave no synthetic jobs behind for later scenarios.
        conn.execute(
            "DELETE FROM job_attempts WHERE job_id IN (SELECT id FROM jobs WHERE client_id = %s)",
            (plans_client,),
        )
        conn.execute("DELETE FROM jobs WHERE client_id = %s", (plans_client,))


def scenario_purge(args: argparse.Namespace) -> None:
    """Run the real purge over the preloaded history; measure speed and what it leaves behind."""
    from queueforge.services.retention import RetentionPolicy, count_eligible, run_purge_cycle

    ensure_stack(0)
    sessions = purge_sessions()
    policy = RetentionPolicy(
        succeeded_days=args.days, failed_days=args.days, dead_letter_days=args.days,
        batch_size=args.batch_size,
    )  # fmt: skip
    with connect() as conn:
        tables_before = table_stats(conn)
        with sessions() as session:
            eligible = count_eligible(session, policy)
        print(f"  eligible before: {eligible}")
        cycles: list[dict[str, Any]] = []
        started = time.perf_counter()
        while True:
            t0 = time.perf_counter()
            result = run_purge_cycle(sessions, policy, max_batches=args.batches_per_cycle)
            seconds = time.perf_counter() - t0
            if result.total_rows == 0:
                break
            cycles.append(
                {"jobs": result.jobs, "rows": result.total_rows, "batches": result.batches, "seconds": seconds}
            )  # fmt: skip
            if len(cycles) % 5 == 0:
                done = sum(c["jobs"] for c in cycles)
                print(f"  {done} jobs deleted, {time.perf_counter() - started:.0f}s", flush=True)
        total_seconds = time.perf_counter() - started
        tables_after_purge = table_stats(conn)
        conn.execute("VACUUM (ANALYZE) jobs, job_attempts")
        tables_after_vacuum = table_stats(conn)
        jobs = sum(c["jobs"] for c in cycles)
        rows = sum(c["rows"] for c in cycles)
        batch_ms = [1000 * c["seconds"] / c["batches"] for c in cycles if c["batches"]]
        print(
            f"  purged {jobs} jobs ({rows} rows) in {total_seconds:.0f}s = {jobs / total_seconds:.0f} jobs/s; "
            f"per-batch ms p50={pct(batch_ms, 50):.0f} p95={pct(batch_ms, 95):.0f} max={max(batch_ms):.0f}"
        )  # fmt: skip
        save(
            "purge", args.label,
            {
                "parameters": vars(args), "eligible_before": eligible,
                "jobs_purged": jobs, "rows_purged": rows, "seconds": total_seconds,
                "jobs_per_second": jobs / total_seconds if total_seconds else None,
                "batch_milliseconds": summarize(batch_ms),
                "tables_before": tables_before, "tables_after_purge": tables_after_purge,
                "tables_after_vacuum": tables_after_vacuum,
            },
            conn,
        )  # fmt: skip


def scenario_backup_drill(args: argparse.Namespace) -> None:
    """Time a real backup and a verified restore of the current database (plain and encrypted)."""
    ensure_stack(0)
    with connect() as conn:
        tables = table_stats(conn)
        size_row = conn.execute("SELECT pg_database_size(current_database())").fetchone()
        database_bytes = int(size_row[0]) if size_row else 0
        print(f"  database before: {tables} ({database_bytes / 1e6:.0f} MB)")
        results = []
        for label, passphrase in (("plain", ""), ("encrypted", "benchmark-only-passphrase")):
            for run in range(args.repeat):
                out = compose(
                    "--profile", "tools", "run", "--rm", "-T", "dbtools", "drill",
                    env={"BACKUP_PASSPHRASE": passphrase}, capture=True,
                )  # fmt: skip
                values = dict(
                    line.split("=", 1) for line in out.splitlines() if re.match(r"^[a-z_]+=", line)
                )
                row = {
                    "kind": label,
                    "backup_file_mb": int(values["backup_bytes"]) / 1e6,
                    "backup_seconds": int(values["backup_seconds"]),
                    "restore_seconds": int(values["verify_restore_seconds"]),
                    "verify_total_seconds": int(values["verify_total_seconds"]),
                    "verify_ok": "VERIFY OK" in out,
                }
                results.append(row)
                print(f"  {label} run {run + 1}: {row}")
        save(
            "backup-drill", args.label,
            {"parameters": vars(args), "tables": tables, "database_bytes": database_bytes,
             "results": results},
            conn,
        )  # fmt: skip


def scenario_claimcost(args: argparse.Namespace) -> None:
    """How much does picking a client fairly cost, compared with plain oldest-first?"""
    from queueforge.worker.claims import _PICK_FAIR_CLIENT_SQL

    fair_sql = str(_PICK_FAIR_CLIENT_SQL).replace(":max_clients", "%(max_clients)s")
    oldest_sql = (
        "SELECT id FROM jobs WHERE status = 'queued' AND run_at <= now() "
        "ORDER BY run_at, created_at LIMIT 1 FOR UPDATE SKIP LOCKED"
    )
    ensure_stack(0)
    with connect() as conn:
        results: list[dict[str, Any]] = []
        for k in args.client_counts:
            reset_data(conn)
            conn.execute("DELETE FROM api_clients WHERE name LIKE 'cc-%'")
            conn.execute(
                "INSERT INTO api_clients (id, name, key_hash, role) "
                "SELECT gen_random_uuid(), 'cc-' || g, encode(sha256(('cc' || g)::bytea), 'hex'), "
                "'client' FROM generate_series(1, %s) g",
                (k,),
            )
            conn.execute(
                "INSERT INTO jobs (id, client_id, task_type, payload, status, run_at, created_at) "
                "SELECT gen_random_uuid(), c.id, 'echo', '{\"message\":\"x\"}', 'queued', "
                "now() - (g * interval '1 millisecond'), now() "
                "FROM api_clients c CROSS JOIN generate_series(1, %s) g WHERE c.name LIKE 'cc-%%'",
                (args.jobs_per_client,),
            )
            conn.execute("ANALYZE jobs")
            for label, sql, params in (
                ("oldest_first", oldest_sql, {}),
                ("fair_pick_max64", fair_sql, {"max_clients": 64}),
                ("fair_pick_max1000", fair_sql, {"max_clients": 1000}),
            ):
                runs = [explain(conn, sql, params)["execution_ms"] for _ in range(args.repeat)]
                results.append({"clients_with_queued_jobs": k, "query": label, "ms_each_run": runs})
                shown = [round(x or 0, 2) for x in runs]
                print(f"  clients={k:<5} {label:<18} {shown} ms")
        conn.execute(
            "DELETE FROM job_attempts WHERE job_id IN (SELECT id FROM jobs WHERE client_id IN (SELECT id FROM api_clients WHERE name LIKE 'cc-%'))"
        )
        conn.execute(
            "DELETE FROM jobs WHERE client_id IN (SELECT id FROM api_clients WHERE name LIKE 'cc-%')"
        )
        conn.execute("DELETE FROM api_clients WHERE name LIKE 'cc-%'")
        save("claimcost", args.label, {"parameters": vars(args), "results": results}, conn)


def scenario_scrape(args: argparse.Namespace) -> None:
    """Cost of GET /metrics scrapes, back to back, at the current database size."""
    ensure_stack(args.workers)
    with connect() as conn:
        _, admin_key = make_client(conn, "bench-admin", role="admin")
        times = []
        for _ in range(args.repeat):
            t0 = time.perf_counter()
            r = httpx.get(f"{API}/metrics", headers={"X-API-Key": admin_key}, timeout=120)
            times.append(time.perf_counter() - t0)
            assert r.status_code == 200
        print(f"  /metrics seconds: {[round(t, 3) for t in times]}  tables={table_stats(conn)}")
        save("scrape", args.label, {"parameters": vars(args), "seconds": times,
                                    "tables": table_stats(conn)}, conn)  # fmt: skip


# --- command line --------------------------------------------------------------------------------


def cmd_up(args: argparse.Namespace) -> None:
    # Images are only (re)built when asked: building needs the network and can be slow.
    build = ["--build"] if args.build else ["--no-build"]
    compose("up", "-d", *build, "--scale", f"worker={args.workers}")
    wait_until(api_healthy, 120, "the API to become healthy")
    with connect() as conn:
        wait_until(lambda: workers_alive(conn) == args.workers, 120, "workers alive")
    print(f"benchmark stack is up with {args.workers} worker(s) at {API}")


def cmd_down(args: argparse.Namespace) -> None:
    compose("down", "-v")
    print("benchmark stack removed (including its database volume)")


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True)

    def add(
        name: str, fn: Callable[[argparse.Namespace], None], **defaults: Any
    ) -> argparse.ArgumentParser:
        sp = sub.add_parser(name)
        sp.set_defaults(fn=fn)
        sp.add_argument("--label", default="")
        for key, value in defaults.items():
            sp.add_argument(f"--{key.replace('_', '-')}", type=type(value), default=value)
        return sp

    sp = sub.add_parser("up")
    sp.add_argument("--workers", type=int, default=1)
    sp.add_argument(
        "--build",
        action="store_true",
        help="rebuild the images first (first run, or after code changes)",
    )
    sp.set_defaults(fn=cmd_up)
    sub.add_parser("down").set_defaults(fn=cmd_down)

    b = add("burst", scenario_burst, workers=4, jobs=2000, concurrency=16, repeat=3,
            drain_timeout=600.0, kind="echo")  # fmt: skip
    b.add_argument("--keep-data", action="store_true")
    add("steady", scenario_steady, workers=4, seconds=60.0, concurrency=32,
        drain_timeout=300.0, kind="mixed").add_argument("--rates", type=float, nargs="+", required=True)  # fmt: skip
    add("idle", scenario_idle, workers=2, jobs=40)
    add("submit-only", scenario_submit_only, jobs=2000, concurrency=16, deep_queue=0).add_argument(
        "--keep-history", action="store_true"
    )
    drain_cmd = add(
        "drain", scenario_drain, workers=4, jobs=3000, repeat=3, drain_timeout=900.0, kind="echo"
    )
    drain_cmd.add_argument("--keep-history", action="store_true")
    drain_cmd.add_argument("--clients", type=int, default=1)
    drain_cmd.add_argument("--purge-concurrently", action="store_true")
    drain_cmd.add_argument("--purge-days", type=int, default=1)
    drain_cmd.add_argument("--purge-batch", type=int, default=1000)
    drain_cmd.add_argument("--vacuum-first", action="store_true")
    drain_cmd.add_argument("--without-new-indexes", action="store_true")
    add("fairness", scenario_fairness, workers=4, backlog=3000, b_jobs=20, concurrency=16,
        drain_timeout=600.0)  # fmt: skip
    add("overload", scenario_overload, workers=2, clients=4, rate_per_client=15.0, seconds=90.0,
        concurrency=24, per_client_cap=150, total_cap=400, drain_timeout=900.0)  # fmt: skip
    add("soak", scenario_soak, workers=4, rate=25.0, seconds=600.0, concurrency=32,
        sample_seconds=30, drain_timeout=300.0)  # fmt: skip
    add("preload", scenario_preload, rows=1_000_000)
    add("plans", scenario_plans, queued=20_000, repeat=3)
    add("scrape", scenario_scrape, repeat=5, workers=0)
    add("backup-drill", scenario_backup_drill, repeat=2)
    add("purge", scenario_purge, days=1, batch_size=1000, batches_per_cycle=50)
    cc = add("claimcost", scenario_claimcost, repeat=5, jobs_per_client=50)
    cc.add_argument("--client-counts", type=int, nargs="+", default=[1, 10, 100, 1000])

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
