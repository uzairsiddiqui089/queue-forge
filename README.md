# QueueForge

A distributed job processing platform built with Python, FastAPI and PostgreSQL.
Clients submit jobs over HTTP; separate worker processes claim and execute them
with at-least-once semantics. It is a learning and portfolio project that
focuses on correct, explainable backend engineering.

> **Status: Milestone 5 (security and observability).** You can submit jobs, and
> separate worker processes claim and run them. Failed jobs are retried with
> backoff, jobs from crashed workers are recovered, and jobs that keep failing are
> dead-lettered (admins can inspect and re-queue them). The system is
> **at-least-once**: a job can run more than once (see
> [architecture](docs/architecture.md)), so handlers must be idempotent.
> Docs: [project brief](docs/project-brief.md) · [architecture](docs/architecture.md) ·
> [operations](docs/operations.md) · [security](docs/security.md).

## Implemented

- A repeatable benchmark harness (`scripts/benchmark/`) with measured results and findings in [docs/benchmarking.md](docs/benchmarking.md)
- Structured JSON logs with a field allow-list (payloads, keys and query strings are never logged; tested)
- `GET /metrics` (Prometheus format, admin key): jobs per state, oldest queued job age, expired leases, attempts by outcome
- Admin endpoints: list dead-lettered jobs, view a job's attempt history, re-queue a dead-lettered job
- Per-key rate limiting with Redis (Redis holds only counters; if it is down the API keeps working), 64 KB request body limit, generic `500` errors without stack traces

- Separate worker processes (`python -m queueforge.worker`) that claim jobs safely with `FOR UPDATE SKIP LOCKED`, so no two workers take the same job
- Four task handlers (`echo`, `sleep`, `flaky`, `simulated_notification`), results and sanitized errors saved, every attempt recorded in `job_attempts`
- Fenced result writes: a worker that lost its lease cannot overwrite a job
- Retries: permanent vs temporary errors, exponential backoff with full jitter, 5 attempts, then `dead_lettered`
- Crash recovery: leases with a heartbeat, plus a sweeper that re-queues jobs from dead workers (tested with a real hard-killed worker process)
- `POST /v1/jobs` and `GET /v1/jobs/{id}`, with API-key authentication
- Allow-listed task types with validated payloads: `echo`, `sleep`, `flaky`, `simulated_notification`
- Jobs stored in PostgreSQL (Alembic migrations run automatically in the API container)
- Idempotency: the same `Idempotency-Key` + same body returns the original job (24 h)
- Per-client isolation: another client's job returns `404`
- FastAPI app with `GET /healthz` (liveness) and `GET /readyz` (readiness, checks the database)
- Environment-based configuration (no secrets in code)
- Docker Compose stack: PostgreSQL 16, Redis 7, one-time migrations, API and scalable workers
- Tests (pytest), linting and formatting (Ruff), strict type checking (mypy)
- GitHub Actions CI running the same checks against a Postgres service container

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose v2
- [uv](https://docs.astral.sh/uv/) (installs Python 3.12 and the dependencies)

## Setup

```bash
cp .env.example .env          # then edit .env: set a real POSTGRES_PASSWORD
                              # (and the same password inside DATABASE_URL)
docker compose up -d --build            # db, redis, one-time migrate, api, one worker
docker compose up -d --scale worker=4   # run 4 workers instead
curl localhost:8000/healthz   # {"status":"ok"}
curl localhost:8000/readyz    # {"status":"ready"}
```

Stop everything with `docker compose down` (add `-v` to also delete the database volume).

## Development and tests

```bash
uv sync                       # install dependencies into .venv
uv run ruff check .           # lint
uv run ruff format --check .  # formatting
uv run mypy                   # type check
uv run pytest                 # tests
```

Integration tests need a running PostgreSQL and a `TEST_DATABASE_URL` environment
variable pointing at a **separate** database whose name ends in `_test` (the tests
wipe its tables and refuse to run otherwise). Without it they are skipped. The
rate-limit tests also need `TEST_REDIS_URL` (use a separate Redis database number,
for example `/15`; the tests flush it) and are skipped without it.

```bash
docker compose up -d db redis
docker compose exec db psql -U <POSTGRES_USER> -d postgres -c "CREATE DATABASE queueforge_test"
```

Tests run against real PostgreSQL, never SQLite, because later milestones rely on
PostgreSQL-specific locking behaviour. Tip: use `127.0.0.1` rather than `localhost`
in database URLs on Windows; `localhost` made the test suite roughly 13x slower here.

## Using the API

Create a client (the key is shown once; only its hash is stored):

```bash
uv run python scripts/create_client.py my-client
```

Submit and read a job:

```bash
curl -X POST localhost:8000/v1/jobs \
  -H "X-API-Key: <your key>" -H "Idempotency-Key: order-1" \
  -H "Content-Type: application/json" \
  -d '{"task_type":"echo","payload":{"message":"hi"}}'

curl localhost:8000/v1/jobs/<id> -H "X-API-Key: <your key>"
```

| Status | Meaning |
|---|---|
| `201` | Job created |
| `200` | Same `Idempotency-Key` and body: the original job is returned |
| `401` | Missing, wrong or disabled API key |
| `404` | Job not found (or not yours) |
| `409` | `Idempotency-Key` reused with a different body |
| `422` | Invalid request (unknown task type, bad payload) |

## Health endpoints

| Endpoint | Meaning | Depends on the database? |
|---|---|---|
| `GET /healthz` | The process is up | No |
| `GET /readyz` | The service can do useful work | Yes; returns `503` if the database is unreachable |

## Limitations

- At-least-once, **not** exactly-once: after a crash or a slow worker, a job can run twice. Only the stored result is protected (by the lease-token fence); side effects must be made safe by the handler.
- Recovery needs at least one worker running (the sweeper runs inside each worker).
- Retry settings are global (env vars), not per task type. Task time limits are set per task type in code.
- Rate limiting fails open: if Redis is down there is no limit (a deliberate trade-off, see [operations](docs/operations.md)). Failed logins are not rate limited.
- No endpoint to create API clients; use the script above. No key rotation or expiry.
- Job payloads are stored unencrypted. No cleanup of old jobs or keys. See [security](docs/security.md).
- The Compose file is for local development only (ports bound to localhost, no TLS, no Redis password).
- Performance figures exist but come from **one low-end laptop** and are not capacity numbers; see [docs/benchmarking.md](docs/benchmarking.md) for what was measured, what was found and fixed, and the limits. Not yet validated in any production-like environment.
- **No fairness between clients:** claiming is oldest-first overall, so one client's large backlog delays everyone else (measured: 116 s vs 0.19 s). A fix is designed but not built.

## License

MIT, see [LICENSE](LICENSE).
