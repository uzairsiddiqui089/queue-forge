# QueueForge

A distributed job processing platform built with Python, FastAPI and PostgreSQL.
Clients submit jobs over HTTP; separate worker processes claim and execute them
with at-least-once semantics. It is a learning and portfolio project that
focuses on correct, explainable backend engineering.

> **Status: Milestone 2 (durable job API).** You can submit jobs and read them
> back; nothing executes them yet. Workers, retries and recovery are planned, not
> implemented. See [docs/project-brief.md](docs/project-brief.md) for the plan and
> [docs/architecture.md](docs/architecture.md) for how it works.

## Implemented

- `POST /v1/jobs` and `GET /v1/jobs/{id}`, with API-key authentication
- Allow-listed task types with validated payloads: `echo`, `sleep`, `flaky`, `simulated_notification`
- Jobs stored in PostgreSQL (Alembic migrations run automatically in the API container)
- Idempotency: the same `Idempotency-Key` + same body returns the original job (24 h)
- Per-client isolation: another client's job returns `404`
- FastAPI app with `GET /healthz` (liveness) and `GET /readyz` (readiness, checks the database)
- Environment-based configuration (no secrets in code)
- Docker Compose stack: API + PostgreSQL 16
- Tests (pytest), linting and formatting (Ruff), strict type checking (mypy)
- GitHub Actions CI running the same checks against a Postgres service container

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose v2
- [uv](https://docs.astral.sh/uv/) (installs Python 3.12 and the dependencies)

## Setup

```bash
cp .env.example .env          # then edit .env: set a real POSTGRES_PASSWORD
                              # (and the same password inside DATABASE_URL)
docker compose up -d --build
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
wipe its tables and refuse to run otherwise). Without it they are skipped.

```bash
docker compose up -d db
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

- Jobs are stored but never executed: no workers, retries or recovery yet.
- No request body size limit or rate limiting yet (planned for Milestone 5).
- No endpoint to create API clients; use the script above.
- The Compose file is for local development only (ports bound to localhost, no TLS).

## License

MIT, see [LICENSE](LICENSE).
