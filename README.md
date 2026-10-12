# QueueForge

A distributed job processing platform built with Python, FastAPI and PostgreSQL.
Clients submit jobs over HTTP; separate worker processes claim and execute them
with at-least-once semantics. It is a learning and portfolio project that
focuses on correct, explainable backend engineering.

> **Status: Milestone 1 (repository foundation).** Only the project skeleton and
> health endpoints exist so far. Job submission, workers, retries and recovery are
> planned, not implemented. See [docs/project-brief.md](docs/project-brief.md)
> for the full plan.

## Implemented

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

Integration tests need a running PostgreSQL and a `DATABASE_URL` environment
variable (the Compose `db` service works: `docker compose up -d db`). Without it
they are skipped. Tests run against real PostgreSQL, never SQLite, because later
milestones rely on PostgreSQL-specific locking behaviour.

## Health endpoints

| Endpoint | Meaning | Depends on the database? |
|---|---|---|
| `GET /healthz` | The process is up | No |
| `GET /readyz` | The service can do useful work | Yes; returns `503` if the database is unreachable |

## Limitations

- No job API, workers, retries or authentication yet.
- The Compose file is for local development only (ports bound to localhost, no TLS).

## License

MIT, see [LICENSE](LICENSE).
