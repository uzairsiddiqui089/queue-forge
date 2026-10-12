# Operations guide

How to run QueueForge, see what it is doing, and fix it when something goes wrong.

## Components you run

| Program | Command | How many |
|---|---|---|
| PostgreSQL | `db` service | 1 (holds all job data) |
| Migrations | `migrate` service | runs once at start, then exits |
| API | `api` service | 1 or more (stateless) |
| Worker | `worker` service | 1 or more; `docker compose up -d --scale worker=4` |
| Redis | `redis` service | 1 (rate limiting only; optional) |

## Configuration

All settings are environment variables. Copy `.env.example` to `.env` and edit it. Never commit `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | none (required) | Database credentials for Docker Compose |
| `DATABASE_URL` | none (required) | Connection string for the API and workers |
| `REDIS_URL` | unset | Enables rate limiting. If unset, there is no rate limit |
| `RATE_LIMIT_PER_MINUTE` | 120 | Requests per API key per minute |
| `MAX_REQUEST_BODY_BYTES` | 65536 | Larger request bodies get `413` |
| `LOG_LEVEL` | INFO | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `WORKER_LEASE_SECONDS` | 30 | How long a worker owns a job without a sign of life |
| `WORKER_HEARTBEAT_INTERVAL_SECONDS` | 10 | How often a working worker renews its lease (keep well below the lease) |
| `WORKER_SWEEP_INTERVAL_SECONDS` | 10 | How often each worker looks for abandoned jobs |
| `WORKER_POLL_INTERVAL_SECONDS` | 1 | How often an idle worker looks for work |
| `RETRY_BASE_DELAY_SECONDS`, `RETRY_MAX_DELAY_SECONDS` | 2, 300 | Retry backoff (see below) |

## Health checks

| Endpoint | Meaning |
|---|---|
| `GET /healthz` | The API process is up. Does not touch the database. Use for "restart if dead" checks |
| `GET /readyz` | The API can reach PostgreSQL. `503` if not. Use for "send traffic here" checks |

`/readyz` does **not** depend on Redis, on purpose (see rate limiting).

## Logs

Every API and worker log line is one JSON object on stdout (`docker compose logs api worker`).

```json
{"ts": "...", "level": "INFO", "logger": "queueforge.worker", "message": "job claimed",
 "job_id": "...", "task_type": "echo", "attempt": 1, "max_attempts": 5, "worker_id": "..."}
```

- Only a fixed list of fields can appear: `job_id`, `task_type`, `client_id`, `worker_id`, `attempt`, `max_attempts`, `status`, `new_status`, `method`, `path`, `status_code`, `duration_ms`, `recovered`, `limit`. Anything else is dropped. Payloads and API keys are never logged (this is tested).
- The request log records the **path only**, not the query string or headers.
- uvicorn's own access log is switched off (`--no-access-log`), because it prints full URLs including query strings.
- Stack traces appear in an `exception` field for unexpected errors. Exception text can contain anything, so restrict who can read the logs.

Useful messages: `job claimed`, `job recorded` (with `new_status`), `lease lost`, `recovered jobs with expired leases`, `unexpected error in handler`, `unhandled error`, `rate limiter unavailable, allowing request`.

## Metrics

`GET /metrics` (Prometheus text format). It needs an **admin** API key in the `X-API-Key` header. Numbers come straight from PostgreSQL when scraped, so they stay correct across restarts and cover all workers.

| Metric | What it tells you |
|---|---|
| `queueforge_jobs{status="..."}` | How many jobs are in each state |
| `queueforge_oldest_queued_job_age_seconds` | How long the longest-waiting ready job has waited. **Rising = not enough workers, or workers are down** |
| `queueforge_oldest_running_job_age_seconds` | Longest current attempt. Very large = a stuck handler |
| `queueforge_expired_leases` | Jobs whose worker stopped renewing and are not yet recovered. **More than 0 for long = no sweeper running (all workers down)** |
| `queueforge_job_attempts_total{outcome="..."}` | Total attempts by result: `succeeded`, `retryable_error`, `permanent_error`, `lease_expired` |

Things worth alerting on: `queueforge_jobs{status="dead_lettered"}` growing; oldest queued age above what your users accept; expired leases above 0 for more than a minute or two.

## Admin tasks

Create a client or admin (the key is shown once):

```bash
uv run python scripts/create_client.py my-service
uv run python scripts/create_client.py ops --admin
```

Look at jobs that ran out of attempts, and why:

```bash
curl localhost:8000/v1/admin/dead-letter -H "X-API-Key: <admin key>"
curl localhost:8000/v1/admin/jobs/<id>   -H "X-API-Key: <admin key>"   # includes attempt_history
```

Put one back in the queue after you fixed the cause:

```bash
curl -X POST localhost:8000/v1/admin/jobs/<id>/requeue -H "X-API-Key: <admin key>"
```

A re-queued job gets 5 more attempts. Its history is kept: `attempts` is not reset, `max_attempts` is raised instead.

## Retries and recovery (summary)

- Temporary failures are retried up to 5 attempts, waiting a random time up to `min(300 s, 2 s x 2^(attempt-1))` between tries ("full jitter"). Permanent failures stop at once. After the last attempt the job is `dead_lettered`.
- If a worker dies, its job's lease expires (30 s by default) and a sweeper in any running worker re-queues it, or dead-letters it if attempts are used up. **At least one worker must be running for recovery to happen.**
- Jobs can run more than once (at-least-once). Task handlers must be safe to repeat. See [architecture.md](architecture.md).

## Rate limiting and Redis

Each API key gets `RATE_LIMIT_PER_MINUTE` requests per calendar minute. Over the limit: `429` with `Retry-After`. Responses carry `X-RateLimit-Limit` and `X-RateLimit-Remaining`.

- **Redis holds only short-lived counters.** Jobs, results and idempotency keys are never in Redis. Losing Redis loses nothing important.
- **If Redis is down or slow, requests are allowed** (it "fails open") and a warning is logged. The limit protects the service; it is not needed for correctness, and a Redis outage should not take the API down. The cost: during an outage there is no rate limit.
- Fixed windows are simple but allow up to double the limit around a minute boundary.
- Failed logins are not rate limited per key (there is no key to count against). They are cheap to reject, but a flood of bad requests still reaches the database.

## Troubleshooting

| Symptom | Likely cause | What to check |
|---|---|---|
| Jobs stay `queued` | No worker running, or all busy | `docker compose ps`; `queueforge_oldest_queued_job_age_seconds`; start workers with `--scale worker=N` |
| A job is `queued` but not starting | It is waiting out a retry delay (`run_at` in the future) | `GET /v1/admin/jobs/<id>`; `last_error` shows the last failure |
| A job is stuck `running` | Its worker died; waiting for the lease to expire | Wait up to the lease time. If it never recovers, no worker is running a sweeper |
| `queueforge_expired_leases` stays above 0 | All workers are down | Start a worker |
| Job is `dead_lettered` | It failed 5 times (or its worker died 5 times) | Admin job detail shows each attempt; fix the cause, then `requeue` |
| Job `failed` with `TaskError` | A permanent error: the input can never work | Fix the input and submit a new job |
| API returns `429` | Over the rate limit | Wait `Retry-After` seconds, or raise `RATE_LIMIT_PER_MINUTE` |
| API returns `413` | Body over `MAX_REQUEST_BODY_BYTES` | Send less data |
| API returns `401` | Missing, wrong or disabled key | Check `X-API-Key`; create a new client |
| `/readyz` returns `503` | API cannot reach PostgreSQL | `docker compose ps db`; check `DATABASE_URL` |
| "rate limiter unavailable" warnings | Redis is down or unreachable | `docker compose ps redis`. The API keeps working without a limit |
| Tests are very slow on Windows | `localhost` resolution | Use `127.0.0.1` in database URLs |

## Known operational limits

- Single PostgreSQL instance: it is the queue and the only source of truth. Throughput is limited by it (Milestone 6 will measure this).
- No automatic cleanup of old jobs, attempts or expired idempotency keys.
- The sweeper runs inside workers: no workers means no recovery.
- Local Compose setup has no TLS and no Redis password. Do not expose it to a network as is.
