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
| Maintenance | `maintenance` service | 1 (deletes old finished jobs; see "Data retention") |

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
| `ENVIRONMENT` | development | `production` hides `/docs`, `/redoc`, `/openapi.json` and refuses to start with a weak database password (under 12 characters, or a placeholder like `change-me`) |
| `MAX_QUEUED_JOBS_PER_CLIENT`, `MAX_QUEUED_JOBS_TOTAL` | 10000, 100000 | Backpressure limits; over them, submits get `429` / `503`. The per-client cap is exact (advisory lock); the global cap is a soft limit that can be exceeded slightly under simultaneous submits |
| `FAIR_CLAIMING` | true | Workers serve the client with the fewest running jobs first. `false` = oldest job overall first (the earlier behaviour) |
| `FAIR_CLAIM_MAX_CLIENTS` | 64 | How many waiting clients are compared per claim. Higher costs more per claim (about 23 ms at 1,000 clients) |
| `RETENTION_SUCCEEDED_DAYS`, `RETENTION_FAILED_DAYS`, `RETENTION_DEAD_LETTER_DAYS` | 7, 30, 30 | How long finished jobs are kept before the maintenance process deletes them (see "Data retention") |
| `PURGE_BATCH_SIZE`, `PURGE_MAX_BATCHES_PER_CYCLE`, `MAINTENANCE_INTERVAL_SECONDS` | 1000, 50, 60 | Jobs deleted per transaction, the most batches in one cycle (so one cycle deletes at most 50,000 jobs), and the pause between cycles |
| `QUEUE_DEPTH_CACHE_SECONDS` | 1 | How long the "is the whole queue full?" answer is reused. Counting a deep queue on every submit cost about 48 ms at 100,000 queued jobs. 0 = count every time |
| `METRICS_CACHE_SECONDS` | 10 | How long `/metrics` results are reused (two queries scan whole tables; a scrape took 0.7-4.4 s with 1,000,000 jobs). The numbers can be up to this many seconds old. 0 = never |
| `WORKER_SHUTDOWN_DRAIN_SECONDS` | 25 | How long a stopping worker lets its job finish before handing it back. Keep Docker's `stop_grace_period` (40 s) above it |
| `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT_SECONDS` | 10, 10, 10 | Database connection pool per process |
| `DB_STATEMENT_TIMEOUT_SECONDS`, `DB_LOCK_TIMEOUT_SECONDS`, `DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS` | 30, 5, 60 | Server-side limits on every connection |

**Settings are checked at startup.** A bad combination stops the process at once with a message. For example, `WORKER_HEARTBEAT_INTERVAL_SECONDS` must be at most a third of `WORKER_LEASE_SECONDS` (otherwise running jobs would lose their lease), and the per-client queue cap cannot exceed the global one.

**Connection budget.** PostgreSQL allows 100 connections by default. Each API process can use up to `DB_POOL_SIZE + DB_MAX_OVERFLOW` (20), and each worker uses about 3 (claim, heartbeat, handler). So `API processes x 20 + workers x 3` must stay well below `max_connections`. If you need more, raise `max_connections` or put PgBouncer in front.

**Task time limits** are in `src/queueforge/tasks/handlers.py` (`HANDLER_TIMEOUT_SECONDS`, 60 s default for unlisted types). Set each limit above the task's real maximum.

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
| `queueforge_oldest_finished_job_age_seconds` | Age of the oldest finished job still stored. **Alert when it is more than a day above your longest retention period** (default 30 days): the maintenance process has stopped deleting |
| `queueforge_workers_alive` | Worker processes seen in the last 30 s. **Alert when this is 0 for more than a minute**: nothing is running jobs or recovering crashed ones |
| `queueforge_job_attempts_total{outcome="..."}` | Total attempts by result: `succeeded`, `retryable_error`, `permanent_error`, `lease_expired` |

Things worth alerting on: `queueforge_jobs{status="dead_lettered"}` growing; oldest queued age above what your users accept; expired leases above 0 for more than a minute or two.

Ready-made Prometheus alert rules are in [deploy/monitoring/alerts.yml](../deploy/monitoring/alerts.yml) (with an example scrape config), and [runbooks.md](runbooks.md) says what each alert means and what to do. The rules file is checked by a unit test (well formed, only real metrics, every alert has a runbook); it has **not** been run against a live Prometheus here, and the scrape example is untested.

## Data retention (cleanup)

Without cleanup the database only grows. Measured with 1,000,000 finished jobs: every worker's lease sweep scanned the whole table, `/metrics` took seconds, and throughput was 25-35% lower (see [benchmarking.md](benchmarking.md)). The `maintenance` service deletes old data continuously.

**What is deleted**

| Data | When it is deleted |
|---|---|
| `succeeded` jobs | more than `RETENTION_SUCCEEDED_DAYS` (7) after they finished |
| `failed` jobs | more than `RETENTION_FAILED_DAYS` (30) after they finished |
| `dead_lettered` jobs | more than `RETENTION_DEAD_LETTER_DAYS` (30) after they were dead-lettered |
| A deleted job's attempts, idempotency keys and notification rows | with the job, in the same statement |
| Idempotency keys | once their 24-hour window has passed |
| Worker presence rows | not seen for a day |

**What is never deleted:** jobs that are still `queued` or `running`, however old. Clients that look up a deleted job get `404` from then on, so keep retention longer than the longest time any client may look at a result. There is **no archive**: deleted means gone (restoring from a backup is the only way back).

**How it works.** Every `MAINTENANCE_INTERVAL_SECONDS` (60 s) it deletes up to `PURGE_MAX_BATCHES_PER_CYCLE` batches of `PURGE_BATCH_SIZE` jobs, oldest first, each batch in its own short transaction. If a cycle hit its limit it runs again after one second until it has caught up. Rows locked by someone else at that moment (for example an admin re-queueing a dead-lettered job) are skipped and picked up next time, and several purgers can run together safely. An unexpected error makes it back off and retry rather than die; the container has a healthcheck.

**Look before you delete** (changes nothing):

```bash
docker compose run --rm maintenance python -m queueforge.maintenance --dry-run
```

**Run one purge by hand** (until nothing eligible is left, then exit):

```bash
docker compose run --rm maintenance python -m queueforge.maintenance --once
```

**Disk space.** Deleted rows leave space that PostgreSQL reuses; the files do not shrink on their own. Autovacuum (default settings) cleans up the dead rows. After a very large one-off purge you can reclaim disk space with `VACUUM FULL jobs, job_attempts`, but it **locks the tables while it runs**, so do that in a maintenance window.

**Changing retention.** Lowering a period makes the next cycles delete everything now older than it, which can be a lot: run `--dry-run` first.

## Backups and restore

PostgreSQL holds everything, so losing it loses everything. The scripts in `scripts/backup/` take consistent backups, restore them safely, and **prove that a backup works** by restoring it into a throwaway database and checking it.

**What a backup is.** A full copy of the database taken with `pg_dump` (one consistent snapshot; it does not block the API or the workers). It contains job payloads and results, attempt history, idempotency keys and the *hashes* of API keys (never the keys themselves). That is sensitive data: backup files are created readable by their owner only, and can be encrypted (below).

**Taking backups**

```bash
docker compose --profile tools run --rm dbtools backup
```

This writes `queueforge-<database>-<UTC time>.dump`, a `.sha256` checksum and a `.manifest` (schema version and row counts) to the `backups` volume, checks the dump can be read back, and deletes all but the newest `BACKUP_KEEP` (7). A failure leaves no half-written backup and exits non-zero, so a scheduler can alert on it. To list them: `docker compose --profile tools run --rm dbtools list`.

- **Schedule it yourself.** Compose has no scheduler. For example, nightly at 02:00 with cron: `0 2 * * * cd /path/to/queue-forge && docker compose --profile tools run --rm -T dbtools backup`.
- **Keep copies somewhere else.** The default `backups` volume sits on the same disk as the database, so it does *not* survive losing the machine. Mount a path on another machine or disk instead, or copy the files off after each run.
- **Encryption (optional).** Set `BACKUP_PASSPHRASE` and the backup file is encrypted (AES-256 via openssl) and the plain copy is never left behind. Store the passphrase separately and safely: **without it an encrypted backup cannot be restored**, and nobody can recover it for you.

**What you can recover to (read this).** (With the optional point-in-time recovery below this limit is much smaller.) A backup is a snapshot: restoring it brings back the database **as it was when the backup was taken**, and everything after that is lost. With a nightly backup you can lose up to 24 hours of jobs. The 5-minute recovery target set when the roadmap was planned is **not met** by this; reaching it needs *point-in-time recovery* (continuous WAL archiving, or a managed PostgreSQL service that provides it), which is **not built here**.

**How long a restore takes (RTO).** Measured on the benchmark laptop with 1,000,000 jobs and as many attempts: see section 3.10 of [benchmarking.md](benchmarking.md). Your hardware will differ; measure your own with the drill below.

**Prove your backups work (do this on a schedule, for example weekly)**

```bash
docker compose --profile tools run --rm dbtools drill            # back up now, then verify that backup
docker compose --profile tools run --rm dbtools verify /backups/<file>   # verify an existing backup
```

`verify` restores the backup into a throwaway database and checks: the checksum, that the restore succeeds as a whole, that the schema version matches the manifest, that every table's row count lies between the counts recorded just before and just after the dump, and that every constraint is valid. It prints `VERIFY OK` and exits 0, or says what failed and exits 1. It then deletes the throwaway database.

**Disaster recovery: restoring from a backup**

1. **Stop everything that writes:** `docker compose stop api worker maintenance`.
2. **Choose the backup:** the newest one that verifies (`dbtools list`, then `dbtools verify <file>`).
3. **Restore into a NEW database** (it will not overwrite anything): `docker compose --profile tools run --rm dbtools restore /backups/<file> --into queueforge_recovered`. Add `-e BACKUP_PASSPHRASE=...` if encrypted.
4. **Check it** (`dbtools verify <file>` has already done the automated checks; look at a few rows if you like).
5. **Switch over.** Either point the services at the new database (change `POSTGRES_DB`/`DATABASE_URL`), or replace the original in place. The in-place route deletes the target database, so it needs an explicit confirmation and refuses while anything is still connected: `docker compose --profile tools run --rm -e CONFIRM_REPLACE=yes-replace-the-database dbtools restore /backups/<file> --into <name> --replace`.
6. **Bring the system back:** `docker compose up -d` (the `migrate` step upgrades the schema if the backup is older than the code). Check `/readyz`, `queueforge_workers_alive` and the queue metrics.

**What a restore means for your data, in plain terms**
- **Jobs that finished after the backup will run again.** In the restored data they are still queued or running. QueueForge is at-least-once, so task handlers must tolerate this (that is already a requirement).
- **Jobs submitted after the backup are gone, and so are their idempotency keys.** A client retrying such a submit creates a new job; if the original was lost this is what you want, but a duplicate is possible if the client had already seen it succeed elsewhere.
- **Jobs that were `running` at backup time** have stale leases; the workers' sweeper recovers them once the lease time has passed.
- **API clients created after the backup are gone.** Their keys stop working; create them again.

**Limits.** Backups restore the whole database or nothing (there is no restore of a single job). The default location is on the same host. Backups taken while the system is busy contain whatever was committed at one instant, which is consistent but may include jobs mid-flight (see above). Nothing here backs up Redis (it holds only rate-limit counters, which do not matter after a restore) or your `.env` file (keep your secrets in your own secret store).

## Point-in-time recovery (optional)

The plain backups above are snapshots: a restore loses everything since the last backup. **Point-in-time recovery (PITR)** closes that gap. PostgreSQL copies every finished write-ahead-log (WAL) file to an archive, and a *base backup* plus that archive can be restored to **any moment** after the base backup, for example "one minute before the bad deploy".

**Turn it on** (restarts the database once):

```bash
docker compose -f docker-compose.yml -f docker-compose.pitr.yml up -d
```

This sets `archive_mode=on` and copies each WAL segment to the `wal_archive` volume. A segment is closed at least every 60 s (`archive_timeout`), so after a disaster you lose **at most about a minute of recent data plus any WAL that had not been copied yet**. That is the recovery-point figure to plan with; it is not a guarantee.

**Take base backups** (for example daily; keep `BASE_KEEP`, default 3, newest). On Windows with Git Bash, put `MSYS_NO_PATHCONV=1` in front of the command so `/pitr/...` is not rewritten.

```bash
docker compose -f docker-compose.yml -f docker-compose.pitr.yml exec -T -u postgres db bash /pitr/basebackup.sh
```

It also deletes archived WAL that no kept base backup can need; without that the archive would grow forever.

**Watch it.** If copying WAL fails (disk full, archive volume gone), PostgreSQL keeps the WAL in its own `pg_wal` folder and **that can fill the database disk**. Alert on `SELECT failed_count, last_failed_time FROM pg_stat_archiver;` and on the archive volume's free space.

**Recover to a moment**

1. Stop everything that writes: `docker compose stop api worker maintenance db`.
2. Pick the time (UTC). Choose a time *after* the base backup you will use finished and *before* the damage.
3. Restore the base backup and WAL settings into a **new, empty** volume. The script refuses a non-empty target, so nothing is overwritten:

```bash
docker volume create pgdata_recovered
docker run --rm -u root -v pgdata_recovered:/restore \
  -v <project>_backups:/backups:ro -v <project>_wal_archive:/wal_archive:ro \
  -v "$PWD/scripts/pitr":/pitr:ro postgres:16 \
  bash /pitr/restore.sh /backups/base/<base-backup-name> /restore --to "2026-10-13 12:00:00+00"
```

   Use `--latest` instead of `--to` to replay all archived WAL.
4. Start PostgreSQL on that volume (the drill below does exactly this). PostgreSQL replays WAL up to the target, then opens for writes.
5. Point the `db` service at the recovered volume (for example with a `docker-compose.override.yml` that declares the `pgdata` volume as `external` with the recovered volume's name), start the stack, and check `/readyz` and the metrics. **This last switch-over step is described but was not rehearsed**; the drill rehearses steps 3 and 4.

**Prove it works (run it before you need it)**

```bash
bash scripts/pitr/drill.sh
```

It builds a throwaway database with archiving, takes two base backups (checking the old one is pruned), adds rows, notes the time T, **deletes every row**, then restores to T and requires exactly the rows that existed at T. As a control it restores to the latest point and requires the damage (0 rows), so a restore that ignored the target time would fail. It prints `PITR DRILL OK` or says what failed, and removes its containers and volumes.

**Limits (read these)**
- Measured only on a tiny database: the base backup was about 47 MB and took 1 to 2 s, the whole drill about a minute. **Restore time for a large database, and how long replaying a busy day of WAL takes, were not measured.**
- By default the archive and base backups sit on the **same disk as the database**, so they do not survive losing the machine. Point the `wal_archive` and `backups` volumes at another disk or machine, or copy them off.
- Base backups and the WAL archive are **not encrypted** (the `BACKUP_PASSPHRASE` encryption applies to the `pg_dump` backups only).
- The restore must use the same PostgreSQL major version (16).
- The logical `pg_dump` backups remain useful: they restore a single database anywhere and are what `verify` checks.

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
| `queueforge_workers_alive` is 0 | No worker is running | `docker compose ps worker`; the container healthcheck reads a file the worker touches every few seconds |
| Attempts end as `HandlerTimeout` | A task ran past its time limit | Check what the task is waiting on; raise its limit only if the task is legitimately slow |
| Worker logs `too many stuck handlers; worker will exit` | Several handlers hung and could not be stopped | The worker exits (code 3) and Docker restarts it. Find out why handlers hang |
| Attempt outcome `released` | A worker was shut down mid-job and handed the job back | Normal during deploys; it does not use up an attempt |
| API returns `503` with `Retry-After` | Database unreachable, or all connections busy | `/readyz`; DB health; pool settings and the connection budget |
| `POST /v1/jobs` returns `429` "Too many queued jobs" | That client has too many waiting jobs | Add workers or slow the client; `Retry-After` says when to try again |
| `POST /v1/jobs` returns `503` "Queue is full" | The whole queue is at its limit | Add workers; check for stuck or missing workers first |
| `queueforge_oldest_finished_job_age_seconds` keeps growing | The maintenance process stopped (or cannot keep up) | `docker compose ps maintenance`; its logs ("purge cycle" lines, "purge cycle failed"); `--dry-run` shows how much is waiting. If it cannot keep up, raise `PURGE_MAX_BATCHES_PER_CYCLE` |
| `dbtools verify` or `restore` says "checksum mismatch" | The backup file was damaged or changed after it was written | Do not use it; verify an older one. This is why backups are verified regularly and copied elsewhere |
| `dbtools restore` says "is the live database" or "already exists" | Restore will not overwrite anything by default | Restore into a new name with `--into`, or use `--replace` with the confirmation (see "Disaster recovery") |
| A backup run exits non-zero | The dump failed (database unreachable, disk full, ...) | The message says why; no partial backup is left. Alert on a failed or missing backup |
| A client gets `404` for an old job | The job passed its retention period and was deleted | Raise the `RETENTION_*_DAYS` setting; deleted jobs cannot be recovered except from a backup |
| Worker logs "worker loop error; backing off" | An unexpected error (often the database) | The worker keeps retrying with growing waits; a "worker recovered" line follows when it works again |
| Job is `dead_lettered` | It failed 5 times (or its worker died 5 times) | Admin job detail shows each attempt; fix the cause, then `requeue` |
| Job `failed` with `TaskError` | A permanent error: the input can never work | Fix the input and submit a new job |
| API returns `429` | Over the rate limit | Wait `Retry-After` seconds, or raise `RATE_LIMIT_PER_MINUTE` |
| API returns `413` | Body over `MAX_REQUEST_BODY_BYTES` | Send less data |
| API returns `401` | Missing, wrong or disabled key | Check `X-API-Key`; create a new client |
| `/readyz` returns `503` | API cannot reach PostgreSQL | `docker compose ps db`; check `DATABASE_URL` |
| "rate limiter unavailable" warnings | Redis is down or unreachable | `docker compose ps redis`. The API keeps working without a limit |
| Tests are very slow on Windows | `localhost` resolution | Use `127.0.0.1` in database URLs |

## Known operational limits

- Backups are snapshots: recovery loses everything since the last backup, and there is no point-in-time recovery (see "Backups and restore"). They are stored beside the database unless you mount another location.
- Single PostgreSQL instance: it is the queue and the only source of truth. Throughput is limited by it (Milestone 6 will measure this).
- Cleanup deletes data for good (no archive), and does not shrink the database files on its own (see "Data retention").
- The sweeper runs inside workers: no workers means no recovery (watch `queueforge_workers_alive`).
- Fair claiming compares at most `FAIR_CLAIM_MAX_CLIENTS` (64) waiting clients per claim; with more, fairness is approximate (the 64 lowest client ids are compared). It balances the NUMBER of running jobs per client, not run time. It costs a few percent of worker throughput for do-nothing jobs (see `docs/benchmarking.md`).
- Performance figures come from a single low-end laptop (see `docs/benchmarking.md`); size the queue caps as throughput x the longest wait you accept, because the defaults allow hours of waiting at low throughput.
- Local Compose setup has no TLS and no Redis password. Do not expose it to a network as is.
