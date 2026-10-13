# Runbooks for the alerts

One section per alert in [deploy/monitoring/alerts.yml](../deploy/monitoring/alerts.yml). Each says what it means, what to check first, and what usually fixes it. The commands assume the Docker Compose setup from the README. See [operations](operations.md) for the full operating guide.

## QueueForgeMetricsMissing

**Means:** Prometheus cannot read `/metrics`. While this fires, no other alert can.

**Check:** `docker compose ps` (is `api` running and healthy?), `curl http://127.0.0.1:8000/readyz` (a `503` means the database is unreachable), `docker compose logs --tail 50 api`. If the API is fine, the scrape key may have been disabled or is wrong (a `401` in the Prometheus targets page).

**Fix:** restart what is down (`docker compose up -d`); fix the database if `readyz` says so; recreate the scrape client key.

## QueueForgeNoWorkers

**Means:** no worker has reported in the last 30 seconds. New jobs queue up and crashed jobs are not recovered.

**Check:** `docker compose ps worker`, `docker compose logs --tail 50 worker`. A worker that cannot reach the database exits with code 3 and is restarted by Compose; repeated restarts point to a database problem.

**Fix:** `docker compose up -d worker` (or `--scale worker=N`). Jobs are safe: they stay queued, and any that were running recover after their lease expires.

## QueueForgeQueueLagHigh

**Means:** the oldest job that is ready to run has waited more than 5 minutes.

**Check:** is `queueforge_workers_alive` above 0? Is one client's large backlog the cause (`queueforge_jobs{status="queued"}`; fair claiming keeps other clients moving but total throughput is still limited)? Are workers busy with slow jobs (`queueforge_oldest_running_job_age_seconds`)? Is the database slow?

**Fix:** add workers (`docker compose up -d --scale worker=N`); find and ask a client submitting a flood to slow down (the per-client queue limit returns `429` to them once it is full); investigate stuck handlers.

## QueueForgeExpiredLeases

**Means:** some running jobs belong to workers that stopped heartbeating, and no sweeper has recovered them for 5 minutes. Normally the sweeper in any live worker does this within seconds of the lease expiring.

**Check:** usually fires together with `QueueForgeNoWorkers`. If workers are alive, look at `docker compose logs worker` for sweeper errors and check the database is not overloaded.

**Fix:** bring workers back. Recovery is automatic once one is alive. Remember QueueForge is at-least-once: recovered jobs run again.

## QueueForgeDeadLetterGrowing

**Means:** jobs used all their attempts and were set aside. They need a person.

**Check:** list them as an admin: `GET /v1/admin/dead-letter`, then `GET /v1/admin/jobs/{id}` for the attempt history and error. Is it one task type, one client, or everything (a broken dependency or a bad deploy)?

**Fix:** fix the cause first, then re-queue with `POST /v1/admin/jobs/{id}/requeue`. Re-queueing without fixing the cause just fills the dead-letter list again.

## QueueForgeRetentionStalled

**Means:** a finished job older than the longest retention period (plus a day) still exists, so the maintenance service is not deleting. The database grows until it is fixed.

**Check:** `docker compose ps maintenance`, `docker compose logs --tail 50 maintenance`. Failing batches are logged with an error.

**Fix:** restart it (`docker compose up -d maintenance`); to catch up manually: `docker compose run --rm maintenance python -m queueforge.maintenance --once`. Use `--dry-run` first to see what would be deleted.

## Not covered by an alert (yet)

These matter but QueueForge exposes no metric for them, so they need your own monitoring:
- **Backups:** a backup that did not run or failed (the backup script exits non-zero; alert on that in your scheduler), and a restore drill that has not been run recently.
- **WAL archiving** (if you use point-in-time recovery): `SELECT failed_count, last_failed_time FROM pg_stat_archiver;` (a failing archive can fill the database disk).
- **Disk space** and **database connections**: use your host monitoring or a PostgreSQL exporter.
- **API error rates and latency:** the API exposes no request metrics; use your reverse proxy's logs or metrics.
