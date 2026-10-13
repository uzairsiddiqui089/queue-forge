# Demo walkthrough

A five-minute tour that shows the main behaviours on your own machine. Every command below was run against this repository; the outputs shown are shortened, and the ids and keys are different every time.

You need Docker and [uv](https://docs.astral.sh/uv/). Run everything from the repository root.

## 1. Start the system

```bash
cp .env.example .env     # then set a real POSTGRES_PASSWORD (and the same one inside DATABASE_URL)
docker compose up -d --build
curl http://127.0.0.1:8000/readyz      # {"status":"ready"}
```

## 2. Create two clients (one normal, one admin)

```bash
uv sync
uv run python scripts/create_client.py alice
uv run python scripts/create_client.py ops --admin
```

Each command prints an API key **once**. Only its hash is stored. In the commands below, replace `ALICE_KEY` and `OPS_KEY`.

## 3. Submit a job and watch it finish

```bash
curl -s -X POST http://127.0.0.1:8000/v1/jobs \
  -H "X-API-Key: ALICE_KEY" -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" \
  -d '{"task_type":"echo","payload":{"message":"hello"}}'
```

You get `201` and a job id with status `queued`. Read it a moment later:

```bash
curl -s http://127.0.0.1:8000/v1/jobs/JOB_ID -H "X-API-Key: ALICE_KEY"
```

The status is now `succeeded`, with a result. A worker (a separate process) did the work; the API never runs jobs.

## 4. Idempotency: sending the same request twice

Run the same `POST` from step 3 again (same `Idempotency-Key`, same body). You get `200` and the **same job id**, not a second job. Change the body but keep the key and you get `409`.

## 5. Retries: a job that fails twice, then succeeds

```bash
curl -s -X POST http://127.0.0.1:8000/v1/jobs \
  -H "X-API-Key: ALICE_KEY" -H "Content-Type: application/json" \
  -d '{"task_type":"flaky","payload":{"fail_times":2}}'
```

Poll the job. It goes back to `queued` after each failure (waiting a growing, random delay) and ends `succeeded` on the third attempt. Admins can see each attempt:

```bash
curl -s http://127.0.0.1:8000/v1/admin/jobs/JOB_ID -H "X-API-Key: OPS_KEY"
```

## 6. Dead-lettering and re-queue

A flaky job that fails more times than the attempt limit (5) ends as `dead_lettered`:

```bash
curl -s -X POST http://127.0.0.1:8000/v1/jobs \
  -H "X-API-Key: ALICE_KEY" -H "Content-Type: application/json" \
  -d '{"task_type":"flaky","payload":{"fail_times":10}}'
curl -s http://127.0.0.1:8000/v1/admin/dead-letter -H "X-API-Key: OPS_KEY"
curl -s -X POST http://127.0.0.1:8000/v1/admin/jobs/JOB_ID/requeue -H "X-API-Key: OPS_KEY"
```

This takes a minute or two because of the backoff delays. After the re-queue the job gets more attempts and its history is kept.

## 7. Crash recovery

Submit a 20-second sleep job, then kill the worker's container while it runs:

```bash
curl -s -X POST http://127.0.0.1:8000/v1/jobs \
  -H "X-API-Key: ALICE_KEY" -H "Content-Type: application/json" \
  -d '{"task_type":"sleep","payload":{"seconds":20}}'
docker compose kill worker
docker compose up -d worker
```

When the dead worker's lease runs out (30 s by default), the sweeper in the new worker puts the job back in the queue and it runs again. The attempt history shows a `lease_expired` attempt followed by a new one. This is why QueueForge is **at-least-once**: a job can run more than once, so handlers must be safe to repeat.

## 8. Metrics

```bash
curl -s http://127.0.0.1:8000/metrics -H "X-API-Key: OPS_KEY" | grep queueforge_
```

Shows jobs per state, the age of the oldest queued job, live workers and more.

## 9. Clean up

```bash
docker compose down        # add -v to also delete the database
```
