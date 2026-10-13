# Architecture

> Status: describes what exists after Milestone 5 (security and observability).

## Components

- **FastAPI API** (stateless): checks the API key, validates the request, saves the job, serves job status. It never runs jobs.
- **PostgreSQL**: the only place job data lives. All times come from the database clock (`now()`).
- **Workers**: separate processes (`python -m queueforge.worker`) that claim jobs, run an allow-listed handler and save the result. Run as many as you like.
- **Redis**: per-API-key rate limiting only (short-lived counters). It never stores jobs, results or idempotency keys, and the API keeps working if it is down.

## Job states

`queued` → `running` → `succeeded` / `failed` / `dead_lettered`. A running job can also go back to `queued` for a retry. `succeeded`, `failed` and `dead_lettered` are final.

The rules live in `src/queueforge/domain/states.py`. Workers will apply every change as `UPDATE ... WHERE status = <expected>`, so an out-of-date or illegal change updates 0 rows instead of corrupting data.

## Tables

| Table | Purpose |
|---|---|
| `api_clients` | Who may call the API. Stores only a SHA-256 hash of each key. |
| `jobs` | One row per job: type, payload, status, attempts, schedule (`run_at`), lease fields (used from Milestone 3), result and error. |
| `job_attempts` | One row per execution try: which worker, how long, and how it ended. |
| `notifications` | Side effect of the demo task; one row per job at most. |
| `workers` | One row per worker process, refreshed every few seconds, so you can see which are alive. |
| `idempotency_keys` | Remembers `(client, key) → job` for 24 hours so a repeated submit does not create a second job. |



## Submitting a job: what happens, in order

1. Check the API key (`401` if missing, wrong or disabled).
2. Validate the body against the allow-list of task types (`422` if invalid). Nothing sent by a client is ever executed as code.
3. Open one database transaction:
   - If an `Idempotency-Key` was sent and a live record exists: same body returns the original job (`200`); a different body is `409`.
   - Insert the `jobs` row (status `queued`).
   - Insert the `idempotency_keys` row.
4. **Commit.** Only after the commit succeeds does the API answer `201`.

So there are two separate facts: the API *accepted* the request, and the job is *persisted*. The client is told "created" only once it is persisted. If the server dies before the commit, no job exists and the client never got an ID, so retrying is safe. If it dies after the commit but before the reply, the job exists but the client didn't hear back; retrying with the same `Idempotency-Key` returns that job instead of creating another.

## Duplicate submissions at the same moment

If two identical requests arrive together, both try to insert the same `(client_id, key)` primary key. The database lets exactly one succeed. The loser's transaction is rolled back (its job disappears too), and it then reads and returns the winner's job. This is tested with 8 threads sending the same key at once.

## Reading a job

`GET /v1/jobs/{id}` filters by the caller's `client_id`. A job that belongs to another client gets `404`, the same answer as a job that does not exist, so job IDs cannot be probed.

## Guarantees and limits (so far)

- A job returned with `201` is durably stored.
- Duplicate submissions are suppressed only when the client sends an `Idempotency-Key`, and only for 24 hours.
- Nothing executes jobs yet, so every job stays `queued`.
- The system will be **at-least-once**, never exactly-once, once workers exist.

## How a worker runs a job (Milestone 3)

1. **Claim** (one short transaction, committed before any work starts):
   - Pick the oldest runnable job: `status = 'queued' AND run_at <= now()`, using `FOR UPDATE SKIP LOCKED`.
   - Mark it `running`, add 1 to `attempts`, set a fresh random `lease_token`, the worker's name and a lease expiry, and insert a `job_attempts` row.
2. **Execute** the handler. No database transaction is open while it runs.
3. **Record** the result with a *fenced* update: it only changes the job if the job is still `running` **and** still has this worker's `lease_token`. Otherwise it changes nothing and the worker throws its result away.

### Why two workers cannot take the same job

`SKIP LOCKED` means: if another worker has already locked a row, skip it and look at the next one, instead of waiting or taking it too. The row lock is held until the claim commits, so by the time anyone else can see the job it is already `running`. This is tested with 4 workers and 200 jobs (each job ends with exactly one attempt row), and was also run with 4 real worker containers.

### What if a worker crashes?

See "Failure handling and recovery" below for the full story. In short: the job is left `running`, its lease runs out, and a sweeper puts it back in the queue (or dead-letters it if it has used all its attempts).

The attempt counter goes up at *claim* time, not at the end. So a job that crashes its worker every time still uses up its attempts and ends up dead-lettered instead of looping forever.

### Handlers

Only four task types exist (`echo`, `sleep`, `flaky`, `simulated_notification`). The worker re-validates the stored payload before running it. `simulated_notification` shows how to stay correct when run twice: it writes to a table whose primary key is the job ID, so a second run inserts nothing.

### What gets stored when a job fails

- Errors we raise on purpose (`TaskError`) store their message.
- Any other exception stores only a generic message. The real message and stack trace go to the worker log, because exception text can contain secrets.
- Whether a failure is retried depends on its kind (next section).

## Failure handling and recovery (Milestone 4)

### Three kinds of failure

| What went wrong | Kind | What happens |
|---|---|---|
| The handler raises `TaskError` | Permanent: retrying cannot help | Job becomes `failed` at once |
| The handler raises `RetryableError`, or any unknown exception | Temporary | Retried after a delay, up to the attempt limit |
| The worker disappears (crash, kill, power cut) | Temporary | Recovered by the sweeper when its lease expires |

Unknown exceptions count as temporary on purpose. It is safe because retries are limited: a buggy job cannot loop forever, it just ends up dead-lettered.

### Retries and backoff

- Every job gets **5 attempts** in total (`max_attempts`). The first try is attempt 1.
- After a temporary failure the job goes back to `queued` with `run_at` set a little in the future. Workers ignore it until then.
- **Exponential backoff:** the longest possible wait doubles each time (2 s, 4 s, 8 s, 16 s, ... capped at 5 minutes).
- **Full jitter:** the actual wait is a random time between 0 and that limit. Without the randomness, many jobs that failed together would all retry at the same moment and overload the system again.
- When attempts run out the job becomes `dead_lettered`, a final state kept for inspection.

### Leases, heartbeats and the sweeper

- When a worker claims a job it gets a **lease**: a deadline (30 s by default) by which it must show it is still alive.
- While the handler runs, a **heartbeat** thread pushes the deadline forward every 10 s. A long job therefore keeps its lease, while a dead worker stops renewing it.
- Each worker also runs a **sweeper** every 10 s: it finds `running` jobs whose lease deadline has passed, marks that attempt `lease_expired`, and re-queues the job (or dead-letters it if attempts are used up). `SKIP LOCKED` makes sure each expired job is handled by exactly one sweeper, even with many workers.
- A job cannot stay `running` forever, because each way it can get stuck has an answer:
  - **The worker dies:** the lease expires and a sweeper recovers the job. (This needs at least one worker running.)
  - **The handler hangs but the worker lives:** the heartbeat alone would renew the lease forever, so the worker also enforces a **time limit** per task type (`HANDLER_TIMEOUT_SECONDS`). When it passes, the attempt is recorded as a retryable `HandlerTimeout` and the worker moves on. Python cannot kill a thread, so the stuck thread is abandoned; if 3 pile up, the worker exits and its supervisor restarts it with a clean process.
  - **The worker is told to stop:** see "Shutdown" below.

  *(An earlier version of this document claimed this without the time limit. That was wrong for hung handlers: the heartbeat kept renewing the lease. It was found in the production-readiness review and fixed in Phase A.)*

### Shutdown (SIGTERM)

A stopping worker claims nothing new. It lets its current job run for up to `WORKER_SHUTDOWN_DRAIN_SECONDS` (25 s). If the job finishes, the result is recorded as usual. If not, the worker **hands the job back**: the job returns to `queued`, the attempt is recorded as `released`, and `max_attempts` goes up by one so the interrupted try is not charged. Then the worker exits cleanly. Docker's `stop_grace_period` (40 s) is longer than the drain so Docker does not kill it first. Without this, every deploy or scale-down would leave jobs `running` until their lease expired, and cost them an attempt.

### Worker supervision

- Any unexpected error in the worker loop is caught. The worker backs off (growing, jittered wait, capped at 30 s) and carries on. The first failure logs a full stack trace; repeated failures log a short line at 2, 4, 8, 16... failures, so an outage does not flood the logs. A "worker recovered" line is logged when it works again.
- Containers use `restart: unless-stopped`, and workers have a healthcheck (a file touched every few seconds while the loop is alive). Each worker also records itself in the `workers` table, which feeds `queueforge_workers_alive`.

### Backpressure

The API refuses new jobs when the queue is too long: `429` if one client has too many waiting (`MAX_QUEUED_JOBS_PER_CLIENT`), `503` if the whole queue is full (`MAX_QUEUED_JOBS_TOTAL`), both with `Retry-After`. A replay of an earlier idempotent submit is never refused. The count stops at the limit, but it still costs time as the queue deepens (measured: about 48 ms per check at 100,000 queued jobs), so the global check runs **first, without the lock**, and its answer is reused for `QUEUE_DEPTH_CACHE_SECONDS` (1 s). Counting alone is not safe when requests arrive at the same moment (two requests can both see "one below the cap" and both insert), so the **per-client cap is enforced under a PostgreSQL advisory lock** keyed on the client: that client's check-and-insert steps run one at a time, the lock is released automatically at commit or rollback, and other clients are not slowed. This is tested with 24 simultaneous submits against a cap of 5 (exactly 5 accepted). The **global cap is a soft limit**: it is not locked, so simultaneous submits from different clients can exceed it slightly. An exact global cap would serialize every submit, so that trade-off is left for the benchmark phase to measure. 

### Fair claiming

Claiming used to be oldest-first across all clients, so one client's large backlog made every other client wait behind it (measured: a small client waited about 120 s instead of 0.2 s). Now a worker first picks the **client** with the fewest jobs currently running, then takes that client's oldest runnable job. Ties go to the client whose oldest job has waited longest.

How it works, in order:
1. List the clients that have runnable queued jobs by jumping from one `client_id` to the next in the index (one cheap lookup per client), looking at no more than `FAIR_CLAIM_MAX_CLIENTS` (64) of them.
2. Count each client's running jobs (only a handful of rows are ever in the `running` state).
3. Choose the client with the fewest running jobs.
4. Lock and claim that client's oldest runnable job with `FOR UPDATE SKIP LOCKED`, as before. Everything after this point (lease token, attempt row, fenced writes) is unchanged.

A worker is never left idle while work exists: if the chosen client's jobs are all being claimed by other workers at that moment, it falls back to the oldest runnable job overall.

Limits, stated plainly: fairness is *approximate* when more than 64 clients are waiting at once (only 64 are compared per claim); it balances the number of running jobs, not run time, so a client with long jobs still holds workers longer; and it adds a small cost per claim (about 0.2 ms with a few clients, about 1.2 ms with hundreds). Set `FAIR_CLAIMING=false` to go back to oldest-first. Measurements are in [benchmarking.md](benchmarking.md).

### Data lifecycle (retention)

Finished jobs are kept for a limited time and then deleted by the `maintenance` process (`src/queueforge/maintenance/`, using `src/queueforge/services/retention.py`). One SQL statement deletes a batch of up to 1,000 jobs *together with* the rows that point at them (attempts, idempotency keys, notifications). PostgreSQL checks foreign keys at the end of a statement, so parent and child rows can go in the same statement and nothing is ever left dangling. The batch is chosen oldest-first with `FOR UPDATE SKIP LOCKED`, so concurrent purgers and anyone touching a row (an admin re-queueing, say) never block each other; each batch is its own short transaction. Only `succeeded`, `failed` and `dead_lettered` jobs are ever candidates, so a `queued` or `running` job is never deleted however old. Three small indexes (`ix_jobs_finished_status_completed`, `ix_idempotency_keys_expires_at`, `ix_idempotency_keys_job_id`) keep each batch from scanning a table. The metric `queueforge_oldest_finished_job_age_seconds` shows when purging has stopped. Periods, limits and operating notes: [operations.md](operations.md); measurements: [benchmarking.md](benchmarking.md).

### When the database is down or busy

The API answers `503` with `Retry-After` (not `500`) when PostgreSQL is unreachable or every connection in the pool is busy. Every connection has a statement timeout (30 s), a lock timeout (5 s) and an idle-in-transaction timeout (60 s), so a stuck query cannot hold a connection forever.

### Duplicate execution: it can happen, and here is why

Recovery cannot tell a *dead* worker from a *slow* one (for example a long network pause or a frozen machine). If worker A is only slow, the sweeper re-queues the job and worker B runs it **while A is still running it**, or after A already did the work but before it saved the result. So a job can run more than once. That is what "at-least-once" means, and why QueueForge never claims exactly-once.

What keeps this safe:

1. **The fence.** A's late result is rejected, because the job now carries B's lease token. Only one result is ever stored.
2. **Idempotent handlers.** The *side effects* of a handler must be safe to repeat. The demo `simulated_notification` task writes to a table keyed by job ID, so a second run changes nothing.

### Duplicate execution timeline (what each crash point means)

| Crash happens... | Result |
|---|---|
| Before the claim commits | Job still `queued`; another worker takes it |
| After the claim, before or during the handler | Lease expires, job re-queued, runs again (the first run did not finish) |
| After the handler, before the result is saved | Lease expires, job re-queued, **the work is done a second time** |
| After the result is saved | Nothing to recover |

### Tested

Retry then succeed; retries exhausted; permanent error; unknown exception without leaking its message; a job that crashes every worker is eventually dead-lettered; a late result from a stale worker is rejected; many sweepers at once recover each job exactly once; the heartbeat keeps a long job alive; and a real worker process killed with a hard kill mid-job is recovered by another worker.

## Protecting and observing the system (Milestone 5)

- **Who can do what:** every `/v1` call needs an API key. Ordinary clients see only their own jobs. Admin keys can also list dead-lettered jobs, see any job with its attempt history, re-queue dead-lettered jobs and read `/metrics`.
- **Re-queue** is a new legal transition, `dead_lettered` to `queued`, available only to admins. It raises `max_attempts` by 5 instead of resetting `attempts`, so the attempt history stays truthful and unique.
- **Rate limiting** uses Redis counters per key per minute. If Redis is unavailable the request is allowed (fail open): the limit protects the service, correctness never depends on it.
- **Request limits:** bodies over 64 KB are refused with `413`, checked both by the `Content-Length` header and by counting the bytes actually received (a client can lie, or send a chunked body).
- **Logs and metrics:** JSON logs with a fixed allow-list of fields; `/metrics` computed from PostgreSQL at scrape time, so one scrape covers every worker. Details and troubleshooting in [operations.md](operations.md); security notes in [security.md](security.md).
