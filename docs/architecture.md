# Architecture

> Status: describes what exists after Milestone 3 (workers). Retries and
> crash recovery are planned (Milestone 4) and are marked as such.

## Components

- **FastAPI API** (stateless): checks the API key, validates the request, saves the job, serves job status. It never runs jobs.
- **PostgreSQL**: the only place job data lives. All times come from the database clock (`now()`).
- **Workers**: separate processes (`python -m queueforge.worker`) that claim jobs, run an allow-listed handler and save the result. Run as many as you like.
- **Redis** *(planned, only if needed in Milestone 5)*: possibly rate limiting. Never stores jobs.

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

| Crash happens... | What is left | What happens |
|---|---|---|
| Before the claim commits | Job still `queued` | Nothing lost; another worker claims it |
| After the claim, before or during the handler | Job `running`, attempt row open | **Milestone 3: stuck `running`.** Milestone 4 adds lease expiry and a sweeper to re-queue it |
| After the handler, before the result is saved | Same as above, and the work was done | Milestone 4 will run it again. This is why handlers must be safe to run twice (at-least-once) |
| After the result is saved | Job `succeeded` | Nothing to recover |

The attempt counter goes up at *claim* time, not at the end. So a job that crashes its worker every time still uses up its attempts and will eventually be dead-lettered (Milestone 4) instead of looping forever.

### Handlers

Only four task types exist (`echo`, `sleep`, `flaky`, `simulated_notification`). The worker re-validates the stored payload before running it. `simulated_notification` shows how to stay correct when run twice: it writes to a table whose primary key is the job ID, so a second run inserts nothing.

### What gets stored when a job fails

- Errors we raise on purpose (`TaskError`) store their message.
- Any other exception stores only a generic message. The real message and stack trace go to the worker log, because exception text can contain secrets.
- For now every failure is final (`failed`). Telling temporary errors from permanent ones, and retrying, is Milestone 4.
