# Architecture

> Status: describes what exists after Milestone 2 (job API). Workers, retries and
> recovery are planned (Milestones 3 and 4) and are marked as such.

## Components

- **FastAPI API** (stateless): checks the API key, validates the request, saves the job, serves job status. It never runs jobs.
- **PostgreSQL**: the only place job data lives. All times come from the database clock (`now()`).
- **Workers** *(planned, Milestone 3)*: separate processes that claim and run jobs.
- **Redis** *(planned, only if needed in Milestone 5)*: possibly rate limiting. Never stores jobs.

## Job states

`queued` → `running` → `succeeded` / `failed` / `dead_lettered`. A running job can also go back to `queued` for a retry. `succeeded`, `failed` and `dead_lettered` are final.

The rules live in `src/queueforge/domain/states.py`. Workers will apply every change as `UPDATE ... WHERE status = <expected>`, so an out-of-date or illegal change updates 0 rows instead of corrupting data.

## Tables

| Table | Purpose |
|---|---|
| `api_clients` | Who may call the API. Stores only a SHA-256 hash of each key. |
| `jobs` | One row per job: type, payload, status, attempts, schedule (`run_at`), lease fields (used from Milestone 3), result and error. |
| `idempotency_keys` | Remembers `(client, key) → job` for 24 hours so a repeated submit does not create a second job. |

`job_attempts` (one row per execution try) arrives with workers in Milestone 3.

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
