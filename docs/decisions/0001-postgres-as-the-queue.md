# 0001. PostgreSQL is the queue and the source of truth

**Decision.** Jobs live in a PostgreSQL table. Workers claim them with `SELECT ... FOR UPDATE SKIP LOCKED`. There is no separate message broker.

**Why.**
- A job is created in the same transaction as its idempotency key, and acknowledged only after commit. With a separate broker, "saved the job" and "queued the job" are two systems that can disagree.
- `SKIP LOCKED` lets many workers claim at once without two taking the same row, and without waiting for each other.
- One system to run, back up, restore and reason about. State, history (attempts) and the queue are in one place, and a backup of one database is a backup of everything that matters.
- Time comparisons use the database clock, so worker clocks do not matter.

**Cost.**
- Throughput is limited by one PostgreSQL instance. Measured on a low-end laptop: about 90 jobs/s for do-nothing jobs with 8 workers (see [benchmarking](../benchmarking.md); not a capacity figure).
- Workers poll, so a job waits up to the poll interval (idle pickup median 0.56 s in the benchmark).
- A queue table needs care: partial indexes, retention (finished jobs are deleted by the maintenance service), and attention to vacuum.

**Revisit when** sustained load needs more than one PostgreSQL instance can give, or pickup latency must be well under the poll interval.

**Alternatives considered.** Redis lists or streams (fast, but a second source of truth and harder to make durable and consistent with the job record); RabbitMQ or SQS (a real broker, more moving parts, still needs a database for job status).
