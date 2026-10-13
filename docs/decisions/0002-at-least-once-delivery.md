# 0002. At-least-once delivery, not exactly-once

**Decision.** A job is guaranteed to run **at least once** if it is accepted, and may run more than once. QueueForge never claims exactly-once.

**Why.** If a worker dies after doing the work but before recording the result, nobody can tell, from outside, whether the work happened. The only safe choices are "maybe never" (at-most-once) or "maybe twice" (at-least-once). For a job queue, losing work is worse than repeating it, so jobs are retried.

**What it means in practice.**
- Handlers must be safe to repeat. The demo handler `simulated_notification` writes to a table with a unique `job_id`, so a repeat does nothing.
- Attempts are counted when a job is claimed, so a job that crashes its worker still uses up attempts and ends dead-lettered instead of looping forever.
- Only the **stored result** is protected from duplicates (see [0003](0003-leases-and-fenced-writes.md)); side effects outside the database are the handler's responsibility.
- After a restore from backup, jobs that had finished since the backup run again (see the runbook in [operations](../operations.md)).

**Cost.** Handler authors carry the idempotency burden. This is stated in the README and security docs.
