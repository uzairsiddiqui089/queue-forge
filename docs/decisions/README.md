# Decision records

Short notes on the big design choices: what was decided, why, what it costs, and when to revisit it. Each is a trade-off, not a claim that the choice is best everywhere.

| # | Decision |
|---|---|
| [0001](0001-postgres-as-the-queue.md) | PostgreSQL is the queue and the source of truth |
| [0002](0002-at-least-once-delivery.md) | At-least-once delivery, not exactly-once |
| [0003](0003-leases-and-fenced-writes.md) | Leases with a heartbeat, and fenced result writes |
| [0004](0004-redis-for-rate-limiting-only.md) | Redis is used only for rate limiting |
| [0005](0005-fair-claiming.md) | Fair claiming by number of running jobs |
| [0006](0006-snapshot-backups-no-pitr.md) | Snapshot backups (point-in-time recovery came later, see 0007) |
| [0007](0007-optional-point-in-time-recovery.md) | Optional point-in-time recovery with WAL archiving |
