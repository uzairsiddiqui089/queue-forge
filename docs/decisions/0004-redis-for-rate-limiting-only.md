# 0004. Redis is used only for rate limiting

**Decision.** Redis holds per-API-key request counters and nothing else. The queue, job state and idempotency records are never in Redis.

**Why.** Counters are cheap, expire by themselves and are fine to lose. Keeping the queue out of Redis avoids a second source of truth (see [0001](0001-postgres-as-the-queue.md)).

**Failure behaviour.** If Redis is down, the rate limiter **fails open**: requests are allowed and no limit applies. This was chosen on purpose so a cache outage cannot take down job submission. The protection that does not depend on Redis stays active: the per-client and global queue limits (`429`/`503`) are enforced in PostgreSQL.

**Cost.** During a Redis outage the per-minute limit is off. Failed logins are not rate limited at all (listed in the README limitations).

**Revisit when** abuse protection matters more than availability, in which case fail closed or use a local fallback limit.
