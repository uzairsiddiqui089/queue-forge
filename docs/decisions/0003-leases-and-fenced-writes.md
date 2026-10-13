# 0003. Leases with a heartbeat, and fenced result writes

**Decision.** Claiming a job gives the worker a time-limited **lease** (30 s) and a random `lease_token`. While the handler runs, a heartbeat renews the lease. If the worker dies, the lease expires and a sweeper puts the job back in the queue (or dead-letters it when attempts are used up). Every write by a worker is conditional on the `lease_token` still matching.

**Why.**
- Without a lease, a job taken by a dead worker would stay `running` forever.
- Without the token check, a slow worker that lost its lease could come back later and overwrite the result of the worker that took over. With it, the late write matches zero rows and is discarded. This is tested with a stale worker.
- A handler time limit exists because the heartbeat would otherwise keep renewing the lease of a hung handler forever.

**Cost.**
- A crashed worker's job waits up to one lease period before it is recovered. Shorter leases recover faster but need a heartbeat at most a third of the lease length.
- The fence protects the stored result only, not external side effects (see [0002](0002-at-least-once-delivery.md)).
- Recovery needs at least one worker running, because the sweeper runs inside the workers.
