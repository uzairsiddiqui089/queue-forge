# 0005. Fair claiming by number of running jobs

**Problem (measured).** Claiming oldest-first meant one client with a 3,000-job backlog made every other client wait behind it: a small client's job waited about 121 s in the benchmark.

**Decision.** A worker picks the client with the **fewest running jobs** that has a queued job ready, then takes that client's oldest job. If that finds nothing, it falls back to oldest-first. A switch (`FAIR_CLAIMING`) turns it off.

**Result (measured, one laptop).** The small client's wait fell from about 121 s to 0.06 s. The price was roughly 4 to 12% of worker throughput for do-nothing jobs; the cost matters less when real jobs take longer.

**Limits.**
- It looks at no more than 64 clients at once (`FAIR_CLAIM_MAX_CLIENTS`); beyond that fairness is approximate.
- "Fair" means an equal number of running jobs per client, not an equal share of run time. A client submitting long jobs still holds its workers longer.

**Alternatives considered.** Per-client queues with round-robin (more tables and moving parts); weighted shares (no one asked for them, so not built).
