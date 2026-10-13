# 0006. Snapshot backups, no point-in-time recovery

**Decision.** Backups are full `pg_dump` snapshots with a checksum and a manifest. A restore goes into a new database by default. A `verify`/`drill` command restores a backup into a throwaway database and checks it.

**Why.** It is simple, runs on any PostgreSQL, and can be **tested end to end**. An untested backup is only a hope, so the drill is part of the design. Measured on one laptop with about 1,000,000 jobs (534 MB): backup 14 s, restore 33 to 45 s ([benchmarking](../benchmarking.md)).

**What it does not give.** Restoring brings back the database as of the last backup; everything after is lost. With nightly backups that is up to 24 hours. A 5-minute recovery goal is **not met**. Backups also sit beside the database unless another location is mounted.

**Revisit when** losing hours of jobs is not acceptable. The next step is continuous WAL archiving (point-in-time recovery) or a managed PostgreSQL service that provides it. That is not built here.
