# 0007. Optional point-in-time recovery with WAL archiving

**Decision.** Add PITR as an opt-in layer (`docker-compose.pitr.yml`, `scripts/pitr/`): PostgreSQL archives every WAL segment (at least once a minute), `basebackup.sh` takes physical base backups and prunes old ones with the WAL only they needed, and `restore.sh` prepares a new data directory that replays WAL to a chosen time. `drill.sh` proves it.

**Why.** Snapshot backups ([0006](0006-snapshot-backups-no-pitr.md)) lose everything since the last snapshot. A bad deploy or a mistaken bulk delete at 14:00 should be recoverable to 13:59, not to last night. PostgreSQL's own WAL archiving does this without extra services.

**Why opt-in.** It needs a restart to turn on, a second volume, and monitoring (a failing archive can fill the database disk). A project that only wants nightly snapshots should not carry that risk by default.

**Proof.** The drill deletes every row after a noted time T, restores to T, and requires exactly the rows present at T; a control restore to the latest point must show the deletion; and removing the target time from the restore made the drill fail (mutation-checked).

**Cost and limits.** Recovery point is about a minute plus any not-yet-copied WAL, not zero. Measured on a tiny database only; large-database restore time and WAL replay time are unknown. Archive and base backups are unencrypted and stay on the same disk unless the volumes are moved. The final switch-over of the compose stack to the recovered volume is documented but was not rehearsed.

**Revisit when** a real deployment needs a smaller recovery point (synchronous replication), faster recovery of a large database (a standby), or encrypted off-site archives (a tool such as pgBackRest or WAL-G, or a managed PostgreSQL service).
