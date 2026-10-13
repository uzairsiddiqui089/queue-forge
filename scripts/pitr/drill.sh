#!/usr/bin/env bash
# Prove point-in-time recovery works, end to end, in throwaway containers (the real database
# and its volumes are never touched).
#
#   bash scripts/pitr/drill.sh
#
# The story it plays out:
#   1. a database with WAL archiving on gets 1,000 rows, then a base backup is taken
#   2. 1,000 more rows are added, and we note the exact time T
#   3. "disaster": every row is deleted
#   4. we restore the base backup and replay the WAL only up to T
#   5. the restored database must have exactly the 2,000 rows that existed at T
# It also checks that restoring to the LATEST point gives 0 rows (so the target time really
# decided the result), and that a second base backup prunes old WAL and still restores.
# Needs: docker, and the postgres:16 image (already present if you use this project).
set -euo pipefail

IMAGE="${PG_IMAGE:-postgres:16}"
id="qfpitr$$"
src="${id}-src"
vol_data="${id}-data"
vol_wal="${id}-wal"
vol_bak="${id}-bak"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Git Bash on Windows needs a Windows-style path for bind mounts.
scripts_dir="$(cd "$here" && (pwd -W 2>/dev/null || pwd))"
password="$(head -c 18 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')"
export MSYS_NO_PATHCONV=1

cleanup() {
  docker rm -f "$src" "${id}-restore" "${id}-restore2" >/dev/null 2>&1 || true
  docker volume rm -f "$vol_data" "$vol_wal" "$vol_bak" "${id}-r1" "${id}-r2" >/dev/null 2>&1 || true
}
trap cleanup EXIT

step() { echo; echo "== $*"; }
fail() { echo "DRILL FAILED: $*" >&2; exit 1; }
sql() { docker exec -e PGPASSWORD="$password" "$1" psql -h 127.0.0.1 -U postgres -d drill -tAc "$2"; }
wait_ready() {
  for _ in $(seq 1 60); do
    if docker exec "$1" pg_isready -h 127.0.0.1 -U postgres -d drill >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "$1 did not become ready"
}
wait_promoted() {
  for _ in $(seq 1 120); do
    if [ "$(sql "$1" 'SELECT NOT pg_is_in_recovery()' 2>/dev/null || true)" = "t" ]; then return 0; fi
    sleep 1
  done
  docker logs --tail 20 "$1" >&2 || true
  fail "$1 never finished recovery"
}

step "1. start a database with WAL archiving on"
docker volume create "$vol_data" >/dev/null
docker volume create "$vol_wal" >/dev/null
docker volume create "$vol_bak" >/dev/null
docker run --rm -u root -v "$vol_wal":/wal_archive -v "$vol_bak":/backups "$IMAGE" \
  chown -R postgres:postgres /wal_archive /backups
docker run -d --name "$src" -e POSTGRES_PASSWORD="$password" -e POSTGRES_DB=drill \
  -v "$vol_data":/var/lib/postgresql/data -v "$vol_wal":/wal_archive -v "$vol_bak":/backups \
  -v "$scripts_dir":/pitr:ro "$IMAGE" postgres -c archive_mode=on \
  -c "archive_command=test ! -f /wal_archive/%f && cp %p /wal_archive/%f" -c archive_timeout=60 >/dev/null
sleep 5
wait_ready "$src"
sql "$src" "CREATE TABLE t (id int PRIMARY KEY, batch text)" >/dev/null
sql "$src" "INSERT INTO t SELECT g, 'before-backup' FROM generate_series(1,1000) g" >/dev/null

step "2. take a base backup with scripts/pitr/basebackup.sh"
first_out="$(docker exec -u postgres -e BASE_KEEP=1 "$src" bash /pitr/basebackup.sh)"
echo "$first_out"
first="$(echo "$first_out" | sed -n 's/^base_backup=//p')"
sleep 1
# A second base backup with BASE_KEEP=1 must remove the first one (and the WAL only it needed),
# and we restore from the second, which also proves the pruning did not break recovery.
second_out="$(docker exec -u postgres -e BASE_KEEP=1 "$src" bash /pitr/basebackup.sh)"
echo "$second_out"
base1="$(echo "$second_out" | sed -n 's/^base_backup=//p')"
[ -n "$first" ] && [ -n "$base1" ] || fail "no base backup path reported"
docker exec "$src" test ! -e "$first" || fail "old base backup was not pruned"
docker exec "$src" test -e "$base1" || fail "new base backup is missing"
echo "pruning ok: only $(basename "$base1") is kept"

step "3. add more data, then note the exact time T"
sql "$src" "INSERT INTO t SELECT g, 'after-backup' FROM generate_series(1001,2000) g" >/dev/null
sleep 2
target="$(sql "$src" "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '+00'")"
sleep 2
echo "T = $target (2,000 rows exist at T)"

step "4. disaster: delete everything, then close and archive the WAL"
sql "$src" "DELETE FROM t" >/dev/null
sql "$src" "SELECT pg_switch_wal()" >/dev/null
for _ in $(seq 1 90); do
  last="$(sql "$src" "SELECT last_archived_wal FROM pg_stat_archiver")"
  current_done="$(sql "$src" "SELECT (SELECT last_archived_time FROM pg_stat_archiver) > (SELECT now() - interval '2 minutes')")"
  if [ -n "$last" ] && [ "$current_done" = "t" ] && [ "$(sql "$src" "SELECT count(*) FROM t")" = 0 ]; then break; fi
  sleep 1
done
sleep 3
docker stop "$src" >/dev/null
echo "rows in the damaged database: 0"

restore_to() { # name, data volume, mode args...
  local name="$1" vol="$2"; shift 2
  docker volume create "$vol" >/dev/null
  docker run --rm -u root -v "$vol":/restore -v "$vol_bak":/backups:ro -v "$vol_wal":/wal_archive:ro \
    -v "$scripts_dir":/pitr:ro "$IMAGE" bash /pitr/restore.sh "$base1" /restore "$@"
  docker run -d --name "$name" -e POSTGRES_PASSWORD="$password" -e POSTGRES_DB=drill \
    -v "$vol":/var/lib/postgresql/data -v "$vol_wal":/wal_archive:ro "$IMAGE" >/dev/null
  wait_ready "$name"
  wait_promoted "$name"
}

step "5. restore the base backup and replay WAL only up to T"
restore_to "${id}-restore" "${id}-r1" --to "$target"
rows="$(sql "${id}-restore" "SELECT count(*) FROM t")"
late="$(sql "${id}-restore" "SELECT count(*) FROM t WHERE batch = 'after-backup'")"
echo "rows after restore to T: $rows (of which added after the base backup: $late)"
[ "$rows" = 2000 ] || fail "expected 2000 rows at T, got $rows"
[ "$late" = 1000 ] || fail "expected 1000 rows from after the backup, got $late"

step "6. control: restoring to the latest point must show the disaster (0 rows)"
restore_to "${id}-restore2" "${id}-r2" --latest
latest_rows="$(sql "${id}-restore2" "SELECT count(*) FROM t")"
echo "rows after restore to latest: $latest_rows"
[ "$latest_rows" = 0 ] || fail "expected 0 rows when replaying everything, got $latest_rows"

echo
echo "PITR DRILL OK: restored to T (2000 rows) although the live data had been deleted"
