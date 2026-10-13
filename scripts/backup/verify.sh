#!/usr/bin/env bash
# Prove a backup is usable: restore it into a throwaway database, check it, delete it.
#
#   verify.sh BACKUP_FILE
#
# Checks:
#   - the file's checksum still matches (done by restore.sh)
#   - the restore itself succeeds (one transaction, so a bad file fails as a whole)
#   - the schema version equals the one recorded in the backup's manifest
#   - every table's row count lies between the counts recorded just before and just after the
#     dump (the same number when the system was quiet)
#   - every constraint, including every foreign key, is validated
# Exit status 0 means "this backup can be restored and looks right". Run it regularly (for
# example weekly): an untested backup is only a hope.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$here/common.sh"

file="${1:-}"
[ -n "$file" ] || die "usage: verify.sh BACKUP_FILE"
[ -f "$file" ] || die "no such backup file: $file"
manifest="$file.manifest"
[ -f "$manifest" ] || die "no manifest next to the backup ($manifest): cannot check row counts"
require_env PGDATABASE PGUSER

scratch="qf_verify_$(date -u +%Y%m%d%H%M%S)_$$"
cleanup() { dropdb --if-exists "$scratch" >/dev/null 2>&1 || true; }
trap cleanup EXIT

started="$(date +%s)"
restore_output="$(bash "$here/restore.sh" "$file" --into "$scratch")" || die "the restore failed"
restore_seconds="$(echo "$restore_output" | sed -n 's/^restore_seconds=//p')"

failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }

expected_schema="$(manifest_get "$manifest" schema_version)"
actual_schema="$(psql_q -d "$scratch" -c "SELECT version_num FROM alembic_version")"
[ "$actual_schema" = "$expected_schema" ] \
  || fail "schema version is $actual_schema but the manifest says $expected_schema"

for t in $TABLES; do
  before="$(manifest_get "$manifest" "rows_before.$t")"
  after="$(manifest_get "$manifest" "rows_after.$t")"
  actual="$(psql_q -d "$scratch" -c "SELECT count(*) FROM $t")"
  low="$before"; high="$after"
  if [ "$after" -lt "$before" ]; then low="$after"; high="$before"; fi
  if [ "$actual" -lt "$low" ] || [ "$actual" -gt "$high" ]; then
    fail "table $t has $actual rows; the manifest expects between $low and $high"
  fi
done

unvalidated="$(psql_q -d "$scratch" -c "SELECT count(*) FROM pg_constraint WHERE NOT convalidated")"
[ "$unvalidated" = 0 ] || fail "$unvalidated constraint(s) are not validated"

orphans="$(psql_q -d "$scratch" -c "
  SELECT (SELECT count(*) FROM job_attempts a LEFT JOIN jobs j ON j.id = a.job_id WHERE j.id IS NULL)
       + (SELECT count(*) FROM idempotency_keys k LEFT JOIN jobs j ON j.id = k.job_id WHERE j.id IS NULL)
       + (SELECT count(*) FROM notifications n LEFT JOIN jobs j ON j.id = n.job_id WHERE j.id IS NULL)
       + (SELECT count(*) FROM jobs j LEFT JOIN api_clients c ON c.id = j.client_id WHERE c.id IS NULL)")"
[ "$orphans" = 0 ] || fail "$orphans row(s) point at a parent row that does not exist"

total="$(( $(date +%s) - started ))"
echo "verify_restore_seconds=$restore_seconds"
echo "verify_total_seconds=$total"
if [ "$failures" -eq 0 ]; then
  echo "VERIFY OK: $(basename "$file") can be restored and passed every check"
else
  echo "VERIFY FAILED: $failures check(s) failed" >&2
  exit 1
fi
