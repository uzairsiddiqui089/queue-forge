#!/usr/bin/env bash
# Take a consistent backup of the QueueForge database.
#
#   backup.sh
#
# Environment:
#   PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE   which database (standard libpq variables)
#   BACKUP_DIR         where to write (default /backups)
#   BACKUP_KEEP        how many backups to keep (default 7); older ones are deleted
#   BACKUP_PASSPHRASE  if set, the backup file is encrypted with it (AES-256, via openssl)
#
# What it does, in order:
#   1. pg_dump in the custom format. pg_dump reads one consistent snapshot, so it is safe to
#      run while the system is busy and it does not block workers or the API.
#   2. Checks the dump can be read back (pg_restore --list).
#   3. Optionally encrypts it. (Backups contain job payloads and API key hashes: protect them.)
#   4. Writes a SHA-256 checksum and a small manifest (schema version and row counts).
#   5. Deletes the oldest backups beyond BACKUP_KEEP.
# A failure at any step leaves no half-written backup behind, and exits non-zero.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require_env PGDATABASE PGUSER
dir="${BACKUP_DIR:-/backups}"
keep="${BACKUP_KEEP:-7}"
case "$keep" in ''|*[!0-9]*) die "BACKUP_KEEP must be a whole number" ;; esac
[ "$keep" -ge 1 ] || die "BACKUP_KEEP must be at least 1"
if [ -n "${BACKUP_PASSPHRASE:-}" ]; then
  command -v openssl >/dev/null || die "BACKUP_PASSPHRASE is set but openssl is not installed"
fi

umask 077   # backups are readable by their owner only
mkdir -p "$dir"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
base="queueforge-${PGDATABASE}-${ts}"
partial="$dir/$base.dump.partial"
cleanup() { rm -f "$partial" "$dir/$base.dump.enc.partial" "$dir/$base.manifest.partial"; }
trap cleanup EXIT

started="$(date +%s)"
schema_version="$(psql_q -c "SELECT version_num FROM alembic_version")"
server_version="$(psql_q -c "SHOW server_version")"
counts_before="$(table_counts)"

log "dumping database '$PGDATABASE' (schema version $schema_version) ..."
pg_dump --format=custom --no-owner --no-privileges --file "$partial"
counts_after="$(table_counts)"

pg_restore --list "$partial" >/dev/null || die "the dump cannot be read back; discarding it"

if [ -n "${BACKUP_PASSPHRASE:-}" ]; then
  final="$dir/$base.dump.enc"
  openssl enc -aes-256-cbc -pbkdf2 -salt -pass env:BACKUP_PASSPHRASE \
    -in "$partial" -out "$final.partial"
  mv "$final.partial" "$final"
  encrypted=yes
else
  final="$dir/$base.dump"
  mv "$partial" "$final"
  encrypted=no
fi

( cd "$dir" && sha256sum "$(basename "$final")" > "$(basename "$final").sha256" )

# Row counts are taken just before and just after the dump. On a busy system the dump may
# fall anywhere between them, so verify.sh accepts any value in that range.
{
  echo "created_utc=$ts"
  echo "database=$PGDATABASE"
  echo "postgres_version=$server_version"
  echo "schema_version=$schema_version"
  echo "encrypted=$encrypted"
  echo "$counts_before" | sed 's/^/rows_before./'
  echo "$counts_after" | sed 's/^/rows_after./'
} > "$final.manifest"

# Keep only the newest $keep backups of this database (sets of dump + checksum + manifest).
# shellcheck disable=SC2012
ls -1 "$dir" | grep -E "^queueforge-${PGDATABASE}-[0-9TZ]+\.dump(\.enc)?$" | sort -r \
  | tail -n +"$((keep + 1))" | while read -r old; do
      log "removing old backup $old"
      rm -f "$dir/$old" "$dir/$old.sha256" "$dir/$old.manifest"
    done

size="$(wc -c < "$final" | tr -d ' ')"
seconds="$(( $(date +%s) - started ))"
echo "backup_file=$final"
echo "backup_bytes=$size"
echo "backup_seconds=$seconds"
echo "encrypted=$encrypted"
log "backup written: $final ($size bytes, ${seconds}s)"
