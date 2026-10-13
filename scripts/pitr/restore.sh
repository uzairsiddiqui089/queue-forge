#!/usr/bin/env bash
# Prepare a data directory that, when PostgreSQL starts on it, recovers to a chosen moment.
#
#   restore.sh BASE_BACKUP_DIR TARGET_DATA_DIR --to "2026-10-13 12:00:00+00"
#   restore.sh BASE_BACKUP_DIR TARGET_DATA_DIR --latest
#
# It copies the base backup into TARGET_DATA_DIR (which must be EMPTY, so nothing is ever
# overwritten) and tells PostgreSQL how to fetch the archived WAL. Then start PostgreSQL on that
# directory (see "Point-in-time recovery" in docs/operations.md). PostgreSQL replays the WAL up
# to the target, then opens for writes.
#
# Environment: WAL_ARCHIVE_DIR (default /wal_archive). Run as the user that will own the data
# (root or postgres inside the postgres image).
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

base="${1:-}"
target="${2:-}"
mode="${3:-}"
archive="${WAL_ARCHIVE_DIR:-/wal_archive}"
[ -n "$base" ] && [ -n "$target" ] || die "usage: restore.sh BASE_BACKUP_DIR TARGET_DATA_DIR (--to TIME | --latest)"
[ -f "$base/PG_VERSION" ] || die "$base is not a base backup (no PG_VERSION)"
[ -f "$base/backup_label" ] || die "$base has no backup_label: not a base backup made by basebackup.sh"
[ -d "$archive" ] || die "WAL archive $archive not found"

case "$mode" in
  --to)
    when="${4:-}"
    [ -n "$when" ] || die "--to needs a time, for example \"2026-10-13 12:00:00+00\""
    case "$when" in *"'"*) die "the time must not contain a quote" ;; esac
    ;;
  --latest) when="" ;;
  *) die "choose --to TIME or --latest" ;;
esac

mkdir -p "$target"
[ -z "$(ls -A "$target")" ] || die "$target is not empty; refusing to overwrite it"

cp -a "$base"/. "$target"/
# The streamed WAL inside the backup is only needed to start it; recovery reads the archive.
{
  echo "restore_command = 'cp $archive/%f %p'"
  if [ -n "$when" ]; then
    echo "recovery_target_time = '$when'"
  fi
  echo "recovery_target_action = 'promote'"
} >>"$target/postgresql.auto.conf"
touch "$target/recovery.signal"
chmod 700 "$target"
if [ "$(id -u)" = 0 ]; then chown -R postgres:postgres "$target"; fi

echo "restore_prepared=$target"
echo "recovery_target=${when:-latest archived WAL}"
