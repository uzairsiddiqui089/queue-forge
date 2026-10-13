#!/usr/bin/env bash
# Restore a backup into a database.
#
#   restore.sh BACKUP_FILE [--into DATABASE_NAME] [--replace]
#
# SAFE BY DEFAULT: the backup goes into a NEW database called "<PGDATABASE>_restore". It will
# not overwrite the live database, and it will not touch a database that already exists.
#
# To replace a database for real (disaster recovery), stop the API, workers and maintenance
# first, then:
#   CONFIRM_REPLACE=yes-replace-the-database restore.sh FILE --into NAME --replace
# which refuses to run while anything else is connected to that database.
#
# Environment: the libpq variables (PGHOST ... PGDATABASE), and BACKUP_PASSPHRASE if the backup
# is encrypted.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/common.sh"

file=""
target=""
replace=no
while [ $# -gt 0 ]; do
  case "$1" in
    --into) target="${2:-}"; shift 2 ;;
    --replace) replace=yes; shift ;;
    -*) die "unknown option: $1" ;;
    *) [ -z "$file" ] || die "only one backup file can be given"; file="$1"; shift ;;
  esac
done

require_env PGDATABASE PGUSER
[ -n "$file" ] || die "usage: restore.sh BACKUP_FILE [--into DATABASE_NAME] [--replace]"
[ -f "$file" ] || die "no such backup file: $file"
target="${target:-${PGDATABASE}_restore}"
case "$target" in *[!a-zA-Z0-9_]*|'') die "database name may only contain letters, digits and _" ;; esac

# 1. The checksum written at backup time must still match.
if [ -f "$file.sha256" ]; then
  ( cd "$(dirname "$file")" && sha256sum --check --status "$(basename "$file").sha256" ) \
    || die "checksum mismatch: the backup file is damaged or was changed; refusing to restore it"
else
  log "WARNING: no checksum file next to the backup; its integrity is not verified"
fi

# 2. The target database: never the live one, never silently overwritten.
exists="$(psql_q -d postgres -c "SELECT count(*) FROM pg_database WHERE datname = '$target'")"
if [ "$replace" = yes ]; then
  [ "${CONFIRM_REPLACE:-}" = "yes-replace-the-database" ] \
    || die "--replace needs CONFIRM_REPLACE=yes-replace-the-database (this DELETES database '$target')"
  if [ "$exists" = 1 ]; then
    others="$(psql_q -d postgres -c "SELECT count(*) FROM pg_stat_activity WHERE datname = '$target' AND pid <> pg_backend_pid()")"
    [ "$others" = 0 ] || die "$others connection(s) are open to '$target'; stop the API, workers and maintenance first"
  fi
else
  [ "$target" != "$PGDATABASE" ] || die "'$target' is the live database; restore into another name, or use --replace"
  [ "$exists" = 0 ] || die "database '$target' already exists; drop it first or choose another name"
fi

# 3. Decrypt if needed (to a private temporary file that is always removed).
work=""
cleanup() { [ -z "$work" ] || rm -f "$work"; }
trap cleanup EXIT
source_file="$file"
case "$file" in
  *.enc)
    [ -n "${BACKUP_PASSPHRASE:-}" ] || die "this backup is encrypted: set BACKUP_PASSPHRASE"
    work="$(mktemp)"
    openssl enc -d -aes-256-cbc -pbkdf2 -pass env:BACKUP_PASSPHRASE -in "$file" -out "$work" \
      || die "could not decrypt the backup (wrong passphrase?)"
    source_file="$work"
    ;;
esac
pg_restore --list "$source_file" >/dev/null || die "the backup file cannot be read"

# 4. Create the database and load everything in ONE transaction: a failure leaves nothing behind.
started="$(date +%s)"
if [ "$replace" = yes ] && [ "$exists" = 1 ]; then
  log "dropping database '$target' ..."
  dropdb --if-exists "$target"
fi
log "creating database '$target' and restoring ..."
createdb "$target"
if ! pg_restore --no-owner --no-privileges --exit-on-error --single-transaction \
      --dbname "$target" "$source_file"; then
  dropdb --if-exists "$target"
  die "restore failed; the partly restored database '$target' was removed"
fi
seconds="$(( $(date +%s) - started ))"
echo "restored_database=$target"
echo "restore_seconds=$seconds"
log "restored into '$target' in ${seconds}s"
