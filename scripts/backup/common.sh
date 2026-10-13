#!/usr/bin/env bash
# Shared helpers for the backup scripts. Source this file; do not run it.
#
# The scripts talk to PostgreSQL through the standard libpq environment variables, so they
# work the same inside a container, in CI, or on a host with the PostgreSQL client tools:
#   PGHOST  PGPORT  PGUSER  PGPASSWORD  PGDATABASE

# The tables whose row counts are recorded with each backup and checked after a restore.
TABLES="api_clients jobs job_attempts idempotency_keys notifications workers"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_env() {
  for name in "$@"; do
    [ -n "${!name:-}" ] || die "environment variable $name must be set"
  done
}

# Run one SQL statement and print the bare result (no headers, no alignment).
# Usage: psql_q [-d database] -c "SQL"
psql_q() { psql -X -At -v ON_ERROR_STOP=1 "$@"; }

# "table=count" for every table of interest, one per line.
table_counts() {
  local db="${1:-$PGDATABASE}" t
  for t in $TABLES; do
    printf '%s=%s\n' "$t" "$(psql_q -d "$db" -c "SELECT count(*) FROM $t")"
  done
}

# Read "key=value" from a manifest file (first match).
manifest_get() { grep -m1 "^$2=" "$1" | cut -d= -f2-; }
