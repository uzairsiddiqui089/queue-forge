#!/usr/bin/env bash
# One entry point for the backup tools.
#
#   dbtools.sh backup                       take a backup
#   dbtools.sh restore FILE [--into NAME]   restore into a NEW database (safe by default)
#   dbtools.sh verify FILE                  restore into a throwaway database and check it
#   dbtools.sh drill                        backup the live database, then verify that backup
#   dbtools.sh list                         list the backups
#
# With Docker Compose (the `tools` profile keeps it out of a normal `docker compose up`):
#   docker compose --profile tools run --rm dbtools backup
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
command="${1:-help}"
[ $# -eq 0 ] || shift

case "$command" in
  backup)  exec bash "$here/backup.sh" "$@" ;;
  restore) exec bash "$here/restore.sh" "$@" ;;
  verify)  exec bash "$here/verify.sh" "$@" ;;
  list)    ls -l "${BACKUP_DIR:-/backups}" ;;
  drill)
    # Back up the live database, then prove that exact backup restores correctly.
    output="$(bash "$here/backup.sh")"
    echo "$output"
    file="$(echo "$output" | sed -n 's/^backup_file=//p')"
    bash "$here/verify.sh" "$file"
    ;;
  *)
    sed -n '2,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    [ "$command" = help ] || exit 2
    ;;
esac
