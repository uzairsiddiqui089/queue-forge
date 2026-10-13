#!/usr/bin/env bash
# Backup and restore at a realistic size: how long do they take, and does the restore verify?
#
# Run from the repository root with the database credentials exported, on the benchmark stack:
#     set -a; . ./.env; set +a
#     bash scripts/benchmark/run_backup.sh
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"
$B up --workers 0
$B preload --rows 1000000 --label "backup-1m"

echo "### backup + verified restore of 1,000,000 jobs (and as many attempts), plain and encrypted"
$B backup-drill --repeat 2 --label "1m"

echo "### backup matrix finished"
