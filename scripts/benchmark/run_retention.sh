#!/usr/bin/env bash
# Retention: how fast can 1,000,000 old jobs be purged, does purging slow down live work, and
# what is left behind?
#
# Run from the repository root with the database credentials exported, on a benchmark stack
# whose images contain the retention code (see docs/benchmarking.md, section 7):
#     set -a; . ./.env; set +a
#     bash scripts/benchmark/run_retention.sh
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"
$B up --workers 4          # the one-time `migrate` service applies migration 0006

echo "### 1,000,000 finished jobs (and as many attempts), all more than a day old"
$B preload --rows 1000000 --label "retention-1m"

echo "### Does a purge running flat out slow the workers? Interleaved A-B-A-B, 4 workers, echo jobs"
echo "### A = no purge, B = the purge running continuously in the background (worst case)"
for round in 1 2; do
  $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --label "retention-r${round}-A-no-purge"
  $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --purge-concurrently --label "retention-r${round}-B-purge-running"
done

echo "### Purge whatever history is left, measuring speed"
$B purge --days 1 --batch-size 1000 --label "retention-rest-of-history"

echo "### retention matrix finished"
