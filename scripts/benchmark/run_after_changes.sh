#!/usr/bin/env bash
# Re-measure after the Phase B changes (two partial indexes, a short cache on the global
# queue-depth check, a short cache on /metrics), and check nothing got slower.
#
# Run from the repository root with the database credentials exported, on the benchmark
# stack that still holds the 1,000,000-row history created by run_extended.sh:
#     set -a; . ./.env; set +a
#     bash scripts/benchmark/run_after_changes.sh
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"

# (The images must already be built with the new code: run `$B up --build` once first.)
# The one-time `migrate` service applies migration 0004 to the big database.
$B up --workers 4   # add --build after changing application code

echo "### Query plans and timings with the indexes in place (1M history)"
$B plans --queued 20000 --repeat 3 --label "1m-after-indexes"

echo "### Drain capacity with 1M history, after the indexes (before: 57.7-61.3 jobs/s)"
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --label "echo-w4-1m-history-after"

echo "### /metrics scrapes: cache 10 s vs cache off"
$B scrape --repeat 6 --label "1m-cache10s"
BENCH_METRICS_CACHE_SECONDS=0 $B scrape --repeat 6 --label "1m-cache0"

echo "### Submit cost with 100,000 jobs already queued: global-check cache 1 s vs off"
for i in 1 2 3; do
  $B submit-only --jobs 500 --concurrency 16 --keep-history --deep-queue 100000 --label "deepq100k-cache1s-rep$i"
  BENCH_QUEUE_DEPTH_CACHE_SECONDS=0 $B submit-only --jobs 500 --concurrency 16 --keep-history --deep-queue 100000 --label "deepq100k-cache0-rep$i"
done

echo "### Regression check on an empty database (these wipe the history)"
for w in 1 4 8; do $B drain --workers "$w" --jobs 3000 --repeat 3 --kind echo --label "after-echo-w$w"; done
$B drain --workers 4 --jobs 600 --repeat 3 --kind mixed --label "after-mixed-w4"

echo "### after-changes matrix finished"
