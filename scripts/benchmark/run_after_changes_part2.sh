#!/usr/bin/env bash
# Second half of the after-changes measurements (the first half, in run_after_changes.sh,
# stopped at the deep-queue step because of a mistake in the harness: 100,000 queued jobs
# equals the global cap, so every submit was correctly refused).
#
# Same setup as run_after_changes.sh: benchmark stack up, 1,000,000-row history present.
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"

echo "### Controlled A/B on the 1M history: vacuumed first, 4 workers, echo jobs"
echo "### (the earlier un-vacuumed run was confounded by ~165,000 dead rows and a Docker restart)"
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --vacuum-first --label "echo-w4-1m-vacuumed-with-indexes"
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --vacuum-first --without-new-indexes --label "echo-w4-1m-vacuumed-without-indexes"
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --vacuum-first --label "echo-w4-1m-vacuumed-with-indexes-again"

echo "### Submit cost with 90,000 jobs already queued (cap is 100,000): global-check cache 1 s vs off"
for i in 1 2 3; do
  $B submit-only --jobs 500 --concurrency 16 --keep-history --deep-queue 90000 --label "deepq90k-cache1s-rep$i"
  BENCH_QUEUE_DEPTH_CACHE_SECONDS=0 $B submit-only --jobs 500 --concurrency 16 --keep-history --deep-queue 90000 --label "deepq90k-cache0-rep$i"
done

echo "### Regression check on an empty database (these wipe the history)"
for w in 1 4 8; do $B drain --workers "$w" --jobs 3000 --repeat 3 --kind echo --label "after-echo-w$w"; done
$B drain --workers 4 --jobs 600 --repeat 3 --kind mixed --label "after-mixed-w4"

echo "### part 2 finished"
