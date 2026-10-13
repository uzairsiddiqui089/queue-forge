#!/usr/bin/env bash
# Deep-queue submit cost again, after moving the global queue check in front of the
# per-client lock (before: with the cache off, 5.6-6.3 accepted/s and some spurious 503s;
# with the 1 s cache, 34-44 accepted/s). The 1M-row history is no longer present here (part 2
# wiped it), so the only large table is the 90,000 queued jobs added by the test itself.
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"
$B up --workers 0
for i in 1 2 3; do
  BENCH_QUEUE_DEPTH_CACHE_SECONDS=0 $B submit-only --jobs 500 --concurrency 16 --deep-queue 90000 --label "reordered-deepq90k-cache0-rep$i"
  $B submit-only --jobs 500 --concurrency 16 --deep-queue 90000 --label "reordered-deepq90k-cache1s-rep$i"
done
$B submit-only --jobs 500 --concurrency 16 --label "reordered-no-deep-queue-baseline"
echo "### part 4 finished"
