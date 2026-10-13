#!/usr/bin/env bash
# Time a COMPLETE purge of 1,000,000 old jobs (run_retention.sh ends with only the leftovers).
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"
$B preload --rows 1000000 --label "retention-1m-again"
$B purge --days 1 --batch-size 1000 --label "retention-full-1m"
echo "### full purge finished"
