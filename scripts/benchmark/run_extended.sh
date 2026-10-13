#!/usr/bin/env bash
# Extended benchmark matrix: fairness, overload, steady state, soak, and a large history.
#
# Run from the repository root with the database credentials exported:
#     set -a; . ./.env; set +a
#     bash scripts/benchmark/run_extended.sh
#
# The exact commands below are the ones whose results are in docs/benchmarking.md.
# The steady-state rates are fractions (50%, 80%, 95%) of the 2-worker "mixed" drain
# capacity measured by run_core.sh (about 11.9 jobs/s at the median).
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"

echo "### Fairness: client B behind client A's backlog of 3000 jobs (4 workers)"
$B fairness --workers 4 --backlog 3000 --b-jobs 20 --label "w4-backlog3000"

echo "### Overload: 4 clients x 15 req/s for 90 s, 2 workers, small queue limits"
$B overload --workers 2 --clients 4 --rate-per-client 15 --seconds 90 \
  --per-client-cap 150 --total-cap 400 --label "w2-caps150-400"

echo "### Steady state: 2 workers, mixed jobs, arrival rates of ~50%, 80%, 95% of capacity"
$B steady --workers 2 --seconds 60 --rates 6 9.5 11.3 --label "w2-mixed"

echo "### Soak: 10 minutes at 15 jobs/s (4 workers, mixed jobs)"
$B soak --workers 4 --rate 15 --seconds 600 --label "w4-15ps-10min"

echo "### Large history: 1,000,000 finished jobs (and as many attempts) already in the database"
$B preload --rows 1000000 --label "1m"
$B plans --queued 20000 --repeat 3 --label "1m"
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --keep-history --label "echo-w4-1m-history"
$B submit-only --jobs 500 --concurrency 16 --keep-history --label "limiter-on-c16-1m-history"

echo "### extended matrix finished"
