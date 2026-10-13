#!/usr/bin/env bash
# Fair claiming: does it fix the starvation measured in docs/benchmarking.md, what does it
# cost per claim, and does it slow the workers down?
#
# Run from the repository root with the database credentials exported, on a benchmark stack
# whose images contain the fair-claiming code (see docs/benchmarking.md, section 7):
#     set -a; . ./.env; set +a
#     bash scripts/benchmark/run_fairness.sh
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"
$B up --workers 4          # the one-time `migrate` service applies migration 0005

echo "### Fairness: client B's wait behind client A's 3000-job backlog (4 workers)"
BENCH_FAIR_CLAIMING=false $B fairness --workers 4 --backlog 3000 --b-jobs 20 --label "fair-off-control"
$B fairness --workers 4 --backlog 3000 --b-jobs 20 --label "fair-on"

echo "### What picking a client fairly costs, as the number of waiting clients grows"
$B claimcost --repeat 5 --label "fair-claim"

echo "### Does fair claiming slow the workers? Interleaved A-B-A-B, 4 workers, echo jobs"
echo "### A = fair claiming on, B = off (oldest-first). 10 clients, then 1 client."
for round in 1 2; do
  $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --clients 10 --label "fair-abab-r${round}-A-fair-10clients"
  BENCH_FAIR_CLAIMING=false $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --clients 10 --label "fair-abab-r${round}-B-oldest-10clients"
done
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --label "fair-abab-A-fair-1client"
BENCH_FAIR_CLAIMING=false $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --label "fair-abab-B-oldest-1client"
$B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --label "fair-abab-A2-fair-1client"
BENCH_FAIR_CLAIMING=false $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --label "fair-abab-B2-oldest-1client"

echo "### fairness matrix finished"
