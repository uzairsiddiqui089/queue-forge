#!/usr/bin/env bash
# Core benchmark matrix: API cost, worker drain capacity, idle pickup latency.
#
# Run from the repository root with the database credentials exported:
#     set -a; . ./.env; set +a
#     bash scripts/benchmark/run_core.sh
#
# The exact commands below are the ones whose results are in docs/benchmarking.md.
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"

$B up --workers 2

echo "### API submit cost, limiter ON, no workers (so only the API and database are working)"
for c in 1 4 16 64; do $B submit-only --jobs 500 --concurrency "$c" --label "limiter-on-c$c"; done
for i in 2 3; do $B submit-only --jobs 500 --concurrency 16 --label "limiter-on-c16-rep$i"; done

echo "### API submit cost, rate limiter OFF (BENCH_REDIS_URL empty)"
for i in 1 2 3; do BENCH_REDIS_URL="" $B submit-only --jobs 500 --concurrency 16 --label "limiter-off-c16-rep$i"; done

echo "### Worker drain capacity: a queue of echo jobs appears at once"
for w in 1 2 4 8; do $B drain --workers "$w" --jobs 3000 --repeat 3 --kind echo --label "echo-w$w"; done

echo "### Worker drain capacity: mixed 0.05-0.2 s sleep jobs (mean 0.125 s)"
for w in 1 2 4 8; do $B drain --workers "$w" --jobs 600 --repeat 3 --kind mixed --label "mixed-w$w"; done

echo "### Idle pickup latency (one job at a time into an idle system)"
$B idle --workers 2 --jobs 40 --label "w2"

echo "### core matrix finished"
