#!/usr/bin/env bash
# Is the lower echo throughput seen in the empty-database regression check caused by the new
# indexes, or by the machine? Interleaved A-B-A-B on an empty database, 4 workers, echo jobs:
#   A = with the two new indexes,  B = without them (dropped for the run, re-created after).
# Interleaving means slow drift of the machine hits A and B alike.
set -euo pipefail
B="uv run python scripts/benchmark/bench.py"
for round in 1 2; do
  $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --label "abab-r${round}-A-with-indexes"
  $B drain --workers 4 --jobs 3000 --repeat 3 --kind echo --without-new-indexes --label "abab-r${round}-B-without-indexes"
done
echo "### part 3 finished"
