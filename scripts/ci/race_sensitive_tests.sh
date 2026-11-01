#!/usr/bin/env bash
# race_sensitive_tests.sh - run, without the race detector, the tests that behave
# differently under it.
#
# The `test` job runs every test under -race and the conformance gate reads its
# results. A few tests skip or loosen an assertion there (the metadata scan
# simulations call skipSimulationUnderRace; the buffered-read bounds tests ask
# requireBoundedAlloc; both, and the peer zstd bounds test, read the
# raceDetectorEnabled constant), so under -race they either do not run or assert
# less. This runs every test of the files that use those names once more, plain,
# and writes `go test -json` events to <out.json>. The gate merges those events
# with the -race results: a plain pass supplies proof for a race skip, while
# any observed failure remains a failure.
#
# The files are found by name, so a new test in such a file, or a new file that
# uses one of the names, is picked up without changing this script.
#
# Usage: scripts/ci/race_sensitive_tests.sh <out.json>
# Exit status: that of the failing `go test`, after every package ran.
set -euo pipefail

OUT="${1:?usage: race_sensitive_tests.sh <out.json>}"
cd "$(dirname "$0")/../.."

NAMES='raceDetectorEnabled|skipSimulationUnderRace|requireBoundedAlloc'

: > "$OUT"
status=0
# Directories, then the test functions of the files in each that use the names.
while IFS= read -r dir; do
  files="$(grep -lE "$NAMES" "$dir"/*_test.go || true)"
  tests="$(grep -hoE '^func Test[A-Za-z0-9_]+' $files | sed 's/^func //' | sort -u | paste -sd'|' -)"
  [ -n "$tests" ] || continue
  echo "[race-sensitive] go test ./$dir -run '^($tests)\$'" >&2
  go test "./$dir" -count=1 -json -run "^(${tests})\$" >> "$OUT" || status=$?
done < <(grep -rlE "$NAMES" --include='*_test.go' internal | xargs -n1 dirname | sort -u)

if [ "$status" -ne 0 ]; then
  echo "[race-sensitive] failing tests:" >&2
  jq -r 'select(.Action == "fail" and .Test != null) | "\(.Package) \(.Test)"' "$OUT" | sort -u >&2
fi
exit "$status"
