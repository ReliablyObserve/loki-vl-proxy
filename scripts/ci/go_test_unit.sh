#!/usr/bin/env bash
# go_test_unit.sh - run the unit tests of the root module with Go's test cache.
#
# The `test` job used to pass -count=1, which makes Go rerun every package. Without
# it Go replays a package's result while its test binary, flags, the environment
# variables it read and the files it opened inside the module are unchanged, and
# runs it otherwise. -race, -coverprofile and -json do not disable the cache; a
# replay writes the same coverage profile and the same per-test events.
#
# The packages in scripts/ci/test-cache-uncached.tsv read state the cache cannot see
# (host files outside the module); they run with -count=1. Run
# `go run ./test/testcache/cmd/normalize-mtimes` first in a fresh checkout (see test/testcache).
#
# Writes the `go test -json` events of both runs to stdout, one after the other,
# and the merged coverage profile to $COVERPROFILE (default coverage.out). The
# exit status is that of the failing `go test`, after both runs.
#
# Usage: scripts/ci/go_test_unit.sh [extra go test flags]
set -uo pipefail

cd "$(dirname "$0")/../.."
COVERPROFILE="${COVERPROFILE:-coverage.out}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

module="$(go list -m)"
uncached=()
while IFS=$'\t' read -r dir _; do
  case "$dir" in ''|'#'*) continue ;; esac
  uncached+=("$module/$dir")
done < scripts/ci/test-cache-uncached.tsv
cached=()
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  skip=0
  for u in "${uncached[@]}"; do [ "$pkg" = "$u" ] && skip=1; done
  [ "$skip" -eq 1 ] || cached+=("$pkg")
done < <(go list ./...)
if [ "${#cached[@]}" -eq 0 ] || [ "${#uncached[@]}" -eq 0 ]; then
  echo "go_test_unit: empty package set (cached=${#cached[@]} uncached=${#uncached[@]})" >&2
  exit 2
fi

status=0
# The small uncached set first: it takes seconds and its failures show early.
go test "${uncached[@]}" -count=1 -race -timeout=50m -coverprofile="$tmp/uncached.out" -json "$@" || status=$?
go test "${cached[@]}" -race -timeout=50m -coverprofile="$tmp/cached.out" -json "$@" || status=$?

# One "mode:" line, then the blocks of both runs (the sets of packages are disjoint).
: > "$COVERPROFILE"
header=1
for f in "$tmp/uncached.out" "$tmp/cached.out"; do
  [ -f "$f" ] || continue
  if [ "$header" -eq 1 ]; then cat "$f" >> "$COVERPROFILE"; header=0; else tail -n +2 "$f" >> "$COVERPROFILE"; fi
done
exit "$status"
