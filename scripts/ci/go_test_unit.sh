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
# internal/proxy is ~96% of the time, so CI splits the suite over parallel jobs
# (UNIT_SHARD), each writing its own `go test -json` events and coverage profile;
# the `test` aggregate job merges them (scripts/ci/merge_unit_shards.sh):
#   UNIT_SHARD=<1..UNIT_SHARDS>  that share of internal/proxy's top-level tests, chosen
#                                by scripts/ci/test_shards.py (every test in exactly one shard)
#   UNIT_SHARD=rest              every other package, the uncached ones included
#   UNIT_SHARD unset             everything in one job, as before (local runs)
#   UNIT_SHARDS                  number of internal/proxy shards (default 4)
#   UNIT_FRESH=1                 -count=1 on every run: no replay (main, dispatch, ci-full)
#   UNIT_SHARD_DIR               where a proxy shard writes all-tests.txt and its assigned tests
#
# Writes the `go test -json` events of the runs to stdout, one after the other,
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
proxy="$module/internal/proxy"
shard="${UNIT_SHARD:-}"
shards="${UNIT_SHARDS:-4}"
fresh=()
[ "${UNIT_FRESH:-}" = "1" ] && fresh=(-count=1)

if [ -n "$shard" ] && [ "$shard" != "rest" ]; then
  # One share of internal/proxy's tests. The list comes from the same race build the run
  # uses, so it compiles once. Every test is in exactly one shard (test_shards.py check).
  dir="${UNIT_SHARD_DIR:-$tmp}"
  mkdir -p "$dir"
  if ! go test -race -coverprofile="$tmp/list.out" -list '.*' ./internal/proxy/ > "$tmp/list.txt" 2> "$tmp/list.err"; then
    cat "$tmp/list.txt" "$tmp/list.err" >&2
    echo "go_test_unit: go test -list failed" >&2
    exit 2
  fi
  scripts/ci/test_shards.py check --shards "$shards" --list "$tmp/list.txt" >&2 || exit 2
  scripts/ci/test_shards.py assign --shards "$shards" --list "$tmp/list.txt" > "$tmp/assign.tsv" || exit 2
  cut -f2 "$tmp/assign.tsv" | LC_ALL=C sort > "$dir/all-tests.txt"
  awk -F'\t' -v s="$shard" '$1 == s { print $2 }' "$tmp/assign.tsv" | LC_ALL=C sort > "$dir/assigned.txt"
  run="$(scripts/ci/test_shards.py regex --shards "$shards" --shard "$shard" --list "$tmp/list.txt")" || exit 2
  echo "go_test_unit: internal/proxy shard $shard of $shards: $(wc -l < "$dir/assigned.txt" | tr -d ' ') of $(wc -l < "$dir/all-tests.txt" | tr -d ' ') tests" >&2
  status=0
  go test ./internal/proxy/ ${fresh[@]+"${fresh[@]}"} -race -timeout=50m -run "$run" -coverprofile="$COVERPROFILE" -json "$@" || status=$?
  exit "$status"
fi

uncached=()
while IFS=$'\t' read -r dir _; do
  case "$dir" in ''|'#'*) continue ;; esac
  uncached+=("$module/$dir")
done < scripts/ci/test-cache-uncached.tsv
cached=()
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  [ "$shard" = "rest" ] && [ "$pkg" = "$proxy" ] && continue
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
go test "${cached[@]}" ${fresh[@]+"${fresh[@]}"} -race -timeout=50m -coverprofile="$tmp/cached.out" -json "$@" || status=$?

# One "mode:" line, then the blocks of both runs (the sets of packages are disjoint).
: > "$COVERPROFILE"
header=1
for f in "$tmp/uncached.out" "$tmp/cached.out"; do
  [ -f "$f" ] || continue
  if [ "$header" -eq 1 ]; then cat "$f" >> "$COVERPROFILE"; header=0; else tail -n +2 "$f" >> "$COVERPROFILE"; fi
done
exit "$status"
