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
# internal/proxy and internal/querycorpus are nearly all of the time (under -race with
# coverage, minutes each), so CI splits the suite over parallel jobs (UNIT_SHARD), each
# writing its own `go test -json` events and coverage profile; the `test` aggregate job
# merges them (scripts/ci/merge_unit_shards.py):
#   UNIT_SHARD=<1..UNIT_SHARDS>  that share of the top-level tests of each package in
#                                UNIT_SHARD_PACKAGES, chosen by scripts/ci/test_shards.py
#                                (every test of a package in exactly one shard); one
#                                `go test -run <share>` per package, the profiles merged
#   UNIT_SHARD=rest              every other package, the uncached ones included
#   UNIT_SHARD unset             everything in one job, as before (local runs)
#   UNIT_SHARDS                  number of shards (default 12; keep equal to the numbered entries of the
#                                test-shard matrix in .github/workflows/ci.yaml)
#   UNIT_SHARD_PACKAGES          the split packages, directories of the module, space separated
#                                (default "internal/proxy internal/querycorpus"); a package
#                                added here leaves `rest` and is split like the others; keep
#                                equal to PACKAGES in scripts/ci/merge_unit_shards.py
#   UNIT_FRESH=1                 expire every stored test result first (`go clean -testcache`), then run
#                                normally: every test executes and its result is stored for later pull
#                                requests (main, dispatch, ci-full). Not -count=1, which stores nothing.
#   UNIT_RACE=0                  no -race (the PR quality report's coverage run); default on
#   UNIT_COUNT1=1                -count=1 on every package: no replay from the test cache (the PR quality report)
#   UNIT_TAGS                    -tags value of every go test (corpuscapture, for the PR quality report's corpus)
#   UNIT_TIMEOUT                 the go test package timeout (default 50m)
#   UNIT_ROOT                    the module to test (default: this checkout)
#   UNIT_SHARD_DIR               where a numbered shard writes all-tests.txt and assigned.txt: one
#                                "<package directory>:<test>" line per test
#
# Writes the `go test -json` events of the runs to stdout, one after the other,
# and the merged coverage profile to $COVERPROFILE (default coverage.out). The
# exit status is that of the failing `go test`, after both runs.
#
# Usage: scripts/ci/go_test_unit.sh [extra go test flags]
set -uo pipefail

ci_dir="$(cd "$(dirname "$0")" && pwd)"
# The tree under test is this checkout, or UNIT_ROOT (the PR quality report runs the base commit's tests with
# these scripts).
cd "${UNIT_ROOT:-$ci_dir/../..}" || exit 2
COVERPROFILE="${COVERPROFILE:-coverage.out}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

module="$(go list -m)"
# shellcheck disable=SC2206 # word splitting of the list is intended
split_dirs=(${UNIT_SHARD_PACKAGES:-internal/proxy internal/querycorpus})
shard="${UNIT_SHARD:-}"
shards="${UNIT_SHARDS:-12}"
timeout="${UNIT_TIMEOUT:-50m}"
# Flags every go test below takes; arrays so an unset option adds nothing.
race=(-race)
[ "${UNIT_RACE:-1}" = "0" ] && race=()
tags=()
[ -n "${UNIT_TAGS:-}" ] && tags=(-tags "$UNIT_TAGS")
count1=()
[ "${UNIT_COUNT1:-}" = "1" ] && count1=(-count=1)
joint=()
for d in "${split_dirs[@]}"; do joint+=(--joint "$d"); done
# Fresh: forget every stored result (the compiled packages stay), so each test runs once and its
# result is stored for the next pull request. -count=1 would run it too but store nothing.
if [ "${UNIT_FRESH:-}" = "1" ]; then
  go clean -testcache || exit 2
fi

# merge_profiles OUT IN...: one "mode:" line, then the blocks of every input (the packages are disjoint).
merge_profiles() {
  local out="$1" header=1 f
  shift
  : > "$out"
  for f in "$@"; do
    [ -f "$f" ] || continue
    if [ "$header" -eq 1 ]; then cat "$f" >> "$out"; header=0; else tail -n +2 "$f" >> "$out"; fi
  done
}

if [ -n "$shard" ] && [ "$shard" != "rest" ]; then
  # One share of the tests of each split package. Each list comes from the same race build the
  # run uses, so it compiles once. Every test is in exactly one shard (test_shards.py check).
  dir="${UNIT_SHARD_DIR:-$tmp}"
  mkdir -p "$dir"
  : > "$dir/all-tests.txt"
  : > "$dir/assigned.txt"
  status=0
  profiles=()
  i=0
  for pkgdir in "${split_dirs[@]}"; do
    i=$((i + 1))
    if ! go test ${race[@]+"${race[@]}"} ${tags[@]+"${tags[@]}"} -coverprofile="$tmp/list-$i.out" -list '.*' "./$pkgdir/" > "$tmp/list-$i.txt" 2> "$tmp/list-$i.err"; then
      cat "$tmp/list-$i.txt" "$tmp/list-$i.err" >&2
      echo "go_test_unit: go test -list failed for $pkgdir" >&2
      exit 2
    fi
    "$ci_dir/test_shards.py" check --package "$pkgdir" "${joint[@]}" --shards "$shards" --list "$tmp/list-$i.txt" >&2 || exit 2
    "$ci_dir/test_shards.py" assign --package "$pkgdir" "${joint[@]}" --shards "$shards" --list "$tmp/list-$i.txt" > "$tmp/assign-$i.tsv" || exit 2
    cut -f2 "$tmp/assign-$i.tsv" | LC_ALL=C sort | sed "s|^|$pkgdir:|" >> "$dir/all-tests.txt"
    awk -F'\t' -v s="$shard" '$1 == s { print $2 }' "$tmp/assign-$i.tsv" | LC_ALL=C sort | sed "s|^|$pkgdir:|" >> "$dir/assigned.txt"
    run="$("$ci_dir/test_shards.py" regex --package "$pkgdir" "${joint[@]}" --shards "$shards" --shard "$shard" --list "$tmp/list-$i.txt")" || exit 2
    echo "go_test_unit: $pkgdir shard $shard of $shards: $(grep -c "^$pkgdir:" "$dir/assigned.txt") of $(grep -c "^$pkgdir:" "$dir/all-tests.txt") tests" >&2
    if [ -z "$run" ]; then
      echo "go_test_unit: $pkgdir has no test in shard $shard; nothing to run" >&2
      continue
    fi
    profiles+=("$tmp/shard-$i.out")
    go test "./$pkgdir/" ${race[@]+"${race[@]}"} ${tags[@]+"${tags[@]}"} ${count1[@]+"${count1[@]}"} -timeout="$timeout" -run "$run" -coverprofile="$tmp/shard-$i.out" -json "$@" || status=$?
  done
  merge_profiles "$COVERPROFILE" "${profiles[@]}"
  exit "$status"
fi

uncached=()
# With -count=1 on every package nothing replays from the cache, so the uncached list has no meaning.
if [ "${UNIT_COUNT1:-}" != "1" ]; then
  while IFS=$'\t' read -r dir _; do
    case "$dir" in ''|'#'*) continue ;; esac
    uncached+=("$module/$dir")
  done < "$ci_dir/test-cache-uncached.tsv"
fi
cached=()
while IFS= read -r pkg; do
  [ -n "$pkg" ] || continue
  if [ "$shard" = "rest" ]; then
    for d in "${split_dirs[@]}"; do [ "$pkg" = "$module/$d" ] && continue 2; done
  fi
  skip=0
  for u in ${uncached[@]+"${uncached[@]}"}; do [ "$pkg" = "$u" ] && skip=1; done
  [ "$skip" -eq 1 ] || cached+=("$pkg")
done < <(go list ./...)
if [ "${#cached[@]}" -eq 0 ] || { [ "${#uncached[@]}" -eq 0 ] && [ "${UNIT_COUNT1:-}" != "1" ]; }; then
  echo "go_test_unit: empty package set (cached=${#cached[@]} uncached=${#uncached[@]})" >&2
  exit 2
fi

status=0
# The small uncached set first: it takes seconds and its failures show early.
if [ "${#uncached[@]}" -gt 0 ]; then
  go test "${uncached[@]}" -count=1 ${race[@]+"${race[@]}"} ${tags[@]+"${tags[@]}"} -timeout="$timeout" -coverprofile="$tmp/uncached.out" -json "$@" || status=$?
fi
go test "${cached[@]}" ${race[@]+"${race[@]}"} ${tags[@]+"${tags[@]}"} ${count1[@]+"${count1[@]}"} -timeout="$timeout" -coverprofile="$tmp/cached.out" -json "$@" || status=$?

# The sets of packages of the two runs are disjoint.
merge_profiles "$COVERPROFILE" "$tmp/uncached.out" "$tmp/cached.out"
exit "$status"
