#!/usr/bin/env bash
# Memory-capped local unit test run. Skips the tests that take 5s or more
# (they run in CI, which does not pass -short). Pass packages and go test flags
# to narrow it, e.g. scripts/test-short.sh -run 'TestLabel' ./internal/proxy
set -euo pipefail
export GOMEMLIMIT="${GOMEMLIMIT:-3GiB}" GOMAXPROCS="${GOMAXPROCS:-4}"
if [ "$#" -eq 0 ]; then
  set -- ./...
fi
exec go test -short -count=1 -p 1 "$@"
