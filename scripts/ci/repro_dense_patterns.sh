#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "[dense-patterns] root: ${ROOT_DIR}"
echo "[dense-patterns] config:"
echo "  DENSE_PATTERNS_TOTAL_LINES=${DENSE_PATTERNS_TOTAL_LINES:-80000}"
echo "  DENSE_PATTERNS_PATTERN_COUNT=${DENSE_PATTERNS_PATTERN_COUNT:-49}"
echo "  DENSE_PATTERNS_BATCH_SIZE=${DENSE_PATTERNS_BATCH_SIZE:-4000}"
echo "  DENSE_PATTERNS_RANGE_MINUTES=${DENSE_PATTERNS_RANGE_MINUTES:-90}"
echo "  DENSE_PATTERNS_STEP_SECONDS=${DENSE_PATTERNS_STEP_SECONDS:-60}"
echo "  DENSE_PATTERNS_LIMIT=${DENSE_PATTERNS_LIMIT:-50}"
echo "  DENSE_PATTERNS_REFRESH_LOOPS=${DENSE_PATTERNS_REFRESH_LOOPS:-6}"

pushd "${ROOT_DIR}/test/e2e-compat" >/dev/null
docker compose up -d --build
../../scripts/ci/wait_e2e_stack.sh 180
popd >/dev/null

LVP_DENSE_PATTERNS_REPRO=1 \
go test -count=1 -v -tags=e2e -run '^TestPatternsDenseRepro_FullRangeAndRefreshStability$' ./test/e2e-compat
