#!/usr/bin/env bash
# validate_helm_flags.sh — CI gate ensuring Helm chart extraArgs stay in sync
# with the binary's registered flags.
#
# Two checks:
#   1) Render `helm template` with default values and verify every rendered
#      -flag=value argument corresponds to a flag the binary accepts.
#   2) Run the Go parity test (TestHelmExtraArgsFlagParity +
#      TestHelmExtraArgsCoverage) which validates the static values.yaml
#      against the source-level flag definitions.
#
# Usage:
#   ./scripts/ci/validate_helm_flags.sh          # full check (build + template + Go tests)
#   ./scripts/ci/validate_helm_flags.sh --quick   # Go tests only (no binary build)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

MODE="${1:-full}"

echo "=== Helm ↔ Binary Flag Parity Check ==="
echo ""

# -------------------------------------------------------------------
# Step 1: Go parity tests (source-level validation)
# -------------------------------------------------------------------
echo "--- Running Go parity tests ---"
go test ./cmd/proxy/ -run 'TestHelmExtraArgs' -count=1 -v
echo ""

if [ "$MODE" = "--quick" ]; then
  echo "OK (quick mode — skipped template rendering)"
  exit 0
fi

# -------------------------------------------------------------------
# Step 2: Build binary and validate rendered Helm template args
# -------------------------------------------------------------------
echo "--- Extracting valid flags from source ---"
VALID_FLAGS=$(grep -E 'fs\.(String|Int|Bool|Duration|Float64|Int64)\(' cmd/proxy/main.go \
  | sed -E 's/.*fs\.[A-Za-z0-9]+\("([^"]+)".*/\1/' \
  | sort -u)

echo "  $(echo "$VALID_FLAGS" | wc -l | tr -d ' ') flags registered"
echo ""

echo "--- Rendering Helm template ---"
RENDERED="$(helm template test-release charts/loki-vl-proxy/ 2>&1)"

# Extract -flag=value from rendered args lines (format: - "-flag=value").
# Only look at lines inside the args: block that start with - "-.
RENDERED_FLAGS=$(echo "$RENDERED" \
  | sed -n '/^[[:space:]]*args:/,/^[[:space:]]*[a-z]/p' \
  | grep -E '^\s+- "-' \
  | sed -E 's/.*"-([a-zA-Z][a-zA-Z0-9._-]*)=.*/\1/' \
  | sort -u)

echo "  $(echo "$RENDERED_FLAGS" | wc -l | tr -d ' ') unique flags in rendered template"
echo ""

echo "--- Validating rendered flags ---"
EXIT=0
while IFS= read -r flag; do
  [ -z "$flag" ] && continue
  if ! echo "$VALID_FLAGS" | grep -qx "$flag"; then
    echo "ERROR: rendered flag '$flag' is not a valid binary flag"
    EXIT=1
  fi
done <<< "$RENDERED_FLAGS"

if [ "$EXIT" -eq 0 ]; then
  echo "OK: All rendered Helm template flags are valid binary flags"
fi

exit "$EXIT"
