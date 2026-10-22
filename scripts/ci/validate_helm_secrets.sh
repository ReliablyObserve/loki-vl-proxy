#!/usr/bin/env bash
# validate_helm_secrets.sh — CI gate for the chart's "secret from an existing
# Secret" values (adminAuth, backendAuth, observability.otlp).
#
#   1) With each existingSecret set, the rendered manifest references the Secret
#      through a secretKeyRef env var and contains no plain secret value.
#   2) Setting the plain value and the existingSecret together fails rendering.
#   3) Plain values keep rendering exactly as before.
#
# Usage: ./scripts/ci/validate_helm_secrets.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"
CHART=charts/loki-vl-proxy
FAIL=0

fail() { echo "FAIL: $*"; FAIL=1; }

# --- 1) existingSecret renders a secretKeyRef and no plain value -------------
OUT="$(helm template rel "$CHART" \
  --set adminAuth.existingSecret=admin-sec \
  --set backendAuth.existingSecret=backend-sec \
  --set observability.otlp.existingSecret=otlp-sec)"

expect() {
  if ! grep -qF -- "$1" <<<"$OUT"; then fail "rendered manifest is missing: $1"; fi
}
expect '"-server.admin-auth-token=$(ADMIN_AUTH_TOKEN)"'
expect '"-backend-basic-auth=$(BACKEND_BASIC_AUTH)"'
for pair in ADMIN_AUTH_TOKEN:admin-sec:token BACKEND_BASIC_AUTH:backend-sec:basic-auth OTLP_HEADERS:otlp-sec:headers; do
  IFS=: read -r var name key <<<"$pair"
  block="$(grep -A5 -F -- "- name: $var" <<<"$OUT" || true)"
  grep -qF "name: \"$name\"" <<<"$block" || fail "$var does not reference Secret $name"
  grep -qF "key: \"$key\"" <<<"$block" || fail "$var does not use key $key"
done

# Custom keys are honoured.
OUT2="$(helm template rel "$CHART" --set adminAuth.existingSecret=s --set adminAuth.existingSecretKey=custom)"
grep -qF 'key: "custom"' <<<"$OUT2" || fail "adminAuth.existingSecretKey is ignored"

# --- 2) plain value + existingSecret must fail -------------------------------
must_fail() {
  if helm template rel "$CHART" "$@" >/dev/null 2>&1; then
    fail "rendering should have failed for: $*"
  fi
}
must_fail --set adminAuth.existingSecret=s --set 'extraArgs.server\.admin-auth-token=plain'
must_fail --set backendAuth.existingSecret=s --set extraArgs.backend-basic-auth=user:plain
must_fail --set observability.otlp.existingSecret=s --set observability.otlp.headers=a=b
must_fail --set observability.otlp.existingSecret=s --set extraArgs.otlp-headers=a=b

# --- 3) plain values still render --------------------------------------------
PLAIN="$(helm template rel "$CHART" \
  --set 'extraArgs.server\.admin-auth-token=plain-admin' \
  --set extraArgs.backend-basic-auth=user:plain)"
grep -qF '"-server.admin-auth-token=plain-admin"' <<<"$PLAIN" || fail "plain admin token no longer renders"
grep -qF '"-backend-basic-auth=user:plain"' <<<"$PLAIN" || fail "plain backend basic auth no longer renders"
if grep -qE 'ADMIN_AUTH_TOKEN|BACKEND_BASIC_AUTH' <<<"$PLAIN"; then fail "plain values must not add the secretKeyRef env vars"; fi

# --- defaults must not mention the new env vars ------------------------------
if helm template rel "$CHART" | grep -qE 'ADMIN_AUTH_TOKEN|BACKEND_BASIC_AUTH|OTLP_HEADERS'; then
  fail "default render must not reference the secret env vars"
fi

if [ "$FAIL" -eq 0 ]; then echo "OK: chart existingSecret values render and conflict as designed"; fi
exit "$FAIL"
