#!/usr/bin/env bash
# validate_helm_secrets.sh — CI gate for the chart's "secret from an existing
# Secret" values (adminAuth, deleteAuth, backendAuth, backendVersionAuth, cacheKeySecret, observability.otlp), and
# for the delete API's own token.
#
#   1) With each existingSecret set, the rendered manifest mounts the Secret key
#      as a file named by a -...-file flag (OTLP headers: a secretKeyRef env
#      var), and no secret reaches the process arguments.
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
  --set backendVersionAuth.existingSecret=version-sec \
  --set cacheKeySecret.existingSecret=cache-key-sec \
  --set observability.otlp.existingSecret=otlp-sec)"

expect() {
  if ! grep -qF -- "$1" <<<"$OUT"; then fail "rendered manifest is missing: $1"; fi
}
expect '"-server.admin-auth-token-file=/var/run/secrets/loki-vl-proxy/admin-auth/token"'
expect '"-backend-basic-auth-file=/var/run/secrets/loki-vl-proxy/backend-auth/basic-auth"'
expect '"-backend-version-basic-auth-file=/var/run/secrets/loki-vl-proxy/backend-version-auth/basic-auth"'
expect '"-cache-key-secret-file=/var/run/secrets/loki-vl-proxy/cache-key/secret"'
for pair in admin-auth:admin-sec:token backend-auth:backend-sec:basic-auth backend-version-auth:version-sec:basic-auth cache-key:cache-key-sec:secret; do
  IFS=: read -r volume name key <<<"$pair"
  block="$(grep -A5 -F -- "- name: secret-$volume" <<<"$OUT" || true)"
  grep -qF "secretName: \"$name\"" <<<"$block" || fail "secret-$volume does not mount Secret $name"
  grep -qF "key: \"$key\"" <<<"$block" || fail "secret-$volume does not use key $key"
done
block="$(grep -A5 -F -- "- name: OTLP_HEADERS" <<<"$OUT" || true)"
grep -qF 'name: "otlp-sec"' <<<"$block" || fail "OTLP_HEADERS does not reference Secret otlp-sec"
grep -qF 'key: "headers"' <<<"$block" || fail "OTLP_HEADERS does not use key headers"
if grep -qE '\$\((ADMIN_AUTH_TOKEN|DELETE_API_AUTH_TOKEN|BACKEND_BASIC_AUTH|BACKEND_VERSION_BASIC_AUTH|PEER_AUTH_TOKEN|CACHE_KEY_SECRET)\)|-(server\.admin-auth-token|delete-api-auth-token|backend-basic-auth|backend-version-basic-auth|peer-auth-token|cache-key-secret)=' <<<"$OUT"; then
  fail "a secret is still passed in the process arguments"
fi

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
must_fail --set backendVersionAuth.existingSecret=s --set extraArgs.backend-version-basic-auth=user:plain
must_fail --set adminAuth.existingSecret=s --set 'extraArgs.server\.admin-auth-token-file=/mnt/token'
must_fail --set backendAuth.existingSecret=s --set extraArgs.backend-basic-auth-file=/mnt/auth
must_fail --set backendVersionAuth.existingSecret=s --set extraArgs.backend-version-basic-auth-file=/mnt/auth
must_fail --set cacheKeySecret.existingSecret=s --set extraArgs.cache-key-secret=plain
must_fail --set cacheKeySecret.existingSecret=s --set extraArgs.cache-key-secret-file=/mnt/secret
must_fail --set peerCache.enabled=true --set extraArgs.peer-auth-token=plain
must_fail --set peerCache.enabled=true --set extraArgs.peer-auth-token-file=/mnt/token
must_fail --set observability.otlp.existingSecret=s --set observability.otlp.headers=a=b
must_fail --set observability.otlp.existingSecret=s --set extraArgs.otlp-headers=a=b

# The delete API needs its own token; enabling it leaves the admin listener alone.
must_fail --set extraArgs.delete-api-enabled=true
must_fail --set extraArgs.delete-api-enabled=true --set 'extraArgs.delete-api-auth-token=  '
must_fail --set extraArgs.delete-api-enabled=true --set adminAuth.existingSecret=s
must_fail --set deleteAuth.existingSecret=s --set extraArgs.delete-api-auth-token=plain
must_fail --set deleteAuth.existingSecret=s --set extraArgs.delete-api-auth-token-file=/mnt/token
DEL="$(helm template rel "$CHART" --set extraArgs.delete-api-enabled=true --set deleteAuth.existingSecret=del-sec)" ||
  fail "delete API with deleteAuth.existingSecret should render"
grep -qF '"-delete-api-auth-token-file=/var/run/secrets/loki-vl-proxy/delete-auth/token"' <<<"$DEL" || fail "deleteAuth.existingSecret does not render the flag"
block="$(grep -A5 -F -- "- name: secret-delete-auth" <<<"$DEL" || true)"
grep -qF 'secretName: "del-sec"' <<<"$block" || fail "secret-delete-auth does not mount Secret del-sec"
grep -qF '"-admin-listen=127.0.0.1:3101"' <<<"$DEL" || fail "enabling deletes moved the admin listener"
if grep -qE 'ADMIN_AUTH_TOKEN|server.admin-auth-token|secret-admin-auth' <<<"$DEL"; then fail "enabling deletes set an admin token"; fi
helm template rel "$CHART" --set extraArgs.delete-api-enabled=true --set extraArgs.delete-api-auth-token=plain >/dev/null 2>&1 ||
  fail "delete API with a plain delete token should render"
helm template rel "$CHART" --set extraArgs.delete-api-enabled=true --set extraArgs.delete-api-auth-token-file=/mnt/token >/dev/null 2>&1 ||
  fail "delete API with a delete token file should render"
helm template rel "$CHART" --set extraArgs.delete-api-enabled=false >/dev/null 2>&1 ||
  fail "delete API off should render without a token"

# Per-tenant limits need tenancy; default limits do not.
# (--set-json: helm's --set reads a value that starts with "{" as a list)
LIMITS='extraArgs.tenant-limits="{\"a\":{\"max_query_series\":1}}"'
TENANCY_ERR="$(helm template rel "$CHART" --set-json "$LIMITS" 2>&1 >/dev/null || true)"
grep -qF "tenancy is off" <<<"$TENANCY_ERR" || fail "tenant-limits without tenancy should fail rendering with the tenancy message: $TENANCY_ERR"
helm template rel "$CHART" --set-json "$LIMITS" --set-string 'extraArgs.tenant\.numeric-passthrough=true' >/dev/null 2>&1 ||
  fail "tenant-limits with numeric passthrough should render"
helm template rel "$CHART" --set-json "$LIMITS" --set-json 'extraArgs.tenant-map="{\"a\":{\"account_id\":\"1\",\"project_id\":\"0\"}}"' >/dev/null 2>&1 ||
  fail "tenant-limits with a tenant map should render"
helm template rel "$CHART" --set-json 'extraArgs.tenant-default-limits="{\"max_query_series\":1}"' >/dev/null 2>&1 ||
  fail "tenant-default-limits alone should render"

# --- 3) plain values still render --------------------------------------------
PLAIN="$(helm template rel "$CHART" \
  --set 'extraArgs.server\.admin-auth-token=plain-admin' \
  --set extraArgs.backend-basic-auth=user:plain)"
grep -qF '"-server.admin-auth-token=plain-admin"' <<<"$PLAIN" || fail "plain admin token no longer renders"
grep -qF '"-backend-basic-auth=user:plain"' <<<"$PLAIN" || fail "plain backend basic auth no longer renders"
if grep -qE 'ADMIN_AUTH_TOKEN|BACKEND_BASIC_AUTH|secrets/loki-vl-proxy' <<<"$PLAIN"; then fail "plain values must not add secret env vars or mounts"; fi

# --- defaults must not mention secret env vars or mounts ---------------------
if helm template rel "$CHART" | grep -qE 'ADMIN_AUTH_TOKEN|DELETE_API_AUTH_TOKEN|BACKEND_BASIC_AUTH|BACKEND_VERSION_BASIC_AUTH|OTLP_HEADERS|secrets/loki-vl-proxy'; then
  fail "default render must not reference secret env vars or mounts"
fi

if [ "$FAIL" -eq 0 ]; then echo "OK: chart existingSecret values render and conflict as designed"; fi
exit "$FAIL"
