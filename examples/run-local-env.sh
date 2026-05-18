#!/usr/bin/env bash
# loki-vl-proxy — standalone local run script (env-file-based configuration)
#
# This script sources examples/loki-vl-proxy-full.env for the options that
# have env-var form, then passes the remaining flag-only options on the
# command line.  Edit the env file for most settings; edit the args below
# only for options that have no env-var equivalent.
#
# Usage:
#   chmod +x examples/run-local-env.sh
#   ./examples/run-local-env.sh
#
# Or point to a custom env file:
#   ENV_FILE=/etc/loki-vl-proxy/loki-vl-proxy.env ./examples/run-local-env.sh

set -euo pipefail

BINARY="${BINARY:-./loki-vl-proxy}"
ENV_FILE="${ENV_FILE:-$(dirname "$0")/loki-vl-proxy-full.env}"

# Load env vars from the file.
# Strips comment lines and blank lines; exports the rest.
if [[ -f "$ENV_FILE" ]]; then
  set -o allexport
  # shellcheck disable=SC1090
  source <(grep -v '^\s*#' "$ENV_FILE" | grep -v '^\s*$')
  set +o allexport
else
  echo "env file not found: $ENV_FILE" >&2
  exit 1
fi

# ── Flag-only options (no env-var form) ───────────────────────────────────────
# These cannot be set via environment variables and must be passed as flags.

args=()

# Grafana 10+ requires 3-tuple [timestamp, line, metadata] stream values
# for per-line label context in the log details panel.
args+=( -emit-structured-metadata=true )

# Grafana Logs Drilldown patterns tab.
args+=( -patterns-enabled=true )

# Warm patterns cache from successful query responses.
# args+=( -patterns-autodetect-from-queries=true )

# Persist patterns across restarts.
# args+=( -patterns-persist-path=/tmp/loki-vl-proxy-patterns.json )

# ── Tenant map hot-reload (only when TENANT_MAP_FILE is set in env file) ──────
# args+=( -tenant-map-reload-interval=30s )

# ── In-memory cache ───────────────────────────────────────────────────────────
# args+=( -cache-ttl=60s )
# args+=( -cache-max-bytes=104857600 )   # 100 MiB
# args+=( -cache-disabled=false )

# ── Disk cache (L2) ───────────────────────────────────────────────────────────
# args+=( -disk-cache-path=/tmp/loki-vl-proxy-cache.db )
# args+=( -disk-cache-max-bytes=10737418240 )   # 10 GiB
# args+=( -disk-cache-compress=true )

# ── query_range splitting & parallelism ────────────────────────────────────────
# args+=( -query-range-split-interval=1h )
# args+=( -query-range-adaptive-parallel=true )
# args+=( -query-range-adaptive-min-parallel=2 )
# args+=( -query-range-adaptive-max-parallel=8 )
# args+=( -query-range-history-cache-ttl=24h )

# ── Backend connection ─────────────────────────────────────────────────────────
# args+=( -backend-timeout=120s )
# args+=( -backend-compression=auto )
# args+=( -backend-tls-skip-verify=false )

# ── HTTP server hardening ──────────────────────────────────────────────────────
# args+=( -max-concurrent=100 )
# args+=( -rate-limit-per-second=50 )
# args+=( -rate-limit-burst=100 )

# ── TLS server ─────────────────────────────────────────────────────────────────
# args+=( -tls-cert-file=/path/to/tls.crt )
# args+=( -tls-key-file=/path/to/tls.key )

# ── Admin / debug ──────────────────────────────────────────────────────────────
# args+=( -server.enable-pprof=false )

# ── Logging ────────────────────────────────────────────────────────────────────
# LOG_LEVEL has no env-var form; set it here.
args+=( -log-level=info )

exec "$BINARY" "${args[@]}"
