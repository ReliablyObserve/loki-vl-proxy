#!/usr/bin/env bash
# Run the full Loki vs VL+proxy read-path comparison benchmark.
#
# Prerequisites:
#   - Loki running at $LOKI_URL (default: http://localhost:3101)
#   - loki-vl-proxy running at $PROXY_URL (default: http://localhost:3100)
#   - Both have data ingested (use the e2e compose stack: cd test/e2e-compat && docker compose up -d)
#
# Usage:
#   ./bench/run-comparison.sh                          # full suite (all workloads, 10/50/100/500 clients)
#   ./bench/run-comparison.sh --workloads=small        # quick smoke test
#   ./bench/run-comparison.sh --clients=10,50          # fewer concurrency levels
#   ./bench/run-comparison.sh --duration=60s           # longer per-level runs
#   ./bench/run-comparison.sh --skip-loki              # proxy only (no Loki comparison)
#   ./bench/run-comparison.sh --version=v1.17.1        # tag results for tracking
#   PROXY_NO_CACHE_URL=http://localhost:3199 ./bench/run-comparison.sh  # pre-started no-cache proxy
#
# No-cache proxy auto-spawn:
#   If loki-vl-proxy binary is in $PATH or at $PROXY_BINARY, the script starts a no-cache
#   proxy instance automatically on port 3199 and kills it when done.
#
# All extra flags are forwarded to loki-bench.
set -euo pipefail

LOKI_URL="${LOKI_URL:-http://localhost:3101}"
PROXY_URL="${PROXY_URL:-http://localhost:3100}"
VL_URL="${VL_URL:-http://localhost:9428}"
LOKI_METRICS="${LOKI_METRICS:-}"
PROXY_METRICS="${PROXY_METRICS:-http://localhost:3100/metrics}"
VL_METRICS="${VL_METRICS:-}"
VL_DIRECT_URL="${VL_DIRECT_URL:-}"
PROXY_NO_CACHE_URL="${PROXY_NO_CACHE_URL:-}"
NO_CACHE_PORT="${NO_CACHE_PORT:-3199}"
NO_CACHE_PID=""

# Auto-detect Loki metrics if available.
if [ -z "$LOKI_METRICS" ]; then
  if curl -sf "$LOKI_URL/metrics" -o /dev/null 2>/dev/null; then
    LOKI_METRICS="$LOKI_URL/metrics"
    echo "✓ Loki metrics detected at $LOKI_METRICS"
  fi
fi

# Auto-detect VictoriaLogs metrics if available.
if [ -z "$VL_METRICS" ]; then
  if curl -sf "$VL_URL/metrics" -o /dev/null 2>/dev/null; then
    VL_METRICS="$VL_URL/metrics"
    echo "✓ VictoriaLogs metrics detected at $VL_METRICS"
  fi
fi

# Auto-detect VL native LogsQL API for 3-way comparison.
if [ -z "$VL_DIRECT_URL" ]; then
  if curl -sf "$VL_URL/select/logsql/field_names?query=*" -o /dev/null 2>/dev/null; then
    VL_DIRECT_URL="$VL_URL"
    echo "✓ VictoriaLogs native LogsQL detected at $VL_DIRECT_URL (4-way comparison enabled)"
  fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR/results}"

# Build the benchmark tool first (needed before no-cache proxy spawn check).
echo "Building loki-bench..."
(cd "$SCRIPT_DIR" && go build -o /tmp/loki-bench ./cmd/loki-bench/)
echo "✓ Built /tmp/loki-bench"
echo

# Auto-spawn no-cache proxy if not pre-configured and binary is available.
# A no-cache proxy runs with -cache-max=0 -cache-max-bytes=0 so every request
# hits VL — this measures the proxy translation overhead and raw VL query speed
# through the Loki-compatible API, without cache masking the numbers.
if [ -z "$PROXY_NO_CACHE_URL" ]; then
  PROXY_BINARY="${PROXY_BINARY:-}"
  if [ -z "$PROXY_BINARY" ]; then
    if command -v loki-vl-proxy &>/dev/null; then
      PROXY_BINARY="loki-vl-proxy"
    elif [ -f "$REPO_ROOT/dist/loki-vl-proxy" ]; then
      PROXY_BINARY="$REPO_ROOT/dist/loki-vl-proxy"
    elif [ -f "/tmp/loki-vl-proxy" ]; then
      PROXY_BINARY="/tmp/loki-vl-proxy"
    fi
  fi

  if [ -n "$PROXY_BINARY" ]; then
    echo "Starting no-cache proxy on port $NO_CACHE_PORT (binary: $PROXY_BINARY)..."
    "$PROXY_BINARY" \
      -listen=":$NO_CACHE_PORT" \
      -backend="$VL_URL" \
      -cache-ttl=1ns \
      -log-level=warn \
      &>/tmp/proxy-nocache.log &
    NO_CACHE_PID=$!
    trap 'kill "$NO_CACHE_PID" 2>/dev/null; echo "no-cache proxy stopped"' EXIT
    sleep 1
    if curl -sf "http://localhost:$NO_CACHE_PORT/loki/api/v1/labels" -o /dev/null 2>/dev/null; then
      PROXY_NO_CACHE_URL="http://localhost:$NO_CACHE_PORT"
      echo "✓ No-cache proxy ready at $PROXY_NO_CACHE_URL"
    else
      echo "⚠ No-cache proxy did not start — skipping cold-cache comparison"
      PROXY_NO_CACHE_URL=""
      kill "$NO_CACHE_PID" 2>/dev/null || true
      NO_CACHE_PID=""
    fi
  else
    echo "ℹ No loki-vl-proxy binary found — skipping cold-cache comparison"
    echo "  Build with: go build -o /tmp/loki-vl-proxy ./cmd/proxy/ && PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh"
  fi
fi

echo "════════════════════════════════════════════════════════════"
echo " loki-vl-proxy Read Performance Benchmark"
echo "════════════════════════════════════════════════════════════"
echo " Loki target:    $LOKI_URL"
echo " Proxy (warm):   $PROXY_URL"
echo " Proxy (cold):   ${PROXY_NO_CACHE_URL:-not configured}"
echo " VL backend:     ${VL_URL:-not configured}"
echo " VL native:      ${VL_DIRECT_URL:-not configured (2-way only)}"
echo " Loki metrics:   ${LOKI_METRICS:-not configured}"
echo " Proxy metrics:  ${PROXY_METRICS:-not configured}"
echo " VL metrics:     ${VL_METRICS:-not configured}"
echo " Output:         $OUTPUT_DIR"
echo "════════════════════════════════════════════════════════════"
echo

# Wait for an endpoint to be reachable (any HTTP response = up; circuit breaker 502 is ok).
wait_ready() {
  local url="$1"
  local name="$2"
  local max_wait=30
  local waited=0
  printf "Waiting for %s at %s" "$name" "$url"
  while true; do
    local code
    code=$(curl -so /dev/null -w "%{http_code}" "$url/loki/api/v1/labels" 2>/dev/null || echo "000")
    # Any non-zero, non-connection-refused HTTP code means the process is up.
    if [ "$code" != "000" ]; then
      break
    fi
    if [ $waited -ge $max_wait ]; then
      echo " TIMEOUT — is $name running?"
      exit 1
    fi
    printf "."
    sleep 2
    waited=$((waited + 2))
  done
  echo " ✓"
}

wait_ready "$LOKI_URL" "Loki"
wait_ready "$PROXY_URL" "proxy"

mkdir -p "$OUTPUT_DIR"

/tmp/loki-bench \
  --loki="$LOKI_URL" \
  --proxy="$PROXY_URL" \
  --proxy-no-cache="${PROXY_NO_CACHE_URL}" \
  --vl="$VL_URL" \
  --vl-direct="${VL_DIRECT_URL}" \
  --loki-metrics="$LOKI_METRICS" \
  --proxy-metrics="$PROXY_METRICS" \
  --vl-metrics="$VL_METRICS" \
  --output="$OUTPUT_DIR" \
  "$@"

echo
echo "════════════════════════════════════════════════════════════"
echo " Results saved to $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"/*.json "$OUTPUT_DIR"/*.md 2>/dev/null || true
echo "════════════════════════════════════════════════════════════"
