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
#   ./bench/run-comparison.sh --jitter=2h              # randomize time windows (realistic cache sim)
#   ./bench/run-comparison.sh --skip-loki              # proxy only (no Loki comparison)
#   ./bench/run-comparison.sh --version=v1.17.1        # tag results for tracking
#   PROXY_NO_CACHE_URL=http://localhost:3199 ./bench/run-comparison.sh  # pre-started no-cache proxy
#   PROXY_PARTIAL_URL=http://localhost:3198 ./bench/run-comparison.sh   # pre-started partial-cache proxy
#
# Partial-cache proxy auto-spawn:
#   Spawned automatically with -cache-ttl=6s (≈20% hit rate for 30s run) and
#   coalescer enabled (≈25% backend forwarding at moderate concurrency).
#   Models a partially-warm production scenario between warm and cold extremes.
#   Override port: PARTIAL_PORT=3198 (default)
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
PROXY_PARTIAL_URL="${PROXY_PARTIAL_URL:-}"
NO_CACHE_PORT="${NO_CACHE_PORT:-3199}"
PARTIAL_PORT="${PARTIAL_PORT:-3198}"
NO_CACHE_PID=""
PARTIAL_PID=""

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
# A no-cache proxy runs with -cache-disabled so every request hits VL — this
# measures the proxy translation overhead and raw VL query speed through the
# Loki-compatible API, without cache masking the numbers.
#
# We always build the proxy binary from source before spawning it. This
# ensures the no-cache instance runs the same code as the warm proxy and has
# all current flags (including -server.enable-pprof). Relying on a pre-built
# binary risks using a stale build that lacks recently added flags.
if [ -z "$PROXY_NO_CACHE_URL" ]; then
  PROXY_BINARY="${PROXY_BINARY:-}"
  if [ -z "$PROXY_BINARY" ]; then
    echo "Building loki-vl-proxy for no-cache instance..."
    go build -o /tmp/loki-vl-proxy "$REPO_ROOT/cmd/proxy/"
    echo "✓ Built /tmp/loki-vl-proxy"
    PROXY_BINARY="/tmp/loki-vl-proxy"
  fi

  if [ -n "$PROXY_BINARY" ]; then
    # Find a free port starting from NO_CACHE_PORT (avoids killing unrelated processes).
    while lsof -ti:"$NO_CACHE_PORT" &>/dev/null 2>&1; do
      echo "Port $NO_CACHE_PORT in use — trying $((NO_CACHE_PORT + 1))..."
      NO_CACHE_PORT=$((NO_CACHE_PORT + 1))
    done

    echo "Starting no-cache proxy on port $NO_CACHE_PORT (binary: $PROXY_BINARY)..."
    "$PROXY_BINARY" \
      -listen=":$NO_CACHE_PORT" \
      -backend="$VL_URL" \
      -cache-disabled \
      -rate-limit-per-second=0 \
      -max-concurrent=0 \
      -server.enable-pprof \
      "-server.admin-auth-token=${PPROF_AUTH_TOKEN:-bench-pprof-token}" \
      -log-level=warn \
      -cb-fail-threshold=1000 \
      -cb-open-duration=1s \
      &>/tmp/proxy-nocache.log &
    NO_CACHE_PID=$!
    trap 'kill "$NO_CACHE_PID" 2>/dev/null; echo "no-cache proxy stopped"' EXIT
    sleep 2
    if curl -sf "http://localhost:$NO_CACHE_PORT/loki/api/v1/labels" -o /dev/null 2>/dev/null; then
      PROXY_NO_CACHE_URL="http://localhost:$NO_CACHE_PORT"
      echo "✓ No-cache proxy ready at $PROXY_NO_CACHE_URL"
    else
      echo "⚠ No-cache proxy did not start — skipping cold-cache comparison"
      echo "  Last log lines:"
      tail -5 /tmp/proxy-nocache.log 2>/dev/null | sed 's/^/  /'
      PROXY_NO_CACHE_URL=""
      kill "$NO_CACHE_PID" 2>/dev/null || true
      NO_CACHE_PID=""
    fi
  else
    echo "ℹ No loki-vl-proxy binary found — skipping cold-cache comparison"
    echo "  Build with: go build -o /tmp/loki-vl-proxy ./cmd/proxy/ && PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh"
  fi
fi

# Auto-spawn partial-cache proxy if not pre-configured and binary is available.
# Partial-cache proxy uses a short cache TTL (6s) so only ~20% of requests hit
# the cache during a 30s run. The singleflight coalescer remains enabled, giving
# ~25% backend forwarding at moderate concurrency. This models a partially-warm
# production instance between the warm (high hit rate) and cold (0% hit rate) extremes.
if [ -z "$PROXY_PARTIAL_URL" ]; then
  PROXY_BINARY="${PROXY_BINARY:-/tmp/loki-vl-proxy}"
  if [ -n "$PROXY_BINARY" ] && [ -x "$PROXY_BINARY" ]; then
    # Find a free port.
    while lsof -ti:"$PARTIAL_PORT" &>/dev/null 2>&1; do
      echo "Port $PARTIAL_PORT in use — trying $((PARTIAL_PORT + 1))..."
      PARTIAL_PORT=$((PARTIAL_PORT + 1))
    done

    echo "Starting partial-cache proxy on port $PARTIAL_PORT (cache-ttl=6s, coalescer=on)..."
    "$PROXY_BINARY" \
      -listen=":$PARTIAL_PORT" \
      -backend="$VL_URL" \
      -cache-ttl=6s \
      -query-range-history-cache-ttl=6s \
      -query-range-recent-cache-ttl=0 \
      -rate-limit-per-second=0 \
      -rate-limit-burst=0 \
      -server.enable-pprof \
      "-server.admin-auth-token=${PPROF_AUTH_TOKEN:-bench-pprof-token}" \
      -log-level=warn \
      -cb-fail-threshold=1000 \
      -cb-open-duration=1s \
      &>/tmp/proxy-partial.log &
    PARTIAL_PID=$!
    OLD_TRAP=$(trap -p EXIT | sed "s/trap -- '//;s/' EXIT//")
    trap "${OLD_TRAP}; kill \"$PARTIAL_PID\" 2>/dev/null; echo \"partial-cache proxy stopped\"" EXIT
    sleep 2
    if curl -sf "http://localhost:$PARTIAL_PORT/loki/api/v1/labels" -o /dev/null 2>/dev/null; then
      PROXY_PARTIAL_URL="http://localhost:$PARTIAL_PORT"
      echo "✓ Partial-cache proxy ready at $PROXY_PARTIAL_URL"
    else
      echo "⚠ Partial-cache proxy did not start — skipping partial-cache comparison"
      echo "  Last log lines:"
      tail -5 /tmp/proxy-partial.log 2>/dev/null | sed 's/^/  /'
      PROXY_PARTIAL_URL=""
      kill "$PARTIAL_PID" 2>/dev/null || true
      PARTIAL_PID=""
    fi
  else
    echo "ℹ No loki-vl-proxy binary at $PROXY_BINARY — skipping partial-cache comparison"
  fi
fi

echo "════════════════════════════════════════════════════════════"
echo " loki-vl-proxy Read Performance Benchmark"
echo "════════════════════════════════════════════════════════════"
echo " Loki target:    $LOKI_URL"
echo " Proxy (warm):   $PROXY_URL"
echo " Proxy (cold):   ${PROXY_NO_CACHE_URL:-not configured}"
echo " Proxy (partial): ${PROXY_PARTIAL_URL:-not configured (cache-ttl=6s, ~20% hit rate)}"
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
  --proxy-partial="${PROXY_PARTIAL_URL}" \
  --vl="$VL_URL" \
  --vl-direct="${VL_DIRECT_URL}" \
  --loki-metrics="$LOKI_METRICS" \
  --proxy-metrics="$PROXY_METRICS" \
  --vl-metrics="$VL_METRICS" \
  --proxy-partial-metrics="${PROXY_PARTIAL_URL:+${PROXY_PARTIAL_URL}/metrics}" \
  --pprof-proxy="$PROXY_URL" \
  --pprof-no-cache="${PROXY_NO_CACHE_URL}" \
  --pprof-partial="${PROXY_PARTIAL_URL}" \
  --pprof-auth-token="${PPROF_AUTH_TOKEN:-bench-pprof-token}" \
  --output="$OUTPUT_DIR" \
  "$@"

echo
echo "════════════════════════════════════════════════════════════"
echo " Results saved to $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"/*.json "$OUTPUT_DIR"/*.md 2>/dev/null || true
echo "════════════════════════════════════════════════════════════"
