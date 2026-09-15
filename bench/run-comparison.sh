#!/usr/bin/env bash
# Run the full Loki vs VL+proxy read-path comparison benchmark.
#
# Prerequisites (defaults match test/e2e-compat compose ports):
#   - Loki running at $LOKI_URL (default: http://localhost:13101)
#   - loki-vl-proxy running at $PROXY_URL (default: http://localhost:13100)
#   - VictoriaLogs at $VL_URL (default: http://localhost:19428)
#   - Identical data seeded into both backends with bench/cmd/seed, on a stack
#     started with docker-compose.bench.yml layered on docker-compose.yml
#
# Equivalent-work guards (see bench/README.md):
#   - DATA_END (default "auto" when VictoriaLogs is reachable) pins every workload
#     window to the seeded data end instead of the wall clock.
#   - ENTRY_CHECK_TIMEOUT (default 45m) waits until Loki's line count over the
#     seeded span equals VictoriaLogs'; LOKI_FLUSH=true (default) first asks Loki
#     to flush its ingesters. ENTRY_CHECK_TIMEOUT=0 disables the check.
#   - VERIFY_STRICT=true (default) compares every query on Loki and every timed
#     proxy target (status, degraded answers, shape and content) before timing
#     and aborts the run on a mismatch.
#   - CACHE_MODE=warm (default) compares Loki with its results caches against
#     the cached proxy; CACHE_MODE=cold compares Loki with every results cache
#     off (test/e2e-compat/loki-bench-nocache-config.yaml) against proxies
#     without a response cache. loki-bench checks Loki's /config matches.
#   - MAX_ERROR_RATE=0 (default): any error or degraded answer in a timed run
#     stops the benchmark with a non-zero exit and NOT-PUBLISHABLE output.
#
# Usage:
#   ./bench/run-comparison.sh                          # full suite (all workloads, 10/50/100/500 clients)
#   ./bench/run-comparison.sh --workloads=small        # quick smoke test
#   ./bench/run-comparison.sh --clients=10,50          # fewer concurrency levels
#   ./bench/run-comparison.sh --duration=60s           # longer per-level runs
#   ./bench/run-comparison.sh --jitter=2h              # randomize time windows (realistic cache sim)
#   ./bench/run-comparison.sh --skip-loki              # proxy only (no Loki comparison; not publishable)
#   CACHE_MODE=cold ./bench/run-comparison.sh          # cold comparison (Loki started with the no-cache config)
#   ./bench/run-comparison.sh --version=v1.17.1        # tag results for tracking
#   PROXY_NO_CACHE_URL=http://localhost:3199 ./bench/run-comparison.sh  # pre-started no-cache proxy
#   PROXY_PARTIAL_URL=http://localhost:3198 ./bench/run-comparison.sh   # pre-started partial-cache proxy
#
# Partial-cache proxy auto-spawn (CACHE_MODE=warm):
#   Spawned automatically with -cache-ttl=6s (≈20% hit rate for 30s run) and
#   coalescer enabled (≈25% backend forwarding at moderate concurrency).
#   Models a partially-warm production scenario between warm and cold extremes.
#   Override port: PARTIAL_PORT=3198 (default)
#
# No-cache proxy auto-spawn (CACHE_MODE=cold):
#   The script builds loki-vl-proxy (or uses $PROXY_BINARY), starts a no-cache
#   proxy instance on port 3199 and kills it when done.
#
# All extra flags are forwarded to loki-bench.
set -euo pipefail

LOKI_URL="${LOKI_URL:-http://localhost:13101}"
PROXY_URL="${PROXY_URL:-http://localhost:13100}"
VL_URL="${VL_URL:-http://localhost:19428}"
LOKI_METRICS="${LOKI_METRICS:-}"
PROXY_METRICS="${PROXY_METRICS:-${PROXY_URL}/metrics}"
DATA_END="${DATA_END:-}"
ENTRY_CHECK_TIMEOUT="${ENTRY_CHECK_TIMEOUT:-45m}"
LOKI_FLUSH="${LOKI_FLUSH:-true}"
VERIFY_STRICT="${VERIFY_STRICT:-true}"
CACHE_MODE="${CACHE_MODE:-warm}"
MAX_ERROR_RATE="${MAX_ERROR_RATE:-0}"
case "$CACHE_MODE" in
  warm|cold) ;;
  *) echo "CACHE_MODE must be warm or cold, got '$CACHE_MODE'" >&2; exit 1 ;;
esac
# The cache mode decides which proxies are spawned and whether the machinery
# pass runs, so it is set only through the environment.
for arg in "$@"; do
  case "$arg" in
    --cache-mode|--cache-mode=*|--max-error-rate|--max-error-rate=*)
      echo "Set CACHE_MODE / MAX_ERROR_RATE in the environment instead of passing $arg" >&2
      exit 1 ;;
  esac
done
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

# Pin workload windows to the seeded data end (max(_time) in VictoriaLogs).
# Without it windows follow the wall clock and drift past historical data.
if [ -z "$DATA_END" ]; then
  if [ -n "$VL_DIRECT_URL" ]; then
    DATA_END="auto"
  else
    echo "⚠ VictoriaLogs not reachable: workload windows follow the wall clock (set DATA_END to pin them)"
  fi
fi
DATA_ARGS=()
if [ -n "$DATA_END" ]; then
  DATA_ARGS+=("--data-end=$DATA_END")
  if [ "$DATA_END" = "auto" ]; then
    DATA_ARGS+=("--data-start=auto")
  fi
fi

# Entry check and strict shape verification run once, in the main pass.
CHECK_ARGS=()
if [ -n "$DATA_END" ] && [ "$ENTRY_CHECK_TIMEOUT" != "0" ]; then
  CHECK_ARGS+=("--wait-ingested=$ENTRY_CHECK_TIMEOUT")
  if [ "$LOKI_FLUSH" = "true" ]; then
    # Ask Loki's ingesters to flush in-memory chunks so seeded history becomes
    # visible to the read path sooner; the entry check still waits for parity.
    curl -s -o /dev/null -X POST "$LOKI_URL/flush" 2>/dev/null || true
  fi
fi
if [ "$VERIFY_STRICT" = "true" ]; then
  CHECK_ARGS+=("--verify-strict")
fi
MODE_ARGS=("--cache-mode=$CACHE_MODE" "--max-error-rate=$MAX_ERROR_RATE")

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
if [ -z "$PROXY_NO_CACHE_URL" ] && [ "$CACHE_MODE" = "cold" ]; then
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
if [ -z "$PROXY_PARTIAL_URL" ] && [ "$CACHE_MODE" = "warm" ]; then
  if [ -z "${PROXY_BINARY:-}" ]; then
    echo "Building loki-vl-proxy for partial-cache instance..."
    go build -o /tmp/loki-vl-proxy "$REPO_ROOT/cmd/proxy/"
    PROXY_BINARY="/tmp/loki-vl-proxy"
  fi
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
echo " Cache mode:     $CACHE_MODE"
echo " Proxy (warm):   $([ "$CACHE_MODE" = "warm" ] && echo "$PROXY_URL" || echo "not timed in cold mode")"
echo " Proxy (cold):   $([ "$CACHE_MODE" = "cold" ] && echo "${PROXY_NO_CACHE_URL:-not configured}" || echo "not timed in warm mode")"
echo " Proxy (partial): $([ "$CACHE_MODE" = "warm" ] && echo "${PROXY_PARTIAL_URL:-not configured (cache-ttl=6s, ~20% hit rate)}" || echo "not timed in cold mode")"
echo " VL backend:     ${VL_URL:-not configured}"
echo " VL native:      ${VL_DIRECT_URL:-not configured (2-way only)}"
echo " Loki metrics:   ${LOKI_METRICS:-not configured}"
echo " Proxy metrics:  ${PROXY_METRICS:-not configured}"
echo " VL metrics:     ${VL_METRICS:-not configured}"
echo " Data end:       ${DATA_END:-wall clock}"
echo " Entry check:    $([ -n "$DATA_END" ] && [ "$ENTRY_CHECK_TIMEOUT" != "0" ] && echo "up to $ENTRY_CHECK_TIMEOUT" || echo "disabled")"
echo " Verification:   $([ "$VERIFY_STRICT" = "true" ] && echo "strict (every timed proxy target vs Loki)" || echo "disabled (results not publishable)")"
echo " Error gate:     max error and degraded-answer rate $MAX_ERROR_RATE"
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
  ${DATA_ARGS[@]+"${DATA_ARGS[@]}"} \
  ${CHECK_ARGS[@]+"${CHECK_ARGS[@]}"} \
  "${MODE_ARGS[@]}" \
  "$@"

# --- Machinery pass: unique windows defeat caches + coalescer ---------------
# Every request is shifted back by a distinct number of whole steps inside the
# data, so step alignment cannot map two requests onto one window and the
# singleflight coalescer cannot answer one request from another. Queries with
# fewer distinct windows inside the data than the highest client count are
# excluded by loki-bench. Only equal against targets without response caches,
# so it runs in CACHE_MODE=cold only (loki-bench refuses --unique-windows
# otherwise). It repeats the entry check and strict verification, so its
# results carry the same publishability guarantees.
# Forwards the same workload/clients/duration/version flags from $@ but
# overrides --output and injects --unique-windows.
# ------------------------------------------------------------------------------
SKIP_MACHINERY="${SKIP_MACHINERY:-false}"
if [ "$CACHE_MODE" != "cold" ] && [ "$SKIP_MACHINERY" != "true" ]; then
  echo
  echo "ℹ Machinery pass skipped: unique windows need CACHE_MODE=cold"
  SKIP_MACHINERY=true
fi
if [ "$SKIP_MACHINERY" != "true" ]; then
  # Build passthrough args: drop --output=*, --unique-windows, --pprof-*,
  # --proxy-partial*, --proxy-coalescer*, and --proxy-*-metrics flags
  # (those targets are not run in machinery mode).
  MACHINERY_PASSTHROUGH=()
  for arg in "$@"; do
    case "$arg" in
      --output=*|--unique-windows|--unique-windows=*) ;;
      --proxy-partial*|--proxy-coalescer*) ;;
      --pprof-*) ;;
      *) MACHINERY_PASSTHROUGH+=("$arg") ;;
    esac
  done

  MACHINERY_DIR="$OUTPUT_DIR/machinery"
  mkdir -p "$MACHINERY_DIR"

  echo
  echo "════════════════════════════════════════════════════════════"
  echo " Machinery Pass — unique whole-step windows, no cache/coalescer benefit"
  echo " (raw proxy translation + HTTP overhead)"
  echo "════════════════════════════════════════════════════════════"

  /tmp/loki-bench \
    --loki="$LOKI_URL" \
    --proxy="$PROXY_URL" \
    --proxy-no-cache="${PROXY_NO_CACHE_URL}" \
    --vl="$VL_URL" \
    --vl-direct="${VL_DIRECT_URL}" \
    --loki-metrics="$LOKI_METRICS" \
    --proxy-metrics="$PROXY_METRICS" \
    --vl-metrics="$VL_METRICS" \
    --unique-windows \
    --output="$MACHINERY_DIR" \
    ${DATA_ARGS[@]+"${DATA_ARGS[@]}"} \
    ${CHECK_ARGS[@]+"${CHECK_ARGS[@]}"} \
    "${MODE_ARGS[@]}" \
    ${MACHINERY_PASSTHROUGH[@]+"${MACHINERY_PASSTHROUGH[@]}"}
fi

echo
echo "════════════════════════════════════════════════════════════"
echo " Results saved to $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"/*.json "$OUTPUT_DIR"/*.md 2>/dev/null || true
if [ "$SKIP_MACHINERY" != "true" ]; then
  echo "  Machinery: $MACHINERY_DIR"
  ls -lh "$MACHINERY_DIR"/*.md 2>/dev/null || true
fi
echo "════════════════════════════════════════════════════════════"
