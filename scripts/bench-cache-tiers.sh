#!/usr/bin/env bash
#
# bench-cache-tiers.sh — exercise all four loki-vl-proxy cache tiers (L0 hot
# index, L1 memory, L2 disk, L3 peer) against the e2e-compat compose stack and
# print before/after counter deltas per tier.
#
# Modes:
#   l1     Cold-vs-warm — same /label/{name}/values call N times against main
#          proxy. Hits the unified per-key cache (L0 hot-index, L1 memory).
#   l2     L2 promotion across restart — warm a /label/{name}/values call,
#          `docker compose restart` the main proxy, re-run; reports L2 disk
#          hit on second call (L1 in-memory is gone after restart).
#   l3     L3 cross-peer — warm a key on peer-a, then query peer-b for the
#          same key; reports peer-b's L3 hit count.
#   long   Long-range windowed reuse — 7d /query_range, then 7d-1h overlap,
#          reports loki_vl_proxy_cache_window_hits_total delta.
#   lb     Drive LB_RUNS (default 30) requests through the vmauth-ring LB on
#          port 13200. vmauth round-robins across proxy-0/1/2; reports the
#          full cache cascade per proxy so you can see L0/L1/L2/L3 reuse and
#          peer write-through under realistic distributed traffic.
#   load   High-rate concurrent load through the LB. By default fires
#          LOAD_TOTAL (600) requests at LOAD_PARALLEL (50) workers; set
#          SUSTAINED_SECONDS=N to drive traffic continuously for N seconds
#          instead (recommended: 90+ to populate rate()[1m] dashboard
#          panels with a visible steady plateau). Rotates through
#          LOAD_FAN_KEYS (default app,job,service_name,namespace,level) so
#          multiple distinct cache keys are exercised at once.
#   all    Run l1, then l2, then l3, then long sequentially.
#
# For a Drilldown endpoint × filter timing matrix (different concern —
# query performance, not cache tier behaviour), use bench/drilldown-filter-matrix.sh.
#
# Why label-values for L1/L2/L3 and query_range only for long-range?
# The unified per-key cache (loki_vl_proxy_cache_tier_*) wraps label/series
# lookups. /query_range goes through a separate windowed cache
# (loki_vl_proxy_cache_window_*). The L1/L2/L3 modes exercise the per-key
# cache because that's what the tier counters track.
#
# Required: jq, docker, curl
# Optional env overrides:
#   PROXY_MAIN_URL      (default http://localhost:13100)
#   PROXY_PEER_A_URL    (default http://localhost:13150)
#   PROXY_PEER_B_URL    (default http://localhost:13151)
#   QUERY               (default {job=~".+"})
#   COLD_WARM_RUNS      (default 6 — total runs in l1 mode)
#   COMPOSE_DIR         (default test/e2e-compat)
#   MAIN_SERVICE        (default loki-vl-proxy)

set -euo pipefail

PROXY_MAIN_URL="${PROXY_MAIN_URL:-http://localhost:13100}"
PROXY_PEER_A_URL="${PROXY_PEER_A_URL:-http://localhost:13150}"
PROXY_PEER_B_URL="${PROXY_PEER_B_URL:-http://localhost:13151}"
PROXY_LB_URL="${PROXY_LB_URL:-http://localhost:13200}"
LB_RUNS="${LB_RUNS:-30}"
LB_CONCURRENCY="${LB_CONCURRENCY:-1}"
# High-rate mode defaults: 600 requests at 50 in-flight concurrency
# (~hundreds of req/s on a laptop). Tune with LOAD_TOTAL / LOAD_PARALLEL.
LOAD_TOTAL="${LOAD_TOTAL:-600}"
LOAD_PARALLEL="${LOAD_PARALLEL:-50}"
LOAD_FAN_KEYS="${LOAD_FAN_KEYS:-app,job,service_name,namespace,level}"
# Mix of real log-query LogQL strings the load mode rotates through so
# the dashboard sees actual query traffic (not just metadata calls).
# These exercise stream selectors, line filters, and parsers — the full
# query_range translation + windowed-cache path that Drilldown/Explore hit.
LOAD_QUERIES_DEFAULT='{job=~".+"}
{service_name=~".+"} |= "GET"
{service_name=~".+"} |= "POST"
{job=~".+"} | json | level="info"
{job=~".+"} | json | duration > 100
sum by (service_name) (rate({job=~".+"}[1m]))
sum by (level) (rate({service_name=~".+"}[5m]))
count_over_time({job=~".+"}[1m])'
LOAD_QUERIES="${LOAD_QUERIES:-$LOAD_QUERIES_DEFAULT}"
LOAD_WINDOW_SECONDS="${LOAD_WINDOW_SECONDS:-3600}"
LOAD_STEP="${LOAD_STEP:-15s}"

# Endpoint mix for the load mode. Each call type exercises a different
# cache path in the proxy. Weights below control relative frequency; the
# mix is normalised so sums need not be 1. Set MIX_<KIND>=0 to disable.
MIX_QUERY_RANGE="${MIX_QUERY_RANGE:-40}"
MIX_QUERY="${MIX_QUERY:-15}"
MIX_LABELS="${MIX_LABELS:-10}"
MIX_LABEL_VALUES="${MIX_LABEL_VALUES:-10}"
MIX_SERIES="${MIX_SERIES:-5}"
MIX_DETECTED_LABELS="${MIX_DETECTED_LABELS:-5}"
MIX_DETECTED_FIELDS="${MIX_DETECTED_FIELDS:-5}"
MIX_INDEX_STATS="${MIX_INDEX_STATS:-5}"
MIX_INDEX_VOLUME="${MIX_INDEX_VOLUME:-5}"
# Sustained mode: drive traffic for SUSTAINED_SECONDS instead of a fixed
# request count. Dashboard rate() panels use [1m] / [5m] windows so a
# sub-minute burst won't register meaningfully — set to >= 90 to see a
# steady plateau in Grafana.
SUSTAINED_SECONDS="${SUSTAINED_SECONDS:-0}"
# pprof capture during the load mode. Set CAPTURE_PPROF=1 to fetch CPU
# (duration=PPROF_CPU_SECONDS), heap, allocs, goroutine, mutex, block
# profiles from all 3 ring proxies into PPROF_DIR for offline analysis
# with `go tool pprof <file>`. Requires the proxies to have
# -server.enable-pprof + -server.admin-auth-token (the compose stack does).
CAPTURE_PPROF="${CAPTURE_PPROF:-0}"
PPROF_DIR="${PPROF_DIR:-test/e2e-compat/profiles/$(date +%Y%m%dT%H%M%S)}"
PPROF_CPU_SECONDS="${PPROF_CPU_SECONDS:-30}"
PPROF_AUTH_TOKEN="${PPROF_AUTH_TOKEN:-bench-pprof-token}"
QUERY="${QUERY:-{job=~\".+\"}}"
COLD_WARM_RUNS="${COLD_WARM_RUNS:-6}"
COMPOSE_DIR="${COMPOSE_DIR:-test/e2e-compat}"
MAIN_SERVICE="${MAIN_SERVICE:-loki-vl-proxy}"

for tool in jq curl docker; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    exit 1
  fi
done

# Optional: if the local shell wraps curl with a token-counting proxy (e.g. RTK)
# that truncates large output, run curl through `rtk proxy` to bypass the wrapper.
# Set BENCH_CURL_BIN to override (e.g. "curl" to force direct invocation).
if [[ -n "${BENCH_CURL_BIN:-}" ]]; then
  # shellcheck disable=SC2206
  CURL_BIN=( ${BENCH_CURL_BIN} )
elif command -v rtk >/dev/null 2>&1; then
  CURL_BIN=( rtk proxy curl )
else
  CURL_BIN=( curl )
fi

now_ns() {
  printf '%s\n' "$(($(date +%s) * 1000000000))"
}

# Fixed window for unified-cache tests so the cache key (start/end-derived) is
# stable across repeated calls — otherwise each call gets a fresh key and
# always misses. The long-range window test uses its own fresh `now` because
# it tests overlap, not key stability.
BENCH_END_NS=$(now_ns)
BENCH_START_NS=$(( BENCH_END_NS - 3600 * 1000000000 ))
BENCH_LABEL_NAME="${BENCH_LABEL_NAME:-job}"

# Read a single Prometheus counter/gauge from /metrics. Returns 0 if absent.
read_metric() {
  local proxy_url="$1" metric="$2" label_match="$3"
  local pattern
  if [[ -n "$label_match" ]]; then
    pattern="^${metric}\\{${label_match}\\}"
  else
    pattern="^${metric} "
  fi
  "${CURL_BIN[@]}" -fsS "${proxy_url}/metrics" 2>/dev/null \
    | awk -v pat="$pattern" '$0 ~ pat { print $NF; found=1; exit } END { if (!found) print 0 }'
}

snapshot_tier_counters() {
  local proxy_url="$1" tier="$2"
  local req hit miss
  req=$(read_metric "$proxy_url" loki_vl_proxy_cache_tier_requests_total "tier=\"${tier}\"")
  hit=$(read_metric "$proxy_url" loki_vl_proxy_cache_tier_hits_total "tier=\"${tier}\"")
  miss=$(read_metric "$proxy_url" loki_vl_proxy_cache_tier_misses_total "tier=\"${tier}\"")
  printf '%s %s %s\n' "$req" "$hit" "$miss"
}

# Snapshot the full cache cascade for one proxy. Captures every layer the
# bench needs to demonstrate reuse: top-level response cache → L0 hot
# index → L1 memory → L2 disk → L3 peer → backend fallthrough.
snapshot_full_cache() {
  local proxy_url="$1"
  local top_hit top_miss bf
  top_hit=$(read_metric "$proxy_url" loki_vl_proxy_cache_hits_total "")
  top_miss=$(read_metric "$proxy_url" loki_vl_proxy_cache_misses_total "")
  bf=$(read_metric "$proxy_url" loki_vl_proxy_cache_backend_fallthrough_total "")
  local l0 l1 l2 l3
  l0=$(snapshot_tier_counters "$proxy_url" l0)
  l1=$(snapshot_tier_counters "$proxy_url" l1_memory)
  l2=$(snapshot_tier_counters "$proxy_url" l2_disk)
  l3=$(snapshot_tier_counters "$proxy_url" l3_peer)
  printf '%s|%s|%s|%s|%s|%s|%s\n' "$top_hit" "$top_miss" "$bf" "$l0" "$l1" "$l2" "$l3"
}

# Print the cascade as a labelled delta table. Demonstrates the full
# L0 → L1 → L2 → L3 → backend reuse story.
print_full_cascade_delta() {
  local before="$1" after="$2"
  local t0_th t0_tm t0_bf t0_l0 t0_l1 t0_l2 t0_l3
  local t1_th t1_tm t1_bf t1_l0 t1_l1 t1_l2 t1_l3
  IFS='|' read -r t0_th t0_tm t0_bf t0_l0 t0_l1 t0_l2 t0_l3 <<<"$before"
  IFS='|' read -r t1_th t1_tm t1_bf t1_l0 t1_l1 t1_l2 t1_l3 <<<"$after"
  printf '  %-20s hits+%-3d  miss+%-3d  fallthrough+%d\n' \
    "Response cache" \
    "$((t1_th - t0_th))" "$((t1_tm - t0_tm))" "$((t1_bf - t0_bf))"
  diff_tier "$t0_l0" "$t1_l0" "L0 hot-index"
  diff_tier "$t0_l1" "$t1_l1" "L1 memory"
  diff_tier "$t0_l2" "$t1_l2" "L2 disk"
  diff_tier "$t0_l3" "$t1_l3" "L3 peer"
}

run_query_range() {
  local proxy_url="$1"
  local end_ns start_ns
  end_ns=$(now_ns)
  start_ns=$(( end_ns - 3600 * 1000000000 ))
  "${CURL_BIN[@]}" -fsS -G "${proxy_url}/loki/api/v1/query_range" \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${end_ns}" \
    --data-urlencode "step=15s" \
    --data-urlencode "limit=100" \
    >/dev/null
}

# Stable request that hits the unified per-key cache (L0 hot-index update +
# L1 in-memory + L2 disk + L3 peer chain). Uses fixed BENCH_START_NS /
# BENCH_END_NS so repeated calls share a cache key.
run_label_values() {
  local proxy_url="$1"
  "${CURL_BIN[@]}" -fsS -G "${proxy_url}/loki/api/v1/label/${BENCH_LABEL_NAME}/values" \
    --data-urlencode "start=${BENCH_START_NS}" \
    --data-urlencode "end=${BENCH_END_NS}" \
    >/dev/null
}

run_query_range_window() {
  # Run a metric query_range over a $1-hour window ending now.
  local proxy_url="$1" hours="$2" step="$3"
  local end_ns start_ns
  end_ns=$(now_ns)
  start_ns=$(( end_ns - hours * 3600 * 1000000000 ))
  "${CURL_BIN[@]}" -fsS -G "${proxy_url}/loki/api/v1/query_range" \
    --data-urlencode 'query=sum(rate({job=~".+"}[5m]))' \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${end_ns}" \
    --data-urlencode "step=${step}" \
    >/dev/null
}

print_delta_row() {
  printf '  %-10s req+%s  hits+%s  miss+%s\n' "$1" "$2" "$3" "$4"
}

diff_tier() {
  local before="$1" after="$2" name="$3"
  local r0 h0 m0 r1 h1 m1
  read -r r0 h0 m0 <<<"$before"
  read -r r1 h1 m1 <<<"$after"
  print_delta_row "$name" "$((r1 - r0))" "$((h1 - h0))" "$((m1 - m0))"
}

bench_l1_cold_vs_warm() {
  echo "=== L1 cold-vs-warm — ${COLD_WARM_RUNS} sequential runs against ${PROXY_MAIN_URL} ==="
  echo "Query: /label/${BENCH_LABEL_NAME}/values  (cache key stable via fixed start/end)"
  local before after
  before=$(snapshot_full_cache "$PROXY_MAIN_URL")
  local i
  for ((i = 1; i <= COLD_WARM_RUNS; i++)); do
    local t0 t1 dur_ms
    t0=$(($(date +%s%N) / 1000000))
    run_label_values "$PROXY_MAIN_URL"
    t1=$(($(date +%s%N) / 1000000))
    dur_ms=$(( t1 - t0 ))
    printf '  run %d: %d ms\n' "$i" "$dur_ms"
  done
  after=$(snapshot_full_cache "$PROXY_MAIN_URL")
  echo "Cache cascade deltas:"
  print_full_cascade_delta "$before" "$after"
  echo "Expectation: response-cache hits ≈ ${COLD_WARM_RUNS}-1 (warm fast path); L1 req+1 miss+1 on cold drop-through."
}

bench_l2_promotion() {
  echo "=== L2 promotion across restart — warm, flush, restart ${MAIN_SERVICE}, re-query ==="
  echo "Warming with one query against ${PROXY_MAIN_URL}..."
  run_label_values "$PROXY_MAIN_URL"
  # Sleep > disk-cache-flush-interval (default 5s) so the in-memory write
  # buffer actually lands on bbolt before we kill L1 with a restart.
  echo "Sleeping 7s so the disk-cache flush interval (5s) fires..."
  sleep 7
  echo "Restarting ${MAIN_SERVICE} (preserves named volume, drops L1 memory)..."
  ( cd "$COMPOSE_DIR" && docker compose restart "$MAIN_SERVICE" ) >/dev/null
  local retries=20
  until "${CURL_BIN[@]}" -fsS "${PROXY_MAIN_URL}/metrics" >/dev/null 2>&1; do
    retries=$((retries - 1))
    if (( retries <= 0 )); then
      echo "main proxy did not come back after restart" >&2
      return 1
    fi
    sleep 1
  done
  local before after
  before=$(snapshot_full_cache "$PROXY_MAIN_URL")
  echo "Re-running the same query — L1 cold (memory was wiped); L2 disk should hit:"
  run_label_values "$PROXY_MAIN_URL"
  after=$(snapshot_full_cache "$PROXY_MAIN_URL")
  echo "Cache cascade deltas after restart:"
  print_full_cascade_delta "$before" "$after"
  echo "Expectation: L1 req+1 miss+1 (memory wiped), L2 req+1 hit+1 (bbolt survived restart)."
}

bench_l3_cross_peer() {
  echo "=== L3 cross-peer — warm peer-a (twice), query peer-b ==="
  echo "Warming peer-a (${PROXY_PEER_A_URL}) — two calls so the L0 hot-index advertises the key:"
  run_label_values "$PROXY_PEER_A_URL"
  run_label_values "$PROXY_PEER_A_URL"
  # Sleep so peer-a's hot-index advertisement propagates and peer-b's hot
  # read-ahead has a chance to pull. Without this, peer-b races the warmup
  # write and the consistent-hash ring may not have settled.
  echo "Sleeping 3s for peer ring / hot-index propagation..."
  sleep 3
  local before after
  before=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  echo "Querying peer-b (${PROXY_PEER_B_URL}) for same key — should fetch from peer-a, not VL:"
  run_label_values "$PROXY_PEER_B_URL"
  after=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  echo "Cache cascade deltas on peer-b:"
  print_full_cascade_delta "$before" "$after"
  echo "Expectation: peer-b L3 req+1 hit+1; backend fallthrough unchanged (peer served it)."
}

bench_long_range_window() {
  echo "=== Long-range windowed reuse — 7d full, then 7d-1h overlap ==="
  echo "First call: 7d window (cold — fills unified cache with chunked sub-windows)..."
  local before_first after_first
  before_first=$(snapshot_full_cache "$PROXY_MAIN_URL")
  run_query_range_window "$PROXY_MAIN_URL" 168 60s
  after_first=$(snapshot_full_cache "$PROXY_MAIN_URL")
  echo "Cache cascade deltas — first 7d call (mostly misses):"
  print_full_cascade_delta "$before_first" "$after_first"
  echo ""
  echo "Second call: 7d window shifted by 1h (overlapping 6d23h with first call)..."
  local before_second after_second
  before_second=$(snapshot_full_cache "$PROXY_MAIN_URL")
  run_query_range_window "$PROXY_MAIN_URL" 167 60s
  after_second=$(snapshot_full_cache "$PROXY_MAIN_URL")
  echo "Cache cascade deltas — second overlapping call (mostly hits):"
  print_full_cascade_delta "$before_second" "$after_second"
  echo ""
  echo "Expectation: first call drives L1 misses (chunks not yet cached);"
  echo "             second call reuses overlapping chunks — L1 hits dominate."
}

bench_lb_round_robin() {
  echo "=== LB round-robin — ${LB_RUNS} requests through vmauth-ring (${PROXY_LB_URL}) ==="
  echo "vmauth round-robins across proxy-0/1/2; same cache key on each call."
  local before_p0 before_p1 before_p2
  before_p0=$(snapshot_full_cache "$PROXY_MAIN_URL")
  before_p1=$(snapshot_full_cache "$PROXY_PEER_A_URL")
  before_p2=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  local i
  local total_ms=0 cold_runs=0 warm_runs=0
  for ((i = 1; i <= LB_RUNS; i++)); do
    local t0 t1 dur_ms
    t0=$(($(date +%s%N) / 1000000))
    "${CURL_BIN[@]}" -fsS -G "${PROXY_LB_URL}/loki/api/v1/label/${BENCH_LABEL_NAME}/values" \
      --data-urlencode "start=${BENCH_START_NS}" \
      --data-urlencode "end=${BENCH_END_NS}" >/dev/null
    t1=$(($(date +%s%N) / 1000000))
    dur_ms=$(( t1 - t0 ))
    total_ms=$((total_ms + dur_ms))
    if (( dur_ms > 50 )); then cold_runs=$((cold_runs+1)); else warm_runs=$((warm_runs+1)); fi
  done
  local avg_ms=$(( total_ms / LB_RUNS ))
  printf '  %d runs, avg %d ms (heuristic: %d cold >50ms, %d warm)\n' \
    "$LB_RUNS" "$avg_ms" "$cold_runs" "$warm_runs"
  local after_p0 after_p1 after_p2
  after_p0=$(snapshot_full_cache "$PROXY_MAIN_URL")
  after_p1=$(snapshot_full_cache "$PROXY_PEER_A_URL")
  after_p2=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  echo ""
  echo "--- proxy-0 (loki-vl-proxy) ---"
  print_full_cascade_delta "$before_p0" "$after_p0"
  echo ""
  echo "--- proxy-1 (loki-vl-proxy-peer-a) ---"
  print_full_cascade_delta "$before_p1" "$after_p1"
  echo ""
  echo "--- proxy-2 (loki-vl-proxy-peer-b) ---"
  print_full_cascade_delta "$before_p2" "$after_p2"
  echo ""
  echo "Expectation: ~${LB_RUNS}/3 requests per proxy. First call on each is cold;"
  echo "             subsequent calls hit response cache. Peer write-through pushes the"
  echo "             warm entry to the owner proxy so other peers can L3-fetch on miss."
}

capture_pprof_one() {
  local proxy_url="$1" instance_label="$2" outdir="$3"
  mkdir -p "$outdir"
  # CPU profile runs in the background during the load test; the others are
  # instantaneous snapshots taken at the END.
  local prof
  for prof in heap allocs goroutine mutex block threadcreate; do
    "${CURL_BIN[@]}" -fsS -H "X-Admin-Token: ${PPROF_AUTH_TOKEN}" \
      "${proxy_url}/debug/pprof/${prof}" -o "${outdir}/${instance_label}-${prof}.pprof" 2>/dev/null \
      && printf '  %s %-12s saved %s\n' "$instance_label" "$prof" "${outdir}/${instance_label}-${prof}.pprof" \
      || printf '  %s %-12s FAILED\n' "$instance_label" "$prof"
  done
}

capture_pprof_cpu_background() {
  local proxy_url="$1" instance_label="$2" outdir="$3" seconds="$4"
  mkdir -p "$outdir"
  # Returns immediately; the curl runs in the background for `seconds`
  # while the load test drives traffic. The CPU profile reflects what the
  # proxy actually spent CPU on during the load window.
  ("${CURL_BIN[@]}" -fsS -H "X-Admin-Token: ${PPROF_AUTH_TOKEN}" \
    "${proxy_url}/debug/pprof/profile?seconds=${seconds}" \
    -o "${outdir}/${instance_label}-cpu-${seconds}s.pprof" >/dev/null 2>&1 \
    && printf '  %s cpu-%ds      saved %s\n' "$instance_label" "$seconds" "${outdir}/${instance_label}-cpu-${seconds}s.pprof" \
    || printf '  %s cpu-%ds      FAILED\n' "$instance_label" "$seconds") &
}

bench_high_rate_load() {
  local mode_desc
  if (( SUSTAINED_SECONDS > 0 )); then
    mode_desc="sustained for ${SUSTAINED_SECONDS}s"
  else
    mode_desc="${LOAD_TOTAL} requests"
  fi
  echo "=== High-rate load — ${mode_desc} at parallelism ${LOAD_PARALLEL} through LB ${PROXY_LB_URL} ==="
  # Materialise the LogQL query list once.
  local qlist
  qlist=$(mktemp -t bench-cache-queries-XXXXXX)
  printf '%s\n' "$LOAD_QUERIES" > "$qlist"
  local n_queries
  n_queries=$(wc -l <"$qlist" | tr -d ' ')
  echo "LogQL: rotating ${n_queries} queries; window=${LOAD_WINDOW_SECONDS}s step=${LOAD_STEP}"
  # Build the weighted endpoint kind list — kinds repeated MIX_KIND times.
  local kinds_list
  kinds_list=$(mktemp -t bench-cache-kinds-XXXXXX)
  trap 'rm -f "$qlist" "$kinds_list"' RETURN
  local k w
  for k in query_range query labels label_values series detected_labels detected_fields index_stats index_volume; do
    local var="MIX_$(echo "$k" | tr a-z A-Z)"
    w="${!var}"
    for ((i = 0; i < w; i++)); do echo "$k"; done
  done > "$kinds_list"
  local n_kinds
  n_kinds=$(wc -l <"$kinds_list" | tr -d ' ')
  echo "Endpoint mix: $(awk -F: '{print $0}' "$kinds_list" | sort | uniq -c | awk '{printf "%s=%s ", $2, $1}')"
  local before_p0 before_p1 before_p2
  before_p0=$(snapshot_full_cache "$PROXY_MAIN_URL")
  before_p1=$(snapshot_full_cache "$PROXY_PEER_A_URL")
  before_p2=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  # Start a CPU profile capture against each proxy in parallel — it runs
  # for the duration of the load window (or PPROF_CPU_SECONDS, whichever
  # is smaller). Heap / allocs / goroutine snapshots are taken at the end.
  if [[ "$CAPTURE_PPROF" == "1" ]]; then
    local cpu_secs=$PPROF_CPU_SECONDS
    if (( SUSTAINED_SECONDS > 0 )) && (( SUSTAINED_SECONDS < cpu_secs )); then
      cpu_secs=$SUSTAINED_SECONDS
    fi
    echo "Starting CPU pprof capture (${cpu_secs}s) on all 3 proxies into ${PPROF_DIR}..."
    capture_pprof_cpu_background "$PROXY_MAIN_URL"   proxy-0 "$PPROF_DIR" "$cpu_secs"
    capture_pprof_cpu_background "$PROXY_PEER_A_URL" proxy-1 "$PPROF_DIR" "$cpu_secs"
    capture_pprof_cpu_background "$PROXY_PEER_B_URL" proxy-2 "$PPROF_DIR" "$cpu_secs"
  fi
  local wall_start wall_end
  wall_start=$(date +%s.%N)
  local total_sent=0
  local round_size=$(( LOAD_PARALLEL ))
  # Single-call fire helper — fire_one <kind> <query> picks the right
  # endpoint based on kind and issues one curl.
  fire_one() {
    local kind="$1" query="$2"
    local end_ns start_ns end_s start_s lname
    end_ns=$(($(date +%s) * 1000000000))
    start_ns=$(( end_ns - LOAD_WINDOW_SECONDS * 1000000000 ))
    end_s=$(date +%s)
    start_s=$(( end_s - LOAD_WINDOW_SECONDS ))
    # Pick a label name from a small rotation for label-scoped endpoints.
    lname=$(awk -v r="$RANDOM" 'BEGIN{
      n=split("app job service_name namespace level pod container", a, " ");
      print a[(r % n) + 1]
    }')
    case "$kind" in
      query_range)
        "${CURL_BIN[@]}" -s -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/query_range" \
          --data-urlencode "query=${query}" --data-urlencode "start=${start_ns}" \
          --data-urlencode "end=${end_ns}" --data-urlencode "step=${LOAD_STEP}" \
          --data-urlencode "limit=100"
        ;;
      query)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/query" \
          --data-urlencode "query=${query}" --data-urlencode "time=${end_ns}" \
          --data-urlencode "limit=100"
        ;;
      labels)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/labels" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
      label_values)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/label/${lname}/values" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
      series)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/series" \
          --data-urlencode "match[]={job=~\".+\"}" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
      detected_labels)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/detected_labels" \
          --data-urlencode "query={job=~\".+\"}" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
      detected_fields)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/detected_fields" \
          --data-urlencode "query={job=~\".+\"}" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
      index_stats)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/index/stats" \
          --data-urlencode "query={job=~\".+\"}" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
      index_volume)
        "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/index/volume" \
          --data-urlencode "query={job=~\".+\"}" \
          --data-urlencode "start=${start_ns}" --data-urlencode "end=${end_ns}"
        ;;
    esac
  }
  # Cache the lists into bash arrays once for fast lookup per round.
  mapfile -t _KINDS < "$kinds_list"
  mapfile -t _QUERIES < "$qlist"
  local _kinds_n="${#_KINDS[@]}" _queries_n="${#_QUERIES[@]}"
  fire_round() {
    # Spawn round_size curls in background with LOAD_PARALLEL in-flight
    # cap (semaphore via /dev/fd lock).
    local i kind q pid_count=0
    for ((i = 0; i < round_size; i++)); do
      kind="${_KINDS[$(( RANDOM % _kinds_n ))]}"
      q="${_QUERIES[$(( RANDOM % _queries_n ))]}"
      fire_one "$kind" "$q" &
      pid_count=$((pid_count + 1))
      if (( pid_count >= LOAD_PARALLEL )); then
        # Wait for any one of the running jobs to free a slot.
        wait -n 2>/dev/null || wait
        pid_count=$((pid_count - 1))
      fi
    done
    wait
  }
  if (( SUSTAINED_SECONDS > 0 )); then
    local deadline
    deadline=$(awk "BEGIN{printf \"%.3f\", $wall_start + $SUSTAINED_SECONDS}")
    local progress_t=$wall_start
    while :; do
      local now
      now=$(date +%s.%N)
      if awk "BEGIN{exit !($now >= $deadline)}"; then break; fi
      fire_round
      total_sent=$((total_sent + round_size))
      local pnow elapsed
      pnow=$(date +%s.%N)
      if awk "BEGIN{exit !($pnow - $progress_t >= 10)}"; then
        elapsed=$(awk "BEGIN{printf \"%.0f\", $pnow - $wall_start}")
        local rps_so_far
        rps_so_far=$(awk "BEGIN{printf \"%.0f\", $total_sent / ($pnow - $wall_start)}")
        printf '  [%3ds] sent %d reqs (~%s req/s)\n' "$elapsed" "$total_sent" "$rps_so_far"
        progress_t=$pnow
      fi
    done
  else
    # Fixed-count mode: fire rounds of round_size until LOAD_TOTAL reached.
    while (( total_sent < LOAD_TOTAL )); do
      fire_round
      total_sent=$((total_sent + round_size))
    done
  fi
  wall_end=$(date +%s.%N)
  local wall_s rps
  wall_s=$(awk "BEGIN{printf \"%.2f\", $wall_end - $wall_start}")
  rps=$(awk "BEGIN{printf \"%.0f\", $total_sent / ($wall_end - $wall_start)}")
  printf '\nFinished %d mixed-endpoint calls in %ss (~%s req/s average)\n\n' "$total_sent" "$wall_s" "$rps"
  if [[ "$CAPTURE_PPROF" == "1" ]]; then
    echo "Waiting for CPU pprof captures to flush..."
    wait
    echo "Capturing instantaneous heap/allocs/goroutine/mutex/block/threadcreate on all 3 proxies:"
    capture_pprof_one "$PROXY_MAIN_URL"   proxy-0 "$PPROF_DIR"
    capture_pprof_one "$PROXY_PEER_A_URL" proxy-1 "$PPROF_DIR"
    capture_pprof_one "$PROXY_PEER_B_URL" proxy-2 "$PPROF_DIR"
    echo ""
    echo "pprof files in ${PPROF_DIR}:"
    ls -lh "$PPROF_DIR" 2>/dev/null | tail -n +2
    echo ""
    echo "Inspect with e.g.:"
    echo "  go tool pprof -http=:8888 ${PPROF_DIR}/proxy-0-cpu-${PPROF_CPU_SECONDS}s.pprof"
    echo "  go tool pprof -top ${PPROF_DIR}/proxy-0-heap.pprof"
  fi
  local after_p0 after_p1 after_p2
  after_p0=$(snapshot_full_cache "$PROXY_MAIN_URL")
  after_p1=$(snapshot_full_cache "$PROXY_PEER_A_URL")
  after_p2=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  echo "--- proxy-0 (loki-vl-proxy) ---"
  print_full_cascade_delta "$before_p0" "$after_p0"
  echo ""
  echo "--- proxy-1 (loki-vl-proxy-peer-a) ---"
  print_full_cascade_delta "$before_p1" "$after_p1"
  echo ""
  echo "--- proxy-2 (loki-vl-proxy-peer-b) ---"
  print_full_cascade_delta "$before_p2" "$after_p2"
  echo ""
  echo "Expectation: response cache hits dominate (warm fast path);"
  echo "             L3 hits prove cross-peer fetches under contention;"
  echo "             peer write-through pushes warm entries to owner peers."
}

mode="${1:-all}"
case "$mode" in
  l1) bench_l1_cold_vs_warm ;;
  l2) bench_l2_promotion ;;
  l3) bench_l3_cross_peer ;;
  long) bench_long_range_window ;;
  lb) bench_lb_round_robin ;;
  load) bench_high_rate_load ;;
  all)
    bench_l1_cold_vs_warm
    echo
    bench_l2_promotion
    echo
    bench_l3_cross_peer
    echo
    bench_long_range_window
    echo
    bench_lb_round_robin
    echo
    bench_high_rate_load
    ;;
  *)
    echo "usage: $0 [l1|l2|l3|long|lb|load|all]" >&2
    exit 2
    ;;
esac
