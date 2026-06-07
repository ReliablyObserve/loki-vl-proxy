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
#   load   High-rate concurrent load through the LB. LOAD_TOTAL (default 600)
#          requests fanned across LOAD_PARALLEL (default 50) workers, rotating
#          through LOAD_FAN_KEYS (default app,job,service_name,namespace,level)
#          to fill the caches with several distinct keys. Demonstrates cache
#          fill / propagation / peer write-through under sustained pressure.
#   all    Run l1, then l2, then l3, then long sequentially.
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

bench_high_rate_load() {
  echo "=== High-rate load — ${LOAD_TOTAL} requests at parallelism ${LOAD_PARALLEL} through LB ${PROXY_LB_URL} ==="
  echo "Fanning across keys: ${LOAD_FAN_KEYS}"
  local before_p0 before_p1 before_p2
  before_p0=$(snapshot_full_cache "$PROXY_MAIN_URL")
  before_p1=$(snapshot_full_cache "$PROXY_PEER_A_URL")
  before_p2=$(snapshot_full_cache "$PROXY_PEER_B_URL")
  local wall_start wall_end
  wall_start=$(date +%s.%N)
  # Generate the request list: rotate through keys so we exercise multiple
  # cache entries. Pipe to xargs -P for in-flight parallelism.
  local IFS=','; read -ra _KEYS <<<"$LOAD_FAN_KEYS"; unset IFS
  local n_keys="${#_KEYS[@]}"
  local i
  for ((i = 0; i < LOAD_TOTAL; i++)); do
    printf '%s\n' "${_KEYS[$(( i % n_keys ))]}"
  done | xargs -P "$LOAD_PARALLEL" -I{} \
    "${CURL_BIN[@]}" -fsS -o /dev/null -G "${PROXY_LB_URL}/loki/api/v1/label/{}/values" \
      --data-urlencode "start=${BENCH_START_NS}" \
      --data-urlencode "end=${BENCH_END_NS}"
  wall_end=$(date +%s.%N)
  local wall_s
  wall_s=$(awk "BEGIN{printf \"%.2f\", $wall_end - $wall_start}")
  local rps
  rps=$(awk "BEGIN{printf \"%.0f\", $LOAD_TOTAL / ($wall_end - $wall_start)}")
  printf '\nFinished %d requests in %ss (~%s req/s sustained)\n\n' "$LOAD_TOTAL" "$wall_s" "$rps"
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
