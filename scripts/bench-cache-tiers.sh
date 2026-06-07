#!/usr/bin/env bash
#
# bench-cache-tiers.sh — exercise all four loki-vl-proxy cache tiers (L0 hot
# index, L1 memory, L2 disk, L3 peer) against the e2e-compat compose stack and
# print before/after counter deltas per tier.
#
# Modes:
#   l1     Cold-vs-warm — same query N times against main proxy; reports
#          first-call miss vs subsequent hit ratio.
#   l2     L2 promotion across restart — warm a query, `docker compose restart`
#          the main proxy, re-run; reports L2 disk hit on second call.
#   l3     L3 cross-peer — warm a key on peer-a, then query peer-b for the
#          same key; reports peer-b's L3 hit count.
#   long   Long-range windowed reuse — 7d query, then 7d-1h overlap, reports
#          window cache reuse via cache_window_hits_total / bytes.
#   all    Run l1, then l2, then l3, then long sequentially.
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

now_ns() {
  printf '%s\n' "$(($(date +%s) * 1000000000))"
}

# Read a single Prometheus counter/gauge from /metrics. Returns 0 if absent.
read_metric() {
  local proxy_url="$1" metric="$2" label_match="$3"
  local pattern
  if [[ -n "$label_match" ]]; then
    pattern="^${metric}\\{${label_match}\\}"
  else
    pattern="^${metric} "
  fi
  curl -fsS "${proxy_url}/metrics" 2>/dev/null \
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

run_query_range() {
  local proxy_url="$1"
  local end_ns start_ns
  end_ns=$(now_ns)
  start_ns=$(( end_ns - 3600 * 1000000000 ))
  curl -fsS -G "${proxy_url}/loki/api/v1/query_range" \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${end_ns}" \
    --data-urlencode "step=15s" \
    --data-urlencode "limit=100" \
    >/dev/null
}

run_query_range_window() {
  # Run a metric query_range over a $1-hour window ending now.
  local proxy_url="$1" hours="$2" step="$3"
  local end_ns start_ns
  end_ns=$(now_ns)
  start_ns=$(( end_ns - hours * 3600 * 1000000000 ))
  curl -fsS -G "${proxy_url}/loki/api/v1/query_range" \
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
  echo "Query: ${QUERY}"
  local before_l0 before_l1 before_l2 before_l3
  before_l0=$(snapshot_tier_counters "$PROXY_MAIN_URL" l0)
  before_l1=$(snapshot_tier_counters "$PROXY_MAIN_URL" l1_memory)
  before_l2=$(snapshot_tier_counters "$PROXY_MAIN_URL" l2_disk)
  before_l3=$(snapshot_tier_counters "$PROXY_MAIN_URL" l3_peer)
  local i
  for ((i = 1; i <= COLD_WARM_RUNS; i++)); do
    local t0 t1 dur_ms
    t0=$(($(date +%s%N) / 1000000))
    run_query_range "$PROXY_MAIN_URL"
    t1=$(($(date +%s%N) / 1000000))
    dur_ms=$(( t1 - t0 ))
    printf '  run %d: %d ms\n' "$i" "$dur_ms"
  done
  local after_l0 after_l1 after_l2 after_l3
  after_l0=$(snapshot_tier_counters "$PROXY_MAIN_URL" l0)
  after_l1=$(snapshot_tier_counters "$PROXY_MAIN_URL" l1_memory)
  after_l2=$(snapshot_tier_counters "$PROXY_MAIN_URL" l2_disk)
  after_l3=$(snapshot_tier_counters "$PROXY_MAIN_URL" l3_peer)
  diff_tier "$before_l0" "$after_l0" L0
  diff_tier "$before_l1" "$after_l1" L1
  diff_tier "$before_l2" "$after_l2" L2
  diff_tier "$before_l3" "$after_l3" L3
  echo "Expectation: first run misses L1 (req+1 miss+1); subsequent runs hit L1 (hit+${COLD_WARM_RUNS}-1 ish)."
}

bench_l2_promotion() {
  echo "=== L2 promotion across restart — warm, restart ${MAIN_SERVICE}, re-query ==="
  echo "Warming with one query against ${PROXY_MAIN_URL}..."
  run_query_range "$PROXY_MAIN_URL"
  local warm_l1 warm_l2
  warm_l1=$(snapshot_tier_counters "$PROXY_MAIN_URL" l1_memory)
  warm_l2=$(snapshot_tier_counters "$PROXY_MAIN_URL" l2_disk)
  echo "Restarting ${MAIN_SERVICE} (preserves named volume, drops L1 memory)..."
  ( cd "$COMPOSE_DIR" && docker compose restart "$MAIN_SERVICE" ) >/dev/null
  local retries=20
  until curl -fsS "${PROXY_MAIN_URL}/-/ready" >/dev/null 2>&1 \
        || curl -fsS "${PROXY_MAIN_URL}/healthz" >/dev/null 2>&1 \
        || curl -fsS "${PROXY_MAIN_URL}/metrics" >/dev/null 2>&1; do
    retries=$((retries - 1))
    if (( retries <= 0 )); then
      echo "main proxy did not come back after restart" >&2
      return 1
    fi
    sleep 1
  done
  local before_l1 before_l2
  before_l1=$(snapshot_tier_counters "$PROXY_MAIN_URL" l1_memory)
  before_l2=$(snapshot_tier_counters "$PROXY_MAIN_URL" l2_disk)
  echo "Re-running the same query — L1 cold, L2 disk should hit..."
  run_query_range "$PROXY_MAIN_URL"
  local after_l1 after_l2
  after_l1=$(snapshot_tier_counters "$PROXY_MAIN_URL" l1_memory)
  after_l2=$(snapshot_tier_counters "$PROXY_MAIN_URL" l2_disk)
  diff_tier "$before_l1" "$after_l1" L1
  diff_tier "$before_l2" "$after_l2" L2
  echo "Expectation: L1 req+1 miss+1, L2 req+1 hit+1 (entry persisted on disk across restart)."
}

bench_l3_cross_peer() {
  echo "=== L3 cross-peer — warm peer-a, query peer-b ==="
  echo "Warming peer-a (${PROXY_PEER_A_URL})..."
  run_query_range "$PROXY_PEER_A_URL"
  local before_l3
  before_l3=$(snapshot_tier_counters "$PROXY_PEER_B_URL" l3_peer)
  echo "Querying peer-b (${PROXY_PEER_B_URL}) for same key..."
  run_query_range "$PROXY_PEER_B_URL"
  local after_l3
  after_l3=$(snapshot_tier_counters "$PROXY_PEER_B_URL" l3_peer)
  diff_tier "$before_l3" "$after_l3" "L3@peer-b"
  echo "Expectation: peer-b L3 req+1 hit+1 (fetched from peer-a instead of VL backend)."
}

bench_long_range_window() {
  echo "=== Long-range windowed reuse — 7d full, then 7d-1h overlap ==="
  local before_window_hits before_window_bytes
  before_window_hits=$(read_metric "$PROXY_MAIN_URL" loki_vl_proxy_cache_window_hits_total "")
  before_window_bytes=$(read_metric "$PROXY_MAIN_URL" loki_vl_proxy_cache_window_hits_bytes_total "")
  echo "First call: 7d window..."
  run_query_range_window "$PROXY_MAIN_URL" 168 60s
  echo "Second call: overlapping 7d window shifted by 1h..."
  run_query_range_window "$PROXY_MAIN_URL" 167 60s
  local after_window_hits after_window_bytes
  after_window_hits=$(read_metric "$PROXY_MAIN_URL" loki_vl_proxy_cache_window_hits_total "")
  after_window_bytes=$(read_metric "$PROXY_MAIN_URL" loki_vl_proxy_cache_window_hits_bytes_total "")
  printf '  window_hits delta:  %d\n' "$((after_window_hits - before_window_hits))"
  printf '  window_bytes delta: %d\n' "$((after_window_bytes - before_window_bytes))"
  echo "Expectation: second call reuses overlapping 6d23h of the window cache; window_hits goes up."
}

mode="${1:-all}"
case "$mode" in
  l1) bench_l1_cold_vs_warm ;;
  l2) bench_l2_promotion ;;
  l3) bench_l3_cross_peer ;;
  long) bench_long_range_window ;;
  all)
    bench_l1_cold_vs_warm
    echo
    bench_l2_promotion
    echo
    bench_l3_cross_peer
    echo
    bench_long_range_window
    ;;
  *)
    echo "usage: $0 [l1|l2|l3|long|all]" >&2
    exit 2
    ;;
esac
