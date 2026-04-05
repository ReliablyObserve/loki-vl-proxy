#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "usage: $0 <output-json>" >&2
  exit 1
fi

OUTPUT_JSON="$1"
ROOT_DIR="$(pwd)"
TMP_DIR="$(mktemp -d)"
cleanup() {
  if [ -d "$ROOT_DIR/test/e2e-compat" ]; then
    (cd "$ROOT_DIR/test/e2e-compat" && docker compose down -v >/dev/null 2>&1) || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

log_step() {
  echo "[quality] $*" >&2
}

capture_or_default() {
  local label="$1"
  local fallback="$2"
  local timeout_seconds="$3"
  shift 3

  log_step "starting ${label}"
  local out_file="$TMP_DIR/${label}.stdout"
  local err_file="$TMP_DIR/${label}.stderr"
  rm -f "$out_file" "$err_file"

  (
    "$@" >"$out_file" 2>"$err_file"
  ) &
  local pid=$!
  local elapsed=0
  local status=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout_seconds" ]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
      log_step "${label} timed out after ${timeout_seconds}s; using fallback"
      cat "$err_file" >&2 || true
      printf '%s' "$fallback"
      return 0
    fi
    sleep 1
    elapsed=$((elapsed+1))
  done

  if wait "$pid"; then
    log_step "completed ${label}"
    cat "$out_file"
    return 0
  fi

  status=$?
  log_step "${label} failed or timed out (exit=${status}); using fallback"
  cat "$err_file" >&2 || true
  printf '%s' "$fallback"
  return 0
}

count_tests() {
  go test ./... -count=1 -json 2>/dev/null \
    | jq -r 'select(.Action=="pass" and .Test != null) | .Test' \
    | wc -l | tr -d ' '
}

coverage_pct() {
  local cover_file="$TMP_DIR/coverage.out"
  go test ./... -coverprofile="$cover_file" -count=1 >/dev/null
  go tool cover -func="$cover_file" | tail -1 | awk '{print substr($3, 1, length($3)-1)}'
}

run_score() {
  local test_name="$1"
  local output
  output="$(go test -v -tags=e2e -run "^${test_name}$" ./test/e2e-compat/ -timeout=180s 2>&1)"
  echo "$output" >"$TMP_DIR/${test_name}.log"
  local score
  score="$(echo "$output" | grep -oE 'Score: [0-9]+/[0-9]+ \([0-9.]+%\)' | tail -1)"
  if [ -z "$score" ]; then
    echo '{"passed":0,"total":0,"pct":0}'
    return
  fi
  local passed total pct
  passed="$(echo "$score" | sed -E 's/Score: ([0-9]+)\/([0-9]+) \(([0-9.]+)%\)/\1/')"
  total="$(echo "$score" | sed -E 's/Score: ([0-9]+)\/([0-9]+) \(([0-9.]+)%\)/\2/')"
  pct="$(echo "$score" | sed -E 's/Score: ([0-9]+)\/([0-9]+) \(([0-9.]+)%\)/\3/')"
  jq -n --argjson passed "$passed" --argjson total "$total" --argjson pct "$pct" \
    '{passed:$passed,total:$total,pct:$pct}'
}

start_compat_stack() {
  (
    cd "$ROOT_DIR/test/e2e-compat"
    docker compose down -v >&2 || true
    docker compose up -d --build --wait --wait-timeout 180 >&2
  )
}

collect_compat() {
  start_compat_stack
  local loki drilldown vl
  loki="$(run_score TestLokiTrackScore)"
  drilldown="$(run_score TestDrilldownTrackScore)"
  vl="$(run_score TestVLTrackScore)"
  jq -n \
    --argjson loki "$loki" \
    --argjson drilldown "$drilldown" \
    --argjson vl "$vl" \
    '{loki:$loki,drilldown:$drilldown,vl:$vl}'
}

collect_benchmarks() {
  local out="$TMP_DIR/bench.txt"
  go test ./internal/proxy -run '^$' -bench 'BenchmarkProxy_(QueryRange_CacheHit|Labels_CacheHit)$' -benchmem -count=1 >"$out"
  local query_ns labels_ns
  query_ns="$(awk '/BenchmarkProxy_QueryRange_CacheHit/ {print $(NF-5); exit}' "$out")"
  labels_ns="$(awk '/BenchmarkProxy_Labels_CacheHit/ {print $(NF-5); exit}' "$out")"
  jq -n \
    --argjson query_ns "${query_ns:-0}" \
    --argjson labels_ns "${labels_ns:-0}" \
    '{query_range_cache_hit_ns_per_op:$query_ns,labels_cache_hit_ns_per_op:$labels_ns}'
}

collect_load() {
  local out="$TMP_DIR/load.txt"
  go test ./internal/proxy -run '^TestLoad_HighConcurrency_MemoryStability$' -count=1 -v -timeout=180s >"$out"
  local throughput memory_growth
  throughput="$(grep -E 'Throughput: ' "$out" | tail -1 | sed -E 's/.*Throughput: ([0-9.]+) req\/s/\1/')"
  memory_growth="$(grep -E 'Memory growth: ' "$out" | tail -1 | sed -E 's/.*Memory growth: ([0-9.]+) MB.*/\1/')"
  jq -n \
    --argjson throughput "${throughput:-0}" \
    --argjson memory_growth "${memory_growth:-0}" \
    '{high_concurrency_req_per_s:$throughput,high_concurrency_memory_growth_mb:$memory_growth}'
}

TEST_COUNT="$(capture_or_default tests 0 600 count_tests)"
COVERAGE="$(capture_or_default coverage 0 900 coverage_pct)"
COMPAT="$(capture_or_default compat '{"loki":{"passed":0,"total":0,"pct":0},"drilldown":{"passed":0,"total":0,"pct":0},"vl":{"passed":0,"total":0,"pct":0}}' 1800 collect_compat)"
BENCHMARKS="$(capture_or_default benchmarks '{"query_range_cache_hit_ns_per_op":0,"labels_cache_hit_ns_per_op":0}' 900 collect_benchmarks)"
LOAD="$(capture_or_default load '{"high_concurrency_req_per_s":0,"high_concurrency_memory_growth_mb":0}' 600 collect_load)"

jq -n \
  --argjson tests "$TEST_COUNT" \
  --argjson coverage "$COVERAGE" \
  --argjson compat "$COMPAT" \
  --argjson benchmarks "$BENCHMARKS" \
  --argjson load "$LOAD" \
  '{
    tests: {count:$tests, coverage_pct:$coverage},
    compatibility: $compat,
    performance: {
      benchmarks: $benchmarks,
      load: $load
    }
  }' >"$OUTPUT_JSON"
