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

capture_async() {
  local name="$1"
  local fallback="$2"
  local timeout_seconds="$3"
  local output_file="$4"
  local pid_var="$5"
  shift 5

  (
    capture_or_default "$name" "$fallback" "$timeout_seconds" "$@"
  ) >"$output_file" &
  printf -v "$pid_var" '%s' "$!"
}

collect_tests_and_coverage() {
  local cover_file="$TMP_DIR/coverage.out"
  local events_file="$TMP_DIR/go-test-events.json"
  go test ./... -coverprofile="$cover_file" -count=1 -json >"$events_file"
  local test_count coverage
  test_count="$(jq -r 'select(.Action=="pass" and .Test != null) | .Test' "$events_file" | wc -l | tr -d ' ')"
  coverage="$(go tool cover -func="$cover_file" | tail -1 | awk '{print substr($3, 1, length($3)-1)}')"
  jq -n \
    --argjson count "${test_count:-0}" \
    --argjson coverage_pct "${coverage:-0}" \
    '{count:$count,coverage_pct:$coverage_pct}'
}

run_score() {
  local test_name="$1"
  local output
  output="$(go test -v -tags=e2e -run "^${test_name}$" ./test/e2e-compat/ -timeout=180s 2>&1)"
  local log_file="$TMP_DIR/${test_name}.log"
  echo "$output" >"$log_file"
  local score
  score="$(echo "$output" | grep -oE 'Score: [0-9]+/[0-9]+ \([0-9.]+%\)' | tail -1)"
  local components
  components="$(python3 - "$log_file" <<'PY'
import json
import re
import sys

pattern = re.compile(r"\b(PASS|FAIL)\s+([A-Za-z0-9_.\-/]+):")
components = {}

with open(sys.argv[1], "r", encoding="utf-8", errors="ignore") as fh:
    for line in fh:
        match = pattern.search(line)
        if not match:
            continue
        status, component = match.group(1), match.group(2)
        entry = components.setdefault(component, {"passed": 0, "total": 0})
        entry["total"] += 1
        if status == "PASS":
            entry["passed"] += 1

result = {}
for component in sorted(components.keys()):
    passed = components[component]["passed"]
    total = components[component]["total"]
    pct = 100.0 if total == 0 else round((passed * 100.0) / total, 1)
    result[component] = {"passed": passed, "total": total, "pct": pct}

print(json.dumps(result))
PY
)"
  if [ -z "$score" ]; then
    jq -n --argjson components "$components" \
      '{passed:0,total:0,pct:0,components:$components}'
    return
  fi
  local passed total pct
  passed="$(echo "$score" | sed -E 's/Score: ([0-9]+)\/([0-9]+) \(([0-9.]+)%\)/\1/')"
  total="$(echo "$score" | sed -E 's/Score: ([0-9]+)\/([0-9]+) \(([0-9.]+)%\)/\2/')"
  pct="$(echo "$score" | sed -E 's/Score: ([0-9]+)\/([0-9]+) \(([0-9.]+)%\)/\3/')"
  jq -n \
    --argjson passed "$passed" \
    --argjson total "$total" \
    --argjson pct "$pct" \
    --argjson components "$components" \
    '{passed:$passed,total:$total,pct:$pct,components:$components}'
}

start_compat_stack() {
  (
    cd "$ROOT_DIR/test/e2e-compat"
    docker compose down -v >&2 || true
    if [ -n "${PROXY_IMAGE:-}" ] && docker image inspect "${PROXY_IMAGE}" >/dev/null 2>&1; then
      if ! docker compose up -d --no-build >&2; then
        log_step "compat stack --no-build failed; retrying with --build"
        docker compose up -d --build >&2
      fi
    else
      docker compose up -d --build >&2
    fi
    "$ROOT_DIR/scripts/ci/wait_e2e_stack.sh" 180 >&2
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
  GOMAXPROCS=1 go test ./internal/proxy -run '^$' -bench 'BenchmarkProxy_(QueryRange|Labels)_(CacheHit|CacheBypass)$' -benchmem -benchtime=2s -count=7 -cpu=1 >"$out"
  python3 - "$out" <<'PY'
import json
import re
import statistics
import sys

path = sys.argv[1]
metrics = {
    "BenchmarkProxy_QueryRange_CacheHit": {
        "query_range_cache_hit_ns_per_op": [],
        "query_range_cache_hit_bytes_per_op": [],
        "query_range_cache_hit_allocs_per_op": [],
    },
    "BenchmarkProxy_QueryRange_CacheBypass": {
        "query_range_cache_bypass_ns_per_op": [],
        "query_range_cache_bypass_bytes_per_op": [],
        "query_range_cache_bypass_allocs_per_op": [],
    },
    "BenchmarkProxy_Labels_CacheHit": {
        "labels_cache_hit_ns_per_op": [],
        "labels_cache_hit_bytes_per_op": [],
        "labels_cache_hit_allocs_per_op": [],
    },
    "BenchmarkProxy_Labels_CacheBypass": {
        "labels_cache_bypass_ns_per_op": [],
        "labels_cache_bypass_bytes_per_op": [],
        "labels_cache_bypass_allocs_per_op": [],
    },
}

with open(path, "r", encoding="utf-8") as fh:
    for raw in fh:
        parts = raw.split()
        if len(parts) < 7:
            continue
        name = re.sub(r"-\d+$", "", parts[0])
        if name not in metrics:
            continue
        try:
            ns_per_op = float(parts[2])
            bytes_per_op = float(parts[4])
            allocs_per_op = float(parts[6])
        except ValueError:
            continue
        values = metrics[name]
        metric_names = list(values.keys())
        values[metric_names[0]].append(ns_per_op)
        values[metric_names[1]].append(bytes_per_op)
        values[metric_names[2]].append(allocs_per_op)

result = {}
for benchmark in metrics.values():
    for key, samples in benchmark.items():
        result[key] = statistics.median(samples) if samples else 0

print(json.dumps(result))
PY
}

collect_load() {
  local out="$TMP_DIR/load.txt"
  go test ./internal/proxy -run '^TestLoad_HighConcurrency_MemoryStability$' -count=3 -v -timeout=180s >"$out"
  python3 - "$out" <<'PY'
import json
import re
import statistics
import sys

throughputs = []
memory_growth = []

with open(sys.argv[1], "r", encoding="utf-8", errors="ignore") as fh:
    for line in fh:
        throughput_match = re.search(r"Throughput: ([0-9.]+) req/s", line)
        if throughput_match:
            throughputs.append(float(throughput_match.group(1)))
            continue
        memory_match = re.search(r"Memory growth: ([0-9.]+) MB", line)
        if memory_match:
            memory_growth.append(float(memory_match.group(1)))

print(
    json.dumps(
        {
            "high_concurrency_req_per_s": statistics.median(throughputs) if throughputs else 0,
            "high_concurrency_memory_growth_mb": statistics.median(memory_growth) if memory_growth else 0,
        }
    )
)
PY
}

tests_file="$TMP_DIR/tests_and_coverage.json"
compat_file="$TMP_DIR/compat.json"
benchmarks_file="$TMP_DIR/benchmarks.json"
load_file="$TMP_DIR/load.json"

BENCHMARKS_DEFAULT='{"query_range_cache_hit_ns_per_op":0,"query_range_cache_hit_bytes_per_op":0,"query_range_cache_hit_allocs_per_op":0,"query_range_cache_bypass_ns_per_op":0,"query_range_cache_bypass_bytes_per_op":0,"query_range_cache_bypass_allocs_per_op":0,"labels_cache_hit_ns_per_op":0,"labels_cache_hit_bytes_per_op":0,"labels_cache_hit_allocs_per_op":0,"labels_cache_bypass_ns_per_op":0,"labels_cache_bypass_bytes_per_op":0,"labels_cache_bypass_allocs_per_op":0}'
LOAD_DEFAULT='{"high_concurrency_req_per_s":0,"high_concurrency_memory_growth_mb":0}'
PERF_MODE="full"
if [ "${QUALITY_SKIP_PERF:-0}" = "1" ]; then
  PERF_MODE="skipped"
fi

capture_async tests_and_coverage '{"count":0,"coverage_pct":0}' 900 "$tests_file" tests_pid collect_tests_and_coverage
capture_async compat '{"loki":{"passed":0,"total":0,"pct":0},"drilldown":{"passed":0,"total":0,"pct":0},"vl":{"passed":0,"total":0,"pct":0}}' 1800 "$compat_file" compat_pid collect_compat

for pid in "$tests_pid" "$compat_pid"; do
  wait "$pid"
done

if [ "$PERF_MODE" = "full" ]; then
  log_step "running perf smoke in an isolated phase for stability"
  capture_or_default benchmarks "$BENCHMARKS_DEFAULT" 900 collect_benchmarks >"$benchmarks_file"
  capture_or_default load "$LOAD_DEFAULT" 600 collect_load >"$load_file"
else
  log_step "skipping performance smoke collection (QUALITY_SKIP_PERF=1)"
  printf '%s\n' "$BENCHMARKS_DEFAULT" >"$benchmarks_file"
  printf '%s\n' "$LOAD_DEFAULT" >"$load_file"
fi

TESTS_AND_COVERAGE="$(cat "$tests_file")"
COMPAT="$(cat "$compat_file")"
BENCHMARKS="$(cat "$benchmarks_file")"
LOAD="$(cat "$load_file")"

jq -n \
  --argjson tests "$TESTS_AND_COVERAGE" \
  --argjson compat "$COMPAT" \
  --argjson benchmarks "$BENCHMARKS" \
  --argjson load "$LOAD" \
  --arg perf_mode "$PERF_MODE" \
  '{
    tests: $tests,
    compatibility: $compat,
    performance: {
      mode: $perf_mode,
      benchmarks: $benchmarks,
      load: $load
    }
  }' >"$OUTPUT_JSON"
