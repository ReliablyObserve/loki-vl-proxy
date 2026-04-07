#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 3 ]; then
  echo "usage: $0 <base-json> <head-json> <output-md>" >&2
  exit 1
fi

BASE_JSON="$1"
HEAD_JSON="$2"
OUTPUT_MD="$3"

json_field() {
  local file="$1"
  local path="$2"
  jq -r "$path" "$file"
}

format_delta() {
  local current="$1"
  local base="$2"
  local better="$3"
  local pct_threshold="$4"
  local abs_threshold="$5"
  local min_base="$6"
  python3 - "$current" "$base" "$better" "$pct_threshold" "$abs_threshold" "$min_base" <<'PY'
import sys
current=float(sys.argv[1])
base=float(sys.argv[2])
better=sys.argv[3]
pct_threshold=float(sys.argv[4])
abs_threshold=float(sys.argv[5])
min_base=float(sys.argv[6])
if base == 0:
    print("n/a")
    raise SystemExit
delta=((current-base)/base)*100.0
absolute_delta=abs(current-base)
state="stable"
if base < min_base:
    state="stable"
elif better == "higher":
    if delta >= pct_threshold and absolute_delta >= abs_threshold:
        state="improved"
    elif delta <= -pct_threshold and absolute_delta >= abs_threshold:
        state="regressed"
elif better == "lower":
    if delta <= -pct_threshold and absolute_delta >= abs_threshold:
        state="improved"
    elif delta >= pct_threshold and absolute_delta >= abs_threshold:
        state="regressed"
sign="+" if delta > 0 else ""
print(f"{sign}{delta:.1f}% ({state})")
PY
}

HEAD_TESTS="$(json_field "$HEAD_JSON" '.tests.count')"
BASE_TESTS="$(json_field "$BASE_JSON" '.tests.count')"
HEAD_COVERAGE="$(json_field "$HEAD_JSON" '.tests.coverage_pct')"
BASE_COVERAGE="$(json_field "$BASE_JSON" '.tests.coverage_pct')"

HEAD_LOKI_PASS="$(json_field "$HEAD_JSON" '.compatibility.loki.passed')"
HEAD_LOKI_TOTAL="$(json_field "$HEAD_JSON" '.compatibility.loki.total')"
HEAD_LOKI_PCT="$(json_field "$HEAD_JSON" '.compatibility.loki.pct')"
BASE_LOKI_PCT="$(json_field "$BASE_JSON" '.compatibility.loki.pct')"

HEAD_DRILL_PASS="$(json_field "$HEAD_JSON" '.compatibility.drilldown.passed')"
HEAD_DRILL_TOTAL="$(json_field "$HEAD_JSON" '.compatibility.drilldown.total')"
HEAD_DRILL_PCT="$(json_field "$HEAD_JSON" '.compatibility.drilldown.pct')"
BASE_DRILL_PCT="$(json_field "$BASE_JSON" '.compatibility.drilldown.pct')"

HEAD_VL_PASS="$(json_field "$HEAD_JSON" '.compatibility.vl.passed')"
HEAD_VL_TOTAL="$(json_field "$HEAD_JSON" '.compatibility.vl.total')"
HEAD_VL_PCT="$(json_field "$HEAD_JSON" '.compatibility.vl.pct')"
BASE_VL_PCT="$(json_field "$BASE_JSON" '.compatibility.vl.pct')"

HEAD_QUERY_NS="$(json_field "$HEAD_JSON" '.performance.benchmarks.query_range_cache_hit_ns_per_op')"
BASE_QUERY_NS="$(json_field "$BASE_JSON" '.performance.benchmarks.query_range_cache_hit_ns_per_op')"
HEAD_QUERY_BYTES="$(json_field "$HEAD_JSON" '.performance.benchmarks.query_range_cache_hit_bytes_per_op')"
BASE_QUERY_BYTES="$(json_field "$BASE_JSON" '.performance.benchmarks.query_range_cache_hit_bytes_per_op')"
HEAD_QUERY_ALLOCS="$(json_field "$HEAD_JSON" '.performance.benchmarks.query_range_cache_hit_allocs_per_op')"
BASE_QUERY_ALLOCS="$(json_field "$BASE_JSON" '.performance.benchmarks.query_range_cache_hit_allocs_per_op')"
HEAD_QUERY_BYPASS_NS="$(json_field "$HEAD_JSON" '.performance.benchmarks.query_range_cache_bypass_ns_per_op')"
BASE_QUERY_BYPASS_NS="$(json_field "$BASE_JSON" '.performance.benchmarks.query_range_cache_bypass_ns_per_op')"
HEAD_QUERY_BYPASS_BYTES="$(json_field "$HEAD_JSON" '.performance.benchmarks.query_range_cache_bypass_bytes_per_op')"
BASE_QUERY_BYPASS_BYTES="$(json_field "$BASE_JSON" '.performance.benchmarks.query_range_cache_bypass_bytes_per_op')"
HEAD_QUERY_BYPASS_ALLOCS="$(json_field "$HEAD_JSON" '.performance.benchmarks.query_range_cache_bypass_allocs_per_op')"
BASE_QUERY_BYPASS_ALLOCS="$(json_field "$BASE_JSON" '.performance.benchmarks.query_range_cache_bypass_allocs_per_op')"
HEAD_LABELS_NS="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_hit_ns_per_op')"
BASE_LABELS_NS="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_hit_ns_per_op')"
HEAD_LABELS_BYTES="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_hit_bytes_per_op')"
BASE_LABELS_BYTES="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_hit_bytes_per_op')"
HEAD_LABELS_ALLOCS="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_hit_allocs_per_op')"
BASE_LABELS_ALLOCS="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_hit_allocs_per_op')"
HEAD_LABELS_BYPASS_NS="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_bypass_ns_per_op')"
BASE_LABELS_BYPASS_NS="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_bypass_ns_per_op')"
HEAD_LABELS_BYPASS_BYTES="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_bypass_bytes_per_op')"
BASE_LABELS_BYPASS_BYTES="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_bypass_bytes_per_op')"
HEAD_LABELS_BYPASS_ALLOCS="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_bypass_allocs_per_op')"
BASE_LABELS_BYPASS_ALLOCS="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_bypass_allocs_per_op')"
HEAD_THROUGHPUT="$(json_field "$HEAD_JSON" '.performance.load.high_concurrency_req_per_s')"
BASE_THROUGHPUT="$(json_field "$BASE_JSON" '.performance.load.high_concurrency_req_per_s')"
HEAD_MEM_GROWTH="$(json_field "$HEAD_JSON" '.performance.load.high_concurrency_memory_growth_mb')"
BASE_MEM_GROWTH="$(json_field "$BASE_JSON" '.performance.load.high_concurrency_memory_growth_mb')"

COVERAGE_DELTA="$(format_delta "$HEAD_COVERAGE" "$BASE_COVERAGE" higher 0.1 0.1 1)"
LOKI_DELTA="$(format_delta "$HEAD_LOKI_PCT" "$BASE_LOKI_PCT" higher 0.1 0.1 1)"
DRILL_DELTA="$(format_delta "$HEAD_DRILL_PCT" "$BASE_DRILL_PCT" higher 0.1 0.1 1)"
VL_DELTA="$(format_delta "$HEAD_VL_PCT" "$BASE_VL_PCT" higher 0.1 0.1 1)"
QUERY_DELTA="$(format_delta "$HEAD_QUERY_NS" "$BASE_QUERY_NS" lower 35 5000 1000)"
QUERY_BYTES_DELTA="$(format_delta "$HEAD_QUERY_BYTES" "$BASE_QUERY_BYTES" lower 20 512 256)"
QUERY_ALLOCS_DELTA="$(format_delta "$HEAD_QUERY_ALLOCS" "$BASE_QUERY_ALLOCS" lower 25 4 1)"
QUERY_BYPASS_DELTA="$(format_delta "$HEAD_QUERY_BYPASS_NS" "$BASE_QUERY_BYPASS_NS" lower 35 5000 1000)"
QUERY_BYPASS_BYTES_DELTA="$(format_delta "$HEAD_QUERY_BYPASS_BYTES" "$BASE_QUERY_BYPASS_BYTES" lower 20 512 256)"
QUERY_BYPASS_ALLOCS_DELTA="$(format_delta "$HEAD_QUERY_BYPASS_ALLOCS" "$BASE_QUERY_BYPASS_ALLOCS" lower 25 4 1)"
LABELS_DELTA="$(format_delta "$HEAD_LABELS_NS" "$BASE_LABELS_NS" lower 35 5000 1000)"
LABELS_BYTES_DELTA="$(format_delta "$HEAD_LABELS_BYTES" "$BASE_LABELS_BYTES" lower 20 512 256)"
LABELS_ALLOCS_DELTA="$(format_delta "$HEAD_LABELS_ALLOCS" "$BASE_LABELS_ALLOCS" lower 25 4 1)"
LABELS_BYPASS_DELTA="$(format_delta "$HEAD_LABELS_BYPASS_NS" "$BASE_LABELS_BYPASS_NS" lower 35 5000 1000)"
LABELS_BYPASS_BYTES_DELTA="$(format_delta "$HEAD_LABELS_BYPASS_BYTES" "$BASE_LABELS_BYPASS_BYTES" lower 20 512 256)"
LABELS_BYPASS_ALLOCS_DELTA="$(format_delta "$HEAD_LABELS_BYPASS_ALLOCS" "$BASE_LABELS_BYPASS_ALLOCS" lower 25 4 1)"
THROUGHPUT_DELTA="$(format_delta "$HEAD_THROUGHPUT" "$BASE_THROUGHPUT" higher 25 2000 5000)"
MEMORY_DELTA="$(format_delta "$HEAD_MEM_GROWTH" "$BASE_MEM_GROWTH" lower 300 5 5)"

cat >"$OUTPUT_MD" <<EOF
<!-- pr-quality-report -->
## PR Quality Report

Compared against base branch \`main\`.

### Coverage and tests

| Signal | Base | PR | Delta |
|---|---:|---:|---:|
| Test count | ${BASE_TESTS} | ${HEAD_TESTS} | $((HEAD_TESTS-BASE_TESTS)) |
| Coverage | ${BASE_COVERAGE}% | ${HEAD_COVERAGE}% | ${COVERAGE_DELTA} |

### Compatibility

| Track | Base | PR | Delta |
|---|---:|---:|---:|
| Loki API | ${BASE_LOKI_PCT}% | ${HEAD_LOKI_PASS}/${HEAD_LOKI_TOTAL} (${HEAD_LOKI_PCT}%) | ${LOKI_DELTA} |
| Logs Drilldown | ${BASE_DRILL_PCT}% | ${HEAD_DRILL_PASS}/${HEAD_DRILL_TOTAL} (${HEAD_DRILL_PCT}%) | ${DRILL_DELTA} |
| VictoriaLogs | ${BASE_VL_PCT}% | ${HEAD_VL_PASS}/${HEAD_VL_TOTAL} (${HEAD_VL_PCT}%) | ${VL_DELTA} |

### Performance smoke

Lower CPU cost (\`ns/op\`) is better. Lower benchmark memory cost (\`B/op\`, \`allocs/op\`) is better. Higher throughput is better. Lower load-test memory growth is better. Benchmark rows are medians from repeated samples.

| Signal | Base | PR | Delta |
|---|---:|---:|---:|
| QueryRange cache-hit CPU cost | ${BASE_QUERY_NS} ns/op | ${HEAD_QUERY_NS} ns/op | ${QUERY_DELTA} |
| QueryRange cache-hit memory | ${BASE_QUERY_BYTES} B/op | ${HEAD_QUERY_BYTES} B/op | ${QUERY_BYTES_DELTA} |
| QueryRange cache-hit allocations | ${BASE_QUERY_ALLOCS} allocs/op | ${HEAD_QUERY_ALLOCS} allocs/op | ${QUERY_ALLOCS_DELTA} |
| QueryRange cache-bypass CPU cost | ${BASE_QUERY_BYPASS_NS} ns/op | ${HEAD_QUERY_BYPASS_NS} ns/op | ${QUERY_BYPASS_DELTA} |
| QueryRange cache-bypass memory | ${BASE_QUERY_BYPASS_BYTES} B/op | ${HEAD_QUERY_BYPASS_BYTES} B/op | ${QUERY_BYPASS_BYTES_DELTA} |
| QueryRange cache-bypass allocations | ${BASE_QUERY_BYPASS_ALLOCS} allocs/op | ${HEAD_QUERY_BYPASS_ALLOCS} allocs/op | ${QUERY_BYPASS_ALLOCS_DELTA} |
| Labels cache-hit CPU cost | ${BASE_LABELS_NS} ns/op | ${HEAD_LABELS_NS} ns/op | ${LABELS_DELTA} |
| Labels cache-hit memory | ${BASE_LABELS_BYTES} B/op | ${HEAD_LABELS_BYTES} B/op | ${LABELS_BYTES_DELTA} |
| Labels cache-hit allocations | ${BASE_LABELS_ALLOCS} allocs/op | ${HEAD_LABELS_ALLOCS} allocs/op | ${LABELS_ALLOCS_DELTA} |
| Labels cache-bypass CPU cost | ${BASE_LABELS_BYPASS_NS} ns/op | ${HEAD_LABELS_BYPASS_NS} ns/op | ${LABELS_BYPASS_DELTA} |
| Labels cache-bypass memory | ${BASE_LABELS_BYPASS_BYTES} B/op | ${HEAD_LABELS_BYPASS_BYTES} B/op | ${LABELS_BYPASS_BYTES_DELTA} |
| Labels cache-bypass allocations | ${BASE_LABELS_BYPASS_ALLOCS} allocs/op | ${HEAD_LABELS_BYPASS_ALLOCS} allocs/op | ${LABELS_BYPASS_ALLOCS_DELTA} |
| High-concurrency throughput | ${BASE_THROUGHPUT} req/s | ${HEAD_THROUGHPUT} req/s | ${THROUGHPUT_DELTA} |
| High-concurrency memory growth | ${BASE_MEM_GROWTH} MB | ${HEAD_MEM_GROWTH} MB | ${MEMORY_DELTA} |

### State

- Coverage, compatibility, and sampled performance are reported here from the same PR workflow.
- This is a delta report, not a release gate by itself. Required checks still decide merge safety.
- Performance is a smoke comparison, not a full benchmark lab run.
- Delta states use the same noise guards as the quality gate (percent + absolute + low-baseline checks), so report labels match merge-gate behavior.
EOF
