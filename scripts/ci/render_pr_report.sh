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
  python3 - "$current" "$base" "$better" <<'PY'
import sys
current=float(sys.argv[1])
base=float(sys.argv[2])
better=sys.argv[3]
if base == 0:
    print("n/a")
    raise SystemExit
delta=((current-base)/base)*100.0
state="stable"
if better == "higher":
    if delta >= 5:
        state="improved"
    elif delta <= -5:
        state="regressed"
elif better == "lower":
    if delta <= -5:
        state="improved"
    elif delta >= 5:
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
HEAD_LABELS_NS="$(json_field "$HEAD_JSON" '.performance.benchmarks.labels_cache_hit_ns_per_op')"
BASE_LABELS_NS="$(json_field "$BASE_JSON" '.performance.benchmarks.labels_cache_hit_ns_per_op')"
HEAD_THROUGHPUT="$(json_field "$HEAD_JSON" '.performance.load.high_concurrency_req_per_s')"
BASE_THROUGHPUT="$(json_field "$BASE_JSON" '.performance.load.high_concurrency_req_per_s')"
HEAD_MEM_GROWTH="$(json_field "$HEAD_JSON" '.performance.load.high_concurrency_memory_growth_mb')"
BASE_MEM_GROWTH="$(json_field "$BASE_JSON" '.performance.load.high_concurrency_memory_growth_mb')"

COVERAGE_DELTA="$(format_delta "$HEAD_COVERAGE" "$BASE_COVERAGE" higher)"
LOKI_DELTA="$(format_delta "$HEAD_LOKI_PCT" "$BASE_LOKI_PCT" higher)"
DRILL_DELTA="$(format_delta "$HEAD_DRILL_PCT" "$BASE_DRILL_PCT" higher)"
VL_DELTA="$(format_delta "$HEAD_VL_PCT" "$BASE_VL_PCT" higher)"
QUERY_DELTA="$(format_delta "$HEAD_QUERY_NS" "$BASE_QUERY_NS" lower)"
LABELS_DELTA="$(format_delta "$HEAD_LABELS_NS" "$BASE_LABELS_NS" lower)"
THROUGHPUT_DELTA="$(format_delta "$HEAD_THROUGHPUT" "$BASE_THROUGHPUT" higher)"
MEMORY_DELTA="$(format_delta "$HEAD_MEM_GROWTH" "$BASE_MEM_GROWTH" lower)"

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

Lower \`ns/op\` is better. Higher throughput is better. Lower memory growth is better.

| Signal | Base | PR | Delta |
|---|---:|---:|---:|
| QueryRange cache-hit benchmark | ${BASE_QUERY_NS} ns/op | ${HEAD_QUERY_NS} ns/op | ${QUERY_DELTA} |
| Labels cache-hit benchmark | ${BASE_LABELS_NS} ns/op | ${HEAD_LABELS_NS} ns/op | ${LABELS_DELTA} |
| High-concurrency throughput | ${BASE_THROUGHPUT} req/s | ${HEAD_THROUGHPUT} req/s | ${THROUGHPUT_DELTA} |
| High-concurrency memory growth | ${BASE_MEM_GROWTH} MB | ${HEAD_MEM_GROWTH} MB | ${MEMORY_DELTA} |

### State

- Coverage, compatibility, and sampled performance are reported here from the same PR workflow.
- This is a delta report, not a release gate by itself. Required checks still decide merge safety.
- Performance is a smoke comparison, not a full benchmark lab run.
EOF
