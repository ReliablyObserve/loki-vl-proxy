#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "" ]; then
  echo "usage: $0 <output-json>" >&2
  exit 1
fi

OUTPUT_JSON="$1"
ROOT_DIR="$(pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d)"
# The go test package timeout of the test collection and the budget of the whole collection, which stays above it.
QUALITY_TEST_PACKAGE_TIMEOUT="${QUALITY_TEST_PACKAGE_TIMEOUT:-18m}"
QUALITY_TESTS_BUDGET_SECONDS="${QUALITY_TESTS_BUDGET_SECONDS:-1200}"
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
      if [ -n "${QUALITY_DIAGNOSTICS_DIR:-}" ]; then
        mkdir -p "$QUALITY_DIAGNOSTICS_DIR"
        cp "$err_file" "$QUALITY_DIAGNOSTICS_DIR/${label}.stderr"
        if [ "$label" = "tests_and_coverage" ] && [ -f "$TMP_DIR/go-test-events.json" ]; then
          cp "$TMP_DIR/go-test-events.json" "$QUALITY_DIAGNOSTICS_DIR/go-test-events.json"
        fi
      fi
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
  else
    status=$?
  fi

  log_step "${label} failed (exit=${status}); using fallback"
  if [ -n "${QUALITY_DIAGNOSTICS_DIR:-}" ]; then
    mkdir -p "$QUALITY_DIAGNOSTICS_DIR"
    cp "$err_file" "$QUALITY_DIAGNOSTICS_DIR/${label}.stderr"
  fi
  cat "$err_file" >&2 || true
  if [ "$label" = "tests_and_coverage" ] && jq -e '.execution_state == "failed" and (.outcomes | type == "object")' "$out_file" >/dev/null 2>&1; then
    cat "$out_file"
    return 0
  fi
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

# The tests, run by parallel jobs (scripts/ci/go_test_unit.sh with UNIT_RACE=0 UNIT_COUNT1=1, one artifact directory
# $QUALITY_TESTS_PREFIX<shard> per entry of QUALITY_TESTS_SHARDS under $QUALITY_TESTS_FROM), instead of one `go test ./...`:
# the outcome events go to $events_file, the merged coverage profile to $cover_file and, when the corpus is on, the
# shards' recordings into $LVP_QUERY_CORPUS_DIR. A shard job does not fail when its tests do; it records the exit status
# of its `go test` (test-exit-code). Returns that status (the first non-zero one) when tests failed, 0 when every shard
# passed and together they ran every test of the split packages exactly once, and 99 when there is no usable result
# (a shard left nothing, or the shards disagree): the caller reports that like a collection that timed out.
fetch_shard_results() {
  local from="$QUALITY_TESTS_FROM" prefix="${QUALITY_TESTS_PREFIX:-pr-quality-tests-}" shards="${QUALITY_TESTS_SHARDS:-}"
  local shard code status=0 f
  if [ -z "$shards" ]; then
    log_step "QUALITY_TESTS_SHARDS is empty"
    return 99
  fi
  for shard in $shards; do
    if [ ! -f "$from/$prefix$shard/test-exit-code" ]; then
      log_step "test shard ${shard} left no result"
      return 99
    fi
    code="$(tr -d '[:space:]' <"$from/$prefix$shard/test-exit-code")"
    case "$code" in ''|*[!0-9]*) log_step "test shard ${shard} left an unreadable exit status"; return 99 ;; esac
    if [ "$code" != "0" ] && [ "$status" -eq 0 ]; then status="$code"; fi
  done
  # Completeness (every test in exactly one shard, each with an outcome) is required of a passing run only: a run
  # that failed is reported with the outcomes it has.
  local complete=()
  if [ "$status" -eq 0 ]; then complete=(--require-complete); fi
  if ! python3 "$SCRIPT_DIR/merge_unit_shards.py" --shards-dir "$from" --dir-prefix "$prefix" --out-dir "$TMP_DIR/merged-tests" \
      --shards "$shards" --no-cache-profile ${complete[@]+"${complete[@]}"} >&2; then
    log_step "the test shards do not hold every test exactly once"
    return 99
  fi
  cp "$TMP_DIR/merged-tests/unit-results.json" "$events_file"
  if [ -f "$TMP_DIR/merged-tests/coverage.out" ]; then cp "$TMP_DIR/merged-tests/coverage.out" "$cover_file"; else : >"$cover_file"; fi
  if [ -n "${LVP_QUERY_CORPUS_DIR:-}" ]; then
    for shard in $shards; do
      for f in "$from/$prefix$shard"/recordings/*.jsonl; do
        [ -f "$f" ] && cp "$f" "$LVP_QUERY_CORPUS_DIR/${shard}-$(basename "$f")"
      done
    done
  fi
  return "$status"
}

collect_tests_and_coverage() {
  # A snapshot of this commit's count and coverage taken by an earlier run of this
  # script (the PR quality workflow caches the base commit's): the numbers depend
  # only on the commit, so the tests are not run again. A missing, unreadable or
  # all-zero file (a collection that timed out) is ignored.
  if [ -n "${QUALITY_TESTS_SNAPSHOT:-}" ] && jq -e '(.count > 0) and (.coverage_pct | type == "number")' "$QUALITY_TESTS_SNAPSHOT" >/dev/null 2>&1; then
    log_step "using the saved test count and coverage in ${QUALITY_TESTS_SNAPSHOT}"
    jq -c '{count, coverage_pct} + (if has("outcomes") then {outcomes} else {} end) + (if has("execution_state") then {execution_state} else {} end) + (if has("package_coverage") then {package_coverage} else {} end)' "$QUALITY_TESTS_SNAPSHOT"
    return 0
  fi
  local cover_file="$TMP_DIR/coverage.out"
  local events_file="$TMP_DIR/go-test-events.json"
  # With QUALITY_QUERY_CORPUS_OUT set, the same uncached run records every
  # query the tests send in (internal/querycorpus) and the corpus is built
  # from it afterwards, so capturing costs no second test run.
  local tags=()
  if [ -n "${QUALITY_QUERY_CORPUS_OUT:-}" ]; then
    export LVP_QUERY_CORPUS_DIR="$TMP_DIR/query-corpus"
    mkdir -p "$LVP_QUERY_CORPUS_DIR"
    tags=(-tags corpuscapture)
  fi
  # The package timeout stays under the collection's own budget (capture_async tests_and_coverage) so a hang
  # still fails with its goroutine dump. Go's 10m default was too tight: internal/proxy runs about 10 minutes
  # here with coverage and the corpus capture on a busy runner.
  local test_status=0
  if [ -n "${QUALITY_TESTS_FROM:-}" ]; then
    fetch_shard_results || test_status=$?
    if [ "$test_status" -eq 99 ]; then return 99; fi
  else
    go test "${tags[@]}" ./... -coverprofile="$cover_file" -count=1 -timeout="$QUALITY_TEST_PACKAGE_TIMEOUT" -json >"$events_file" || test_status=$?
  fi
  if [ "$test_status" -eq 0 ]; then
    if [ -n "${QUALITY_QUERY_CORPUS_OUT:-}" ]; then
      # A corpus that fails to build must not cost the report its test count.
      # The checks on the corpus run after this timed collection (check_query_corpus).
      LVP_QUERY_CORPUS_OUT="$QUALITY_QUERY_CORPUS_OUT" go test ./internal/querycorpus -run '^TestBuildQueryCorpus$' -count=1 -v >&2 \
        || log_step "query corpus build failed; the report continues without it"
    fi
  else
    if [ -n "${QUALITY_DIAGNOSTICS_DIR:-}" ]; then
      mkdir -p "$QUALITY_DIAGNOSTICS_DIR"
      cp "$events_file" "$QUALITY_DIAGNOSTICS_DIR/go-test-events.json"
    fi
    log_step "go test -json failed (exit=${test_status}); failing test events:"
    jq -r 'select(.Action == "fail") | [.Package, (.Test // "package")] | @tsv' "$events_file" >&2 || true
    # The failing tests' own output (their t.Error/t.Fatal lines), so a failure that does not reproduce
    # elsewhere still shows its cause in the job log; bounded so a noisy test cannot flood it.
    log_step "output of the failing tests (first 400 lines):"
    jq -r -s '
      ([.[] | select(.Action == "fail" and .Test) | .Package + "\t" + .Test]) as $failed
      | .[] | select(.Action == "output" and .Test and ((.Package + "\t" + .Test) as $k | $failed | index($k)))
      | .Test + ": " + (.Output | rtrimstr("\n"))' "$events_file" 2>/dev/null | head -n 400 >&2 || true
    python3 "$SCRIPT_DIR/pr_evidence.py" --summarize-tests "$events_file" --test-exit-code "$test_status"
    return "$test_status"
  fi
  local coverage
  coverage="$(go tool cover -func="$cover_file" | tail -1 | awk '{print substr($3, 1, length($3)-1)}')"
  python3 "$SCRIPT_DIR/pr_evidence.py" --summarize-tests "$events_file" \
    --coverage-profile "$cover_file" --coverage-percent "${coverage:-0}"
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
# Written by collect_tests_and_coverage when QUALITY_QUERY_CORPUS_OUT is set.
QUERY_CORPUS_REPORT="$TMP_DIR/query_corpus.json"
compat_file="$TMP_DIR/compat.json"
benchmarks_file="$TMP_DIR/benchmarks.json"
load_file="$TMP_DIR/load.json"

BENCHMARKS_DEFAULT='{"query_range_cache_hit_ns_per_op":0,"query_range_cache_hit_bytes_per_op":0,"query_range_cache_hit_allocs_per_op":0,"query_range_cache_bypass_ns_per_op":0,"query_range_cache_bypass_bytes_per_op":0,"query_range_cache_bypass_allocs_per_op":0,"labels_cache_hit_ns_per_op":0,"labels_cache_hit_bytes_per_op":0,"labels_cache_hit_allocs_per_op":0,"labels_cache_bypass_ns_per_op":0,"labels_cache_bypass_bytes_per_op":0,"labels_cache_bypass_allocs_per_op":0}'
LOAD_DEFAULT='{"high_concurrency_req_per_s":0,"high_concurrency_memory_growth_mb":0}'
PERF_MODE="full"
if [ "${QUALITY_SKIP_PERF:-0}" = "1" ]; then
  PERF_MODE="skipped"
fi

TESTS_DEFAULT='{"count":0,"coverage_pct":0}'
COMPAT_DEFAULT='{"loki":{"passed":0,"total":0,"pct":0},"drilldown":{"passed":0,"total":0,"pct":0},"vl":{"passed":0,"total":0,"pct":0}}'
if [ "${QUALITY_COMPAT_ONLY:-0}" = "1" ]; then
  # The PR quality workflow measures the compatibility scores in a job of its own (they need the compose stack and
  # nothing else); the snapshot job reads the result back (QUALITY_COMPAT_FROM).
  log_step "collecting the compatibility scores only (QUALITY_COMPAT_ONLY=1)"
  capture_or_default compat "$COMPAT_DEFAULT" 1800 collect_compat >"$OUTPUT_JSON"
  exit 0
fi
if [ "${QUALITY_PERF_ONLY:-0}" = "1" ]; then
  # The PR quality workflow measures performance in a job of its own, for both trees on one runner;
  # this snapshot carries only .performance, and the tests and compatibility come from the other jobs.
  log_step "collecting performance only (QUALITY_PERF_ONLY=1)"
  printf '%s\n' "$TESTS_DEFAULT" >"$tests_file"
  printf '%s\n' "$COMPAT_DEFAULT" >"$compat_file"
else
  tests_pid=""
  compat_pid=""
  capture_async tests_and_coverage "$TESTS_DEFAULT" "$QUALITY_TESTS_BUDGET_SECONDS" "$tests_file" tests_pid collect_tests_and_coverage
  if [ -n "${QUALITY_COMPAT_FROM:-}" ]; then
    # Measured by an earlier job. A missing or unusable file counts as a collection that failed.
    if jq -e '.loki.total != null and .drilldown.total != null and .vl.total != null' "$QUALITY_COMPAT_FROM" >/dev/null 2>&1; then
      log_step "using the compatibility scores in ${QUALITY_COMPAT_FROM}"
      cp "$QUALITY_COMPAT_FROM" "$compat_file"
    else
      log_step "no usable compatibility scores in ${QUALITY_COMPAT_FROM}; using fallback"
      printf '%s' "$COMPAT_DEFAULT" >"$compat_file"
    fi
  else
    capture_async compat "$COMPAT_DEFAULT" 1800 "$compat_file" compat_pid collect_compat
  fi

  for pid in "$tests_pid" "$compat_pid"; do
    [ -z "$pid" ] || wait "$pid"
  done
fi

if [ "$PERF_MODE" = "full" ]; then
  log_step "running perf smoke in an isolated phase for stability"
  capture_or_default benchmarks "$BENCHMARKS_DEFAULT" 900 collect_benchmarks >"$benchmarks_file"
  if [ "${QUALITY_SKIP_LOAD:-0}" = "1" ]; then
    # The caller measures base and head together (scripts/ci/load_abab.py), which a
    # run of one tree cannot do; it fills performance.load afterwards.
    log_step "skipping the load test (QUALITY_SKIP_LOAD=1); load_abab.py measures it"
    printf '%s\n' "$LOAD_DEFAULT" >"$load_file"
  else
    capture_or_default load "$LOAD_DEFAULT" 600 collect_load >"$load_file"
  fi
else
  log_step "skipping performance smoke collection (QUALITY_SKIP_PERF=1)"
  printf '%s\n' "$BENCHMARKS_DEFAULT" >"$benchmarks_file"
  printf '%s\n' "$LOAD_DEFAULT" >"$load_file"
fi

python3 "$SCRIPT_DIR/quality_scope.py" --root "$ROOT_DIR" > "$TMP_DIR/inventory.json"

# Checks on the corpus the test collection built, outside its timeout: VictoriaLogs' and Loki's own parsers, then the
# summary against the committed targets and, with a base corpus, the golden changes and the queries that got worse. The quality gate reads those from the report;
# a failing check never stops the collection.
check_query_corpus() {
  local corpus="${QUALITY_QUERY_CORPUS_OUT:-}"
  if [ -z "$corpus" ] || [ ! -s "${corpus%.jsonl.gz}.summary.json" ]; then
    return 0
  fi
  # Both parse oracles (test/oracle/vl and test/oracle/loki, modules of their own) write their verdicts beside the
  # corpus; the report below reads them. An oracle that does not run leaves its section out.
  # The two oracles write different files, so they run side by side (the VictoriaLogs one takes minutes); each one's
  # log is shown whole when it ends.
  (
    LVP_RUN_ORACLES=1 LVP_QUERY_CORPUS_OUT="$corpus" go test ./internal/querycorpus -run '^TestVLOracle$' -count=1 -v >"$TMP_DIR/oracle-vl.log" 2>&1 \
      || log_step "the VictoriaLogs parse oracle did not run; the report continues without it"
    cat "$TMP_DIR/oracle-vl.log" >&2
  ) &
  local vl_oracle=$!
  (
    LVP_RUN_ORACLES=1 LVP_QUERY_CORPUS_OUT="$corpus" go test ./internal/proxy -run '^TestLokiOracle$' -count=1 -v >"$TMP_DIR/oracle-loki.log" 2>&1 \
      || log_step "the Loki parse oracle did not run; the report continues without it"
    cat "$TMP_DIR/oracle-loki.log" >&2
  ) &
  local loki_oracle=$!
  wait "$vl_oracle" "$loki_oracle"
  # The Loki differential of log queries (internal/querycorpus): the live Loki and VictoriaLogs oracles must answer
  # what its recorded answers hold; the unit tests compare the plan with those answers on every run.
  LVP_RUN_ORACLES=1 LVP_LOKI_DIFF_CHECK=1 go test ./internal/querycorpus -run '^TestPlanMatchesLokiOnLogQueries$' -count=1 -v \
    >"$TMP_DIR/oracle-loki-diff.log" 2>&1 \
    || log_step "the Loki differential's live oracle check failed; its recorded answers are stale"
  cat "$TMP_DIR/oracle-loki-diff.log" >&2
  LVP_QUERY_CORPUS_OUT="$corpus" LVP_QUERY_CORPUS_REPORT="$QUERY_CORPUS_REPORT" \
    LVP_QUERY_CORPUS_BASE="${QUALITY_QUERY_CORPUS_BASE:-}" \
    go test ./internal/querycorpus -run '^TestQueryCorpusReport$' -count=1 -v >&2 \
    || log_step "queries in the corpus got worse; see the report"
}
check_query_corpus
if [ ! -s "$QUERY_CORPUS_REPORT" ]; then echo 'null' >"$QUERY_CORPUS_REPORT"; fi
jq -n \
  --slurpfile tests "$tests_file" \
  --slurpfile corpus "$QUERY_CORPUS_REPORT" \
  --slurpfile compat "$compat_file" \
  --slurpfile benchmarks "$benchmarks_file" \
  --slurpfile load "$load_file" \
  --slurpfile inventory "$TMP_DIR/inventory.json" \
  --arg perf_mode "$PERF_MODE" \
  '{
    tests: $tests[0],
    quality_inventory: $inventory[0],
    query_corpus: $corpus[0],
    compatibility: $compat[0],
    performance: {
      mode: $perf_mode,
      benchmarks: $benchmarks[0],
      load: $load[0]
    }
  }' >"$OUTPUT_JSON"
