#!/usr/bin/env bash
#
# drilldown-filter-matrix.sh — Drilldown endpoint × filter timing matrix.
#
# Sweeps every Drilldown endpoint (detected_fields, detected_labels,
# detected_field/*/values, index/stats, labels, label/*/values) against
# every filter shape (no filter, level=eq, level=regex, line=eq, line=regex,
# logfmt, json+numeric) so latency regressions in any combination are
# visible. Prints a table sorted by duration to surface the slowest combos
# for tuning. Use alongside bench/drilldown-vs-loki.sh — that one compares
# proxy vs Loki for canonical Drilldown queries; this one stress-tests
# every endpoint × filter shape against the proxy alone.
#
# Why this matters: a 5+ second response on detected_field/duration_ms/values
# when a level filter is applied (observed on 2026-06-07) freezes the
# Drilldown UI on "loading" or times the panel out entirely. The matrix
# catches that regression in a single run.
#
# Required: jq, curl, awk
# Optional env overrides:
#   PROXY_URL          (default http://localhost:13200 — vmauth-ring LB)
#   QUERY_BASE         (default '{env="production"}')
#   LOOKBACK_SECONDS   (default 86400 — 24h, matches Drilldown default)
#   SLOW_THRESHOLD_MS  (default 1000 — annotated with ★)
#   VERY_SLOW_MS       (default 3000 — annotated with ★★)
#   OUTPUT_MD          (path to write markdown report; default: stdout only)
#
# Exit code: 0 always (timing matrix is informational). Use --fail-on-slow
# to exit 1 if any combo exceeds VERY_SLOW_MS — useful in CI gates.

set -euo pipefail

PROXY_URL="${PROXY_URL:-http://localhost:13200}"
QUERY_BASE="${QUERY_BASE:-{env=\"production\"}}"
LOOKBACK_SECONDS="${LOOKBACK_SECONDS:-86400}"
SLOW_THRESHOLD_MS="${SLOW_THRESHOLD_MS:-1000}"
VERY_SLOW_MS="${VERY_SLOW_MS:-3000}"
OUTPUT_MD="${OUTPUT_MD:-}"
FAIL_ON_SLOW=0
for arg in "$@"; do
  [[ "$arg" == "--fail-on-slow" ]] && FAIL_ON_SLOW=1
done

for tool in jq curl awk; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    exit 1
  fi
done

# Bypass RTK token proxy if present (otherwise large /metrics responses get
# truncated; same pattern as scripts/bench-cache-tiers.sh).
if command -v rtk >/dev/null 2>&1; then
  CURL=(rtk proxy curl)
else
  CURL=(curl)
fi

end_ns=$(($(date +%s) * 1000000000))
start_ns=$(( end_ns - LOOKBACK_SECONDS * 1000000000 ))

# Endpoints to probe. Each entry is a path under /loki/api/v1.
ENDPOINTS=(
  "labels"
  "label/job/values"
  "label/service_name/values"
  "index/stats"
  "index/volume"
  "detected_labels"
  "detected_fields"
  "detected_field/level/values"
  "detected_field/path/values"
  "detected_field/duration_ms/values"
)

# Filter shapes. Empty = baseline. Suffix appended verbatim to QUERY_BASE.
FILTERS=(
  ""
  "| detected_level=\"error\""
  "| detected_level=~\"error|warn\""
  "|= \"GET\""
  "|~ \"error|fail\""
  "| logfmt | level=\"error\""
  "| json | duration > 100"
)

rows=()
slow_combos=0
very_slow_combos=0

for endpoint in "${ENDPOINTS[@]}"; do
  for filter in "${FILTERS[@]}"; do
    query="$QUERY_BASE $filter"
    t1=$(date +%s%N)
    code=$("${CURL[@]}" -s -G "${PROXY_URL}/loki/api/v1/${endpoint}" \
      --data-urlencode "query=${query}" \
      --data-urlencode "start=${start_ns}" \
      --data-urlencode "end=${end_ns}" \
      --data-urlencode "match[]=${QUERY_BASE}" \
      -o /dev/null -w "%{http_code}" 2>/dev/null || echo "000")
    t2=$(date +%s%N)
    dur_ms=$(( (t2 - t1) / 1000000 ))
    marker=""
    if (( dur_ms > VERY_SLOW_MS )); then
      marker="★★"
      very_slow_combos=$((very_slow_combos + 1))
    elif (( dur_ms > SLOW_THRESHOLD_MS )); then
      marker="★"
      slow_combos=$((slow_combos + 1))
    fi
    # Use literal TAB as field separator — '|' appears in regex filters.
    rows+=("$(printf '%s\t%s\t%07d\t%s\t%s' "$endpoint" "${filter:-no filter}" "$dur_ms" "$code" "$marker")")
  done
done

# Print table sorted by duration ascending (slowest at the bottom).
{
  printf '%-40s %-35s %10s %-5s %s\n' "endpoint" "filter" "duration" "code" "alert"
  printf '%-40s %-35s %10s %-5s %s\n' "----------------------------------------" "-----------------------------------" "----------" "----" "-----"
  printf '%s\n' "${rows[@]}" | sort -t $'\t' -k 3 -n | awk -F '\t' '{printf "%-40s %-35s %7dms %-5s %s\n", $1, $2, $3 + 0, $4, $5}'
  printf '\n★  = >%dms   ★★ = >%dms\n' "$SLOW_THRESHOLD_MS" "$VERY_SLOW_MS"
  printf 'summary: %d slow combos, %d very-slow combos (of %d total)\n' \
    "$slow_combos" "$very_slow_combos" "$(( ${#ENDPOINTS[@]} * ${#FILTERS[@]} ))"
} | tee /tmp/drilldown-filter-matrix.out

if [[ -n "$OUTPUT_MD" ]]; then
  {
    printf '# Drilldown filter × endpoint timing matrix\n\n'
    printf 'Captured: %s\n\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'Proxy URL: `%s`\n\n' "$PROXY_URL"
    printf 'Query base: `%s`  Lookback: %ds  Slow threshold: %dms  Very-slow: %dms\n\n' \
      "$QUERY_BASE" "$LOOKBACK_SECONDS" "$SLOW_THRESHOLD_MS" "$VERY_SLOW_MS"
    printf '```\n'
    cat /tmp/drilldown-filter-matrix.out
    printf '```\n'
  } > "$OUTPUT_MD"
  echo "Markdown report saved to $OUTPUT_MD" >&2
fi

if (( FAIL_ON_SLOW == 1 )) && (( very_slow_combos > 0 )); then
  exit 1
fi
exit 0
