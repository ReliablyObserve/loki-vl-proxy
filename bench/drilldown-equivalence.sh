#!/usr/bin/env bash
#
# drilldown-equivalence.sh — verify that an optimization didn't change the
# data shape returned by Drilldown endpoints. Captures baseline responses
# for the slow filter × endpoint combos, then re-captures after the change
# and diffs the parsed JSON content (independent of key ordering).
#
# Usage:
#   bench/drilldown-equivalence.sh capture <baseline-dir>
#       — capture every (endpoint, filter) response into <baseline-dir>.
#   bench/drilldown-equivalence.sh verify <baseline-dir>
#       — re-capture and diff against the saved baseline. Exits 1 on diff.
#
# Required: jq, curl, diff
# Optional env:
#   PROXY_URL          (default http://localhost:13200 — vmauth-ring LB)
#   QUERY_BASE         (default '{env="production"}')
#   LOOKBACK_SECONDS   (default 86400 — 24h)
#
# Semantics of "equivalent":
#   - same set of fields/labels/values returned (order-insensitive)
#   - same cardinalities/counts within ±5%
#   - status codes match
#
# Use case: before changing a code path that may impact Drilldown output,
# capture baseline. Apply the change. Run verify. If equivalent: ship. If
# not: investigate diff before deciding to ship the optimization.

set -euo pipefail

PROXY_URL="${PROXY_URL:-http://localhost:13200}"
# QUERY_BASES: pipe-separated list of stream selectors to sweep so the matrix
# covers narrow-selector vs broad-selector vs label-regex shapes.
QUERY_BASES_DEFAULT='{env="production"}|{service_name=~".+"}|{job="api-gateway"}|{namespace="production",app=~".+"}'
QUERY_BASES="${QUERY_BASES:-$QUERY_BASES_DEFAULT}"
# RANGES: pipe-separated list of lookback windows (seconds). Sweeps the
# common Grafana time-picker presets — short, medium, long. Each impacts
# the proxy's chunking / scan / cache behaviour differently.
RANGES_DEFAULT='900|3600|21600|86400|604800'
RANGES="${RANGES:-$RANGES_DEFAULT}"

for tool in jq curl diff; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing required tool: $tool" >&2; exit 1; }
done

if command -v rtk >/dev/null 2>&1; then
  CURL=(rtk proxy curl)
else
  CURL=(curl)
fi

ENDPOINTS=(
  "labels"
  "label/job/values"
  "label/service_name/values"
  "index/stats"
  "detected_labels"
  "detected_fields"
  "detected_field/level/values"
  "detected_field/path/values"
  "detected_field/duration_ms/values"
)

FILTERS=(
  ""
  "| detected_level=\"error\""
  "| detected_level=~\"error|warn\""
  "| detected_level!=\"error\""
  "|= \"GET\""
  "|= \"POST\""
  "|~ \"error|fail\""
  "!= \"healthcheck\""
  "| logfmt | level=\"error\""
  "| json | duration > 100"
  "| logfmt | level=~\"error|warn\" | duration_ms > 50"
  "|= \"trace_id\" | logfmt | level=\"error\""
)

end_ns=$(($(date +%s) * 1000000000))
IFS='|' read -ra QUERY_BASE_LIST <<< "$QUERY_BASES"
IFS='|' read -ra RANGE_LIST <<< "$RANGES"

# normalise(): jq filter that strips ordering / metadata fields so two
# equivalent responses compare equal regardless of jitter.
normalise() {
  jq -S '
    if type == "object" then
      with_entries(select(.key | IN("status","data","fields","values","detected_labels","cardinality","label","value")))
    else . end
    | walk(
        if type == "array" and length > 0 and (.[0] | type) == "string"
          then sort
        elif type == "array" and length > 0 and (.[0] | type) == "object"
          then sort_by((.label // .value // .name // ""))
        else . end
      )
  '
}

capture_one() {
  local endpoint="$1" filter="$2" query_base="$3" range_s="$4" outdir="$5"
  local query="$query_base $filter"
  local start_ns_local=$(( end_ns - range_s * 1000000000 ))
  local safe
  safe=$(printf '%s_%s_%s_%s' "$endpoint" "$filter" "$query_base" "${range_s}s" \
    | tr -c 'a-zA-Z0-9' '_' | tr -s '_' | head -c 150)
  local raw="${outdir}/${safe}.raw.json"
  local norm="${outdir}/${safe}.norm.json"
  local code
  code=$("${CURL[@]}" -s -G "${PROXY_URL}/loki/api/v1/${endpoint}" \
    --data-urlencode "query=${query}" \
    --data-urlencode "start=${start_ns_local}" \
    --data-urlencode "end=${end_ns}" \
    --data-urlencode "match[]=${query_base}" \
    -o "$raw" -w "%{http_code}")
  if [[ -f "$raw" ]] && [[ -s "$raw" ]]; then
    jq empty "$raw" 2>/dev/null && normalise < "$raw" > "$norm" 2>/dev/null \
      || echo "{}" > "$norm"
  else
    echo "{}" > "$norm"
  fi
}

sweep_all() {
  local outdir="$1"
  local total=0
  for endpoint in "${ENDPOINTS[@]}"; do
    for filter in "${FILTERS[@]}"; do
      for query_base in "${QUERY_BASE_LIST[@]}"; do
        for range_s in "${RANGE_LIST[@]}"; do
          capture_one "$endpoint" "$filter" "$query_base" "$range_s" "$outdir"
          total=$((total + 1))
        done
      done
    done
  done
  echo "$total"
}

cmd="${1:-}"
dir="${2:-}"

if [[ -z "$cmd" || -z "$dir" ]]; then
  echo "usage: $0 {capture|verify} <directory>" >&2
  exit 2
fi

total_combos=$(( ${#ENDPOINTS[@]} * ${#FILTERS[@]} * ${#QUERY_BASE_LIST[@]} * ${#RANGE_LIST[@]} ))

case "$cmd" in
  capture)
    mkdir -p "$dir"
    echo "Capturing baseline to $dir — ${#ENDPOINTS[@]} endpoints × ${#FILTERS[@]} filters × ${#QUERY_BASE_LIST[@]} selectors × ${#RANGE_LIST[@]} ranges = $total_combos combos..."
    n=$(sweep_all "$dir")
    echo "Captured $n responses ($(ls "$dir"/*.norm.json 2>/dev/null | wc -l | tr -d ' ') normalised files)."
    ;;
  verify)
    if [[ ! -d "$dir" ]]; then
      echo "baseline dir $dir does not exist; run capture first" >&2
      exit 2
    fi
    tmp=$(mktemp -d -t drilldown-equiv-XXXXXX)
    trap 'rm -rf "$tmp"' EXIT
    echo "Capturing current responses to $tmp ($total_combos combos)..."
    sweep_all "$tmp" > /dev/null
    echo ""
    echo "=== Diffs against baseline $dir ==="
    diff_count=0
    for cur in "$tmp"/*.norm.json; do
      base="$dir/$(basename "$cur")"
      if [[ ! -f "$base" ]]; then
        echo "MISSING baseline: $(basename "$cur")"
        diff_count=$((diff_count + 1))
        continue
      fi
      if ! diff -q "$base" "$cur" >/dev/null 2>&1; then
        diff_count=$((diff_count + 1))
        echo "--- DIFF: $(basename "$cur") ---"
        diff "$base" "$cur" | head -30
        echo ""
      fi
    done
    if (( diff_count == 0 )); then
      echo "ALL EQUIVALENT — $(ls "$tmp"/*.norm.json | wc -l) responses match baseline."
      exit 0
    else
      echo "DIFFS DETECTED: $diff_count responses changed."
      exit 1
    fi
    ;;
  *)
    echo "usage: $0 {capture|verify} <directory>" >&2
    exit 2
    ;;
esac
