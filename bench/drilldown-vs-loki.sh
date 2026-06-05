#!/usr/bin/env bash
# Drilldown / Explore real-query benchmark — Loki direct vs loki-vl-proxy.
#
# Replays the exact query shapes that motivated the 2026-06 Drilldown quality
# fix work (sum by pod / trace_id / k8s.pod.name / service_version, patterns,
# detected_fields) at the time ranges that broke (1h / 6h / 24h / 2d / 7d)
# against the live e2e-compat compose stack. Reports cold + warm latency,
# response status, body size, and series count for both targets so the README
# perf section can be written from real measured numbers.
#
# Prereqs:
#   test/e2e-compat compose stack up with log-generator producing data.
#   - Loki at $LOKI_URL   (default http://localhost:13101)
#   - Proxy at $PROXY_URL (default http://localhost:13109 — vmauth fronting
#     the proxy; mirrors the path Grafana actually uses).
#
# Usage:
#   ./bench/drilldown-vs-loki.sh
#   ./bench/drilldown-vs-loki.sh --output=results.md   # write a markdown table
#   ./bench/drilldown-vs-loki.sh --ranges=1h,24h,7d    # subset
#   ./bench/drilldown-vs-loki.sh --queries=pod,trace   # subset
#
# Output (default stdout): TSV with target, query, range, cold_ms, warm_ms,
# status, body_bytes, series_count.

set -euo pipefail

LOKI_URL="${LOKI_URL:-http://localhost:13101}"
PROXY_URL="${PROXY_URL:-http://localhost:13109}"
OUTPUT="${OUTPUT:-/dev/stdout}"

# Range list (overridable).
RANGES="${RANGES:-1h,6h,24h,2d,7d}"

# Query list (overridable). Each key maps to a LogQL expression below.
QUERIES="${QUERIES:-pod,trace_id,k8s_pod_name,service_version,detected_fields,labels}"

for arg in "$@"; do
  case "$arg" in
    --output=*) OUTPUT="${arg#*=}";;
    --ranges=*) RANGES="${arg#*=}";;
    --queries=*) QUERIES="${arg#*=}";;
    --help|-h)
      sed -n '1,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Query catalog — keep in lockstep with what Grafana Drilldown / Explore emit.
# ---------------------------------------------------------------------------

query_for() {
  case "$1" in
    pod)              echo 'sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))';;
    trace_id)         echo 'sum by (trace_id) (count_over_time({namespace="prod"}|json|drop __error__,__error_details__|trace_id!=""[2m]))';;
    k8s_pod_name)     echo 'sum by (k8s_pod_name) (count_over_time({namespace="prod",k8s_pod_name!=""}[2m]))';;
    service_version)  echo 'sum by (service_version) (count_over_time({namespace="prod",service_version!=""}[2m]))';;
    detected_fields)  echo '__detected_fields__';;
    labels)           echo '__labels__';;
    *) echo "" ;;
  esac
}

step_for_range() {
  case "$1" in
    1h)   echo "5s"   ;;
    6h)   echo "30s"  ;;
    24h)  echo "120s" ;;
    2d)   echo "240s" ;;
    7d)   echo "600s" ;;
    *)    echo "60s"  ;;
  esac
}

range_seconds() {
  case "$1" in
    1h)  echo 3600    ;;
    6h)  echo 21600   ;;
    24h) echo 86400   ;;
    2d)  echo 172800  ;;
    7d)  echo 604800  ;;
    *)   echo 3600    ;;
  esac
}

# Measure: cold latency (no cache header), warm latency (immediately replay).
# Output one TSV line per (target, query, range) combination.
measure() {
  local target_name="$1" base_url="$2" query_name="$3" range="$4"
  local query_str
  query_str=$(query_for "$query_name")
  if [[ -z "$query_str" ]]; then
    return
  fi
  local now end start step
  now=$(date +%s)
  end=$now
  start=$((now - $(range_seconds "$range")))
  step=$(step_for_range "$range")
  local path tmp_h tmp_b
  tmp_h=$(mktemp); tmp_b=$(mktemp)
  trap 'rm -f "$tmp_h" "$tmp_b"' RETURN

  # Path + params shape per query type.
  case "$query_name" in
    detected_fields)
      path="/loki/api/v1/detected_fields"
      curl_args=(
        --get
        --data-urlencode "query={namespace=\"prod\"}"
        --data-urlencode "start=$start"
        --data-urlencode "end=$end"
        --data-urlencode "limit=1000"
      )
      ;;
    labels)
      path="/loki/api/v1/labels"
      curl_args=(
        --get
        --data-urlencode "start=$start"
        --data-urlencode "end=$end"
      )
      ;;
    *)
      path="/loki/api/v1/query_range"
      curl_args=(
        --get
        --data-urlencode "query=$query_str"
        --data-urlencode "start=$start"
        --data-urlencode "end=$end"
        --data-urlencode "step=$step"
      )
      ;;
  esac

  local headers=(
    -H "X-Scope-OrgID: default"
    -H "User-Agent: Grafana/11.5.0"  # treat like a Grafana dashboard panel.
  )

  # Cold: cache-miss expected; some queries on Loki may legitimately take 30+s.
  local cold_ms
  cold_ms=$( { TIMEFORMAT=%R; time curl -sS --max-time 60 -D "$tmp_h" -o "$tmp_b" "${curl_args[@]}" "${headers[@]}" "$base_url$path" >/dev/null 2>&1; } 2>&1 )
  local cold_status cold_size
  cold_status=$(head -1 "$tmp_h" | awk '{print $2}')
  cold_size=$(wc -c < "$tmp_b" | tr -d ' ')
  local cold_series
  cold_series=$(python3 -c "
import json,sys
try:
    d=json.load(open('$tmp_b'))
    if 'data' in d:
        r=d['data'].get('result',[])
        if isinstance(r,list):
            print(len(r))
        elif 'fields' in d['data']:
            print(len(d['data']['fields']))
        elif isinstance(d['data'],list):
            print(len(d['data']))
        else:
            print(0)
    else:
        print(0)
except Exception:
    print(-1)
" 2>/dev/null || echo "?")

  # Warm: 200ms gap then replay — most paths should hit the cache.
  sleep 0.2
  local warm_ms warm_status warm_size warm_series
  warm_ms=$( { TIMEFORMAT=%R; time curl -sS --max-time 60 -D "$tmp_h" -o "$tmp_b" "${curl_args[@]}" "${headers[@]}" "$base_url$path" >/dev/null 2>&1; } 2>&1 )
  warm_status=$(head -1 "$tmp_h" | awk '{print $2}')
  warm_size=$(wc -c < "$tmp_b" | tr -d ' ')
  warm_series=$(python3 -c "
import json,sys
try:
    d=json.load(open('$tmp_b'))
    if 'data' in d:
        r=d['data'].get('result',[])
        if isinstance(r,list):
            print(len(r))
        elif 'fields' in d['data']:
            print(len(d['data']['fields']))
        elif isinstance(d['data'],list):
            print(len(d['data']))
        else:
            print(0)
    else:
        print(0)
except Exception:
    print(-1)
" 2>/dev/null || echo "?")

  printf "%s\t%s\t%s\t%.0f\t%.0f\t%s\t%s\t%s\t%s\t%s\n" \
    "$target_name" "$query_name" "$range" \
    "$(printf '%s\n' "$cold_ms" | awk '{print $1*1000}')" \
    "$(printf '%s\n' "$warm_ms" | awk '{print $1*1000}')" \
    "$cold_status" "$cold_size" "$cold_series" "$warm_status" "$warm_series"
}

main() {
  IFS=',' read -ra range_list <<< "$RANGES"
  IFS=',' read -ra query_list <<< "$QUERIES"

  {
    printf "# Drilldown vs Loki — measured %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf "# Loki=%s Proxy=%s\n" "$LOKI_URL" "$PROXY_URL"
    printf "target\tquery\trange\tcold_ms\twarm_ms\tcold_status\tcold_size\tcold_series\twarm_status\twarm_series\n"
    for q in "${query_list[@]}"; do
      for r in "${range_list[@]}"; do
        measure "loki"  "$LOKI_URL"  "$q" "$r" || true
        measure "proxy" "$PROXY_URL" "$q" "$r" || true
      done
    done
  } > "$OUTPUT" 2>&1
}

main "$@"
