#!/usr/bin/env bash
set -euo pipefail

PROXY_URL="${PROXY_URL:-http://127.0.0.1:3100}"
QUERY="${SMOKE_QUERY:-{job=~\".+\"}}"
LIMIT="${SMOKE_LIMIT:-20}"
LOOKBACK_SECONDS="${SMOKE_LOOKBACK_SECONDS:-900}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

now_ns="$(($(date +%s) * 1000000000))"
start_ns="$((now_ns - LOOKBACK_SECONDS * 1000000000))"

check_strict_two_tuple() {
  local payload="$1"
  local endpoint="$2"

  echo "$payload" | jq -e '.status == "success"' >/dev/null

  local tuple_count
  tuple_count="$(echo "$payload" | jq '[.data.result[]?.values[]?] | length')"
  if [[ "$tuple_count" -eq 0 ]]; then
    echo "no log tuples returned for ${endpoint}; cannot validate tuple contract" >&2
    return 1
  fi

  echo "$payload" | jq -e '
    [.data.result[]?.values[]? | (type == "array" and length == 2 and (.[0] | type == "string") and (.[1] | type == "string"))]
    | all
  ' >/dev/null
}

check_categorize_three_tuple() {
  local payload="$1"
  local endpoint="$2"

  echo "$payload" | jq -e '.status == "success"' >/dev/null

  local tuple_count
  tuple_count="$(echo "$payload" | jq '[.data.result[]?.values[]?] | length')"
  if [[ "$tuple_count" -eq 0 ]]; then
    echo "no log tuples returned for ${endpoint} categorize-labels check; cannot validate tuple contract" >&2
    return 1
  fi

  echo "$payload" | jq -e '
    [
      .data.result[]?.values[]?
      | (
          type == "array"
          and length == 3
          and (.[0] | type == "string")
          and (.[1] | type == "string")
          and (.[2] | type == "object")
          and ((.[2] | has("structured_metadata")) | not)
          and (
            (.[2] | length == 0)
            or (.[2] | has("structuredMetadata"))
            or (.[2] | has("parsed"))
          )
        )
    ]
    | all
  ' >/dev/null
}

fetch_query_range() {
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${PROXY_URL}/loki/api/v1/query_range"
}

fetch_query() {
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "time=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${PROXY_URL}/loki/api/v1/query"
}

fetch_query_range_categorized() {
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    -H 'X-Loki-Response-Encoding-Flags: categorize-labels' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${PROXY_URL}/loki/api/v1/query_range"
}

fetch_query_categorized() {
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    -H 'X-Loki-Response-Encoding-Flags: categorize-labels' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "time=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${PROXY_URL}/loki/api/v1/query"
}

range_payload="$(fetch_query_range)"
query_payload="$(fetch_query)"
range_categorized_payload="$(fetch_query_range_categorized)"
query_categorized_payload="$(fetch_query_categorized)"

check_strict_two_tuple "$range_payload" "/query_range"
check_strict_two_tuple "$query_payload" "/query"
check_categorize_three_tuple "$range_categorized_payload" "/query_range"
check_categorize_three_tuple "$query_categorized_payload" "/query"

echo "tuple contract smoke check passed for /query_range and /query (default 2-tuple + categorize-labels 3-tuple)"
