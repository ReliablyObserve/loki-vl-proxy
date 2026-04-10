#!/usr/bin/env bash
set -euo pipefail

DEFAULT_PROXY_URL="${PROXY_URL_DEFAULT:-${PROXY_URL:-http://127.0.0.1:3100}}"
CATEGORIZED_PROXY_URL="${PROXY_URL_CATEGORIZED:-${DEFAULT_PROXY_URL}}"
QUERY="${SMOKE_QUERY:-{job=~\".+\"}}"
LIMIT="${SMOKE_LIMIT:-20}"
LOOKBACK_SECONDS="${SMOKE_LOOKBACK_SECONDS:-900}"
RETRIES="${SMOKE_RETRIES:-15}"
RETRY_SLEEP_SECONDS="${SMOKE_RETRY_SLEEP_SECONDS:-2}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

compute_window() {
  now_ns="$(($(date +%s) * 1000000000))"
  start_ns="$((now_ns - LOOKBACK_SECONDS * 1000000000))"
}

tuple_count() {
  local payload="$1"
  echo "$payload" | jq '[.data.result[]?.values[]?] | length'
}

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

  if ! echo "$payload" | jq -e '
    [.data.result[]?.values[]? | (type == "array" and length == 2 and (.[0] | type == "string") and (.[1] | type == "string"))]
    | all
  ' >/dev/null; then
    local sample
    sample="$(echo "$payload" | jq -c '.data.result[0].values[0] // empty')"
    echo "strict 2-tuple contract failed for ${endpoint}; sample tuple=${sample:-<none>}" >&2
    return 1
  fi
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

  if ! echo "$payload" | jq -e '
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
  ' >/dev/null; then
    local sample
    sample="$(echo "$payload" | jq -c '.data.result[0].values[0] // empty')"
    echo "categorize-labels 3-tuple contract failed for ${endpoint}; sample tuple=${sample:-<none>}" >&2
    return 1
  fi
}

fetch_query_range() {
  local base_url="$1"
  compute_window
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${base_url}/loki/api/v1/query_range"
}

fetch_query() {
  local base_url="$1"
  compute_window
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "time=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${base_url}/loki/api/v1/query"
}

fetch_query_range_categorized() {
  local base_url="$1"
  compute_window
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    -H 'X-Loki-Response-Encoding-Flags: categorize-labels' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "start=${start_ns}" \
    --data-urlencode "end=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${base_url}/loki/api/v1/query_range"
}

fetch_query_categorized() {
  local base_url="$1"
  compute_window
  curl -sS --get \
    -H 'X-Grafana-User: smoke-canary' \
    -H 'X-Grafana-Org-Id: 1' \
    -H 'X-Loki-Response-Encoding-Flags: categorize-labels' \
    --data-urlencode "query=${QUERY}" \
    --data-urlencode "time=${now_ns}" \
    --data-urlencode "limit=${LIMIT}" \
    "${base_url}/loki/api/v1/query"
}

fetch_with_retry() {
  local fetcher="$1"
  local endpoint="$2"
  local base_url="$3"
  local payload=""
  local count=0
  local attempt=1

  while [[ "$attempt" -le "$RETRIES" ]]; do
    payload="$($fetcher "$base_url")"
    count="$(tuple_count "$payload")"
    if [[ "$count" -gt 0 ]]; then
      echo "$payload"
      return 0
    fi
    if [[ "$attempt" -lt "$RETRIES" ]]; then
      sleep "$RETRY_SLEEP_SECONDS"
    fi
    attempt="$((attempt + 1))"
  done

  echo "no log tuples returned for ${endpoint} after ${RETRIES} attempts; cannot validate tuple contract" >&2
  return 1
}

range_payload="$(fetch_with_retry fetch_query_range "/query_range" "$DEFAULT_PROXY_URL")"
query_payload="$(fetch_with_retry fetch_query "/query" "$DEFAULT_PROXY_URL")"
range_categorized_payload="$(fetch_with_retry fetch_query_range_categorized "/query_range categorize-labels" "$CATEGORIZED_PROXY_URL")"
query_categorized_payload="$(fetch_with_retry fetch_query_categorized "/query categorize-labels" "$CATEGORIZED_PROXY_URL")"

check_strict_two_tuple "$range_payload" "/query_range"
check_strict_two_tuple "$query_payload" "/query"
check_categorize_three_tuple "$range_categorized_payload" "/query_range"
check_categorize_three_tuple "$query_categorized_payload" "/query"

echo "tuple contract smoke check passed (default=${DEFAULT_PROXY_URL} strict 2-tuple; categorized=${CATEGORIZED_PROXY_URL} 3-tuple)"
