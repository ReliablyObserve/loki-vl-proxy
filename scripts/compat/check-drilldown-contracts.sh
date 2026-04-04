#!/usr/bin/env bash

set -euo pipefail

TAG="${1:-}"
if [[ -z "${TAG}" ]]; then
  echo "usage: $0 <drilldown-tag>" >&2
  exit 2
fi

WORKDIR="$(mktemp -d)"
cleanup() {
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

git clone --depth 1 --branch "v${TAG#v}" https://github.com/grafana/logs-drilldown.git "${WORKDIR}/logs-drilldown" >/dev/null 2>&1

repo="${WORKDIR}/logs-drilldown"
passed=0
total=0

check() {
  local description="$1"
  local file="$2"
  local pattern="$3"
  total=$((total + 1))
  if rg -q "${pattern}" "${repo}/${file}"; then
    passed=$((passed + 1))
    echo "PASS ${description}"
  else
    echo "FAIL ${description} (${file})"
  fi
}

check "service selection volume contract" "src/services/datasource.ts" "getResource\\(['\"]index/volume['\"]"
check "detected fields contract" "src/services/datasource.ts" "getResource<DetectedFieldsResponse>\\(['\"]detected_fields['\"]"
check "mixed parser service logs contract" "src/services/variables.ts" "LOG_STREAM_SELECTOR_EXPR = .*\\| json .*\\| logfmt \\| drop __error__, __error_details__"
check "detected level field removal contract" "src/services/filters.ts" "FIELDS_TO_REMOVE = .*detected_level"
check "labels field parsing contract" "src/services/labels.ts" "getAllLabelsFromDataFrame"
check "level coloring contract" "src/services/panel.ts" "UNKNOWN_LEVEL_LOGS = 'logs'"

pct="0.0"
if [[ "${total}" -gt 0 ]]; then
  pct=$(awk "BEGIN { printf \"%.1f\", (${passed} * 100) / ${total} }")
fi

echo "Score: ${passed}/${total} (${pct}%)"
