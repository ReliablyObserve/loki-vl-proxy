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
major_family="${TAG%%.*}"

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

check_family_1x() {
  check "1.x keeps service-selection volume buckets on datasource wrapper" \
    "src/services/datasource.ts" \
    "['\"]index/volume['\"]"
  check "1.x strips detected-level style fields from detected_fields responses" \
    "src/services/filters.ts" \
    "FIELDS_TO_REMOVE = \\['level_extracted', LEVEL_VARIABLE_VALUE, LEVEL_INDEX_NAME\\]"
  check "1.x keeps labels parsed from returned log frames" \
    "src/services/labels.ts" \
    "getAllLabelsFromDataFrame"
}

check_family_2x() {
  check "2.x keeps detected_level default URL columns for service-detail panels" \
    "src/Components/Table/constants.ts" \
    "DEFAULT_URL_COLUMNS_LEVELS = \\['detected_level', 'level'\\]"
  check "2.x keeps field values breakdown scenes for service detail" \
    "src/Components/ServiceScene/Breakdowns/FieldValuesBreakdownScene.tsx" \
    "Run the field values breakdown query"
  check "2.x keeps additional label tab wiring in service selection tabs" \
    "src/Components/ServiceSelectionScene/ServiceSelectionTabsScene.tsx" \
    "label=\\{'Add label'\\}"
}

check "service selection volume contract" "src/services/datasource.ts" "['\"]index/volume['\"]"
check "detected fields contract" "src/services/datasource.ts" "['\"]detected_fields['\"]"
check "mixed parser service logs contract" "src/services/variables.ts" "LOG_STREAM_SELECTOR_EXPR = .*\\| json .*\\| logfmt \\| drop __error__, __error_details__"
check "detected level field removal contract" "src/services/filters.ts" "FIELDS_TO_REMOVE = \\['level_extracted', LEVEL_VARIABLE_VALUE, LEVEL_INDEX_NAME\\]"
check "labels field parsing contract" "src/services/labels.ts" "getAllLabelsFromDataFrame"
check "level coloring contract" "src/services/panel.ts" "UNKNOWN_LEVEL_LOGS = 'logs'"

case "${major_family}" in
  1)
    check_family_1x
    ;;
  2)
    check_family_2x
    ;;
  *)
    echo "FAIL unsupported Logs Drilldown family ${major_family}.x" >&2
    exit 1
    ;;
esac

pct="0.0"
if [[ "${total}" -gt 0 ]]; then
  pct=$(awk "BEGIN { printf \"%.1f\", (${passed} * 100) / ${total} }")
fi

echo "Score: ${passed}/${total} (${pct}%)"
