#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 6 ]; then
  echo "usage: $0 <version> <release-date> <tests-count> [coverage-pct] [go-lines] [repo-root]" >&2
  exit 1
fi

VERSION="${1#v}"
RELEASE_DATE="$2"
TEST_COUNT="$3"
COVERAGE_PCT="${4:-}"
GO_LINES="${5:-}"
REPO_ROOT="${6:-.}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$REPO_ROOT"

CHANGELOG_FILE="CHANGELOG.md"
CHART_FILE="charts/loki-vl-proxy/Chart.yaml"
README_FILE="README.md"
OBSERVABILITY_FILE="docs/observability.md"

for path in "$CHANGELOG_FILE" "$CHART_FILE" "$README_FILE" "$OBSERVABILITY_FILE"; do
  if [ ! -f "$path" ]; then
    echo "required file not found: $path" >&2
    exit 1
  fi
done

"$SCRIPT_DIR/materialize_unreleased.sh" "$VERSION" "$RELEASE_DATE" "$CHANGELOG_FILE"

awk -v version="$VERSION" '
  /^version:/ { print "version: " version; next }
  /^appVersion:/ { print "appVersion: \"" version "\""; next }
  { print }
' "$CHART_FILE" > "$CHART_FILE.tmp"
mv "$CHART_FILE.tmp" "$CHART_FILE"

perl -0pi -e 's/"service\.version":\s*"[^"]+"/"service.version": "'"$VERSION"'"/' "$OBSERVABILITY_FILE"

badge_message() {
  local value="$1"
  python3 - "$value" <<'PY'
import sys
import urllib.parse
print(urllib.parse.quote(sys.argv[1], safe=""))
PY
}

coverage_color() {
  local cov="$1"
  python3 - "$cov" <<'PY'
import sys
try:
    value = float(sys.argv[1])
except ValueError:
    print("lightgrey")
    raise SystemExit
if value >= 90:
    print("brightgreen")
elif value >= 80:
    print("green")
elif value >= 70:
    print("yellow")
else:
    print("red")
PY
}

format_kilo_lines() {
  local lines="$1"
  python3 - "$lines" <<'PY'
import sys
try:
    value = int(sys.argv[1])
except ValueError:
    print(sys.argv[1])
    raise SystemExit
if value >= 1000:
    print(f"{value/1000:.1f}k")
else:
    print(str(value))
PY
}

if [ -n "$TEST_COUNT" ]; then
  TESTS_MESSAGE="$(badge_message "${TEST_COUNT} passed")"
  TESTS_BADGE="https://img.shields.io/badge/tests-${TESTS_MESSAGE}-brightgreen"
  awk -v badge="$TESTS_BADGE" '
    /^\[!\[Tests\]\(/ { print "[![Tests](" badge ")](#tests)"; next }
    { print }
  ' "$README_FILE" > "$README_FILE.tmp"
  mv "$README_FILE.tmp" "$README_FILE"
fi

if [ -n "$COVERAGE_PCT" ]; then
  COVERAGE_NUM="${COVERAGE_PCT%\%}"
  COVERAGE_COLOR="$(coverage_color "$COVERAGE_NUM")"
  COVERAGE_MESSAGE="$(badge_message "${COVERAGE_NUM}%")"
  COVERAGE_BADGE="https://img.shields.io/badge/coverage-${COVERAGE_MESSAGE}-${COVERAGE_COLOR}"
  if grep -Fq '[![Coverage](' "$README_FILE"; then
    awk -v badge="$COVERAGE_BADGE" '
      /^\[!\[Coverage\]\(/ { print "[![Coverage](" badge ")](#tests)"; next }
      { print }
    ' "$README_FILE" > "$README_FILE.tmp"
  else
    awk -v badge="$COVERAGE_BADGE" '
      /^\[!\[Tests\]\(/ { print; print "[![Coverage](" badge ")](#tests)"; next }
      { print }
    ' "$README_FILE" > "$README_FILE.tmp"
  fi
  mv "$README_FILE.tmp" "$README_FILE"
fi

if [ -n "$GO_LINES" ]; then
  LINES_KILO="$(format_kilo_lines "${GO_LINES}")"
  LINES_MESSAGE="$(badge_message "${LINES_KILO}")"
  LOC_BADGE="https://img.shields.io/badge/go%20loc-${LINES_MESSAGE}-blue"
  awk -v badge="$LOC_BADGE" '
    /^\[!\[Lines of Code\]\(/ { print "[![Lines of Code](" badge ")](https://github.com/szibis/Loki-VL-proxy)"; next }
    { print }
  ' "$README_FILE" > "$README_FILE.tmp"
  mv "$README_FILE.tmp" "$README_FILE"
fi
