#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <version> <release-date> <tests-count> [repo-root]" >&2
  exit 1
fi

VERSION="${1#v}"
RELEASE_DATE="$2"
TEST_COUNT="$3"
REPO_ROOT="${4:-.}"
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

if [ -n "$TEST_COUNT" ]; then
  TESTS_BADGE="https://img.shields.io/badge/tests-${TEST_COUNT}%20passed-brightgreen"
  awk -v badge="$TESTS_BADGE" '
    /^\[!\[Tests\]\(/ { print "[![Tests](" badge ")](#tests)"; next }
    { print }
  ' "$README_FILE" > "$README_FILE.tmp"
  mv "$README_FILE.tmp" "$README_FILE"
fi
