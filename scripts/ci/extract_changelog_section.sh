#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <version> [changelog-file]" >&2
  exit 1
fi

VERSION="${1#v}"
CHANGELOG_FILE="${2:-CHANGELOG.md}"

if [ ! -f "$CHANGELOG_FILE" ]; then
  echo "changelog file not found: $CHANGELOG_FILE" >&2
  exit 1
fi

SECTION="$(
  awk -v version="$VERSION" '
    BEGIN {
      header = "## [" version "]"
      in_section = 0
    }
    index($0, header) == 1 {
      in_section = 1
      found = 1
    }
    in_section && /^## \[/ && index($0, header) != 1 {
      exit
    }
    in_section {
      print
    }
    END {
      if (!found) {
        exit 2
      }
    }
  ' "$CHANGELOG_FILE"
)" || {
  echo "version section not found in $CHANGELOG_FILE: $VERSION" >&2
  exit 1
}

printf '%s\n' "$SECTION"
