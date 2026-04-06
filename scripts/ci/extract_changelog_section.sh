#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <version-or-section> [changelog-file]" >&2
  exit 1
fi

TARGET="$1"
CHANGELOG_FILE="${2:-CHANGELOG.md}"

if [ ! -f "$CHANGELOG_FILE" ]; then
  echo "changelog file not found: $CHANGELOG_FILE" >&2
  exit 1
fi

if [ "$TARGET" = "Unreleased" ] || [ "$TARGET" = "[Unreleased]" ]; then
  HEADER="## [Unreleased]"
else
  VERSION="${TARGET#v}"
  HEADER="## [${VERSION}]"
fi

SECTION="$(
  awk -v header="$HEADER" '
    BEGIN { in_section = 0 }
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
  echo "section not found in $CHANGELOG_FILE: $TARGET" >&2
  exit 1
}

printf '%s\n' "$SECTION"
