#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <base-json> <head-json> <output-md> [evidence-json]" >&2
  exit 1
fi
exec python3 "$(dirname "$0")/pr_quality_report.py" "$@"
