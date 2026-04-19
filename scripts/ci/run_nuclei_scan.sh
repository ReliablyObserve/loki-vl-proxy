#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${PROXY_BASE_URL:-http://127.0.0.1:3100}"
REPORT_ROOT="${SECURITY_REPORT_ROOT:-security-reports}"
REPORT_DIR="${REPORT_ROOT}/nuclei"
RESULTS_FILE="${REPORT_DIR}/findings.jsonl"

mkdir -p "${REPORT_DIR}"
rm -f "${RESULTS_FILE}"

docker run --rm \
  --network=host \
  -v "${PWD}:/work" \
  -w /work \
  projectdiscovery/nuclei:latest \
  -duc \
  -silent \
  -jsonl \
  -o "${RESULTS_FILE}" \
  -u "${BASE_URL}" \
  -t security/nuclei

if [[ -s "${RESULTS_FILE}" ]]; then
  echo "Curated Nuclei findings detected:"
  cat "${RESULTS_FILE}"
  exit 1
fi
