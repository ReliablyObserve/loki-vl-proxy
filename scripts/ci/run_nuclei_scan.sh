#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${PROXY_BASE_URL:-http://127.0.0.1:3100}"
REPORT_ROOT="${SECURITY_REPORT_ROOT:-security-reports}"
REPORT_DIR="${REPORT_ROOT}/nuclei"
RESULTS_FILE="${REPORT_DIR}/findings.jsonl"

mkdir -p "${REPORT_DIR}"
rm -f "${RESULTS_FILE}"
ABS_REPORT_DIR="$(cd "${REPORT_DIR}" && pwd)"

# Pinned by digest (v3.11.1, released 2026-08-08). Only the curated templates (read-only)
# and the report directory are mounted; the checkout itself is never visible to the scanner.
NUCLEI_IMAGE="${NUCLEI_IMAGE:-projectdiscovery/nuclei:v3.11.1@sha256:582d5546902e67052097cb2d07296c642d50a1afc5e44623cb038845df9a32eb}"

docker run --rm \
  --network=host \
  -v "${PWD}/security/nuclei:/templates:ro" \
  -v "${ABS_REPORT_DIR}:/report" \
  "${NUCLEI_IMAGE}" \
  -duc \
  -silent \
  -jsonl \
  -o "/report/findings.jsonl" \
  -u "${BASE_URL}" \
  -t /templates

if [[ -s "${RESULTS_FILE}" ]]; then
  echo "Curated Nuclei findings detected:"
  cat "${RESULTS_FILE}"
  exit 1
fi
