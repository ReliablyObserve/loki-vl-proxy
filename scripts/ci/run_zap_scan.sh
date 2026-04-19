#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-baseline}"
BASE_URL="${PROXY_BASE_URL:-http://127.0.0.1:3100}"
TARGETS_FILE="${ZAP_TARGETS_FILE:-security/zap/targets.txt}"
REPORT_ROOT="${SECURITY_REPORT_ROOT:-security-reports}"
REPORT_DIR="${REPORT_ROOT}/zap/${MODE}"
IMAGE="${ZAP_IMAGE:-ghcr.io/zaproxy/zaproxy:stable}"

mkdir -p "${REPORT_DIR}"
ABS_REPORT_DIR="${PWD}/${REPORT_DIR}"

case "${MODE}" in
  baseline)
    SCAN_SCRIPT="zap-baseline.py"
    SCAN_ARGS=(--autooff)
    ;;
  active)
    SCAN_SCRIPT="zap-full-scan.py"
    SCAN_ARGS=()
    ;;
  *)
    echo "unsupported ZAP scan mode: ${MODE}" >&2
    exit 1
    ;;
esac

while IFS= read -r raw_path; do
  path="${raw_path%%#*}"
  path="$(printf '%s' "${path}" | xargs)"
  if [[ -z "${path}" ]]; then
    continue
  fi

  target="${BASE_URL}${path}"
  name="$(printf '%s' "${path}" | tr '/:?&=%' '_' | tr -s '_' | sed 's/^_//; s/_$//')"
  if [[ -z "${name}" ]]; then
    name="root"
  fi

  echo "Running ${MODE} ZAP scan against ${target}"
  docker run --rm \
    --network=host \
    --user "$(id -u):$(id -g)" \
    -e HOME=/zap/wrk \
    -w /zap/wrk \
    -v "${ABS_REPORT_DIR}:/zap/wrk" \
    "${IMAGE}" \
    "${SCAN_SCRIPT}" \
    "${SCAN_ARGS[@]}" \
    -I \
    -j \
    -m 1 \
    -t "${target}" \
    -J "${name}.json" \
    -w "${name}.md" \
    -r "${name}.html"
done < "${TARGETS_FILE}"
