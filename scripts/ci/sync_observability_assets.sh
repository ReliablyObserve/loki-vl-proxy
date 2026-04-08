#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-sync}"

if [ "${MODE}" != "sync" ] && [ "${MODE}" != "--check" ] && [ "${MODE}" != "check" ]; then
  echo "usage: $0 [sync|--check]" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT_DIR}"

SRC_ALERT_RULES="alerting/loki-vl-proxy-prometheusrule.yaml"
DST_ALERT_RULES="charts/loki-vl-proxy/alerting/loki-vl-proxy-prometheusrule.yaml"

for path in "${SRC_ALERT_RULES}" "${DST_ALERT_RULES}"; do
  if [ ! -f "${path}" ]; then
    echo "missing required asset: ${path}" >&2
    exit 1
  fi
done

shopt -s nullglob
SRC_DASHBOARDS=(dashboard/*.json)
DST_DASHBOARDS=(charts/loki-vl-proxy/dashboards/*.json)
shopt -u nullglob

if [ ${#SRC_DASHBOARDS[@]} -eq 0 ]; then
  echo "missing required assets: dashboard/*.json" >&2
  exit 1
fi

copy_asset() {
  local src="$1"
  local dst="$2"
  if cmp -s "${src}" "${dst}"; then
    return
  fi
  cp "${src}" "${dst}"
}

check_asset() {
  local src="$1"
  local dst="$2"
  if ! cmp -s "${src}" "${dst}"; then
    echo "asset out of sync: ${dst} differs from ${src}" >&2
    exit 1
  fi
}

if [ "${MODE}" = "--check" ] || [ "${MODE}" = "check" ]; then
  for src in "${SRC_DASHBOARDS[@]}"; do
    dst="charts/loki-vl-proxy/dashboards/$(basename "${src}")"
    if [ ! -f "${dst}" ]; then
      echo "missing dashboard copy: ${dst}" >&2
      exit 1
    fi
    check_asset "${src}" "${dst}"
  done
  for dst in "${DST_DASHBOARDS[@]}"; do
    src="dashboard/$(basename "${dst}")"
    if [ ! -f "${src}" ]; then
      echo "orphan dashboard copy without canonical source: ${dst}" >&2
      exit 1
    fi
  done
  check_asset "${SRC_ALERT_RULES}" "${DST_ALERT_RULES}"
  echo "observability assets are in sync"
  exit 0
fi

for src in "${SRC_DASHBOARDS[@]}"; do
  dst="charts/loki-vl-proxy/dashboards/$(basename "${src}")"
  copy_asset "${src}" "${dst}"
done
copy_asset "${SRC_ALERT_RULES}" "${DST_ALERT_RULES}"
echo "observability assets synced"
