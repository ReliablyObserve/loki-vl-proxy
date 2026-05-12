#!/usr/bin/env bash
set -euo pipefail

timeout_seconds="${1:-180}"

wait_http() {
  local name="$1"
  local url="$2"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "ready: $name"
      return 0
    fi
    sleep 2
  done

  echo "timed out waiting for $name at $url" >&2
  return 1
}

print_debug() {
  docker compose ps || true
  docker compose logs --no-color --tail=120 || true
}

trap 'print_debug' ERR

wait_http "loki" "http://127.0.0.1:13101/ready"
wait_http "victorialogs" "http://127.0.0.1:19428/health"
wait_http "vmalert" "http://127.0.0.1:18880/api/v1/rules?datasource_type=vlogs"
wait_http "proxy" "http://127.0.0.1:13100/ready"
wait_http "proxy-patterns-autodetect" "http://127.0.0.1:13110/ready"
wait_http "proxy-underscore" "http://127.0.0.1:13102/ready"
wait_http "proxy-native-metadata" "http://127.0.0.1:13106/ready"
wait_http "proxy-translated-metadata" "http://127.0.0.1:13107/ready"
wait_http "proxy-no-metadata" "http://127.0.0.1:13108/ready"
wait_http "proxy-tail" "http://127.0.0.1:13103/ready"
wait_http "tail-ingress" "http://127.0.0.1:13104/ready"
wait_http "proxy-tail-native" "http://127.0.0.1:13105/ready"
wait_http "grafana" "http://127.0.0.1:3002/api/health"

docker compose ps
