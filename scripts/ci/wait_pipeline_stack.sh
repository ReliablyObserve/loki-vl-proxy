#!/usr/bin/env bash
set -euo pipefail

timeout_seconds="${1:-120}"

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
  docker compose ps || true
  docker compose logs --no-color --tail=80 || true
  return 1
}

wait_http "victorialogs" "http://127.0.0.1:19528/health"
wait_http "proxy"        "http://127.0.0.1:13200/ready"

docker compose ps
