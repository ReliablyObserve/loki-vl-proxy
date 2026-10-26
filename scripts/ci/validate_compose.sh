#!/usr/bin/env bash
# validate_compose.sh - parse and interpolate every tracked Docker Compose file.
#
# Runs `docker compose config -q` on each standalone file and on each overlay
# stacked on the files it is applied after, so a broken key, a bad merge or an
# undefined reference fails here instead of in the job that starts the stack.
# It never starts a container and needs no Docker daemon.
#
# The script also fails when a tracked docker-compose*.yml / compose*.yml file
# is missing from the stack list below, so a new compose file cannot go
# unchecked.
#
# Usage: ./scripts/ci/validate_compose.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# Placeholders for variables a stack requires (${VAR:?...}). `config` only
# interpolates them; no container starts with these values.
export PEER_AUTH_TOKEN="${PEER_AUTH_TOKEN:-compose-config-placeholder}"

# One stack per line: the files passed to `docker compose -f`, in order,
# separated by spaces. An overlay line lists its base and any overlay it is
# applied after (see the header of each overlay file).
STACKS=(
  "docker-compose.yml"
  "examples/docker-compose.yml"
  "examples/peers/consul-catalog/docker-compose.yml"
  "examples/peers/haproxy-dns/docker-compose.yml"
  "examples/peers/prometheus-sd/docker-compose.yml"
  "examples/peers/static-list/docker-compose.yml"
  "test/e2e-compat/docker-compose.yml"
  "test/e2e-fleet/docker-compose.yml"
  "test/e2e-pipeline/docker-compose.yml"
  "test/e2e-compat/docker-compose.yml test/e2e-compat/docker-compose.bench.yml"
  "test/e2e-compat/docker-compose.yml test/e2e-compat/docker-compose.review.yml"
  "test/e2e-compat/docker-compose.yml bench/ab/docker-compose.ab.yml"
  "test/e2e-compat/docker-compose.yml bench/ab/docker-compose.ab.yml bench/ab/docker-compose.ab-loki-uncached.yml"
  "test/e2e-compat/docker-compose.yml bench/ab/docker-compose.ab.yml bench/visual/docker-compose.visual.yml"
  "test/e2e-compat/docker-compose.yml bench/ab/docker-compose.ab.yml bench/visual/docker-compose.visual.yml bench/parity/docker-compose.parity.yml"
  "test/e2e-fleet/docker-compose.yml test/e2e-fleet/docker-compose.http-discovery.yml"
)

# Every tracked compose file must appear in at least one stack.
missing=0
while IFS= read -r tracked; do
  [ -n "$tracked" ] || continue
  found=0
  for stack in "${STACKS[@]}"; do
    for file in $stack; do
      if [ "$file" = "$tracked" ]; then
        found=1
      fi
    done
  done
  if [ "$found" -eq 0 ]; then
    echo "::error file=$tracked::tracked compose file is not listed in scripts/ci/validate_compose.sh"
    missing=1
  fi
done < <(git ls-files | grep -E '(^|/)(docker-)?compose[^/]*\.ya?ml$' || true)

# Every listed file must exist.
for stack in "${STACKS[@]}"; do
  for file in $stack; do
    if [ ! -f "$file" ]; then
      echo "::error::$file is listed in scripts/ci/validate_compose.sh but does not exist"
      missing=1
    fi
  done
done

if [ "$missing" -ne 0 ]; then
  exit 1
fi

failed=0
checked=0
for stack in "${STACKS[@]}"; do
  args=()
  for file in $stack; do
    args+=(-f "$file")
  done
  checked=$((checked + 1))
  if docker compose "${args[@]}" config -q; then
    echo "ok: $stack"
  else
    echo "::error::docker compose config failed for: $stack"
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  exit 1
fi
echo "validated $checked compose stacks"
