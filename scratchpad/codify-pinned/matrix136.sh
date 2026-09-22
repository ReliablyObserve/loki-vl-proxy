#!/bin/zsh
ROOT=/Users/slawomirskowron/claude_projects/loki-vl-proxy; OUT=$ROOT/scratchpad/codify-pinned
export PROXY_IMAGE=loki-vl-proxy:e2e-local VICTORIALOGS_IMAGE=victoriametrics/victoria-logs:v1.36.0
cd $ROOT/test/e2e-compat
docker compose --profile ui down -v --remove-orphans >/dev/null 2>&1
docker compose up -d --no-build >$OUT/up-136b.log 2>&1 || { echo UP_FAILED; tail -3 $OUT/up-136b.log; }
../../scripts/ci/wait_e2e_stack.sh 180 >$OUT/wait-136b.log 2>&1 || { echo WAIT_FAILED; tail -3 $OUT/wait-136b.log; }
docker inspect -f '{{.Config.Image}} health={{.State.Health.Status}}' e2e-victorialogs
cd $ROOT; go test -count=1 -v -tags=e2e -run '^TestVLTrackScore$' ./test/e2e-compat/ >$OUT/matrix-136b.log 2>&1
grep -E "^--- (PASS|FAIL)|^ok|^FAIL" $OUT/matrix-136b.log | head -3
cd $ROOT/test/e2e-compat; unset VICTORIALOGS_IMAGE
docker compose --profile ui down -v --remove-orphans >/dev/null 2>&1
docker compose --profile ui up -d --no-build >$OUT/upD.log 2>&1
../../scripts/ci/wait_e2e_stack.sh 180 >$OUT/waitD.log 2>&1 && echo "pinned stack restored" || echo RESTORE_FAILED
echo MATRIX136_DONE
