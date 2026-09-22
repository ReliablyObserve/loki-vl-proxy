#!/bin/zsh
set -u
ROOT=/Users/slawomirskowron/claude_projects/loki-vl-proxy
OUT=$ROOT/scratchpad/codify-pinned
export PROXY_IMAGE=loki-vl-proxy:e2e-local
cd $ROOT/test/e2e-compat
echo "== phase A: pinned UI stack =="
docker compose --profile ui down -v --remove-orphans >$OUT/downA.log 2>&1
docker compose --profile ui up -d --no-build >$OUT/upA.log 2>&1 || { echo "UP_A_FAILED"; tail -5 $OUT/upA.log; }
../../scripts/ci/wait_e2e_stack.sh 180 >$OUT/waitA.log 2>&1 || { echo "WAIT_A_FAILED"; tail -5 $OUT/waitA.log; }
docker compose --profile ui ps --format '{{.Name}} {{.Status}}' | grep -v healthy | grep -v "^$" | sed 's/^/notready: /'
cd $ROOT/test/e2e-ui
for tag in @drilldown-core @drilldown-mt @explore-core @regression @comprehensive-ui; do
  name=${tag#@}
  CI=1 GRAFANA_URL=http://127.0.0.1:3002 PROXY_URL=http://127.0.0.1:13100 npx playwright test --grep "$tag" --reporter=line >$OUT/$name.log 2>&1
  p=$(grep -oE "[0-9]+ passed" $OUT/$name.log | tail -1); f=$(grep -oE "[0-9]+ failed" $OUT/$name.log | tail -1); s=$(grep -oE "[0-9]+ skipped" $OUT/$name.log | tail -1)
  echo "pinned $name: ${p:-0 passed} ${f:-0 failed} ${s:-0 skipped}"
done
echo "== phase B: VL matrix =="
for v in v1.52.0 v1.36.0; do
  cd $ROOT/test/e2e-compat
  export VICTORIALOGS_IMAGE=victoriametrics/victoria-logs:$v
  docker compose --profile ui down -v --remove-orphans >$OUT/down-$v.log 2>&1
  docker compose up -d --no-build >$OUT/up-$v.log 2>&1 || { echo "UP_$v_FAILED"; tail -5 $OUT/up-$v.log; }
  ../../scripts/ci/wait_e2e_stack.sh 180 >$OUT/wait-$v.log 2>&1 || { echo "WAIT_${v}_FAILED"; tail -5 $OUT/wait-$v.log; }
  docker compose ps --format '{{.Name}} {{.Status}}' | grep -v healthy | sed "s/^/notready $v: /"
  cd $ROOT
  go test -v -tags=e2e -run '^TestVLTrackScore$' ./test/e2e-compat/ >$OUT/matrix-$v.log 2>&1
  echo "matrix $v: $(grep -E '^(--- |ok|FAIL)' $OUT/matrix-$v.log | grep -E '^--- (PASS|FAIL): TestVLTrackScore|^ok|^FAIL' | head -2 | tr '\n' ' ')"
done
echo "== phase C: restore pinned UI stack =="
cd $ROOT/test/e2e-compat; unset VICTORIALOGS_IMAGE
docker compose --profile ui down -v --remove-orphans >/dev/null 2>&1
docker compose --profile ui up -d --no-build >$OUT/upC.log 2>&1
../../scripts/ci/wait_e2e_stack.sh 180 >$OUT/waitC.log 2>&1 && echo "pinned stack restored" || echo "RESTORE_FAILED"
echo "ALL_PHASES_DONE"
