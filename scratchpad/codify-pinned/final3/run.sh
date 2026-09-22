#!/bin/zsh
ROOT=/Users/slawomirskowron/claude_projects/loki-vl-proxy; OUT=$ROOT/scratchpad/codify-pinned/final3
export PROXY_IMAGE=loki-vl-proxy:e2e-local
cd $ROOT/test/e2e-compat
docker compose --profile ui down -v --remove-orphans >/dev/null 2>&1
docker compose --profile ui up -d --no-build >$OUT/up.log 2>&1 || { echo UP_FAILED; tail -3 $OUT/up.log; }
../../scripts/ci/wait_e2e_stack.sh 180 >$OUT/wait.log 2>&1 || { echo WAIT_FAILED; tail -3 $OUT/wait.log; }
cd $ROOT/test/e2e-ui
echo "start $(date +%T) loki_started=$(docker inspect -f '{{.State.StartedAt}}' e2e-loki)"
for tag in @explore-core @regression @drilldown-core; do
  name=${tag#@}
  CI=1 GRAFANA_URL=http://127.0.0.1:3002 PROXY_URL=http://127.0.0.1:13100 npx playwright test --grep "$tag" >$OUT/$name.log 2>&1
  p=$(grep -oE "[0-9]+ passed" $OUT/$name.log | tail -1); f=$(grep -oE "[0-9]+ failed" $OUT/$name.log | tail -1); s=$(grep -oE "[0-9]+ skipped" $OUT/$name.log | tail -1); fl=$(grep -oE "[0-9]+ flaky" $OUT/$name.log | tail -1); d=$(grep -oE "passed \([0-9.]+m?s\)" $OUT/$name.log | tail -1)
  echo "$(date +%T) final3 $name: ${p:-0 passed} ${f:-0 failed} ${s:-0 skipped} ${fl:-} $d"
done
echo FINAL3_DONE
