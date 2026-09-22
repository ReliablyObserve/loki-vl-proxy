#!/bin/zsh
ROOT=/Users/slawomirskowron/claude_projects/loki-vl-proxy; OUT=$ROOT/scratchpad/codify-pinned/review
cd $ROOT/test/e2e-ui
for tag in "@regression" "@comprehensive-ui" "@explore-core" "@drilldown-core"; do
  name=${tag#@}
  CI=1 GRAFANA_URL=http://127.0.0.1:3002 PROXY_URL=http://127.0.0.1:13100 npx playwright test --grep "$tag" >$OUT/$name.log 2>&1
  p=$(grep -oE "[0-9]+ passed" $OUT/$name.log | tail -1); f=$(grep -oE "[0-9]+ failed" $OUT/$name.log | tail -1); s=$(grep -oE "[0-9]+ skipped" $OUT/$name.log | tail -1); fl=$(grep -oE "[0-9]+ flaky" $OUT/$name.log | tail -1)
  echo "$(date +%T) review $name: ${p:-0 passed} ${f:-0 failed} ${s:-0 skipped} ${fl:-}"
done
CI=1 GRAFANA_URL=http://127.0.0.1:3002 PROXY_URL=http://127.0.0.1:13100 npx playwright test tests/drilldown-cache-regression.spec.ts --grep "1h" >$OUT/cache.log 2>&1
echo "$(date +%T) review cache-1h: $(grep -oE '[0-9]+ passed|[0-9]+ failed' $OUT/cache.log | tr '\n' ' ')"
echo REVIEW_DONE
