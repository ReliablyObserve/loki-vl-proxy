#!/bin/zsh
ROOT=/Users/slawomirskowron/claude_projects/loki-vl-proxy; OUT=$ROOT/scratchpad/codify-pinned/final
cd $ROOT/test/e2e-ui
echo "start $(date +%T) loki_started=$(docker inspect -f '{{.State.StartedAt}}' e2e-loki)"
for tag in @explore-core @regression @drilldown-core @drilldown-mt @comprehensive-ui; do
  name=${tag#@}
  CI=1 GRAFANA_URL=http://127.0.0.1:3002 PROXY_URL=http://127.0.0.1:13100 npx playwright test --grep "$tag" >$OUT/$name.log 2>&1
  p=$(grep -oE "[0-9]+ passed" $OUT/$name.log | tail -1); f=$(grep -oE "[0-9]+ failed" $OUT/$name.log | tail -1); s=$(grep -oE "[0-9]+ skipped" $OUT/$name.log | tail -1); fl=$(grep -oE "[0-9]+ flaky" $OUT/$name.log | tail -1)
  echo "$(date +%T) final $name: ${p:-0 passed} ${f:-0 failed} ${s:-0 skipped} ${fl:-}"
done
docker logs e2e-loki 2>&1 | grep "caller=metrics.go" | grep -E 'range_type=range' | grep -cE "shards=0" | sed 's/^/loki range queries with shards=0: /'
echo FINAL_DONE
