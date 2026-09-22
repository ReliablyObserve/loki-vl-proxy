#!/bin/zsh
# Poll Loki direct vs proxy for the parity metric query on a fresh stack.
q='sum by (level) (rate({app="api-gateway"}[5m]))'
for i in $(seq 1 16); do
  now=$(date +%s); end=$((now-60)); start=$((end-900))
  l=$(curl -s -G -H 'X-Scope-OrgID: 0' "http://127.0.0.1:13101/loki/api/v1/query_range" --data-urlencode "query=$q" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=60" | jq -c '[.status, (.data.result|length), ([.data.result[].metric.level]|sort)]' 2>/dev/null)
  ln=$(curl -s -G -H 'X-Scope-OrgID: 0' -H 'Cache-Control: no-store' "http://127.0.0.1:13101/loki/api/v1/query_range" --data-urlencode "query=$q" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=60" | jq -c '(.data.result|length)' 2>/dev/null)
  p=$(curl -s -G -H 'X-Scope-OrgID: 0' "http://127.0.0.1:13100/loki/api/v1/query_range" --data-urlencode "query=$q" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=60" | jq -c '[.status, (.data.result|length)]' 2>/dev/null)
  logs=$(curl -s -G -H 'X-Scope-OrgID: 0' "http://127.0.0.1:13101/loki/api/v1/query_range" --data-urlencode 'query={app="api-gateway"}' --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "limit=100" | jq -c '[.data.result[].values|length]|add' 2>/dev/null)
  inst=$(curl -s -G -H 'X-Scope-OrgID: 0' "http://127.0.0.1:13101/loki/api/v1/query" --data-urlencode "query=$q" --data-urlencode "time=$end" | jq -c '(.data.result|length)' 2>/dev/null)
  echo "t+$((i*15))s loki_range=$l loki_nostore=$ln loki_instant=$inst loki_logs=$logs proxy=$p"
  sleep 15
done
