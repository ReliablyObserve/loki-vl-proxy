# Peer discovery: Prometheus HTTP SD

An nginx container (`sd-server`) serves a static JSON file in Prometheus HTTP
Service Discovery format.  Each proxy polls the endpoint via
`-peer-discovery=http`, which the proxy parses as a Prometheus HTTP SD target
group list.  Adding or removing a peer requires only editing `peers.json` — no
proxy restart needed.

## When to use

- Environments already using Prometheus HTTP SD for scrape target management
- Simple external-discovery setups with no registry dependency
- CI/CD pipelines where the peer list is generated at deploy time
- Any case where you want a human-readable, file-based peer registry

## Quick start

```bash
export PEER_AUTH_TOKEN=fleet-secret   # same value as -peer-auth-token in docker-compose.yml
docker compose up -d
# Inspect the SD endpoint
curl -s http://localhost:8080/peers | jq .
# Check peer membership (the /_cache/* endpoints require the shared peer token)
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3100/_cache/peers | jq .
```

## How to test

Verify each proxy sees all peers:

```bash
for port in 3100 3101 3102; do
  echo "=== proxy on :${port} ==="
  curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" "http://localhost:${port}/_cache/peers" | jq .
done
```

The proxy is read-only (`/loki/api/v1/push` returns `405`), so write a log line
straight to VictoriaLogs, then read it through proxy-a and again through proxy-c
(exercises the peer cache). VictoriaLogs has no published host port in this
example, so push from a throwaway container on the compose network
(`prometheus-sd_default` when started from this directory):

```bash
docker run --rm --network prometheus-sd_default curlimages/curl:latest \
  -s -XPOST http://victorialogs:9428/insert/loki/api/v1/push \
  -H "Content-Type: application/json" \
  -d '{"streams":[{"stream":{"app":"demo"},"values":[["'"$(date +%s)000000000"'","hello"]]}]}'

curl -s "http://localhost:3100/loki/api/v1/query_range?query=%7Bapp%3D%22demo%22%7D" | jq .
curl -s "http://localhost:3102/loki/api/v1/query_range?query=%7Bapp%3D%22demo%22%7D" | jq .
```

## How to add or remove a proxy

**Add:** Edit `sd-server/peers.json` to include the new address:

```json
[
  {
    "targets": ["proxy-a:3100", "proxy-b:3100", "proxy-c:3100", "proxy-d:3100"],
    "labels": {"job": "loki-vl-proxy"}
  }
]
```

nginx serves the updated file immediately (no reload needed — `no-store` cache
header ensures fresh reads).  The running proxies pick up the new peer on their
next poll cycle.  Start the new proxy with the same `-peer-http-url` flag.

**Remove:** Delete the target from the `targets` array.  Running proxies stop
routing to the removed peer within one poll cycle.

## File layout

```
sd-server/
  nginx.conf    — nginx config, serves /peers as application/json
  peers.json    — Prometheus HTTP SD target group list
```

## Ports

| Service   | Host port | Notes              |
|-----------|-----------|--------------------|
| proxy-a   | 3100      |                    |
| proxy-b   | 3101      |                    |
| proxy-c   | 3102      |                    |
| sd-server | 8080      | GET /peers returns JSON |
