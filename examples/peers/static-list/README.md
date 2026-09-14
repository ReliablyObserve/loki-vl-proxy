# Peer discovery: static list

Three proxy instances share a hardcoded peer list via `-peer-discovery=static`.
No external service dependency.  Suitable for fixed topologies where the set of
proxies is known in advance and changes infrequently.

## When to use

- Small, fixed fleets (2–5 proxies) where instances don't come and go
- Local development and staging environments
- Bare-metal or VM deployments without a service registry
- Quick demos and proofs of concept

## Quick start

```bash
docker compose up -d
# Wait for proxies to become healthy
docker compose ps
```

## How to test

Check that each proxy sees all its peers. The `/_cache/*` endpoints require the
shared peer token (`-peer-auth-token=fleet-secret`) in the `X-Peer-Token` header:

```bash
export PEER_AUTH_TOKEN=fleet-secret   # same value as -peer-auth-token in docker-compose.yml
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3100/_cache/peers | jq .
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3101/_cache/peers | jq .
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3102/_cache/peers | jq .
```

Each response should list `proxy-a:3100`, `proxy-b:3100`, and `proxy-c:3100`.

The proxy is read-only (`/loki/api/v1/push` returns `405`), so write a log line
straight to VictoriaLogs and read it back through any proxy. VictoriaLogs has no
published host port in this example, so push from a throwaway container on the
compose network (`static-list_default` when started from this directory):

```bash
docker run --rm --network static-list_default curlimages/curl:latest \
  -s -XPOST http://victorialogs:9428/insert/loki/api/v1/push \
  -H "Content-Type: application/json" \
  -d '{"streams":[{"stream":{"app":"demo"},"values":[["'"$(date +%s)000000000"'","hello"]]}]}'

curl -s "http://localhost:3101/loki/api/v1/query_range?query=%7Bapp%3D%22demo%22%7D" | jq .
```

## How to add or remove a proxy

Static discovery requires manual steps:

1. Update the `-peer-static` flag in **every** proxy's `command:` list to include
   (or exclude) the new address.
2. Restart the affected services: `docker compose up -d --force-recreate`

There is no live peer join/leave — the list is read at startup only.

## Ports

| Service  | Host port |
|----------|-----------|
| proxy-a  | 3100      |
| proxy-b  | 3101      |
| proxy-c  | 3102      |
