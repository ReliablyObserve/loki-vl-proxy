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

Check that each proxy sees all its peers:

```bash
curl -s http://localhost:3100/_cache/peers | jq .
curl -s http://localhost:3101/_cache/peers | jq .
curl -s http://localhost:3102/_cache/peers | jq .
```

Each response should list `proxy-a:3100`, `proxy-b:3100`, and `proxy-c:3100`.

Send a log line and verify it can be queried through any proxy:

```bash
curl -s -XPOST http://localhost:3100/loki/api/v1/push \
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
