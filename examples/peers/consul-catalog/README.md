# Peer discovery: Consul catalog

Three proxy instances register themselves in Consul and discover peers via the
Consul catalog API.  The `-peer-discovery=http` mode queries
`/v1/catalog/service/loki-vl-proxy`. Catalog results are not filtered by
health; `DeregisterCriticalServiceAfter` in `consul-register.sh` removes
instances whose `/ready` check stays critical for 60s.

## When to use

- Environments already running Consul for service discovery
- Fleets where proxies start and stop dynamically (auto-scaling groups, cron)
- When you want health-gated peer membership without custom tooling
- Multi-datacenter setups (Consul federation handles cross-DC service lookup)

## Architecture

```
proxy-a, proxy-b, proxy-c
    │  (each polls)
    ▼
Consul API: GET /v1/catalog/service/loki-vl-proxy
    │
    └── returns JSON with Address/Port for each passing instance
         (top-level ServiceAddress/ServicePort, used by -peer-discovery=http)
```

## Quick start

```bash
docker compose up -d
# consul-setup registers all three proxies and exits — this is expected
docker compose ps
open http://localhost:8500   # Consul UI
```

## How to test

Query the Consul catalog directly:

```bash
curl -s "http://localhost:8500/v1/health/service/loki-vl-proxy?passing=true" \
  | jq '[.[] | {id: .Service.ID, addr: .Service.Address, port: .Service.Port}]'
```

Check peer membership from a proxy (the `/_cache/*` endpoints require the shared
peer token in the `X-Peer-Token` header):

```bash
export PEER_AUTH_TOKEN=fleet-secret   # same value as -peer-auth-token in docker-compose.yml
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3100/_cache/peers | jq .
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3101/_cache/peers | jq .
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3102/_cache/peers | jq .
```

> **Response shape note.** `-peer-discovery=http` accepts a JSON string array,
> `{"peers": [...]}`, a Prometheus HTTP SD target-group list, or a Consul
> **catalog** list whose entries carry top-level `ServiceAddress` (or `Address`)
> and `ServicePort` fields — the shape returned by
> `/v1/catalog/service/loki-vl-proxy`, which this compose file uses. The
> `/v1/health/service/...` endpoint nests the address and port under `Service`
> (`.Service.Address`, `.Service.Port`, as the `jq` query above shows), which the
> proxy does not parse: each poll logs `peer discovery failed` with an
> `unrecognised HTTP peer list format` error and the peer list is not updated.
> Catalog results are not filtered by health; `DeregisterCriticalServiceAfter`
> in `consul-register.sh` removes instances that stay critical for 60s.

## How to add or remove a proxy

**Add:** Start the new proxy container and register it with Consul:

```bash
curl -XPUT http://localhost:8500/v1/agent/service/register \
  -H "Content-Type: application/json" \
  -d '{"ID":"proxy-d","Name":"loki-vl-proxy","Address":"proxy-d","Port":3100,
       "Check":{"HTTP":"http://proxy-d:3100/ready","Interval":"10s"}}'
```

Once the health check passes, existing proxies include it automatically on their
next poll cycle.

**Remove:** Deregister from Consul — existing proxies stop routing to it within
one poll cycle:

```bash
curl -XPUT http://localhost:8500/v1/agent/service/deregister/proxy-d
```

## Ports

| Service  | Host port | Notes        |
|----------|-----------|--------------|
| proxy-a  | 3100      |              |
| proxy-b  | 3101      |              |
| proxy-c  | 3102      |              |
| Consul   | 8500      | UI + API     |
