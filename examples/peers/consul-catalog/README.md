# Peer discovery: Consul catalog

Three proxy instances register themselves in Consul and discover peers via the
Consul catalog API.  The `-peer-discovery=http` mode queries
`/v1/health/service/loki-vl-proxy?passing=true`, which returns only instances
whose health check is currently passing.  This provides automatic readiness
gating — a proxy that has not yet started or is failing its `/ready` check is
never included in the peer list.

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
Consul API: GET /v1/health/service/loki-vl-proxy?passing=true
    │
    └── returns JSON with Address/Port for each passing instance
         (used by -peer-discovery=http to build the peer list)
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

Check peer membership from a proxy:

```bash
curl -s http://localhost:3100/_cache/peers | jq .
curl -s http://localhost:3101/_cache/peers | jq .
curl -s http://localhost:3102/_cache/peers | jq .
```

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
