# Peer discovery: DNS (CoreDNS + HAProxy)

Simulates the Kubernetes headless-Service A-record pattern outside a cluster.
CoreDNS serves a static zone where `proxy-peers.local` resolves to all three
proxy container IPs — exactly what a k8s headless Service does automatically.
HAProxy provides a single load-balanced entry point on port 3100.

## When to use

- Staging environments that need to mirror the k8s DNS discovery path
- Bare-metal / VM fleets where you can run your own DNS server
- Testing DNS-based peer discovery without spinning up Kubernetes
- Any setup where the peer list changes and you want discovery without a registry

## Architecture

```
Grafana / curl
    │
    ▼ :3100
  HAProxy (round-robin, health-checks /ready)
    │
    ├── proxy-a :3100
    ├── proxy-b :3100
    └── proxy-c :3100
         │  (each resolves proxy-peers.local → 3 A records via CoreDNS)
         ▼
    VictoriaLogs :9428
```

## Quick start

```bash
docker compose up -d
docker compose ps
```

## How to test

The `/_cache/*` endpoints require the shared peer token (`-peer-auth-token=fleet-secret`)
in the `X-Peer-Token` header.

Check peer membership via HAProxy (routes to a random proxy):

```bash
export PEER_AUTH_TOKEN=fleet-secret   # same value as -peer-auth-token in docker-compose.yml
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3100/_cache/peers | jq .
```

Check a specific proxy directly:

```bash
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3200/_cache/peers | jq .  # proxy-a
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3201/_cache/peers | jq .  # proxy-b
curl -s -H "X-Peer-Token: ${PEER_AUTH_TOKEN}" http://localhost:3202/_cache/peers | jq .  # proxy-c
```

Verify DNS resolution against CoreDNS. The proxy image is distroless (no shell or
`nslookup`), so run the lookup from a throwaway container on the compose network
(`haproxy-dns_proxy-net` when started from this directory):

```bash
docker run --rm --network haproxy-dns_proxy-net busybox:latest \
  nslookup proxy-peers.local 172.28.0.2
# Should return A records for .10, .11, .12
```

## How to add or remove a proxy

1. Add the new container to `docker-compose.yml` with a static IP in `172.28.0.0/24`.
2. Add its IP to both the `Corefile` `hosts` block (individual name + `proxy-peers.local`).
3. Add a `server` line to `haproxy.cfg`.
4. Apply: `docker compose up -d --force-recreate coredns haproxy`

The running proxies will pick up the new peer on their next DNS refresh cycle
without needing a full restart.  New proxies start discovering peers immediately
on boot.

## Ports

| Service  | Host port | Notes               |
|----------|-----------|---------------------|
| HAProxy  | 3100      | Load-balanced entry |
| proxy-a  | 3200      | Direct access       |
| proxy-b  | 3201      | Direct access       |
| proxy-c  | 3202      | Direct access       |
