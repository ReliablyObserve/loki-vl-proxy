# Fleet Cache Architecture

## Overview

The fleet cache enables multiple Loki-VL-proxy replicas to share cached data with minimal network overhead. Each key lives on exactly one peer (the **owner**, determined by consistent hashing). Non-owner peers fetch from the owner on local miss and keep short-lived **shadow copies**.

```mermaid
flowchart TD
    LB["Load Balancer<br/>(RR / Least-Conn)"]

    subgraph Fleet["Proxy Fleet"]
        subgraph P1["Proxy A"]
            L1a["L1 Memory<br/>LRU + TTL"]
            L2a["L2 Disk<br/>bbolt + gzip"]
        end
        subgraph P2["Proxy B"]
            L1b["L1 Memory<br/>LRU + TTL"]
            L2b["L2 Disk<br/>bbolt + gzip"]
        end
        subgraph P3["Proxy C"]
            L1c["L1 Memory<br/>LRU + TTL"]
            L2c["L2 Disk<br/>bbolt + gzip"]
        end
    end

    HR["Consistent Hash Ring<br/>SHA256 · 150 vnodes/peer"]
    VL["VictoriaLogs"]

    LB -->|random| P1
    LB -->|random| P2
    LB -->|random| P3

    P1 <-->|"/_cache/get"| P2
    P1 <-->|"/_cache/get"| P3
    P2 <-->|"/_cache/get"| P3

    P1 --> VL
    P2 --> VL
    P3 --> VL

    style HR fill:#e94560,color:#fff
    style VL fill:#0f3460,color:#fff
    style LB fill:#16213e,color:#fff
```

## Request Flow

### Cache Hit (Local) — 0 Hops

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as Load Balancer
    participant A as Proxy A
    participant VL as VictoriaLogs

    C->>LB: query
    LB->>A: (random routing)
    A->>A: L1 check → HIT (fresh)
    A-->>C: response (0 network hops)
```

### Cache Hit (Peer) — 1 Hop

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as Load Balancer
    participant A as Proxy A
    participant B as Proxy B (owner)

    C->>LB: query
    LB->>A: (random routing)
    A->>A: L1 miss, L2 miss
    A->>A: hash("query") → Proxy B
    A->>B: GET /_cache/get?key=...
    B->>B: L1 check → HIT
    B-->>A: value + X-Cache-TTL-Ms: 42000
    A->>A: store shadow (TTL=42s)
    A-->>C: response (1 hop)
```

### Cache Miss (VL Fetch)

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as Load Balancer
    participant A as Proxy A (owner)
    participant VL as VictoriaLogs

    C->>LB: query
    LB->>A: (random routing)
    A->>A: L1 miss, L2 miss
    A->>A: hash("query") → self (I'm owner)
    A->>A: skip L3 (I'm the authority)
    A->>VL: fetch
    VL-->>A: response
    A->>A: store in L1 (default TTL)
    A-->>C: response
```

## TTL Preservation

Shadow copies use the **owner's remaining TTL**, not a fresh default:

```
Owner stores key with TTL=60s
Time passes... 20s elapsed, 40s remaining

Non-owner fetches from owner:
  Owner responds with X-Cache-TTL-Ms: 40000
  Non-owner stores shadow with TTL=40s (not 60s!)

If remaining < 5s (MinUsableTTL):
  Owner responds 404 → non-owner goes to VL for fresh data
```

```mermaid
graph LR
    subgraph "TTL Timeline"
        T0["t=0<br/>Owner stores<br/>TTL=60s"]
        T20["t=20s<br/>Peer fetches<br/>Shadow TTL=40s"]
        T55["t=55s<br/>Peer fetches<br/>remaining=5s<br/>→ 404 (force refresh)"]
        T60["t=60s<br/>Both expire"]
    end
    T0 --> T20 --> T55 --> T60
```

## Consistent Hash Ring

Keys map to peers deterministically — no communication needed:

```mermaid
graph TD
    subgraph Ring["Hash Ring (SHA256)"]
        K1["key: rate(nginx)"] -->|hash| N2["→ Proxy B"]
        K2["key: count(api)"] -->|hash| N1["→ Proxy A"]
        K3["key: sum(errors)"] -->|hash| N3["→ Proxy C"]
    end
```

**150 virtual nodes per peer** ensures even distribution:
- 2 peers → ~50/50 split
- 3 peers → ~33/33/33 split
- Adding a peer moves ~1/N keys (minimal rebalancing)

## Circuit Breaker

Per-peer circuit breaker prevents cascading failures:

```mermaid
stateDiagram-v2
    [*] --> Closed: healthy
    Closed --> Open: 5 failures
    Open --> HalfOpen: 10s cooldown
    HalfOpen --> Closed: success
    HalfOpen --> Open: failure
```

## Configuration

```bash
# Kubernetes (DNS discovery via headless service)
./loki-vl-proxy \
  -peer-self=$(hostname -i):3100 \
  -peer-discovery=dns \
  -peer-dns=proxy-headless.ns.svc.cluster.local

# Static peer list
./loki-vl-proxy \
  -peer-self=10.0.0.1:3100 \
  -peer-discovery=static \
  -peer-static=10.0.0.1:3100,10.0.0.2:3100,10.0.0.3:3100
```

### Helm Values

```yaml
extraArgs:
  peer-self: "$(POD_IP):3100"
  peer-discovery: "dns"
  peer-dns: "loki-vl-proxy-headless.default.svc.cluster.local"
```

## Performance Characteristics

| Metric | Value |
|--------|-------|
| L1 latency | ~2µs |
| L2 latency | ~1ms |
| L3 latency (peer) | ~1-5ms |
| VL latency | ~10-100ms |
| Background traffic | Zero |
| Max VL calls per key | 1 (per owner) |
| Shadow copy overhead | ~0 (uses owner's remaining TTL) |
| Hash ring lookup | O(log N) |
| Discovery refresh | Every 15s (DNS only) |

## Design Decisions

| Decision | Why |
|----------|-----|
| Consistent hashing (not gossip) | Zero background traffic, deterministic routing |
| Shadow copies (not write-through) | Non-owner fetches on demand, no push overhead |
| TTL preservation (not extension) | Never serve stale data beyond original intent |
| MinUsableTTL=5s (force refresh) | Don't transfer data that expires in transit |
| Singleflight per key | Prevent cache stampede on L3 misses |
| Per-peer circuit breaker | Isolate failures, auto-recover after cooldown |
| No disk encryption | Delegated to cloud provider (EBS/PD encryption at rest) |
