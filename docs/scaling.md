# Scaling & Capacity Planning

## Resource Model

Loki-VL-proxy is a stateless HTTP proxy with optional disk cache. Resource consumption scales with:
- **CPU**: proportional to request rate + LogQL translation complexity
- **Memory**: proportional to L1 cache size + concurrent request response buffers
- **Disk**: proportional to L2 cache size (compressed, ~3.4x reduction with gzip)
- **Network**: proportional to request rate × response size

## Resource Projections

Based on benchmarks (Go 1.26.1, single core, typical Grafana dashboard workload with 60% cache hit rate):

### Single Replica

| Metric | 10 req/s | 100 req/s | 1,000 req/s | 10,000 req/s |
|--------|----------|-----------|-------------|--------------|
| **CPU (cores)** | 0.01 | 0.1 | 0.5-1.0 | 4-8 |
| **Memory (MB)** | 50-100 | 100-256 | 256-512 | 512-2048 |
| **L1 Cache (entries)** | 1,000 | 5,000 | 10,000 | 50,000 |
| **L1 Cache (MB)** | 10-50 | 50-100 | 100-256 | 256-512 |
| **Network in (Mbps)** | 0.1 | 1 | 10 | 50-100 |
| **Network out (Mbps)** | 0.5 | 5 | 50 | 200-500 |
| **VL queries/s** | 4 (60% cached) | 40 | 400 | 2,000-4,000 |
| **Goroutines** | 20-50 | 50-200 | 200-1,000 | 1,000-5,000 |

### Disk Cache (L2)

| Disk Size | Entries (compressed) | Hit Rate Boost | Use Case |
|-----------|---------------------|----------------|----------|
| 1 GB | ~18k | +10-15% | Small team, development |
| 5 GB | ~94k | +15-25% | Medium org, shared dashboards |
| 10 GB | ~189k | +20-30% | Large org, heavy dashboard usage |
| 50 GB | ~948k | +25-35% | Enterprise, long TTL requirements |

Compression ratio: ~29% (gzip), meaning 1 GB on disk holds ~3.4 GB of raw cache data.

### Fleet (Multiple Replicas)

```mermaid
graph LR
    subgraph "Resource per Pod"
        CPU["CPU: 0.5-1 core"]
        MEM["Memory: 256-512 MB"]
        DISK["Disk: 1-10 GB"]
    end

    subgraph "Fleet Sizing"
        R10["10 req/s → 1 pod"]
        R100["100 req/s → 1-2 pods"]
        R1K["1,000 req/s → 2-4 pods"]
        R10K["10,000 req/s → 8-16 pods"]
    end
```

| Total req/s | Replicas | CPU (total) | Memory (total) | Disk (total) | Cache Hit Rate |
|-------------|----------|-------------|----------------|--------------|----------------|
| 10 | 1 | 0.1 core | 128 MB | 1 GB | 60-70% |
| 100 | 2 | 0.5 cores | 512 MB | 2 GB | 65-75% |
| 1,000 | 4 | 2-4 cores | 1-2 GB | 10 GB | 70-80% |
| 10,000 | 16 | 8-16 cores | 4-8 GB | 40 GB | 75-85% |

With fleet peer cache, the effective cache size is `L1_per_pod × N + L2_per_pod × N`, and cache hit rate improves because each key lives on exactly one peer (no duplication across pods).

## Workload Profiles

### Dashboard-Heavy (Grafana auto-refresh)

```
Characteristics: 70-85% repeat queries, 15-30 panels per dashboard, 5-60s refresh
Cache behavior: Very high hit rate after first load
Bottleneck: Memory (large response buffers), Network out
```

| Dashboards | Users | req/s | Recommended |
|------------|-------|-------|-------------|
| 10 | 5 | 10-50 | 1 pod, 256MB, 1GB disk |
| 50 | 25 | 50-250 | 2 pods, 512MB, 5GB disk |
| 200 | 100 | 200-1000 | 4 pods, 1GB, 10GB disk |
| 1000 | 500 | 1000-5000 | 8-16 pods, 2GB, 10GB disk |

### Explore-Heavy (Ad-hoc queries)

```
Characteristics: 20-30% repeat queries, unique time ranges, high cardinality
Cache behavior: Lower hit rate, disk cache catches some repeats
Bottleneck: CPU (translation), VL backend throughput
```

| Active Users | req/s | Recommended |
|-------------|-------|-------------|
| 5 | 5-20 | 1 pod, 256MB, 1GB disk |
| 25 | 25-100 | 2 pods, 512MB, 5GB disk |
| 100 | 100-500 | 4 pods, 1GB, 10GB disk |
| 500 | 500-2000 | 8 pods, 2GB, 20GB disk |

### Mixed (Production Typical)

```
Characteristics: 50% dashboards, 30% alerts, 20% explore
Cache behavior: Moderate hit rate, steady state after warm-up
Bottleneck: Balanced CPU/memory/network
```

## Helm Values by Scale

### Small (< 100 req/s)

```yaml
replicaCount: 1
workload:
  kind: StatefulSet
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi
extraArgs:
  cache-ttl: "60s"
  cache-max: "5000"
  disk-cache-path: "/cache/proxy.db"
  disk-cache-compress: "true"
persistence:
  enabled: true
  size: 1Gi
```

### Medium (100-1,000 req/s)

```yaml
replicaCount: 3
workload:
  kind: StatefulSet
resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: "1"
    memory: 512Mi
extraArgs:
  cache-ttl: "60s"
  cache-max: "10000"
  disk-cache-path: "/cache/proxy.db"
  disk-cache-compress: "true"
  peer-self: "$(POD_IP):3100"
  peer-discovery: "dns"
  peer-dns: "loki-vl-proxy-headless.default.svc.cluster.local"
persistence:
  enabled: true
  size: 5Gi
horizontalPodAutoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 6
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

### Large (1,000-10,000 req/s)

```yaml
replicaCount: 8
workload:
  kind: StatefulSet
resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: "2"
    memory: 2Gi
extraArgs:
  cache-ttl: "120s"
  cache-max: "50000"
  disk-cache-path: "/cache/proxy.db"
  disk-cache-compress: "true"
  disk-cache-flush-size: "500"
  peer-self: "$(POD_IP):3100"
  peer-discovery: "dns"
  peer-dns: "loki-vl-proxy-headless.default.svc.cluster.local"
  rate-per-second: "200"
  rate-burst: "500"
  max-concurrent: "500"
persistence:
  enabled: true
  size: 10Gi
  storageClass: gp3  # high IOPS SSD
horizontalPodAutoscaling:
  enabled: true
  minReplicas: 4
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
podDisruptionBudget:
  minAvailable: 2
```

## Monitoring Metrics

### Per-Tenant (Rate, Throughput, Latency)

```promql
# Request rate per tenant
rate(loki_vl_proxy_tenant_requests_total[5m])

# P99 latency per tenant
histogram_quantile(0.99, rate(loki_vl_proxy_tenant_request_duration_seconds_bucket[5m]))

# Error rate per tenant
sum(rate(loki_vl_proxy_tenant_requests_total{status=~"4..|5.."}[5m])) by (tenant)
  / sum(rate(loki_vl_proxy_tenant_requests_total[5m])) by (tenant)
```

### Per-Client (User Identity)

Client identity is resolved from (priority order):
1. `X-Grafana-User` header when `-metrics.trust-proxy-headers=true`
2. `X-Scope-OrgID` header (tenant)
3. Basic auth username
4. `X-Forwarded-For` when `-metrics.trust-proxy-headers=true`
5. Remote IP (fallback)

```promql
# Request rate per client
rate(loki_vl_proxy_client_requests_total[5m])

# Throughput per client (bytes/s)
rate(loki_vl_proxy_client_response_bytes_total[5m])

# P95 latency per client
histogram_quantile(0.95, rate(loki_vl_proxy_client_request_duration_seconds_bucket[5m]))

# Top 10 clients by request count
topk(10, sum(rate(loki_vl_proxy_client_requests_total[5m])) by (client))

# Clients generating the most 429s
topk(10, sum(rate(loki_vl_proxy_client_status_total{status="429"}[5m])) by (client))

# Clients issuing the largest queries
topk(10, histogram_quantile(0.95, rate(loki_vl_proxy_client_query_length_chars_bucket[5m])))
```

### Fleet Peer-Cache

```promql
# Current remote peers per proxy
loki_vl_proxy_peer_cache_peers

# Total fleet members in the hash ring
loki_vl_proxy_peer_cache_cluster_members

# Peer cache effectiveness
rate(loki_vl_proxy_peer_cache_hits_total[5m])
/
(rate(loki_vl_proxy_peer_cache_hits_total[5m]) + rate(loki_vl_proxy_peer_cache_misses_total[5m]))

# Peer fetch failure rate
rate(loki_vl_proxy_peer_cache_errors_total[5m])
```

### Cache Efficiency

```promql
# Overall cache hit rate
loki_vl_proxy_cache_hits_total / (loki_vl_proxy_cache_hits_total + loki_vl_proxy_cache_misses_total)

# VL backend load (actual queries reaching VL)
sum(rate(loki_vl_proxy_backend_duration_seconds_count[5m]))

# Backend requests saved by coalescing
rate(loki_vl_proxy_coalesced_saved_total[5m])
```

### Resource Utilization

```promql
# Heap usage vs limit
go_memstats_alloc_bytes / go_memstats_sys_bytes

# Active goroutines (proxy concurrency)
go_goroutines

# GC pressure
rate(go_gc_cycles_total[5m])
```

## Availability Patterns

### Single-Region HA

```mermaid
flowchart TD
    LB["Load Balancer<br/>Round Robin"] --> PA["Pod A"]
    LB --> PB["Pod B"]
    LB --> PC["Pod C"]
    PA --> VL["VictoriaLogs<br/>(separate HA)"]
    PB --> VL
    PC --> VL

    PDB["PodDisruptionBudget<br/>minAvailable: 2"]
```

- Minimum 3 replicas for HA
- PDB ensures at least 2 pods during rolling updates
- Anti-affinity spreads pods across nodes/zones
- Proxy is stateless — any pod can serve any request

### Multi-Region

```mermaid
flowchart TD
    subgraph Region1["Region 1"]
        LB1["LB"] --> P1a["Proxy"]
        LB1 --> P1b["Proxy"]
        P1a --> VL1["VL Primary"]
        P1b --> VL1
    end

    subgraph Region2["Region 2"]
        LB2["LB"] --> P2a["Proxy"]
        LB2 --> P2b["Proxy"]
        P2a --> VL2["VL Replica"]
        P2b --> VL2
    end

    DNS["Global DNS<br/>Latency-based"] --> LB1
    DNS --> LB2
```

## Disk I/O Characteristics

The L2 disk cache uses bbolt (B+ tree) optimized for sequential I/O:

| Operation | Latency | IOPS | Notes |
|-----------|---------|------|-------|
| Read (cache hit) | ~22µs | ~45,000/s | Sequential B+ tree scan |
| Write (batch flush) | ~200µs/batch | ~5,000 entries/s | Write-back buffer, async |
| Compaction | Background | Minimal | bbolt self-manages |

Disk type recommendations:
- **gp3/gp2 SSD**: Best for < 1,000 req/s
- **io2/io1 SSD**: For > 1,000 req/s with high cache churn
- **st1 HDD**: Viable for read-heavy, low-churn workloads (bbolt's sequential I/O is HDD-friendly)

## Cost Estimation

Example: AWS EKS, us-east-1, 1,000 req/s workload

| Component | Spec | Monthly Cost |
|-----------|------|-------------|
| 4x t3.medium pods | 2 vCPU, 4GB RAM | ~$120 |
| 4x 10GB gp3 volumes | 3,000 IOPS, 125 MB/s | ~$14 |
| VictoriaLogs (backend) | Varies | Varies |
| **Total proxy infra** | | **~$134/month** |

Compare to running Loki at the same scale: Loki typically requires 10-30x more resources due to ingestion, indexing, and compaction overhead.
