# Loki-VL-proxy

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="website/static/img/loki-vl-proxy-logo-white.jpg">
    <img src="website/static/img/loki-vl-proxy-logo-black.jpg" alt="Loki-VL-proxy marketing logo" width="340">
  </picture>
</p>

[![CI](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/ci.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/ci.yaml)
[![Loki Compatibility](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-loki.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-loki.yaml)
[![Drilldown Compatibility](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-drilldown.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-drilldown.yaml)
[![VictoriaLogs Compatibility](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-vl.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-vl.yaml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/ReliablyObserve/Loki-VL-proxy)](https://go.dev/)
[![Release](https://img.shields.io/github/v/release/ReliablyObserve/Loki-VL-proxy)](https://github.com/ReliablyObserve/Loki-VL-proxy/releases)
[![Lines of Code](https://img.shields.io/badge/go%20loc-90.4k-blue)](https://github.com/ReliablyObserve/Loki-VL-proxy)
[![Tests](https://img.shields.io/badge/tests-2063%20passed-brightgreen)](#tests)
[![Coverage](https://img.shields.io/badge/coverage-87.4%25-green)](#tests)
[![LogQL Coverage](https://img.shields.io/badge/LogQL%20coverage-100%25-brightgreen)](#logql-compatibility)
[![License](https://img.shields.io/github/license/ReliablyObserve/Loki-VL-proxy)](LICENSE)
[![CodeQL](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml)

**Keep your entire Loki stack — Grafana Explore, Drilldown, dashboards, API tooling — and run it on VictoriaLogs at a fraction of the cost.**

- **Drop-in Loki API.** Point your existing Grafana Loki datasource at the proxy. Zero plugin changes, zero query rewrites.
- **VictoriaLogs economics.** Up to 30x less RAM, up to 15x less disk vs Loki. TrueFoundry real-world: ~40% less storage, lower CPU and RAM at the same ingestion rate.
- **Proxy intelligence built in.** 4-tier cache, 1h window reuse, adaptive parallelism, circuit breaker, rate limits, tenant isolation. One ~14 MB static binary.

Project site: `https://reliablyobserve.github.io/Loki-VL-proxy/`

---

## Real-World Improvements Over Loki

Benchmarked against a tuned Loki on the same hardware (Apple M5 Pro, 18 cores, 64 GB RAM, ~8M log entries, 7-day dataset). Numbers are from [six-workload comparison](docs/benchmarks.md#six-workload-read-path-comparison-loki-vs-vlproxy-vs-vl-native).

### Throughput at c=100 concurrent clients

| Workload | Proxy (warm) vs Loki | Proxy (cold, no cache) vs Loki | Why |
|----------|:--------------------:|:------------------------------:|-----|
| Metadata / label values | **68×** | **122×** | Coalescer deduplicates Grafana panel fan-out at source |
| Heavy aggregations (JSON, logfmt) | **1,006×** | **1,717×** | Aggregation results cached; coalescer eliminates repeated work |
| Content search (`\|= "word"`) | **4,522×** | **6,332×** | VL inverted token index vs Loki full chunk scan |
| Compute (rate, quantile, topk) | **22.5×** | **101×** | Coalescer collapses ~50% of concurrent identical queries |
| Historical (7-day windows) | **244×** | ~1× | Cache-only win; without cache, VL native is 6.9× faster |
| High-cardinality (pod/container) | **55.9×** | **6.4×** | VL stream-independent index; cache absorbs repeated pod queries |

The **cold proxy** column (cache disabled, coalescer + circuit breaker active) is the best measure of VL's raw query speed with thundering-herd protection. Its large advantage over VL native comes entirely from the coalescer collapsing concurrent duplicate requests before they reach VL.

### Content search: architectural advantage

Loki has no content index — every `|= "word"` filter scans every compressed chunk in the time range. A 24-hour search over high-volume logs can take 30–90 seconds regardless of hardware. VictoriaLogs maintains a word-level inverted token index — the same query completes in milliseconds. The proxy makes this transparent.

### High-cardinality workloads

Loki's ingester holds one in-memory chunk per active stream. High pod cardinality (hundreds of pods × services) multiplies memory linearly. VictoriaLogs uses a stream-independent columnar index — cardinality affects only the label index, not the storage engine. In the benchmark:

- **Loki**: 2,255 MB RSS at c=10 for high-cardinality queries
- **VL native**: 624 MB RSS — **3.6× less memory than Loki**
- Previously this workload caused 100% errors on the proxy (circuit breaker tripping from slow-query connection resets); fixed with sliding-window CB + coalescer coupling → **0% errors**

### Resource efficiency (heavy workload, c=10)

| | CPU consumed | RSS | Notes |
|---|---|---|---|
| Loki | 110.4 cpu·s | 2,185 MB | — |
| VL + Proxy (warm cache) | ~47 cpu·s | ~1,851 MB | Cache RSS ~798 MB (configurable via `-cache-max`) |
| VL + Proxy (no cache) | ~45 cpu·s | ~190 MB | Coalescer-only, 11.5× less RAM than Loki |
| VL native | 386.4 cpu·s | 1,152 MB | 3.5× more CPU than Loki |

The warm proxy uses more RAM than Loki in this table because the L1 response cache stores responses in-memory for microsecond re-serving. Reduce `-cache-max` to trade cache hit rate for memory. Without cache, VL+proxy combined uses **11.5× less RAM** than Loki at steady-state.

### Latency at c=100

| Workload | Loki P50 | Proxy warm P50 | Speedup |
|----------|----------|----------------|---------|
| Metadata | 196 ms | 2 ms | **98×** |
| Heavy aggregation | 2,399 ms | 2 ms | **1,200×** |
| Content search | 13,415 ms | 2 ms | **6,700×** |
| High-cardinality | 1,344 ms | 1 ms | **1,344×** |

### The proxy becomes invisible at scale

At small scale the proxy is a visible addition: ~50 MB RSS baseline + a bounded L1 cache. As your log volume grows, Loki's resource requirements scale fast while VL+proxy stays lean. At production scale **the proxy becomes a rounding error** in total cluster resources.

**Loki's scaling problem:**

Loki is a distributed system in production. Grafana's own sizing guide for a 3–30 TB/day cluster recommends **431 vCPU / 857 Gi RAM** across distributor, ingester, querier, compactor, and ruler components — all at replication factor 3. That RF=3 means every write is replicated 3×, every cross-zone push pays 3× network egress, and every component must be sized 3× for durability.

Ingesters grow linearly with active streams: each stream holds an in-memory chunk. 10,000 active pods × 10 label combinations = 100,000 active streams — each needing a chunk buffer in ingester RAM. Queries that touch high-cardinality streams cause the querier to load hundreds of chunk files simultaneously.

**VL+proxy at the same scale:**

VictoriaLogs runs as a single binary with no mandatory replication overhead. Its inverted index is stream-independent — 100,000 active streams require the same index structures as 1,000 streams (only the label cardinality table grows, not the storage engine). A typical production VL deployment uses **10–30× less RAM and 5–15× less disk** than an equivalent Loki cluster.

**Where the proxy fits:**

| Scale | Loki cluster | VL | Proxy (per instance) | Proxy % of Loki |
|-------|:---:|:---:|:---:|:---:|
| Small (1 GB/day) | 16 vCPU / 32 GB RAM | 2 vCPU / 4 GB | 0.1 vCPU / 0.5 GB | ~2.5% |
| Medium (50 GB/day) | 64 vCPU / 128 GB RAM | 8 vCPU / 16 GB | 0.2 vCPU / 1 GB | ~5% |
| Large (1 TB/day) | 431 vCPU / 857 GB RAM | 32 vCPU / 64 GB | 0.5 vCPU / 2 GB | **<1%** |
| XL (10 TB/day) | 1,221 vCPU / 2,235 GB RAM | 80 vCPU / 160 GB | 1 vCPU / 4 GB | **<0.5%** |

At 1 TB/day the proxy uses under 1% of what Loki requires for the same workload. At 10 TB/day it is below measurement noise. The proxy's response cache actively reduces VL load — at steady-state, 80–99% of Grafana dashboard queries hit cache and never reach VL at all, so the effective VL cluster can be sized for the remaining 1–20% of traffic.

**The actual resource equation at scale:**

```
Loki (1 TB/day):   431 vCPU + 857 GB RAM + 3× cross-AZ egress
VL (1 TB/day):      32 vCPU +  64 GB RAM + no replication overhead
Proxy (fleet of 3):  1 vCPU +   6 GB RAM (3 × 2 GB, shared via peer cache)

Total VL+proxy:     33 vCPU +  70 GB RAM   →  13× less CPU, 12× less RAM than Loki
```

The Loki → VL+proxy migration does not require changing a single Grafana dashboard, alert rule, or API client. The proxy maintains full Loki API compatibility.

---

## The Cost Case

At scale, Loki is expensive. Grafana's own sizing guide puts a 3–30 TB/day cluster at **431 vCPU / 857 Gi RAM**, and a ~30 TB/day cluster at **1,221 vCPU / 2,235 Gi RAM**. Loki's replication factor 3 also means 3x cross-zone write traffic — significant in cloud environments with AZ egress costs.

VictoriaLogs can be deployed AZ-local with no replication overhead, and the proxy overhead itself is small: ~15–30 ms on a cache miss, near-zero on a hit.

| | Loki | VictoriaLogs + proxy |
|---|---|---|
| RAM (vendor benchmarks) | baseline | up to 30x less |
| Disk (vendor benchmarks) | baseline | up to 15x less |
| Storage (TrueFoundry 500 GB / 7 day) | baseline | ~40% less |
| Cross-AZ writes | 3x (RF=3) | AZ-local possible |
| Index model | Chunk-based, heavy | Inverted index, lean |
| Grafana UX | Native | Identical via proxy |
| Caching | None (client-side) | 4-tier, window-aware |
| Proxy overhead | — | ~14 MB binary, 15–30 ms miss, ~0 ms hit |

---

## Quick Start

### Docker

```bash
docker run -p 3100:3100 \
  ghcr.io/reliablyobserve/loki-vl-proxy:latest \
  -backend=http://victorialogs:9428
```

### Helm

```bash
helm install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.patterns-enabled=true
```

### Grafana Datasource

Point your existing Loki datasource at the proxy — no other changes needed.

```yaml
datasources:
  - name: Loki (via VL proxy)
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: team-alpha
```

That's it. Grafana Explore, Drilldown, and all dashboards work immediately.

For StatefulSet persistence, peer-cache fleet setup, OTLP push wiring, and image source options, see [Getting Started](docs/getting-started.md) and [Operations](docs/operations.md).

---

## Why It's Fast

**4-tier cache:**
- **Tier0** — compatibility-edge cache for safe GET responses (no backend hit at all)
- **L1** — in-memory hot path
- **L2** — disk (bbolt), survives restarts, warms historical windows across large working sets
- **L3** — peer cache, lets warm fleet replicas share results instead of all hitting the backend

**Window reuse.** Long `query_range` requests are split into 1h windows. Historical windows are served from cache; only the live edge fetches from VictoriaLogs. A 7-day query with warm cache may hit the backend for a single window.

**Adaptive parallelism.** Parallel window fetches use EWMA-based backpressure — ramps up when VictoriaLogs is fast, backs off automatically before it becomes a problem.

**Request coalescing.** Concurrent identical queries collapse into one upstream request.

---

## What Works Out of the Box

- Grafana Explore — log browsing, filtering, live tail
- Grafana Logs Drilldown — patterns, service view, field breakdown
- Dashboards — all LogQL panel types
- Multi-tenant — `X-Scope-OrgID` isolation with per-tenant rate limits
- Live tail — native WebSocket tail or synthetic polling fallback
- Rules and alerts — read bridge to vmalert (no write lifecycle)
- LogQL — 100% coverage: stream selectors, filters, parsers, metric queries, range functions, vector operators
- OTel labels — dotted structured metadata exposed correctly in detected fields, underscore-safe in stream labels

---

## Production Features

- **Circuit breaker** — opens on backend failure, closes automatically on recovery
- **Per-client rate limits** — token bucket, configurable per tenant
- **Tenant isolation** — strict `X-Scope-OrgID` fanout guardrails; no cross-tenant data bleed
- **TLS / mTLS** — configurable on both northbound (client) and southbound (backend) boundaries
- **OTLP push** — proxy emits its own traces to any OTLP endpoint
- **Operator dashboard** — packaged Grafana dashboard covering Client → Proxy → VictoriaLogs, cache behavior, fanout, and resource utilization
- **Runbook-backed alerts** — 13 alert rules, each with a linked runbook
- **100+ Prometheus metrics** — all under `loki_vl_proxy_*` prefix
- **Read-only by default** — `/push` blocked, delete gated, debug/admin disabled unless explicitly enabled

---

## High-Level Flow

```mermaid
flowchart LR
    A[Clients<br/>Grafana, Loki API tools, MCP/LLM, scripts]
    B[Loki-VL-proxy<br/>Loki API compatibility + translation + shaping + cache]
    C[Upstream<br/>VictoriaLogs data + vmalert rules/alerts]

    A --> B --> C

    classDef client fill:#1f2937,stroke:#60a5fa,color:#f3f4f6,stroke-width:2px;
    classDef proxy fill:#172554,stroke:#22d3ee,color:#f8fafc,stroke-width:2px;
    classDef upstream fill:#052e16,stroke:#34d399,color:#ecfeff,stroke-width:2px;
    class A client;
    class B proxy;
    class C upstream;
```

## Detailed Architecture

```mermaid
flowchart TD
    subgraph L1["Clients"]
        G["Grafana<br/>Explore / Drilldown / Dashboards"]
        M["MCP / LLM / API Tools"]
        C["CLI / SDK Consumers"]
    end

    subgraph L2["Loki Compatibility Layer (Proxy)"]
        API["Loki HTTP + WS API<br/>query / query_range / labels / tail / rules / alerts"]
        GUARD["Tenant guardrails + auth context + rate limits + policy checks"]
        EDGE["Compatibility-edge cache (Tier0)<br/>safe GET response cache"]
    end

    subgraph L3["Execution Paths"]
        Q["Query translation + shaping"]
        T["Tail path (native or synthetic)"]
        R["Rules / alerts read bridge"]
        RESP["Loki-compatible response"]
    end

    subgraph L4["Cache Tiers"]
        L1C["L1 memory cache"]
        L2C["L2 disk cache (optional)"]
        L3C["L3 peer cache (optional)"]
    end

    subgraph L5["Upstream Systems"]
        VL["VictoriaLogs<br/>log data"]
        VMA["vmalert<br/>rules / alerts state"]
        VM["VictoriaMetrics (optional)<br/>recording rule outputs"]
    end

    G --> API
    M --> API
    C --> API

    API --> GUARD
    GUARD --> EDGE
    EDGE -->|hit| RESP
    EDGE -->|miss| Q

    GUARD --> T
    GUARD --> R

    Q --> L1C
    L1C -->|miss| L2C
    L2C -->|miss| L3C
    L3C -->|miss| VL
    VL --> Q
    Q --> RESP

    T --> VL
    R --> VMA
    VMA -. optional remote write .-> VM

    classDef client fill:#1f2937,stroke:#93c5fd,color:#f3f4f6,stroke-width:2px;
    classDef api fill:#0f172a,stroke:#22d3ee,color:#f8fafc,stroke-width:2px;
    classDef exec fill:#172554,stroke:#818cf8,color:#eef2ff,stroke-width:2px;
    classDef cache fill:#3f1d2e,stroke:#f472b6,color:#fdf2f8,stroke-width:2px;
    classDef upstream fill:#052e16,stroke:#34d399,color:#ecfdf5,stroke-width:2px;

    class G,M,C client;
    class API,GUARD,EDGE api;
    class Q,T,R,RESP exec;
    class L1C,L2C,L3C cache;
    class VL,VMA,VM upstream;
```

---

## Compatibility

Loki-VL-proxy is validated continuously in CI against three separate tracks: Loki API, Grafana Logs Drilldown, and VictoriaLogs integration.

### Label and Field Compatibility

| Profile | Stream labels (`/labels`) | Detected fields / metadata | Best for |
|---|---|---|---|
| Loki-conservative | underscore-only | translated underscore aliases | strict Loki UX |
| Mixed (default) | underscore-only | dotted + translated aliases | Grafana + OTel correlation |
| Native-field | underscore-only (`label-style=underscores`) | dotted-native only | VL/OTel-native field workflows |

Grafana query builder works best with underscore aliases. Code mode (`label-style=underscores`, `metadata-field-mode=translated`) handles dotted keys without UI tokenization issues.

**Tuple safety:** Default responses return strict `[timestamp, line]` 2-tuples. 3-tuple metadata mode activates only when the client sends `X-Loki-Response-Encoding-Flags: categorize-labels`. Cache keys are segregated by tuple mode.

### LogQL Compatibility

Stream selectors, filters, parser pipelines, metric queries, range functions, scalar bool comparisons, vector set operators, and invalid LogQL error forms are all covered and machine-validated in CI against a real Loki oracle.

For full detail: [Loki Compatibility](docs/compatibility-loki.md), [Translation Reference](docs/translation-reference.md), [Known Issues](docs/KNOWN_ISSUES.md)

---

## Observability

- **100+ Prometheus metrics** under `loki_vl_proxy_*` — cache hit ratios, window fetch latency, fanout behavior, per-tenant and per-client pressure, circuit breaker state
- **Packaged operator dashboard** — rows for Client-Side Loki API Visibility, Proxy Internal, and Backend-Side VictoriaLogs fanout; fast incident attribution
- **13 runbook-backed alert rules** — backend latency, backend unreachable, circuit breaker open, high error rate, rate limiting, tenant isolation, and more
- **Structured JSON logs** — route-aware, semconv-aligned, with user-pattern attribution from trusted Grafana headers
- **OTLP tracing** — proxy emits traces to any OTLP endpoint

See [Observability](docs/observability.md) and [Alert Runbooks Index](docs/runbooks/alerts.md).

---

## Security

- Read-only API surface by default: `/push` blocked, delete gated, debug/admin disabled
- Non-root runtime image, read-only root filesystem, restricted Helm security contexts
- Hardening headers on all HTTP responses including 404s and disabled routes
- CI security gates: `gitleaks`, `gosec`, Trivy, `actionlint`, `hadolint`, OpenSSF Scorecard, OWASP ZAP, curated Nuclei
- Proxy-specific coverage: tenant isolation, cache boundary enforcement, browser-origin checks on `/tail`, forwarded auth handling

See [Security](docs/security.md) and [Security Policy](SECURITY.md).

---

## UI Gallery

VictoriaLogs backend with Loki-VL-proxy as the Loki-compatible query layer.

<a href="docs/images/ui/explore-main.png">
  <img src="docs/images/ui/explore-main.png" alt="Grafana Explore main view" width="240" />
</a>
<a href="docs/images/ui/explore-details.png">
  <img src="docs/images/ui/explore-details.png" alt="Grafana Explore details view" width="240" />
</a>
<a href="docs/images/ui/drilldown-main.png">
  <img src="docs/images/ui/drilldown-main.png" alt="Grafana Logs Drilldown main view" width="240" />
</a>
<a href="docs/images/ui/drilldown-service.png">
  <img src="docs/images/ui/drilldown-service.png" alt="Grafana Logs Drilldown service detail view" width="240" />
</a>
<a href="docs/images/ui/explore-tail-multitenant.png">
  <img src="docs/images/ui/explore-tail-multitenant.png" alt="Grafana Explore multi-tenant view" width="240" />
</a>

---

## Documentation Map

### Core
- [Getting Started](docs/getting-started.md)
- [Configuration](docs/configuration.md)
- [Operations](docs/operations.md)
- [Architecture](docs/architecture.md)
- [API Reference](docs/api-reference.md)
- [Security](docs/security.md)
- [Observability](docs/observability.md)
- [Performance](docs/performance.md)
- [Scaling](docs/scaling.md)

### Compatibility
- [Compatibility Matrix](docs/compatibility-matrix.md)
- [Loki Compatibility](docs/compatibility-loki.md)
- [Logs Drilldown Compatibility](docs/compatibility-drilldown.md)
- [Grafana Loki Datasource Compatibility](docs/compatibility-grafana-datasource.md)
- [VictoriaLogs Compatibility](docs/compatibility-victorialogs.md)
- [Translation Modes Guide](docs/translation-modes.md)
- [Translation Reference](docs/translation-reference.md)

### Cache and Runtime Design
- [Fleet Cache](docs/fleet-cache.md)
- [Peer Cache Design](docs/peer-cache-design.md)
- [Benchmarks](docs/benchmarks.md)

### Runbooks
- [Alert Runbooks Index](docs/runbooks/alerts.md)
- [Deployment Best Practices](docs/runbooks/deployment-best-practices.md)
- [Backend High Latency](docs/runbooks/loki-vl-proxy-backend-high-latency.md)
- [Backend Unreachable](docs/runbooks/loki-vl-proxy-backend-unreachable.md)
- [Circuit Breaker Open](docs/runbooks/loki-vl-proxy-circuit-breaker-open.md)
- [Client Bad Request Burst](docs/runbooks/loki-vl-proxy-client-bad-request-burst.md)
- [Proxy Down](docs/runbooks/loki-vl-proxy-down.md)
- [Grafana Tuple Contract](docs/runbooks/loki-vl-proxy-grafana-tuple-contract.md)
- [High Error Rate](docs/runbooks/loki-vl-proxy-high-error-rate.md)
- [High Latency](docs/runbooks/loki-vl-proxy-high-latency.md)
- [Rate Limiting](docs/runbooks/loki-vl-proxy-rate-limiting.md)
- [Operational Resources](docs/runbooks/loki-vl-proxy-system-resources.md)
- [Tenant High Error Rate](docs/runbooks/loki-vl-proxy-tenant-high-error-rate.md)

### Testing and Release
- [Testing](docs/testing.md)
- [Release Info](docs/release-info.md)

### Migration and Project Status
- [Rules And Alerts Migration](docs/rules-alerts-migration.md)
- [Known Issues](docs/KNOWN_ISSUES.md)
- [Roadmap](docs/roadmap.md)
- [Changelog](CHANGELOG.md)

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
