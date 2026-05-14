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
[![Source Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/Loki-VL-proxy/badges/.github/badges/loc-code.json)](https://github.com/ReliablyObserve/Loki-VL-proxy)
[![Test Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/Loki-VL-proxy/badges/.github/badges/loc-tests.json)](https://github.com/ReliablyObserve/Loki-VL-proxy)
[![Tests](https://img.shields.io/badge/tests-2527%20passed-brightgreen)](#tests)
[![Coverage](https://img.shields.io/badge/coverage-87.2%25-green)](#tests)
[![LogQL Coverage](https://img.shields.io/badge/LogQL%20coverage-100%25-brightgreen)](#logql-compatibility)
[![License](https://img.shields.io/github/license/ReliablyObserve/Loki-VL-proxy)](LICENSE)
[![CodeQL](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml)

**Keep your entire Loki stack — Grafana Explore, Drilldown, dashboards, API tooling — and run it on VictoriaLogs.**

- **Drop-in Loki API.** Point your existing Grafana Loki datasource at the proxy. Zero plugin changes, zero query rewrites.
- **Measured resource difference.** At 310 GiB/day ingest: VL + proxy runs on **1.4 cores and 6.1 GiB RAM**. Loki's published minimum for that ingest class: 38 cores, 59 GiB. That gap is real — not a benchmark artifact.
- **Proxy intelligence built in.** 4-tier cache, 1h window reuse, adaptive parallelism, circuit breaker, rate limits, tenant isolation. One ~14 MB static binary.

Project site: `https://reliablyobserve.github.io/Loki-VL-proxy/`

---

## Query Performance

Measured head-to-head against tuned Loki: Apple M5 Pro (18 cores, 64 GB RAM), ~8 M log entries across 15 services, 7-day window.

### Cold proxy — honest baseline

No cache, no coalescer. Pure translation overhead + HTTP proxying + VictoriaLogs response time. This is the floor — what you get before any caching kicks in.

| Workload | Concurrency | Cold proxy | Loki | Ratio |
|---|:---:|---:|---:|:---:|
| Small metadata queries | c=10 | 1,212 req/s | ~880 req/s | **1.4× faster** |
| Small metadata queries | c=50 | 1,583 req/s | ~780 req/s | **2× faster** |
| Heavy pipeline queries | c=10 | 126–188 req/s | ~161–472 req/s | **~parity** |
| Heavy pipeline queries | c=100 | 139 req/s | ~33 req/s | **4.2× faster** |
| Long-range (6 h–72 h) | c=10 | 2× faster than Loki | — | parallel sub-window fetching |
| Compute (rate, topk) | c=10 | 210 req/s | ~2,403 req/s | 0.09× — N VL calls per metric query |

- **Small and metadata queries:** 1.4–2× faster than Loki cold — VL scans are faster than Loki's chunk store for label/series queries.
- **Heavy pipeline queries:** parity to 4.2× faster depending on concurrency — `stats_query_range` fast path eliminates 39% cold CPU for `count_over_time`/`rate` queries.
- **Long-range queries:** 2× faster cold — parallel sub-window fetching completes before Loki's sequential chunk scan.
- **Compute aggregations (`quantile_over_time`, `topk`, multi-stage pipelines):** each metric query fans out to N VL calls; pprof-guided alloc fixes lifted cold throughput from 40 to 210 req/s. Historical sub-windows are cached on first fetch (24 h TTL), so repeated compute queries approach warm performance.

### Warm cache — what production steady-state looks like

Grafana dashboards auto-refresh every 30 s. After the first fetch, repeated queries are served from in-memory cache without touching VictoriaLogs. The numbers below are **100% cache-hit results** — they represent the ceiling, not the typical case. Real production performance sits between the cold floor above and these warm numbers depending on your dashboard diversity and refresh interval.

| Workload | Concurrency | Loki req/s | Proxy (warm) req/s | Throughput | P50 Loki | P50 Proxy | Latency |
|---|:---:|---:|---:|:---:|---:|---:|:---:|
| Small panels | c=10 | 2,011 | 15,626 | **7.8× faster** | 4 ms | 587 µs | **6.8× faster** |
| Small panels | c=100 | 2,290 | 27,513 | **12× faster** | 42 ms | 3 ms | **14× faster** |
| Heavy queries | c=10 | 407 | 5,944 | **14.6× faster** | 4 ms | 1 ms | **4× faster** |
| Heavy queries | c=100 | 162† | 7,134 | **44× faster** | ~1,800 ms† | 12 ms | **150× faster** |
| Long-range | c=10 | 8 | 157 | **18.7× faster** | 481 ms | 1 ms | **481× faster** |
| Compute | c=10 | 2,803 | 11,162 | **4× faster** | 1 ms | 675 µs | **1.5× faster** |
| Compute | c=100 | 1,611 | 16,456 | **10.2× faster** | 4 ms | 4 ms | parity |

CPU: **6–408× less** than Loki under cache load. RAM: **1.7–3.9× less** for most workloads.

† Loki heavy c=100 was saturated — P90=1,818 ms, P99=6,950 ms, delivering only 162 req/s vs 7,134 for the proxy.

### Dashboard load spikes — request coalescer

When many panels hit the same query simultaneously, the proxy collapses them into a single backend call. The figures below assume the coalesced result is already cached; first-hit coalescing still avoids the N-fan-out but pays one backend round-trip.

| Workload | Loki P50 | Proxy P50 (warm) |
|---|---:|---:|
| Metadata queries | 196 ms | **1 ms** |
| Heavy aggregations | 2,399 ms | **1 ms** |
| Content search | 13,415 ms | **1 ms** |

Full throughput tables, P90/P99 latency, CPU and RSS breakdowns: [Benchmarks](docs/benchmarks.md) · [Performance](docs/performance.md)

---

## The Cost Case

This is a production deployment, not a synthetic benchmark. The numbers below come from a real VictoriaLogs installation running at **310 GiB/day** raw ingest with **800 M total log entries** and **7.1 days** of retention.

**What VictoriaLogs actually consumed at that load:**

| Component | Cores | Memory |
|---|---:|---:|
| `vlstorage` | 1.0 | 5.0 GiB |
| `vlinsert` | 0.1 | 0.6 GiB |
| `vlselect` | 0.1 | 0.25 GiB |
| **VL + loki-vl-proxy, combined** | **~1.4** | **~6.1 GiB** |

For comparison, [Loki's own documentation](https://grafana.com/docs/loki/latest/setup/size/) puts the **minimum** hardware requirement at **38 cores and 59 GiB** for the same ingest class (`<3 TB/day`). That's the floor — a minimal, single-tenant, non-HA deployment.

**Caveat:** `vlselect` was measured at zero read concurrency — query load will add to that number. If you run heavy aggregation queries at scale, benchmark your own workload with `loki-bench` before sizing.

**Storage:** 2,201 GiB of raw logs (310 GiB/day × 7.1 days) compressed to **40.5 GiB on disk — 54.9× compression**. TrueFoundry ran an independent migration and reported ~40% less storage versus their Loki deployment at the same retention.

**Replication:** Loki's recommended production setup uses RF=3 — tripling write load, disk, and cross-AZ egress. VictoriaLogs is designed for AZ-local deployment with no mandatory replication. If you're paying for cross-AZ data transfer today, that alone can outweigh compute savings.

**Migration cost:** Zero changes to Grafana, dashboards, alerts, or any Loki API client. The proxy handles translation transparently; remove it and point back at Loki if needed.

Full cost worksheet, scaling projections, and EC2/GCP sizing tables: [Cost Model](docs/cost-model.md) · [Scaling](docs/scaling.md)

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
