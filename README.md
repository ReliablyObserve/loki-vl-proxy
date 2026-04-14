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
[![Lines of Code](https://img.shields.io/badge/go%20loc-63.3k-blue)](https://github.com/ReliablyObserve/Loki-VL-proxy)
[![Tests](https://img.shields.io/badge/tests-1574%20passed-brightgreen)](#tests)
[![Coverage](https://img.shields.io/badge/coverage-89.1%25-green)](#tests)
[![LogQL Coverage](https://img.shields.io/badge/LogQL%20coverage-100%25-brightgreen)](#logql-compatibility)
[![License](https://img.shields.io/github/license/ReliablyObserve/Loki-VL-proxy)](LICENSE)
[![CodeQL](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml)

Use **Grafana Loki clients** with **VictoriaLogs** through a **Loki-compatible read proxy**.

No custom Grafana datasource plugin. No sidecar translation service. One small static binary.

Project site: `https://reliablyobserve.github.io/Loki-VL-proxy/`

## Why Teams Use It

- **Drop-in Loki frontend**: Keep Grafana Explore, Drilldown, dashboards, and Loki API tooling.
- **VictoriaLogs backend economics**: Query VL while preserving Loki client experience.
- **Dual-schema compatibility**: Keep Loki-safe underscore labels while exposing OTel/VL dotted structured metadata where needed.
- **Patterns compatibility with persistence**: Loki-compatible `/patterns` support with optional disk + peer-warm persistence for fast Drilldown after restarts.
- **Strict compatibility contracts**: Default 2-tuple responses, explicit 3-tuple only when `categorize-labels` is requested.
- **Production guardrails**: Tenant isolation, bounded fanout, circuit breaking, rate limits, and safe caching.
- **Fast repeat reads**: Tiered cache with optional disk and fleet peer reuse.

Related docs: [Architecture](docs/architecture.md), [Compatibility Matrix](docs/compatibility-matrix.md), [Patterns](docs/patterns.md), [Operations](docs/operations.md), [Known Issues](docs/KNOWN_ISSUES.md)

## Key Features

### Compatibility

- Loki-compatible read API for Grafana datasource, Explore, Drilldown, and API clients.
- Strict tuple contracts: default 2-tuple, explicit 3-tuple only via `categorize-labels`.
- Grafana Logs Drilldown patterns support through `/loki/api/v1/patterns`, with explicit `-patterns-enabled` control when deployments need it disabled.
- Loki-compatible patterns endpoint with optional restart-safe persistence.
- Automatic pattern autodetection from successful `query`/`query_range` responses (`-patterns-autodetect-from-queries`) to keep Drilldown patterns warm behind the scenes.
- Custom pattern overlays via runtime flags or Helm ConfigMap/file wiring (`-patterns-custom`, `-patterns-custom-file`, `patternsCustom.*`).
- Multi-tenant read fanout with tenant isolation guardrails.
- Rules and alerts read compatibility from `vmalert`.

### Performance

- Query-range windowing with historical window reuse.
- Adaptive bounded parallel fanout for long time ranges.
- Tiered caching: compatibility-edge, memory, disk, and optional peer cache.
- Compression-aware read path: `zstd`/`gzip` client responses, compressed peer-cache hops, and negotiated upstream compression with safe decode.
- Request coalescing and protective limits to reduce backend pressure.

### Operations

- Route-aware upstream/downstream metrics and semconv-aligned structured logs for client, proxy, and VictoriaLogs visibility.
- Better read-path control: per-route visibility, user-pattern attribution from trusted Grafana headers, and separate northbound/southbound auth boundaries.
- Packaged operator dashboard covering `Client -> Proxy -> VictoriaLogs`, query-range resilience, cache behavior, and operational resources.
- Helm-ready deployment model for production clusters.
- Compatibility CI tracks for Loki, Logs Drilldown, and VictoriaLogs.
- CI guardrails to keep new app metrics under the `loki_vl_proxy_*` prefix.
- Runbook-backed alerting assets for operational response.

Related docs: [Compatibility Matrix](docs/compatibility-matrix.md), [Patterns](docs/patterns.md), [Observability](docs/observability.md), [Testing](docs/testing.md)

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

Capture/re-generation workflow is documented in [Testing](docs/testing.md#compose-screenshot-workflow).

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

Related docs: [Architecture](docs/architecture.md), [API Reference](docs/api-reference.md)

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

Related docs: [Architecture](docs/architecture.md), [Fleet Cache](docs/fleet-cache.md), [Peer Cache Design](docs/peer-cache-design.md)

## Product Scope

Loki-VL-proxy is intentionally a **read/query proxy**.

- In scope: Loki-compatible query/read endpoints, metadata paths, rules/alerts read views.
- Out of scope: ingestion pipeline ownership (`push` is blocked), rule write lifecycle.

Use VictoriaLogs-side ingestion (`vlagent`, OTLP, native JSON/OTel, Loki-push-to-VL) and query that data through this proxy.

Related docs: [API Reference](docs/api-reference.md), [Rules And Alerts Migration](docs/rules-alerts-migration.md), [Known Issues](docs/KNOWN_ISSUES.md)

## Quick Start

```bash
# Binary
go build -o loki-vl-proxy ./cmd/proxy
./loki-vl-proxy -backend=http://victorialogs:9428

# Docker
docker build -t loki-vl-proxy .
docker run -p 3100:3100 loki-vl-proxy -backend=http://victorialogs:9428

# Compose (includes Grafana)
docker compose up -d
```

### Helm

```bash
helm install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428
```

For Grafana Logs Drilldown pattern discovery, keep the default `patterns-enabled=true` or set it explicitly during rollout:

```bash
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.patterns-enabled=true
```

For high-cardinality label-value browsing, tune indexed browse cache directly via chart flags:

```bash
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.label-values-indexed-cache=true \
  --set extraArgs.label-values-hot-limit=200 \
  --set extraArgs.label-values-index-max-entries=200000
```

For static custom Drilldown patterns (always prepended), configure inline values or a file-backed ConfigMap:

```bash
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set-json 'patternsCustom.inline=["time=\"<_>\" level=info msg=\"finished unary call\"","grpc.code=<_> grpc.method=<_>"]' \
  --set patternsCustom.file.enabled=true
```

For deployment recipes (StatefulSet + persistence, peer-cache fleet setup, OTLP push wiring) and image source selection (GHCR vs Docker Hub vs custom registry), see:
- [Getting Started](docs/getting-started.md)
- [Operations](docs/operations.md)

### Grafana Datasource

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

Related docs: [Getting Started](docs/getting-started.md), [Configuration](docs/configuration.md), [Operations](docs/operations.md)

## Compatibility Guarantees (Operator-Relevant)

- **Loki tuple safety**:
  - default requests return strict `[timestamp, line]`
  - `X-Loki-Response-Encoding-Flags: categorize-labels` enables Loki 3-tuple metadata mode
- **Cache mode segregation**:
  - query cache keys are split by tuple mode to prevent 3-tuple/2-tuple cross-contamination
- **Grafana-first behavior**:
  - compatibility tracks continuously verify Loki API, Logs Drilldown, and VictoriaLogs integration

### Label/Field Compatibility Profiles

| Profile | Label surfaces (`stream`, `/labels`) | Field/metadata surfaces (`/detected_fields`, 3-tuple metadata) | Best fit |
|---|---|---|---|
| Loki-conservative | underscore-only | translated underscore aliases | strict Loki UX |
| Mixed (default) | underscore-only | dotted + translated aliases | Grafana + OTel correlation |
| Native-field | underscore-only (when `label-style=underscores`) | dotted-native only | VL/OTel-native field workflows |

Operational note:
- Grafana datasource queries can use dotted field filters (for example `k8s.cluster.name = \`my-cluster\``) while stream labels remain Loki-compatible underscores.
- Grafana Loki query builder UI may tokenize dotted keys as `label=host`, `operator=.`, `value=id` for `host.id`. Query execution still works in code mode, but builder editing is safest with underscore aliases (`label-style=underscores`, `metadata-field-mode=translated`).
- `extra-label-fields` can extend `/labels` and alias resolution for custom VL fields while keeping Loki-facing labels underscore-safe in conservative mode.

Related docs: [Compatibility Matrix](docs/compatibility-matrix.md), [Loki Compatibility](docs/compatibility-loki.md), [Logs Drilldown Compatibility](docs/compatibility-drilldown.md), [VictoriaLogs Compatibility](docs/compatibility-victorialogs.md)

## LogQL Compatibility

Loki-VL-proxy targets Loki client compatibility while translating execution to VictoriaLogs.

- Stream selectors, filters, parser pipelines, metric queries, and common range functions are supported.
- Proxy-side compatibility logic covers semantic gaps where Loki behavior differs from native VictoriaLogs primitives.
- Compatibility is validated continuously in CI against separate Loki, Drilldown, and VictoriaLogs tracks.

For full detail:
- [Translation Modes Guide](docs/translation-modes.md)
- [Translation Reference](docs/translation-reference.md)
- [Loki Compatibility](docs/compatibility-loki.md)
- [Known Issues](docs/KNOWN_ISSUES.md)

## Performance Model

- Multi-layer cache: compatibility-edge + memory + optional disk + optional peer cache.
- Query-range windowing: historical range reuse with adaptive bounded parallel fetch.
- Built-in metrics/logs for tuning cache hit ratio, backend latency, fanout behavior, and tenant/client pressure.

### Query-Range Tuning (Long-Range Efficiency)

- Split long ranges into cacheable windows (for example `1h`) and reuse historical windows.
- Keep near-now windows uncached (or very short TTL) and use longer TTL for historical windows.
- Use adaptive bounded parallelism to improve long-range latency without overloading VictoriaLogs.
- Track tuning with window cache hit/miss, window fetch latency, and adaptive parallelism metrics.

### Why The Cache Stack Matters

- `Tier0` compatibility-edge cache reduces repeated frontend compatibility work.
- `L1` memory cache gives fastest hot-path reads.
- `L2` disk cache keeps useful historical windows warm across larger working sets.
- `L3` peer cache lets warm replicas help the fleet instead of refetching from backend.

See [Performance](docs/performance.md), [Fleet Cache](docs/fleet-cache.md), [Scaling](docs/scaling.md), and [Observability](docs/observability.md).

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

## License

Apache License 2.0. See [LICENSE](LICENSE).
