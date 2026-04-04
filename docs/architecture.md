# Architecture

## Overview

Loki-VL-proxy is an HTTP proxy that sits between Grafana (or any Loki API client) and VictoriaLogs. It translates Loki's LogQL API into VictoriaLogs' LogsQL API, allowing Grafana's native Loki datasource to query VictoriaLogs without a custom plugin.

## Request Flow

```mermaid
flowchart TD
    subgraph Clients
        G["Grafana<br/>(Loki datasource)"]
        M["MCP Servers<br/>LLM Agents"]
        D["Dashboards<br/>Explore / Drilldown"]
    end

    subgraph Proxy["Loki-VL-proxy :3100"]
        RL["Rate Limiter<br/>per-client token bucket<br/>+ global concurrency"]
        CO["Request Coalescer<br/>singleflight: N queries → 1"]
        NM["Query Normalizer<br/>sort matchers, collapse ws"]
        TR["LogQL → LogsQL<br/>Translator"]
        CA["TTL Cache (L1)<br/>per-endpoint TTLs<br/>max 256MB"]
        RC["Response Converter<br/>VL NDJSON → Loki streams<br/>VL stats → Prom matrix"]
        CB["Circuit Breaker<br/>closed→open→half-open"]
        OB["/metrics + JSON logs"]
    end

    VL["VictoriaLogs<br/>:9428"]

    G --> RL
    M --> RL
    D --> RL
    RL --> CO
    CO --> NM
    NM --> CA
    CA -->|miss| TR
    TR --> CB
    CB --> VL
    VL --> RC
    RC --> CA
    CA -->|hit| G

    style Proxy fill:#1a1a2e,stroke:#e94560,color:#fff
    style VL fill:#0f3460,stroke:#16213e,color:#fff
    style RL fill:#533483,stroke:#e94560,color:#fff
    style CO fill:#533483,stroke:#e94560,color:#fff
    style CA fill:#0f3460,stroke:#16213e,color:#fff
    style TR fill:#e94560,stroke:#fff,color:#fff
    style CB fill:#533483,stroke:#e94560,color:#fff
```

## Protection Layers

| Layer | Purpose | Default Config |
|---|---|---|
| Per-client rate limiter | Prevent individual client abuse | 50 req/s, burst 100 |
| Global concurrent limit | Cap total backend load | 100 concurrent queries |
| Request coalescing | Deduplicate identical queries | Automatic (singleflight) |
| Query normalization | Improve cache hit rate | Sort matchers, collapse whitespace |
| In-memory TTL cache | Reduce backend calls | Per-endpoint TTLs, 256MB max |
| Circuit breaker | Protect VL from cascading failure | Opens after 5 failures, 10s backoff |

### How Coalescing Works

When 50 Grafana dashboards send `{app="nginx"} |= "error"` simultaneously:

```
Client 1 ──┐
Client 2 ──┤
Client 3 ──┤──→ 1 request to VL ──→ response shared to all 50
  ...      │
Client 50 ─┘
```

Only **1** request reaches VictoriaLogs. All clients get the same response. Coalescing keys include the tenant header to prevent cross-tenant data leaks.

## Data Model Mapping

### Loki vs VictoriaLogs

| Loki Concept | VL Equivalent |
|---|---|
| Stream labels | `_stream` fields (declared at ingestion) |
| Structured metadata | Regular fields (all others) |
| Timestamp | `_time` |
| Log line body | `_msg` |
| Parsed labels | Fields from `| unpack_json` / `| unpack_logfmt` |

VictoriaLogs treats all fields equally, while Loki 3.x distinguishes stream labels, structured metadata, and parsed labels. In practice, Grafana Explore handles both transparently.

### Label Translation

VictoriaLogs stores OTel attributes with native dotted names (`service.name`), while Loki uses underscores (`service_name`). The `-label-style` flag controls translation:

| Mode | Response Direction | Query Direction |
|---|---|---|
| `passthrough` | No translation | No translation |
| `underscores` | `service.name` → `service_name` | `{service_name="x"}` → VL `"service.name":"x"` |

Built-in reverse mappings cover 50+ OTel semantic convention fields.

## E2E Test Architecture

```mermaid
flowchart LR
    subgraph TestRunner["go test -tags=e2e"]
        INGEST["Ingest identical<br/>logs to both"]
        COMPARE["Compare responses<br/>endpoint by endpoint"]
        SCORE["Calculate<br/>compatibility %"]
    end

    subgraph Stack
        LOKI["Loki :3101<br/>(ground truth)"]
        VLP["Loki-VL-proxy<br/>:3100"]
        VL["VictoriaLogs<br/>:9428"]
        GF["Grafana :3000<br/>3 datasources"]
    end

    INGEST -->|push| LOKI
    INGEST -->|push| VL
    COMPARE -->|GET /loki/api/v1/*| LOKI
    COMPARE -->|GET /loki/api/v1/*| VLP
    VLP --> VL
    COMPARE --> SCORE
    GF -.->|manual compare| LOKI
    GF -.->|manual compare| VLP
```

## Component Design

### Translator (`internal/translator/`)
Pure string manipulation parser — no external LogQL parser library. Converts LogQL to LogsQL left-to-right using prefix matching and regex for templates.

### Proxy (`internal/proxy/`)
HTTP handlers for all Loki API endpoints. Each handler: validates input, translates the query, calls VL, converts the response to Loki format.

### Middleware (`internal/middleware/`)
- **Rate limiter**: per-client token bucket + global semaphore
- **Coalescer**: singleflight-based request deduplication
- **Circuit breaker**: 3-state (closed/open/half-open) with configurable thresholds

### Cache (`internal/cache/`)
Three-tier: L1 in-memory (sync.Map + atomic counters), optional L2 on-disk (bbolt with gzip compression), and optional L3 peer cache (consistent hash ring). Disk encryption is delegated to cloud provider (EBS, PD, etc.).

### Metrics (`internal/metrics/`)
Prometheus text exposition at `/metrics`. Per-endpoint request counts, per-tenant breakdowns, cache stats, circuit breaker state gauge.
