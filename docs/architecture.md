---
sidebar_label: Architecture Overview
description: How loki-vl-proxy works — runtime paths, protection layers, cache tiers, LogQL translation, and component design.
---

# Architecture

## Overview

Loki-VL-proxy is a read-only Loki compatibility proxy that sits between Grafana (or any Loki API client) and VictoriaLogs. It exposes Loki-compatible HTTP and WebSocket routes on the frontend, translates LogQL into LogsQL where needed, shapes VictoriaLogs responses back into Loki-compatible structures, and optionally exposes rules and alerts reads from a separate backend such as `vmalert`.

## Runtime Paths

```mermaid
flowchart TD
    subgraph Clients["Clients"]
        G["Grafana<br/>Explore / Drilldown / Dashboards"]
        M["MCP / Agents"]
        C["CLI / API Consumers"]
    end

    subgraph Front["Loki-VL-proxy :3100"]
        API["Loki HTTP + WebSocket surface<br/>query / labels / detected_* / tail / rules / alerts"]
        GUARD["Security headers + tenant validation<br/>auth checks + rate limits + request logging"]
        ROUTE["Route-specific execution"]
        EDGE["Tier0 compatibility-edge cache<br/>safe GET Loki-shaped responses only"]
    end

    subgraph Query["Query + Metadata Path"]
        FAN["Optional multi-tenant fanout<br/>and __tenant_id__ narrowing"]
        CACHE["L1 memory -> optional L2 disk -> optional L3 peer cache"]
        CO["Coalescing + query normalization"]
        TR["LogQL -> LogsQL translation"]
        SHAPE["Response shaping<br/>streams / labels / stats / drilldown"]
    end

    subgraph Tail["/tail WebSocket Path"]
        ORIGIN["Origin allowlist + websocket upgrade"]
        MODE["tail.mode = auto | native | synthetic"]
        NATIVE["Native VL tail stream"]
        SYN["Synthetic polling fallback"]
    end

    subgraph Reads["Rules + Alerts Read Path"]
        ALERT["Read-compatible Loki / Prometheus rules and alerts views"]
    end

    subgraph Backends["Backends + Outputs"]
        VL["VictoriaLogs"]
        VMR["vmalert / ruler read backend"]
        VMTS["optional VictoriaMetrics<br/>recording-rule outputs"]
        OBS["Prometheus metrics + OTLP + JSON logs"]
    end

    G --> API
    M --> API
    C --> API
    API --> GUARD --> ROUTE
    ROUTE --> FAN --> EDGE
    EDGE -->|miss| CACHE
    CACHE -->|miss| CO --> TR --> VL
    VL --> SHAPE --> CACHE
    SHAPE --> EDGE
    ROUTE --> ORIGIN --> MODE
    MODE --> NATIVE --> VL
    MODE --> SYN --> VL
    ROUTE --> ALERT --> VMR
    VMR -. recording writes .-> VMTS
    GUARD --> OBS
    SHAPE --> OBS

    style Front fill:#1a1a2e,stroke:#e94560,color:#fff
    style Query fill:#16213e,stroke:#4cc9f0,color:#fff
    style Tail fill:#0f3460,stroke:#90e0ef,color:#fff
    style Reads fill:#1b4332,stroke:#52b788,color:#fff
    style Backends fill:#3b1c32,stroke:#ff7f50,color:#fff
```

## Protection Layers

| Layer | Purpose | Default Config |
|---|---|---|
| Tenant validation | Enforce Loki-style tenant header policy and mapping rules before backend access | Enabled on tenant-scoped routes |
| Per-client rate limiter | Prevent individual client abuse | Built-in default `50 req/s`, burst `100` |
| Global concurrent limit | Cap total backend load | Built-in default `100` concurrent backend queries |
| Request coalescing | Deduplicate identical queries | Automatic (singleflight) |
| Query normalization | Improve cache hit rate | Sort matchers, collapse whitespace |
| Tier0 response cache | Short-circuit repeated safe GET reads after tenant validation | Enabled, 10% of L1 memory budget, safe GET read endpoints only |
| Tiered cache | Reduce backend calls with local, disk, and peer reuse | L1 memory, optional L2 disk, optional L3 peer cache |
| Circuit breaker | Protect VL from cascading failure | Built-in default: opens after `5` failures, `10s` backoff |
| Tail origin allowlist | Reject browser websocket origins unless explicitly trusted | Deny browser origins by default |

### How Coalescing Works

When 50 Grafana dashboards send `{app="nginx"} |= "error"` simultaneously:

```mermaid
flowchart LR
    C1["Client 1"] --> SF["Singleflight<br/>1 request"]
    C2["Client 2"] --> SF
    C3["Client 3"] --> SF
    CN["Client 50"] --> SF
    SF --> VL["VictoriaLogs"]
    VL --> R["Response"]
    R --> C1
    R --> C2
    R --> C3
    R --> CN
```

Only **1** request reaches VictoriaLogs. All clients get the same response. Coalescing keys include the tenant header to prevent cross-tenant data leaks.

## Query And Metadata Flow

```mermaid
flowchart TD
    REQ["Loki read request"] --> WRAP["securityHeaders -> tenantMiddleware<br/>-> limiter -> requestLogger"]
    WRAP --> FAN{"Multi-tenant<br/>query path?"}
    FAN -->|yes| MT["Fan out per tenant<br/>apply __tenant_id__ narrowing<br/>merge Loki-shaped responses"]
    FAN -->|no| ONE["Single-tenant request"]
    MT --> EDGE
    ONE --> EDGE["Tier0 compatibility-edge cache<br/>cacheable GET reads only"]
    EDGE -->|hit| RESP["Return cached Loki-shaped response"]
    EDGE -->|miss| CACHE["L1 memory -> optional L2 disk<br/>-> optional L3 peer cache"]
    CACHE -->|hit| RESP
    CACHE -->|miss| CO["Coalesce identical backend reads"]
    CO --> TR["Translate LogQL / selectors / metadata queries"]
    TR --> VL["VictoriaLogs"]
    VL --> SHAPE["Shape response<br/>streams / labels / stats / drilldown"]
    SHAPE --> STORE["Store cacheable result in Tier0 and deeper caches"]
    STORE --> RESP
```

### Tier0 Cache Guardrails

- Tier0 is a separate cache instance that reuses the same cache implementation, but not the same keyspace, as the deeper L1/L2/L3 caches.
- It runs only after tenant validation, auth checks, request logging setup, and route classification.
- It only serves cacheable `GET` read endpoints such as `query`, `query_range`, `series`, labels, volume, patterns, and Drilldown metadata.
- It never covers `/tail`, write/delete/admin paths, websocket upgrades, or non-JSON responses.
- Its memory budget is derived from `-cache-max-bytes` through `-compat-cache-max-percent`, defaulting to 10% and capped at 50%.
- Tenant-map and field-mapping reloads invalidate Tier0 immediately so label translation and metadata exposure changes cannot go stale.

## Tail Flow

```mermaid
flowchart TD
    WS["GET /loki/api/v1/tail"] --> WRAP["tenant validation + rate limit<br/>+ websocket upgrade checks"]
    WRAP --> ORIGIN{"Browser Origin allowed?"}
    ORIGIN -->|no| DENY["403"]
    ORIGIN -->|yes| MODE{"tail.mode"}
    MODE -->|native| NATIVE["Open native VL tail stream"]
    MODE -->|synthetic| SYN["Poll VL query API<br/>emit Loki tail frames"]
    MODE -->|auto| PREFLIGHT["Try native tail<br/>fallback on backend unavailable"]
    PREFLIGHT --> NATIVE
    PREFLIGHT --> SYN
    NATIVE --> FRAME["Write Loki websocket frames<br/>ping / close handling"]
    SYN --> FRAME
```

## Rules And Alerts Read Flow

```mermaid
flowchart LR
    REQ["/loki/api/v1/rules<br/>/api/prom/rules<br/>/loki/api/v1/alerts"] --> WRAP["tenant validation + logging"]
    WRAP --> BACKEND{"Configured read backend?"}
    BACKEND -->|no| EMPTY["Return empty Loki / Prom-compatible stub"]
    BACKEND -->|yes| PROXY["Forward read request with mapped tenant headers"]
    PROXY --> VMR["vmalert / ruler-compatible backend"]
    VMR --> SHAPE["Return legacy Loki YAML or Prometheus JSON view"]
    VMR -. optional recording-rule writes .-> VMTS["VictoriaMetrics (or compatible remote write sink)"]
```

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
        VM["VictoriaMetrics<br/>:8428 (recording-rule sink)"]
        GF["Grafana :3000<br/>Loki / Drilldown / tail datasources"]
    end

    INGEST -->|push| LOKI
    INGEST -->|push| VL
    COMPARE -->|GET /loki/api/v1/*| LOKI
    COMPARE -->|GET /loki/api/v1/*| VLP
    VLP --> VL
    VLP -. rules / alerts .-> VMA["vmalert"]
    VMA -. optional recording writes .-> VM
    COMPARE --> SCORE
    GF -.->|manual compare| LOKI
    GF -.->|manual compare| VLP
```

## Component Design

### Translator (`internal/translator/`)
Pure string manipulation parser — no external LogQL parser library. Converts LogQL to LogsQL left-to-right using prefix matching and regex for templates.

### Proxy (`internal/proxy/`)
HTTP handlers for Loki-compatible read endpoints. The main execution paths are:
- query and metadata handlers with tenant validation, optional fanout, translation, cache reuse, and response shaping
- `/tail` websocket handling with native and synthetic modes
- rules and alerts read-through compatibility against a configured backend such as `vmalert`

### Middleware (`internal/middleware/`)
- **Rate limiter**: per-client token bucket + global semaphore (current defaults are built in, not user-exposed flags)
- **Coalescer**: singleflight-based request deduplication
- **Circuit breaker**: 3-state (closed/open/half-open) with current built-in defaults

### Cache (`internal/cache/`)
Three-tier: L1 in-memory (sync.Map + atomic counters), optional L2 on-disk (bbolt with gzip compression), and optional L3 peer cache (consistent hash ring, `zstd`/`gzip` on larger peer transfers). Disk encryption is delegated to cloud provider (EBS, PD, etc.).

### Metrics (`internal/metrics/`)
Prometheus text exposition at `/metrics` plus OTLP push. Route-aware downstream and upstream request metrics, tenant/client breakdowns, cache and windowing metrics, peer-cache state, circuit-breaker state, and prefixed process/runtime health.
