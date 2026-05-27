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
        M["MCP / LLM / Agents"]
        C["CLI / API Consumers"]
    end

    subgraph Front["Loki-VL-proxy :3100"]
        API["Loki HTTP + WebSocket surface<br/>query, query_range, labels, series, detected_*,<br/>patterns, volume, tail, rules, alerts, delete"]
        SEC["Security headers<br/>auth forwarding, token redaction,<br/>request fingerprinting"]
        TENANT["Tenant validation<br/>X-Scope-OrgID mapping, isolation,<br/>multi-tenant fanout"]
        RL["Rate limiter<br/>per-client token bucket + global semaphore"]
        REQLOG["Request logger<br/>semconv JSON, Grafana client profiling,<br/>sample rate control"]
    end

    subgraph Edge["Tier0 Edge Cache"]
        T0["Compatibility-edge cache<br/>safe GET responses only<br/>10% of L1 budget, tenant-segregated"]
    end

    subgraph Translation["Query Translation"]
        CO["Coalescer<br/>singleflight dedup + normalization"]
        PARSE["LogQL parser<br/>recursive-descent typed AST<br/>semantic validation"]
        XLATE["Translator<br/>Tier 1: string ops (selectors, filters)<br/>Tier 2: AST-driven (stats, logsql builder)"]
        CAPS["VL Capabilities<br/>backend version gating"]
        LABEL["Label translator<br/>OTel dotted ↔ underscore<br/>custom field mappings<br/>hot-reload (SIGHUP)"]
    end

    subgraph Exec["Execution Paths"]
        subgraph LogQ["Log Queries"]
            WINDOW["Windowed query_range<br/>1h splits, batch retry,<br/>partial responses"]
            ADAPT["Adaptive parallelism<br/>EWMA latency + error tracking<br/>ramp up / back off"]
            PREFILT["Window prefilter<br/>hits estimation, skip empty"]
        end
        subgraph MetricQ["Metric Queries"]
            STATS["stats_query_range<br/>rate, count_over_time, topK"]
            BINARY["Binary metric ops<br/>sum(rate) / sum(rate)"]
            VECM["Vector matching<br/>on/ignoring, group_left/right"]
            SUBQ["Subquery expansion"]
        end
        subgraph StreamP["Stream Processing"]
            STREAM["VL → Loki streams<br/>label shaping, dedup, sorting"]
            DROPKEEP["Drop / keep extraction<br/>from parsed AST"]
            SMETA["Structured metadata<br/>2-tuple / 3-tuple mode"]
            POSTPROC["Post-processing<br/>limit enforcement, ordering"]
        end
        subgraph Meta["Metadata + Patterns"]
            LBLH["Label handlers<br/>labels, label values,<br/>series, detected fields"]
            LBLIDX["Label-values index<br/>hot subset, offset/limit,<br/>disk persist + peer warm"]
            PATTERN["Pattern mining<br/>Drain-like clustering<br/>autodetect from queries"]
            PATSNAP["Pattern persistence<br/>disk snapshot + peer sync<br/>cross-window merge"]
            DRILLDOWN["Drilldown metadata<br/>detected labels, field values<br/>service_name synthesis"]
            VOLUME["Volume / index stats<br/>hits estimation"]
        end
    end

    subgraph CacheTiers["Cache Tiers (L1 / L2 / L3)"]
        L1["L1 in-memory<br/>sync.Map + atomic counters"]
        L2["L2 disk (bbolt)<br/>gzip, survives restarts"]
        L3["L3 peer cache<br/>consistent hash ring<br/>zstd, write-through"]
        WARM["Cache warmer<br/>keep-warm loop + top-N<br/>startup jitter + peer-first"]
    end

    subgraph Tail["/tail WebSocket Path"]
        ORIGIN["Origin allowlist<br/>websocket upgrade checks"]
        MODE{"tail.mode"}
        NATIVE["Native VL tail stream"]
        SYN["Synthetic polling fallback"]
    end

    subgraph Alerts["Rules + Alerts Path"]
        RULER["Read bridge<br/>Loki YAML + Prometheus JSON"]
        LIMITS["Tenant limits publish<br/>drilldown-limits, /config/tenant"]
    end

    subgraph Cold["Cold Storage Routing"]
        COLD{"Query spans<br/>boundary?"}
        HOT["Hot only → VL"]
        COLDONLY["Cold only → Lakehouse"]
        MERGE["Both → merge at proxy"]
    end

    subgraph Backends["Upstream Systems"]
        VL["VictoriaLogs<br/>hot backend"]
        LH[("Victoria Lakehouse<br/>cold backend")]
        CB["Circuit breaker<br/>closed → open → half-open<br/>sliding window failures"]
        VMR["vmalert / ruler backend"]
        VMTS["VictoriaMetrics<br/>recording-rule sink"]
    end

    subgraph Obs["Observability"]
        PROM["100+ Prometheus metrics<br/>per-endpoint, per-tenant,<br/>per-client, cache, windowing"]
        OTLP["OTLP metrics push<br/>lightweight JSON, no SDK"]
        QT["Query tracker<br/>top-N freq, top-N latency,<br/>recent errors"]
        LOGS["Structured JSON logs<br/>route-aware, semconv"]
    end

    subgraph Infra["Infrastructure"]
        HEALTH["Health / ready / alive probes<br/>VL health + CB state"]
        BUILD["buildinfo version gate"]
        ADMIN["Admin endpoints<br/>pprof, cache flush,<br/>query analytics"]
        PEERE["Peer endpoints<br/>/_cache/* (get/set/hot/has/peers)"]
        CONNR["Connection rotation<br/>max-age, jitter, overload shed"]
    end

    G --> API
    M --> API
    C --> API

    API --> SEC --> TENANT --> RL --> REQLOG

    REQLOG --> T0
    T0 -->|hit| G
    T0 -->|miss| CO

    CO --> PARSE --> XLATE
    XLATE --> CAPS
    XLATE --> LABEL

    CAPS --> L1
    L1 -->|miss| L2
    L2 -->|miss| L3
    WARM --> L1
    WARM --> L3

    L3 -->|miss| COLD
    COLD -->|hot| HOT --> CB --> VL
    COLD -->|cold| COLDONLY --> LH
    COLD -->|overlap| MERGE
    MERGE --> VL
    MERGE --> LH

    VL --> STREAM
    VL --> WINDOW
    VL --> STATS
    LH --> STREAM

    WINDOW --> ADAPT
    WINDOW --> PREFILT
    STATS --> BINARY --> VECM
    STREAM --> DROPKEEP --> SMETA --> POSTPROC
    POSTPROC --> T0

    LBLH --> VL
    LBLIDX --> LBLH
    PATTERN --> PATSNAP
    DRILLDOWN --> VL
    VOLUME --> VL

    REQLOG --> ORIGIN --> MODE
    MODE -->|native| NATIVE --> VL
    MODE -->|synthetic| SYN --> VL

    REQLOG --> RULER --> VMR
    VMR -. recording writes .-> VMTS

    REQLOG --> LOGS
    CB --> PROM
    PROM --> OTLP
    POSTPROC --> QT

    HEALTH --> VL
    HEALTH --> CB
    PEERE --> L3
    CONNR --> API

    style Front fill:#1a1a2e,stroke:#e94560,color:#fff
    style Edge fill:#2d1a3e,stroke:#c084fc,color:#fff
    style Translation fill:#16213e,stroke:#4cc9f0,color:#fff
    style Exec fill:#0c1426,stroke:#818cf8,color:#fff
    style LogQ fill:#111827,stroke:#6366f1,color:#fff
    style MetricQ fill:#111827,stroke:#6366f1,color:#fff
    style StreamP fill:#111827,stroke:#6366f1,color:#fff
    style Meta fill:#111827,stroke:#6366f1,color:#fff
    style CacheTiers fill:#3b1c32,stroke:#f472b6,color:#fff
    style Tail fill:#0f3460,stroke:#90e0ef,color:#fff
    style Alerts fill:#1b4332,stroke:#52b788,color:#fff
    style Cold fill:#1c1917,stroke:#a8a29e,color:#fff
    style Backends fill:#052e16,stroke:#34d399,color:#fff
    style Obs fill:#422006,stroke:#f59e0b,color:#fff
    style Infra fill:#1c1917,stroke:#78716c,color:#fff
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
    CO --> TR["Translate LogQL\nlogql AST · logsql builder"]
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

### Cold Storage Routing

When `-cold-enabled=true` and `-cold-backend` is set, the proxy time-splits queries based on their time range:

| Query range | Route |
|---|---|
| Entirely within hot boundary | Hot backend (VictoriaLogs) only |
| Entirely beyond cold boundary | Cold backend (Victoria Lakehouse) only |
| Spans the boundary (overlap zone) | Both backends; results merged at proxy |

The boundary is configured via `-cold-boundary` (default `168h` = 7 days). The overlap window (`-cold-overlap`, default `1h`) ensures no gaps around the boundary by querying both backends in that zone.

For backward-direction queries spanning both backends, the proxy reads the cold response into memory to reverse it before merging — avoid very large backward time ranges when cold storage is enabled.

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

### LogQL Parser (`internal/logql/`)
Typed recursive-descent parser for LogQL. Produces a fully-typed AST (`Expr` interface with concrete node types: `*LogQuery`, `*RangeAggregation`, `*VectorAggregation`, `*BinOpExpr`, `*OpaqueMetricExpr`, …) that drives three subsystems:

- **Validation** — `ValidateLogQL(query)` returns Loki-compatible error strings for invalid queries before any work is done.
- **Routing** — `proxy.go` type-switches on the parsed AST to dispatch subqueries, binary metric expressions, and stream queries to separate execution paths (more reliable than regex-based marker injection).
- **Drop/Keep extraction** — `stream_processing.go` extracts `| drop`/`| keep` matchers from the AST for VL response post-processing.

The parser includes a semantic pass for structural constraints (missing `| unwrap` inside `rate_counter`, `__error__` inside `rate()`, malformed `ip()` filters, quantile phi bounds, line-format template validity). All error messages are formatted to match Loki 3.x exactly so Grafana clients receive the expected error shape.

See [LogQL Parser deep dive](logql-parser.md) for grammar, data flow diagrams, and extension points.

### Translator (`internal/translator/`)
LogQL→LogsQL converter. Receives canonical LogQL (produced by `Expr.String()` after AST normalisation). Translation uses two tiers: stable string operations for well-understood paths (stream selectors, line filters, label format) and typed `logsql` builder calls for complex paths (stats aggregations, IP filters). Remaining string paths are tagged `TODO(ast-migration)` for future migration — run `grep -r "TODO(ast-migration)" internal/` to see the full backlog.

### Translation Pipeline

```mermaid
flowchart LR
    subgraph Input["Input"]
        LQ["LogQL query string"]
    end

    subgraph Parse["Parse (internal/logql/)"]
        PARSE["ParseLogQuery() / ParseExpr()"]
        AST1["Typed LogQL AST\nLogQuery · Stage · LabelFilterStage\nRangeAggregation · BinOpExpr"]
    end

    subgraph Translate["Translate (internal/translator/)"]
        TR["TranslateLogQL() / TranslateMetricQuery()"]
        TIER1["Tier 1: Stable string paths\nstream selectors · line filters\nlabel format · logfmt/json parsers"]
        TIER2["Tier 2: AST-driven paths\nbuildStatsQuery → logsql.PipeStats\n(ip() filter: phase-2 roadmap)"]
    end

    subgraph Build["Build (internal/logsql/)"]
        BUILDER["Builder (version-aware)\nCapabilities struct"]
        LOGSQL["LogsQL query string\nready for VictoriaLogs"]
    end

    LQ --> PARSE --> AST1 --> TR
    TR --> TIER1 --> LOGSQL
    TR --> TIER2 --> BUILDER --> LOGSQL

    style Input fill:#1a1a2e,stroke:#e94560,color:#fff
    style Parse fill:#16213e,stroke:#4cc9f0,color:#fff
    style Translate fill:#0f3460,stroke:#90e0ef,color:#fff
    style Build fill:#1b4332,stroke:#52b788,color:#fff
```

### Proxy (`internal/proxy/`)
HTTP handlers for Loki-compatible read endpoints, split into domain-focused modules:

| Module | Responsibility |
|---|---|
| `proxy.go` | Proxy struct, configuration, constructor, and HTTP router setup |
| `middleware.go` | Request middleware chain: security headers, tenant validation, rate limiting, WebSocket upgrade checks |
| `middleware_security.go` | Security-specific middleware: auth forwarding, token redaction, request fingerprinting |
| `query_translation.go` | LogQL→LogsQL translation per request, structured request logging |
| `query_range_windowing.go` | Time-window splitting and stitching for long-range metric queries |
| `stream_processing.go` | VL→Loki stream conversion, label shaping, log line proxying |
| `postprocess.go` | Post-query response shaping: limit enforcement, deduplication, sorting |
| `multitenant.go` | Multi-tenant fanout, per-tenant narrowing, and Loki-shaped response merging |
| `label_handlers.go` | `/labels`, `/label/{name}/values`, `/series`, `/detected_fields` HTTP handlers |
| `label_metadata.go` | VL metadata fetching: field discovery, OTel attribute detection |
| `label_index.go` | In-process label-values index for low-latency label cardinality queries |
| `cache_keys.go` | Cache key construction: query_range, labels, series, detected_fields, volume |
| `patterns.go` | Pattern query handling, autodetection from query history, pattern clustering |
| `patterns_persistence.go` | Pattern snapshot persistence: load, save, and rotation |
| `metric_agg.go` | Instant-vector queries and post-aggregation metric math |
| `metric_binary.go` | Stats queries and binary metric expression evaluation |
| `volume.go` | `/loki/api/v1/index/volume` and `/index/volume_range` handlers |
| `drilldown.go` | Logs Drilldown plugin metadata endpoints |
| `tail.go` | WebSocket `/tail` with native VL stream and synthetic polling fallback |
| `alerting.go` | Health/readiness probes, rules and alerts read-through handlers |
| `backend.go` | Backend HTTP client construction, TLS config, compression negotiation |
| `telemetry.go` | Per-route Prometheus instrumentation, OTLP push, request duration histograms |
| `time_utils.go` | Timestamp parsing, range normalization, step alignment helpers |
| `http_utils.go` | HTTP error helpers, response header forwarding, Accept-Encoding negotiation |
| `subquery.go` | Subquery expansion and execution planning |
| `range_metric_compat.go` | Range metric compatibility shims for Loki 2.x vs 3.x divergences |
| `vector_matching.go` | Vector matching logic for binary metric operations |
| `unwrap_convert.go` | `unwrap` expression conversion between LogQL and LogsQL forms |
| `redact.go` | Token and credential redaction from logs and error messages |
| `safety.go` | Query safety checks: cardinality limits, expression complexity guards |

### Middleware (`internal/middleware/`)
- **Rate limiter**: per-client token bucket + global semaphore (current defaults are built in, not user-exposed flags)
- **Coalescer**: singleflight-based request deduplication
- **Circuit breaker**: 3-state (closed/open/half-open) with current built-in defaults

### Cache (`internal/cache/`)
Three-tier: L1 in-memory (sync.Map + atomic counters), optional L2 on-disk (bbolt with gzip compression), and optional L3 peer cache (consistent hash ring, `zstd`/`gzip` on larger peer transfers). Disk encryption is delegated to cloud provider (EBS, PD, etc.).

### Metrics (`internal/metrics/`)
Prometheus text exposition at `/metrics` plus OTLP push. Route-aware downstream and upstream request metrics, tenant/client breakdowns, cache and windowing metrics, peer-cache state, circuit-breaker state, and prefixed process/runtime health.
