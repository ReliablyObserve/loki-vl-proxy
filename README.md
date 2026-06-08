# Loki-VL-proxy

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="website/static/img/loki-vl-proxy-logo-white.jpg">
    <img src="website/static/img/loki-vl-proxy-logo-black.jpg" alt="Loki-VL-proxy marketing logo" width="340">
  </picture>
</p>

[![CI](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/ci.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ReliablyObserve/Loki-VL-proxy)](https://goreportcard.com/report/github.com/ReliablyObserve/Loki-VL-proxy)
[![Loki Compatibility](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-loki.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-loki.yaml)
[![Drilldown Compatibility](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-drilldown.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-drilldown.yaml)
[![VictoriaLogs Compatibility](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-vl.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/compat-vl.yaml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/ReliablyObserve/Loki-VL-proxy)](https://go.dev/)
[![Release](https://img.shields.io/github/v/release/ReliablyObserve/Loki-VL-proxy)](https://github.com/ReliablyObserve/Loki-VL-proxy/releases)
[![Docker Hub](https://img.shields.io/docker/pulls/reliablyobserve/loki-vl-proxy?label=Docker%20Hub%20pulls)](https://hub.docker.com/r/reliablyobserve/loki-vl-proxy)
[![GHCR Pulls](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/Loki-VL-proxy/badges/.github/badges/ghcr-pulls.json)](https://github.com/ReliablyObserve/Loki-VL-proxy/pkgs/container/loki-vl-proxy)
[![Helm Chart Pulls](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/Loki-VL-proxy/badges/.github/badges/ghcr-chart-pulls.json)](https://github.com/ReliablyObserve/Loki-VL-proxy/pkgs/container/charts%2Floki-vl-proxy)
[![Source Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/Loki-VL-proxy/badges/.github/badges/loc-code.json)](https://github.com/ReliablyObserve/Loki-VL-proxy)
[![Test Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/Loki-VL-proxy/badges/.github/badges/loc-tests.json)](https://github.com/ReliablyObserve/Loki-VL-proxy)
[![Tests](https://img.shields.io/badge/tests-4670%20passed-brightgreen)](#tests)
[![Coverage](https://img.shields.io/badge/coverage-84.9%25-green)](#tests)
[![LogQL Coverage](https://img.shields.io/badge/LogQL%20coverage-100%25-brightgreen)](#logql-compatibility)
[![License](https://img.shields.io/github/license/ReliablyObserve/Loki-VL-proxy)](LICENSE)
[![CodeQL](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/Loki-VL-proxy/actions/workflows/codeql.yaml)

📖 **Project site:** [reliablyobserve.github.io/Loki-VL-proxy](https://reliablyobserve.github.io/Loki-VL-proxy/) — full docs, benchmarks, runbooks, comparison matrix.

<details>
<summary><strong>How it works (TLDR)</strong></summary>

LogQL queries arrive → parsed into a typed AST (`internal/logql`) → translated into LogsQL via a typed builder (`internal/logsql`) → sent to VictoriaLogs.

Both parsers are hand-written recursive descent. The LogQL side handles the full Loki grammar. The LogsQL side uses a builder API that produces typed, syntactically valid LogsQL at construction time. Translation uses two tiers: stable string operations for well-understood paths (stream selectors, line filters), and typed AST construction for complex paths (stats aggregations, binary metric expressions, `PipeMath`/`PipeStats`/`PipeFilter` nodes).

**Label metadata — fast and complete:** first request returns a 1h VL scan immediately (sub-ms proxy overhead); a background goroutine fetches the full requested range (1h → 7d) so the second request has complete historical label data from cache. Disk-backed cache survives proxy restarts; a 90-second keep-warm loop ensures labels stay hot even with no user queries. Time-bucketed cache keys (5-min / 1h / 6h buckets by range) collapse dashboard refresh drift to the same entry. Label-value routing uses `stream_field_names` as an endpoint gate — only stream-indexed labels use `stream_field_values`; non-stream labels fall through to `field_values` so queries like `cluster` always return results.

**Fleet restart safety:** rolling restarts of N proxy pods don't hammer VL. Startup jitter (`-warmup-max-jitter`) spreads instances across a configurable window; a two-phase peer discovery protocol (`/_cache/has` for batch presence check → `/_cache/get` from the freshest peer) means only the first instance per label window hits VL — the rest pull from peers. For a 30-pod fleet this reduces warmup VL queries from 120 to ≤8 on restart.

</details>

**Keep your entire Loki stack — Grafana Explore, Drilldown, dashboards, API tooling — and run it on VictoriaLogs.**

- **Drop-in Loki API.** Point your existing Grafana Loki datasource at the proxy. Zero plugin changes, zero query rewrites.
- **Measured resource difference.** At 310 GiB/day ingest: VL + proxy runs on **1.4 cores and 6.1 GiB RAM**. Loki's published minimum for that ingest class: 38 cores, 59 GiB. That gap is real — not a benchmark artifact.
- **Proxy intelligence built in.** Disk-backed label cache with keep-warm loop, progressive full-range background fetch, time-bucketed keys, adaptive parallelism, circuit breaker, rate limits, tenant isolation. Fleet restart safety: jitter + peer-first warmup keeps rolling restarts from thundering VL. One ~14 MB static binary.

---

## One-glance comparison

Measured 2026-06-05 on the included e2e stack — 36 cold Drilldown queries (`pod` / `trace_id` / `k8s_pod_name` / `service_version` / `/detected_fields` / `/labels` × 30 m – 24 h ranges) against `namespace=prod` (~5 000 pods, ~50 000 trace_ids/h, 8 M entries over 7 days). Loki given 12 GiB.

| | Loki direct | Proxy (+VL backend) |
|---|---:|---:|
| Cold queries returning real data | **9 / 36** (25 %) | **35 / 36** (97 %) |
| Silent fails (`HTTP 200 + result:[]`) | **13 / 36** ← blank Grafana panel | 0 |
| HTTP 500 / 502 / timeout | 14 / 36 | 1 (known VL `trace_id` 6 h limit) |
| Total CPU consumed | **2 713 cpu·s** (~45 cpu·min) | **5 cpu·s** + VL 117 cpu·s |
| Peak RSS | **10 445 MiB** | **44 MiB** + VL 2 129 MiB |

Proxy + VL together: **~140 cpu·s, ~2.2 GiB peak** for **97 %** of the workload. Loki: **~520× more CPU, ~190× more peak RSS** to serve **fewer than a quarter** of the same queries — the silent-empty bucket is the most user-hostile mode (Grafana shows a blank panel with no signal).

Full per-query breakdown, side-by-side latency table, methodology, and the heap-pool deep-dive: **[docs/honest-tldr.md](docs/honest-tldr.md)**. Reproduce: `./bench/drilldown-vs-loki.sh`. Raw TSV + pprof: `test/e2e-compat/results/`.

---

## Query Performance

Measured head-to-head against tuned Loki: Apple M5 Pro (18 cores, 64 GB RAM), ~8 M log entries across 15 services, 7-day window.

### Production-realistic throughput (v1.50.0)

Measured with `loki-bench --jitter=2h --warmup=5s --duration=30s` — a realistic mix of cache hits and misses simulating Grafana dashboard refresh patterns. Not a 100% cache-hit ceiling; not a 100% cold floor. This is what you get in production.

| Workload | Concurrency | Loki | Proxy | VL native | Proxy / Loki |
|---|:---:|---:|---:|---:|:---:|
| Small (metadata) | c=10 | 919 req/s | 2,785 req/s | 3,756 req/s | **3.0×** |
| Small (metadata) | c=50 | 726 req/s | 2,099 req/s | 3,793 req/s | **2.9×** |
| Small (metadata) | c=100 | 578 req/s | 1,822 req/s | 3,538 req/s | **3.2×** |
| Heavy (pipelines) | c=10 | 146 req/s | 483 req/s | 2,532 req/s | **3.3×** |
| Heavy (pipelines) | c=50 | 247 req/s | 582 req/s | 2,000 req/s | **2.4×** |
| Heavy (pipelines) | c=100 | 279 req/s | 568 req/s | 1,952 req/s | **2.0×** |
| Compute (rate, topk) | c=10 | 625 req/s | 629 req/s | 3,947 req/s | **parity** |
| Compute (rate, topk) | c=50 | 343 req/s† | 539 req/s | 4,338 req/s | **1.6×** |
| Compute (rate, topk) | c=100 | 410 req/s† | 468 req/s | 4,473 req/s | **1.1×** |

- **Small metadata:** 2.9–3.2× faster — VL label/series scans outperform Loki's chunk store, amplified by proxy cache.
- **Heavy pipelines:** 2.0–3.3× faster — `stats_query_range` fast path + response caching.
- **Compute:** parity at c=10; proxy dominates under pressure because Loki saturates.

† Loki compute c=50: **90.55% error rate** (saturated). c=100: **97.49% error rate**. Proxy: 0% errors at all concurrency levels.

### Translation overhead

LogQL→LogsQL translation is **4.7–15.5 µs per query** (arm64). For a 100–500 ms VL round-trip this is under 0.01% of wall time.

| Query type | Time | Allocs |
|---|---:|---:|
| Selector `{app="nginx"}` | 4.7 µs | 18 |
| `rate({...}[5m])` | 6.1 µs | 45 |
| `sum(rate({...}[5m])) by (host)` | 8.9 µs | 48 |
| `ip("10.0.0.0/8")` filter (v1.45+, capability-aware) | 8.4 µs | 36 |
| Binary metric `sum(rate) / sum(rate)` | 12.5 µs | 76 |

Measured: `go test ./internal/translator/ -bench BenchmarkTranslate -benchmem -count=5` (Apple M5 Pro, Go 1.26, darwin/arm64).

### P50 latency

| Workload | Concurrency | Loki | Proxy | VL native |
|---|:---:|---:|---:|---:|
| Small | c=10 | 5 ms | 1 ms | 1 ms |
| Small | c=50 | 57 ms | 5 ms | 6 ms |
| Small | c=100 | 156 ms | 22 ms | 13 ms |
| Heavy | c=10 | 13 ms | 2 ms | 1 ms |
| Heavy | c=50 | 21 ms | 22 ms | 13 ms |
| Heavy | c=100 | 31 ms | 96 ms | 30 ms |
| Compute | c=10 | 2 ms | 2 ms | 1 ms |
| Compute | c=50 | 5 ms | 38 ms | 7 ms |
| Compute | c=100 | 10 ms† | 144 ms | 13 ms |

† Loki compute c=100 P50 is misleading — 97.49% errors, so only 2.51% of requests completed; survivors have low latency.

### Drilldown and label-browser latency

| Path | Before | After (cold) | After (warm) |
|---|---:|---:|---:|
| `service_name/values` | ~5,000ms | **235ms** | **12ms** |
| Background label refresh (6–24h) | 5–10s per call | **<100ms** | — |

The proxy previously called `stream_field_names` — an O(data-volume) scan — for every service-name lookup and every background label refresh. Wide Grafana time windows (6h/24h) produced 5–10s backend calls, appearing as CPU spikes in Grafana. Now uses `field_names` (O(index), ~30ms at any range) for all non-strict paths. The synchronous `/loki/api/v1/labels` endpoint keeps `stream_field_names` (1h-capped) for strict Loki label-only semantics.

### Dashboard load spikes — request coalescer

When many panels hit the same query simultaneously, the proxy collapses them into a single backend call. First-hit coalescing avoids the N-fan-out but pays one backend round-trip; subsequent hits are served from cache.

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
| `loki-vl-proxy` | ~0.1–0.2 | ~0.15–0.26 GiB |
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

**Peer fleet discovery** — four modes, all refresh every 15 s and rebuild the hash ring live:

| Mode | Flag | Best for |
|------|------|----------|
| `dns` | `-peer-dns=proxy-headless.ns.svc.cluster.local` | Kubernetes headless service — only ready pods appear |
| `srv` | `-peer-srv=_loki-vl-proxy._tcp.proxy-headless.ns.svc.cluster.local` | Kubernetes StatefulSet, Consul DNS — port embedded in record |
| `http` | `-peer-http-url=http://consul:8500/v1/health/service/loki-vl-proxy?passing=true` | Outside k8s: Consul, Nomad, Prometheus HTTP SD, or custom endpoint |
| `static` | `-peer-static=10.0.0.1:3100,10.0.0.2:3100` | Fixed fleets, development |

Verify the live ring at any time: `curl http://proxy:3100/_cache/peers` → `{"peers":[...],"self":"...","count":N}`.

Non-Kubernetes examples (static, Consul, Prometheus SD, CoreDNS) are in [`examples/peers/`](examples/peers/).

---

## Why It's Fast

**4-tier cache:**
- **Tier0** — compatibility-edge cache for safe GET responses (no backend hit at all)
- **L1** — in-memory hot path
- **L2** — disk (bbolt), survives restarts, warms historical windows across large working sets
- **L3** — peer cache, lets warm fleet replicas share results instead of all hitting the backend. Four peer discovery modes: `dns` (k8s headless A-records), `srv` (DNS SRV with embedded port, works with Consul DNS and k8s StatefulSets), `http` (polls any JSON endpoint — Consul catalog, Prometheus HTTP SD, custom registry), `static` (fixed list). Discovery refreshes every 15 s; the hash ring updates atomically so peer add/remove is live without restarts.

**Window reuse.** Long `query_range` requests are split into 1h windows. Historical windows are served from cache; only the live edge fetches from VictoriaLogs. A 7-day query with warm cache may hit the backend for a single window.

**Adaptive parallelism.** Parallel window fetches use EWMA-based backpressure — ramps up when VictoriaLogs is fast, backs off automatically before it becomes a problem.

**Request coalescing.** Concurrent identical queries collapse into one upstream request.

**Lock-free hot paths.** Circuit breaker, metrics histograms, and rate limiter use atomic operations instead of mutexes. Structured logging uses an async buffered handler. At 100+ concurrent requests, these eliminate the contention that dominates proxy-added latency.

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

- **Circuit breaker** — opens on backend failure, closes automatically on recovery; lock-free fast path in healthy state
- **Per-client rate limits** — token bucket per tenant with sharded locks; no convoy effects at high tenant count
- **Adaptive log sampling** — below 10 req/s logs everything; above it, OK traffic becomes periodic summaries while errors are always logged
- **Tenant isolation** — strict `X-Scope-OrgID` fanout guardrails; no cross-tenant data bleed
- **TLS / mTLS** — configurable on both northbound (client) and southbound (backend) boundaries
- **OTLP push** — proxy emits its own traces to any OTLP endpoint
- **Operator dashboard** — packaged Grafana dashboard covering Client → Proxy → VictoriaLogs, cache behavior, fanout, and resource utilization
- **Runbook-backed alerts** — 13 alert rules, each with a linked runbook
- **100+ Prometheus metrics** — all under `loki_vl_proxy_*` prefix
- **Read-only by default** — `/push` blocked, delete gated, debug/admin disabled unless explicitly enabled
- **Cold storage routing** — time-boundary split to Victoria Lakehouse for long-range queries
- **Query-length enforcement** — per-tenant max query time range via `-default-max-query-length` flag; per-tenant override via limits config

---

## How It Works

```mermaid
flowchart LR
    G["Grafana / API tools<br/><sub>unchanged Loki HTTP datasource</sub>"]

    subgraph Proxy["Loki-VL-proxy &nbsp;·&nbsp; one ~14 MB Go binary"]
        direction TB
        API["Loki API surface<br/><sub>query · labels · series · tail<br/>detected_fields · patterns · volume</sub>"]
        SMART["<b>Smart layer</b> — the strange stuff that makes this work<br/><sub>• disk-backed label cache + 90 s keep-warm loop<br/>• progressive 1 h → 7 d background backfill<br/>• time-bucketed cache keys (collapse refresh drift)<br/>• fleet jitter + peer-first warmup (≤8 VL hits on 30-pod restart)<br/>• circuit breaker · rate limits · per-tenant isolation</sub>"]
        XLATE["LogQL → LogsQL translator<br/><sub>typed AST, version-gated, hot-reload labels</sub>"]
    end

    VL[("VictoriaLogs")]
    VMA[/"vmalert · alerts/rules<br/>(optional bridge)"/]

    G -->|"Loki API, zero changes"| API
    API --> SMART --> XLATE --> VL
    SMART -. "rules surface" .-> VMA

    classDef client fill:#1f2937,stroke:#93c5fd,color:#f3f4f6,stroke-width:2px;
    classDef proxy fill:#172554,stroke:#38bdf8,color:#f0f9ff,stroke-width:2px;
    classDef smart fill:#3f1d2e,stroke:#f472b6,color:#fdf2f8,stroke-width:2px;
    classDef backend fill:#052e16,stroke:#34d399,color:#ecfdf5,stroke-width:2px;
    classDef opt fill:#1c1917,stroke:#a8a29e,color:#f5f5f4,stroke-width:1px,stroke-dasharray:4 4;

    class G client;
    class API,XLATE proxy;
    class SMART smart;
    class VL backend;
    class VMA opt;
```

**What this picture says:** Grafana keeps talking Loki. The proxy translates LogQL to LogsQL on the way down, and the **Smart layer** is where the work happens — every entry in that pink box is what lets a 14 MB binary serve cold Drilldown queries that the official Loki layout can't run at all on 12 GiB RAM. The full per-box exploded view (10 subsystems, every handler, every cache tier, every backend) lives in [`docs/architecture.md`](docs/architecture.md) for operators and contributors.

---

## Compatibility

Loki-VL-proxy is validated continuously in CI against three separate tracks: Loki API, Grafana Logs Drilldown, and VictoriaLogs integration.

### Label and Field Compatibility

| Profile | Stream labels (`/labels`) | Detected fields / metadata | Best for |
|---|---|---|---|
| Loki-compatible (default) | underscore-only | translated underscore aliases | strict Loki UX, Grafana Explore/Drilldown |
| Mixed | underscore-only | dotted + translated aliases | Grafana + OTel correlation |
| Native-field | underscore-only (`label-style=underscores`) | dotted-native only | VL/OTel-native field workflows |

Default flags: `-label-style=underscores`, `-metadata-field-mode=translated`. Grafana query builder works best with underscore aliases; code mode accepts dotted expressions and translates them to VL-native field matching.

**Tuple safety:** Default responses return strict `[timestamp, line]` 2-tuples. 3-tuple metadata mode activates only when the client sends `X-Loki-Response-Encoding-Flags: categorize-labels`. Cache keys are segregated by tuple mode.

### LogQL Compatibility

Stream selectors, filters, parser pipelines, metric queries, range functions, scalar bool comparisons, vector set operators, and invalid LogQL error forms are all covered and machine-validated in CI against a real Loki oracle. The suite spans 316 exhaustive LogQL parity test cases with machine-validated compatibility scores.

**Typed LogQL parser:** The proxy includes a fully typed recursive-descent LogQL parser (`internal/logql`) that produces a structured AST for query validation, structural routing, and drop/keep extraction — replacing the previous regex-based approach. The parser enforces Loki-compatible semantic constraints (missing `| unwrap` in `rate_counter`, invalid `ip()` filter addresses, unclosed template delimiters, etc.) and generates the exact error messages Loki 3.x returns, so Grafana datasource clients receive the expected error shape.

For full detail: [Loki Compatibility](docs/compatibility-loki.md), [Translation Reference](docs/translation-reference.md), [LogQL Parser](docs/logql-parser.md), [Known Issues](docs/KNOWN_ISSUES.md)

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
