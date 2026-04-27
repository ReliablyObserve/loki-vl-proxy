---
sidebar_label: Comparison Matrix
description: Source-backed comparison matrix for Loki, VictoriaLogs, and Loki-VL-proxy across indexing, search behavior, high-cardinality handling, deployment shape, read path performance, and resource usage.
---

# Comparison Matrix

This page is the behavior, architecture, and read-path performance matrix.

Use it to answer:

- how Loki and VictoriaLogs differ at the data-model and search layer
- what Loki-VL-proxy adds on the Grafana-compatible read path
- where the operational tradeoffs and resource costs really sit

For the separate AWS-style cost worksheet and scenario model, see
[Cost Model](cost-model.md).

## Comparison Matrix

| Dimension | Grafana Loki | VictoriaLogs | VictoriaLogs via Loki-VL-proxy |
|---|---|---|---|
| Indexing model | Loki docs: line content is not indexed; labels index streams. | VictoriaLogs docs: all fields are indexed and full-text search runs across all fields. | Grafana still talks Loki, while the backend keeps the VictoriaLogs field model. |
| High-cardinality posture | Loki docs: labels should stay low-cardinality; high-cardinality labels reduce performance and cost-effectiveness. | VictoriaLogs docs: high-cardinality values such as `trace_id`, `user_id`, and `ip` work as fields unless promoted to stream fields. | The proxy can keep Loki-safe label surfaces while VictoriaLogs keeps richer field semantics underneath. |
| Search-heavy broad scans | Broad searches can become stream selection plus line filtering because line content is not indexed. | VictoriaLogs publishes fast full-text search as a core capability. | The proxy additionally suppresses repeated scans with Tier0, L1/L2/L3, and long-range window cache; prefilters via `/select/logsql/hits` to skip empty windows before issuing log queries. |
| Compression and stored-byte shape | Loki docs say chunk blocks and structured metadata are stored compressed, but the retained footprint still includes label-driven index structures and chunk/object-store layout. | VictoriaLogs docs say logs usually compress by `10x` or more, and some deployments observe `50-60x` on the data-block compression ratio metric excluding `indexdb`. | The proxy adds compressed read-path hops under its control: `gzip` client responses, `zstd`/`gzip` peer-cache transfers, gzip disk-cache values, and negotiated upstream compression with safe decode. |
| Cross-AZ traffic posture | Loki docs say distributors forward writes to a replication factor that is generally `3`, queriers query all ingesters for in-memory data, and the zone-aware replication design explicitly lists minimizing cross-zone traffic costs as a non-goal. | VictoriaLogs cluster docs support independent clusters in separate availability zones and advanced multi-level cluster setup, which allows local reads to stay local unless operators intentionally fan out globally. | The proxy can stay AZ-local on the read path and adds `zstd`/`gzip` compression on the hops it controls, but it does not invent backend replication economics that the VictoriaLogs docs do not quantify. |
| Published large-workload sizing | Grafana's own sizing guide reaches `431 vCPU / 857 Gi` at `3-30 TB/day` and `1221 vCPU / 2235 Gi` at about `30 TB/day` before query spikes. | VictoriaLogs docs do not publish an equivalent distributed throughput tier matrix in the same form; the safer claim is lower RAM/disk posture plus operator reports and backend compression behavior. | The proxy keeps the read-side tax small: one `~14 MB` static Go binary, memory footprint bounded by `-cache-max-bytes` (default `256 MB`), and `~15-30 ms` added latency on cold misses while warm cache hits add near-zero overhead. |
| Deployment shape | Single-binary mode exists, but scalable Loki is documented as a microservices-based system with multiple components. | VictoriaLogs docs position the backend as a simple single executable for the easy path, but they also document cluster mode with `vlinsert`, `vlselect`, `vlstorage`, replication, multi-level cluster setup, and HA patterns across independent availability zones. | Adds one small read-side compatibility layer with explicit route-aware telemetry, and can front either single-node or clustered VictoriaLogs. |
| Grafana integration | Native Loki datasource and native Loki UX. | Native path is VictoriaLogs plugin or direct VictoriaLogs APIs. | Keeps Grafana on the native Loki datasource, Explore, and Drilldown workflows. |
| Read-path caching levers | Loki-specific caches and query path tuning. | Backend-native behavior only. | Adds Tier0 compatibility cache, L1 in-memory cache (256 MB default), optional L2 disk cache (bbolt), optional L3 peer fleet cache, and query-range window cache (historical windows cached 24h). |
| Query range execution | Loki query frontend splits and shards range queries across parallel queriers. | VictoriaLogs executes range queries internally with no external splitting layer. | Proxy implements query-range windowing: splits long time ranges into 1h windows, issues them with EWMA-based adaptive parallelism (2–8 parallel windows), and caches completed historical windows for 24h. |
| Empty-range elimination | Not applicable from client side; Loki internals decide whether to scan. | Not applicable from client side. | Proxy prefilters via VictoriaLogs `/select/logsql/hits` index stats endpoint before issuing log queries: time windows with zero estimated hits are skipped entirely, eliminating unnecessary VL scans. |
| Inflight deduplication | Not applicable; Loki handles internally. | Not applicable. | Request coalescing collapses duplicate inflight requests from concurrent Grafana panels into one backend call. |
| Visibility into compatibility work | No separate compatibility layer exists to observe. | No Loki-compatibility layer exists to observe. | Splits downstream latency, upstream latency, cache efficiency, and proxy overhead by route. |

## What Official Docs Explicitly Support

### Loki

Official Loki docs state the following:

- labels are intended for low-cardinality values
- the content of each log line is not indexed
- high-cardinality labels build a huge index, flush many tiny chunks, and reduce
  performance and cost-effectiveness
- scalable Loki is microservices-based and query-frontend driven
- zone-aware replication design explicitly lists minimizing cross-zone traffic
  costs as a non-goal
- replication factor is generally `3`, meaning `3x` cross-zone writes in
  replicated setups

Sources:

- [Loki labels docs](https://grafana.com/docs/loki/latest/get-started/labels/)
- [Loki architecture docs](https://grafana.com/docs/loki/latest/get-started/architecture/)
- [Loki zone-aware replication docs](https://grafana.com/docs/loki/latest/operations/multi-zone-replication/)

### VictoriaLogs

Official VictoriaLogs docs state the following:

- all fields are indexed
- full-text search works across all fields
- high-cardinality values work fine as ordinary fields unless they are promoted
  to stream fields
- the easy path is a single zero-config executable
- cluster mode is also documented with `vlinsert`, `vlselect`, `vlstorage`,
  replication, and multi-level cluster setup
- HA guidance explicitly discusses sending copies of logs to independent
  VictoriaLogs instances or clusters in separate availability zones
- VictoriaLogs publishes up to `30x` less RAM and up to `15x` less disk than
  Loki or Elasticsearch

Sources:

- [VictoriaLogs overview](https://docs.victoriametrics.com/victorialogs/)
- [VictoriaLogs cluster docs](https://docs.victoriametrics.com/victorialogs/cluster/)
- [VictoriaLogs key concepts](https://docs.victoriametrics.com/victorialogs/keyconcepts/)
- [VictoriaLogs LogsQL docs](https://docs.victoriametrics.com/victorialogs/logsql/)

## Published Measurements Worth Citing Carefully

These are useful signals, but they are not universal truths for every
deployment.

### VictoriaLogs-published claim

VictoriaLogs publishes:

- up to `30x` less RAM
- up to `15x` less disk

That is a vendor claim from VictoriaMetrics documentation. Treat it as a
directional signal, not as a guaranteed result in every environment.

### TrueFoundry case study

TrueFoundry published a `500 GB / 7 day` side-by-side evaluation and reported:

- `≈40%` less storage
- materially lower CPU and RAM than Loki
- faster broad-search results on its workload

That is stronger than a vendor slogan because it is an operator report, but it
is still one workload profile.

### Why VictoriaLogs `50-60x` and Loki side-by-side deltas are different numbers

These numbers answer different questions:

- the VictoriaLogs compression-ratio metric is about original raw data versus
  compressed **data blocks**
- it explicitly excludes `indexdb`
- public Loki-versus-VictoriaLogs case studies such as TrueFoundry are
  comparing **full retained system footprint** on the same workload

That is why a real VictoriaLogs deployment can show `50-60x` on data blocks,
while the cross-system retained-byte delta versus Loki may still land closer to
`≈40%` on one operator's workload. They are not contradictory; they are
measuring different layers of the storage bill.

Source:

- [TrueFoundry benchmark report](https://www.truefoundry.com/blog/victorialogs-vs-loki)

## Resource Usage: Loki Microservices vs VictoriaLogs + Proxy

This section captures the concrete resource shape differences, grounded in
published sizing guides and operator reports.

### Loki at scale: published compute floors

Grafana publishes a Loki sizing guide with base requests such as:

| Ingest rate | vCPU | RAM |
|---|---:|---:|
| `<3 TB/day` | `38` | `59 Gi` |
| `3-30 TB/day` | `431` | `857 Gi` |
| `~30 TB/day` | `1221` | `2235 Gi` |

Those are **base** requests before query spikes, storage, object-store
requests, and inter-component traffic.

Key structural reasons those numbers are large:

- Loki microservices mode runs: distributor, ingester, querier,
  query-frontend, query-scheduler, compactor, ruler, store-gateway, and
  index-gateway as separate components, each with its own replica set and
  resource allocation.
- Ingesters hold chunks in memory until they are flushed to object store;
  memory headroom must cover the active chunk window.
- Replication factor `3` means every write touches three ingesters
  cross-zone, and queriers query all ingesters for in-memory data.
- High-cardinality labels break the indexing model: each unique label
  combination is a separate stream, so label explosion forces more, smaller
  chunks and a proportionally larger index structure.

### VictoriaLogs + proxy: component resource shapes

VictoriaLogs in standalone mode deploys as a single binary. There are no
mandatory microservices components. Cluster mode is available when needed.

| Component | Typical size |
|---|---|
| VictoriaLogs binary | Single executable, no external chunk store required |
| Proxy binary | `~14 MB` static Go binary |
| Proxy memory | Bounded by `-cache-max-bytes` flag, default `256 MB` |
| Proxy CPU | Minimal on cache hits; bounded translation work on misses |

VictoriaLogs docs claim up to `30x` less RAM and `15x` less disk versus Loki.
The TrueFoundry operator report at `500 GB / 7 day` observed approximately
`40%` less storage and materially lower CPU and RAM. Neither number is
universal, but the direction is consistent.

The all-fields indexing model means high-cardinality values (trace IDs,
request IDs, user IDs, IP addresses) do not create stream explosion: they
remain ordinary indexed fields instead of becoming label-cardinality problems
that force smaller chunks and index bloat.

### VL + Proxy Combined Envelope (Reference Estimates)

:::caution Not a 1:1 controlled comparison
The table below mixes three independent data sources that were **not measured
simultaneously against the same workload**. Treat it as a directional reference,
not a benchmark result. Run `loki-bench` (see below) to get real 1:1 numbers
from your own environment.
:::

**Source 1 — VL service envelope** (real, from [Cost Model](cost-model.md)):
A production VictoriaLogs deployment running `310 GiB/day` raw ingest,
`800 M` total entries, `54.9x` compression. Process snapshot **at 0 rps read
traffic**, so `vlselect` reflects only the write-path floor.

| Component | Cores | Memory | Source |
|---|---:|---:|---|
| `vlstorage` | `1.0` | `5.0 GiB` | measured |
| `vlinsert` | `0.1` | `0.6 GiB` | measured |
| `vlselect` (0 read rps) | `0.1` | `0.25 GiB` | measured — floor only |
| **VL total** | **`1.2`** | **`5.85 GiB`** | |

Under active read load `vlselect` grows. Use `loki-bench` to measure the actual
delta in your workload.

**Source 2 — Proxy overhead** (estimated from bench tool, not co-located with Source 1):

| Scenario | Proxy Cores | Proxy Memory |
|---|---|---|
| cache warm, idle | `< 0.01` | `~30–50 MB` |
| 50 concurrent clients | `~0.05` | `~100–150 MB` |
| 500 concurrent clients | `~0.1–0.2` | up to `256 MB` (default L1 cap) |

**Source 3 — Loki floor** (published spec, not measured):
Grafana's published compute floor for a scalable Loki deployment handling
`<3 TB/day` is `38 vCPU / 59 GiB`. This is a *minimum recommended* spec for a
multi-component cluster, not a measured P50 operating point.

**Estimated combined ratio** (directional only):

| Layer | Cores | Memory |
|---|---:|---:|
| VictoriaLogs service (measured, 0 read rps) | `1.2` | `5.85 GiB` |
| loki-vl-proxy (read load, default config) | `~0.1–0.2` | `~0.15–0.26 GiB` |
| **Combined VL + proxy** | **`~1.4`** | **`~6.1 GiB`** |
| **Loki published floor (`<3 TB/day`)** | **`38`** | **`59 GiB`** |
| **Estimated ratio (Loki / VL + proxy)** | **`~27x`** | **`~9.7x`** |

These ratios will differ in your environment because: (a) VL measured at 0 read
rps; (b) Loki's `38 vCPU` is a sizing floor, not a steady-state P50; (c) proxy
overhead scales with workload shape and cache hit rate.

### Getting Real 1:1 Numbers

`loki-bench` runs Loki, VL+proxy, and VL native (LogsQL) against the same
workload simultaneously and reports resource deltas from each service's
`/metrics` endpoint:

```bash
# Start the e2e stack with both Loki and VL
cd test/e2e-compat && docker compose up -d

# Seed realistic data (3 days, dense)
go run ./bench/cmd/seed/ \
  --loki=http://localhost:3101 \
  --vl=http://localhost:9428 \
  --days=3 --lines-per-batch=200

# Run 3-way comparison
./bench/run-comparison.sh \
  --workloads=small,heavy,long_range \
  --clients=10,50,100,500 \
  --duration=60s
```

With `--vl-direct` (auto-detected when VL is reachable), the report adds a
**VL native (LogsQL)** column showing raw VL throughput and resource use
alongside VL+proxy — so you can isolate exactly what the translation and
caching layer costs.

### Loki replication and cross-zone write costs

The Loki zone-aware replication documentation explicitly states that
minimizing cross-zone traffic costs is a non-goal. With replication factor `3`
as the standard configuration:

- every ingested log line generates `3x` write fan-out
- queriers retrieve in-memory data from all ingesters across zones on every
  query (until data is flushed and compacted to object store)

VictoriaLogs can deploy as AZ-local instances. Global fan-out only happens
when operators explicitly configure multi-level cluster or HA replication.

The proxy stays AZ-local on the read path and adds `zstd`/`gzip` compression
on the hops it controls, reducing network transfer on peer-cache exchanges.

## Read Path Performance: How the Proxy Improves Query Behavior

The proxy does not improve VictoriaLogs backend efficiency directly. It adds
a structured read-side layer that changes how many backend calls happen, how
expensive each one is, and how much repeated Grafana dashboard traffic reaches
VictoriaLogs at all.

### 4-tier cache architecture

The proxy implements four cache tiers:

| Tier | Scope | Latency when hit |
|---|---|---|
| T0 (compatibility-edge) | Exact-match compatibility responses | Sub-microsecond |
| L1 (in-memory) | Hot working set, default `256 MB`, configurable | Sub-microsecond |
| L2 (disk, optional) | Persistent cache via bbolt, survives restarts | Low milliseconds |
| L3 (peer fleet, optional) | Distributed warm results across proxy replicas | Tens of milliseconds |

Project benchmarks (from [Benchmarks](benchmarks.md) and [Performance](performance.md)):

- `query_range` warm L1 hits: `0.64–0.67 µs` versus `4.58 ms` on the cold
  delayed path
- `detected_field_values` warm Tier0 hits: `0.71 µs` versus `2.76 ms` without
  Tier0
- peer-cache warm shadow-copy hits: `52 ns`

These are proxy read-path benchmarks on the project's own cache
implementation, not a native Loki versus VictoriaLogs lab comparison.

### Query-range windowing and historical window cache

For time-series range queries (`/loki/api/v1/query_range`), the proxy splits
long time ranges into `1h` windows instead of issuing one large backend
request. Each window is issued and cached independently.

Historical windows (fully elapsed `1h` windows where the data cannot change)
are cached for `24h`. This means:

- the first load of a `7d` dashboard range issues up to `168` backend window
  fetches (more at startup, but collapsed by coalescing)
- subsequent loads of the same dashboard re-play from cache for most windows
  and only fetch the most recent live window from VictoriaLogs
- this directly reduces VictoriaLogs scan work on repeated dashboard opens,
  time-range shifts, and panel refreshes

### Adaptive parallelism via EWMA

The proxy does not issue all split windows simultaneously. It uses an
EWMA-based adaptive parallelism controller that adjusts concurrent backend
window fetches between `2` and `8` based on observed VictoriaLogs response
behavior. When VictoriaLogs is under load, the controller reduces parallelism
to avoid additional overload. When VictoriaLogs responds quickly, it increases
concurrency to minimize total query latency.

### Prefilter via `/select/logsql/hits` index stats

Before issuing a log query for any time window, the proxy can consult
VictoriaLogs' `/select/logsql/hits` endpoint to check whether the target
stream and filter combination has any matching documents in that window.

Empty windows — periods where no logs matching the query exist — are skipped
without issuing a full log-data fetch. The project's own benchmark shape shows
this prefilter cutting backend query calls by approximately `81.6%` on
workloads with sparse data coverage across the queried range.

### Request coalescing

When multiple Grafana panels issue identical or near-identical queries
simultaneously (common on dashboard load), the proxy collapses them into one
backend call. Additional identical inflight requests wait for the first call to
complete and then receive the same result. This prevents N-panel dashboard
loads from generating N simultaneous identical VictoriaLogs queries.

### Proxy overhead on the read path

| Condition | Added latency |
|---|---|
| Cache hit (L1 / Tier0) | Near-zero (sub-microsecond memory lookup) |
| Cache miss (cold, first fetch) | `~15–30 ms` added to translation + VL roundtrip |
| Peer-cache hit (L3) | `~tens of ms` (one extra network hop with compressed payload) |

The proxy binary itself is `~14 MB`. Memory footprint is bounded by
`-cache-max-bytes` (default `256 MB` for L1); L2 disk cache size is
separately configurable. Proxy CPU is minimal on cache hits and bounded
translation work (LogQL → LogsQL conversion) on misses.

## Why The Large-Workload Loki Floor Changes The Cost Discussion

It is common to compare log systems with small-node anecdotes and miss what
happens at larger distributed ingest rates.

Grafana already publishes a Loki sizing guide with base requests such as:

- `<3 TB/day`: `38 vCPU / 59 Gi`
- `3-30 TB/day`: `431 vCPU / 857 Gi`
- `~30 TB/day`: `1221 vCPU / 2235 Gi`

Those published requests matter because they put a concrete floor under the
cost discussion before:

- query spikes
- storage
- object-store requests
- inter-component traffic

This project's [Cost Model](cost-model.md) converts those published Loki tiers
into simple on-demand EC2 floors so the comparison is not just a qualitative
claim about "lighter architecture". Those AWS rows are calculation scaffolding
to show explicit dollar figures, not observed billing from a production cloud
account.

## What Loki-VL-proxy Adds To The Read-Path Story

The proxy does not create VictoriaLogs backend efficiency. It adds three other
things that materially affect read-side cost and operator control.

### 1. Repeated-read suppression

The proxy ships multiple cache layers:

- Tier0 compatibility-edge cache
- L1 in-memory cache (default `256 MB`)
- optional L2 disk cache (bbolt, persistent across restarts)
- optional L3 peer cache (fleet-wide warm result sharing)
- query-range split-window cache (historical `1h` windows cached for `24h`)

This matters because Grafana traffic is usually repetitive. Dashboards,
Explore, and Drilldown often re-issue the same reads across the same routes and
time windows. If those repeated reads hit cache, VictoriaLogs work per user
action goes down.

### 2. Route-aware bottleneck isolation

The proxy exports:

- `loki_vl_proxy_requests_total`
- `loki_vl_proxy_request_duration_seconds`
- `loki_vl_proxy_backend_duration_seconds`
- `loki_vl_proxy_cache_hits_by_endpoint`
- `loki_vl_proxy_cache_misses_by_endpoint`

Those metrics are labeled by:

- `system`
- `direction`
- `endpoint`
- `route`
- `status`

This lets operators tell the difference between:

- client-visible latency
- proxy-added overhead
- upstream VictoriaLogs slowness
- cache collapse on a specific route

That reduces tuning time and makes cost regressions visible earlier.

### 3. Migration cost control

Keeping Grafana on the native Loki datasource means teams do not need to change
every dashboard and user workflow just to evaluate VictoriaLogs.

That does not show up in CPU charts, but it is still real engineering cost.

## Current Project Benchmark Signals

The project's own performance docs and benchmarks currently publish:

- `query_range` warm hits at `0.64-0.67 µs` versus `4.58 ms` on the cold
  delayed path
- `detected_field_values` warm hits at `0.71 µs` versus `2.76 ms` without
  Tier0
- peer-cache warm shadow-copy hits at `52 ns`
- long-range prefiltering cutting backend query calls by about `81.6%` on the
  published benchmark shape

These are **project read-path benchmarks**, not a native Loki versus
VictoriaLogs lab comparison.

Related docs:

- [Performance](performance.md)
- [Benchmarks](benchmarks.md)
- [Fleet Cache](fleet-cache.md)
- [Observability](observability.md)

## Where The Combination Is Usually Strongest

| Workload pattern | Why `VictoriaLogs + Loki-VL-proxy` is attractive |
|---|---|
| Broad text or phrase search | VictoriaLogs field indexing and full-text search are better aligned with this pattern than label-only indexing. |
| High-cardinality fields that should remain queryable | VictoriaLogs tolerates these better as fields, while the proxy keeps Grafana-facing labels conservative. |
| Repeated Grafana reads | Proxy caches can suppress repeated backend work aggressively; historical window cache eliminates most re-fetches on dashboard reload. |
| Sparse data over long time ranges | Prefilter via `/select/logsql/hits` skips empty windows without a full log scan; benchmark shows ~81.6% backend call reduction on sparse shapes. |
| Migration from Loki to VictoriaLogs | Grafana can keep the native Loki datasource while the backend changes behind the proxy. |
| Multi-replica read fleets | Peer cache lets one warm pod reduce repeated backend fetches across the fleet. |
| High-cardinality scale (avoiding label explosion) | VictoriaLogs all-fields indexing does not create stream explosion from high-cardinality label values, avoiding the Loki chunk fragmentation and index bloat that drives large compute requirements at scale. |

## Where The Argument Is Weaker

| Situation | Why |
|---|---|
| You need native Loki writes through the same endpoint | This project is read-focused; normal Loki push remains blocked. |
| Mostly narrow label-first query workloads that already run well on Loki | The benefit of all-field indexing and compatibility caching may be smaller. |
| You want zero extra read components in the path | Native Loki is simpler because there is no compatibility layer at all. |
| You want universal numeric savings promises | No honest doc can guarantee one fixed ratio across every cluster. |

## How To Validate Savings In Another Environment

Use these signals before claiming success:

### Route-aware request and backend behavior

```promql
sum(rate(loki_vl_proxy_requests_total{system="loki",direction="downstream"}[5m])) by (route, status)
```

```promql
histogram_quantile(0.95, sum(rate(loki_vl_proxy_request_duration_seconds_bucket{system="loki",direction="downstream"}[5m])) by (le, route))
```

```promql
histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket{system="vl",direction="upstream"}[5m])) by (le, route))
```

### Cache efficiency by route

```promql
sum(rate(loki_vl_proxy_cache_hits_by_endpoint{system="loki",direction="downstream"}[5m])) by (route)
/
clamp_min(
  sum(rate(loki_vl_proxy_cache_hits_by_endpoint{system="loki",direction="downstream"}[5m])) by (route)
  +
  sum(rate(loki_vl_proxy_cache_misses_by_endpoint{system="loki",direction="downstream"}[5m])) by (route),
  1
)
```

### Per-request decomposition in logs

Use structured request logs for:

- `proxy.overhead_ms`
- `proxy.duration_ms`
- `upstream.duration_ms`
- `http.route`
- `loki.api.system`
- `proxy.direction`

That is the right place to understand exact proxy-only cost for a specific
request.

## Related Docs

- [Cost Model](cost-model.md)
- [Performance](performance.md)
- [Benchmarks](benchmarks.md)
- [Fleet Cache](fleet-cache.md)
- [Observability](observability.md)
