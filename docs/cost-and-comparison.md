---
sidebar_position: 16
description: Source-backed comparison matrix for Loki, VictoriaLogs, and Loki-VL-proxy across indexing, search behavior, high-cardinality handling, deployment shape, and Grafana read-path control.
---

# Comparison Matrix

This page is the behavior and architecture matrix.

Use it to answer:

- how Loki and VictoriaLogs differ at the data-model and search layer
- what Loki-VL-proxy adds on the Grafana-compatible read path
- where the operational tradeoffs really sit

For the separate AWS-style cost worksheet and scenario model, see
[Cost Model](cost-model.md).

## Comparison Matrix

| Dimension | Grafana Loki | VictoriaLogs | VictoriaLogs via Loki-VL-proxy |
|---|---|---|---|
| Indexing model | Loki docs: line content is not indexed; labels index streams. | VictoriaLogs docs: all fields are indexed and full-text search runs across all fields. | Grafana still talks Loki, while the backend keeps the VictoriaLogs field model. |
| High-cardinality posture | Loki docs: labels should stay low-cardinality; high-cardinality labels reduce performance and cost-effectiveness. | VictoriaLogs docs: high-cardinality values such as `trace_id`, `user_id`, and `ip` work as fields unless promoted to stream fields. | The proxy can keep Loki-safe label surfaces while VictoriaLogs keeps richer field semantics underneath. |
| Search-heavy broad scans | Broad searches can become stream selection plus line filtering because line content is not indexed. | VictoriaLogs publishes fast full-text search as a core capability. | The proxy can additionally suppress repeated scans with Tier0, L1/L2/L3, and long-range window cache. |
| Compression and stored-byte shape | Loki docs say chunk blocks and structured metadata are stored compressed, but the retained footprint still includes label-driven index structures and chunk/object-store layout. | VictoriaLogs docs say logs usually compress by `10x` or more, and some deployments observe `50-60x` on the data-block compression ratio metric excluding `indexdb`. | The proxy adds compressed read-path hops under its control: `gzip` client responses, `zstd`/`gzip` peer-cache transfers, gzip disk-cache values, and negotiated upstream compression with safe decode. |
| Cross-AZ traffic posture | Loki docs say distributors forward writes to a replication factor that is generally `3`, queriers query all ingesters for in-memory data, and the zone-aware replication design explicitly lists minimizing cross-zone traffic costs as a non-goal. | VictoriaLogs cluster docs support independent clusters in separate availability zones and advanced multi-level cluster setup, which allows local reads to stay local unless operators intentionally fan out globally. | The proxy can stay AZ-local on the read path and adds `zstd`/`gzip` compression on the hops it controls, but it does not invent backend replication economics that the VictoriaLogs docs do not quantify. |
| Published large-workload sizing | Grafana's own sizing guide reaches `431 vCPU / 857 Gi` at `3-30 TB/day` and `1221 vCPU / 2235 Gi` at about `30 TB/day` before query spikes. | VictoriaLogs docs do not publish an equivalent distributed throughput tier matrix in the same form; the safer claim is lower RAM/disk posture plus operator reports and backend compression behavior. | The proxy keeps the read-side tax small while making downstream, upstream, and cache behavior observable enough to prove whether the smaller stack is really holding. |
| Deployment shape | Single-binary mode exists, but scalable Loki is documented as a microservices-based system with multiple components. | VictoriaLogs docs position the backend as a simple single executable for the easy path, but they also document cluster mode with `vlinsert`, `vlselect`, `vlstorage`, replication, multi-level cluster setup, and HA patterns across independent availability zones. | Adds one small read-side compatibility layer with explicit route-aware telemetry, and can front either single-node or clustered VictoriaLogs. |
| Grafana integration | Native Loki datasource and native Loki UX. | Native path is VictoriaLogs plugin or direct VictoriaLogs APIs. | Keeps Grafana on the native Loki datasource, Explore, and Drilldown workflows. |
| Read-path caching levers | Loki-specific caches and query path tuning. | Backend-native behavior only. | Adds Tier0 compatibility cache, local cache, disk cache, peer cache, and query-range window cache. |
| Visibility into compatibility work | No separate compatibility layer exists to observe. | No Loki-compatibility layer exists to observe. | Splits downstream latency, upstream latency, cache efficiency, and proxy overhead by route. |

## What Official Docs Explicitly Support

### Loki

Official Loki docs state the following:

- labels are intended for low-cardinality values
- the content of each log line is not indexed
- high-cardinality labels build a huge index, flush many tiny chunks, and reduce
  performance and cost-effectiveness
- scalable Loki is microservices-based and query-frontend driven

Sources:

- [Loki labels docs](https://grafana.com/docs/loki/latest/get-started/labels/)
- [Loki architecture docs](https://grafana.com/docs/loki/latest/get-started/architecture/)

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
- L1 in-memory cache
- optional L2 disk cache
- optional L3 peer cache
- query-range split-window cache

This matters because Grafana traffic is usually repetitive. Dashboards,
Explore, and Drilldown often re-issue the same reads across the same routes and
time windows.

If those repeated reads hit cache, VictoriaLogs work per user action goes down.

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

- `query_range` warm hits at `0.64-0.67 us` versus `4.58 ms` on the cold
  delayed path
- `detected_field_values` warm hits at `0.71 us` versus `2.76 ms` without
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
| Repeated Grafana reads | Proxy caches can suppress repeated backend work aggressively. |
| Migration from Loki to VictoriaLogs | Grafana can keep the native Loki datasource while the backend changes behind the proxy. |
| Multi-replica read fleets | Peer cache lets one warm pod reduce repeated backend fetches across the fleet. |

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
