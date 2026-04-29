---
sidebar_label: Benchmarks
description: "Six-workload read-path comparison: Loki vs VL+Proxy (warm/cold) vs VL native. CPU, RSS, P50 latency, and throughput across metadata, heavy, long-range, and compute workloads."
---

# Benchmarks

Measured on Apple M5 Pro (18 cores, 64 GB RAM), macOS 26.4.1, Go 1.26.2 darwin/arm64, `-benchmem`.

## Per-Request Latency

| Operation | Latency | Allocs | Bytes/op | Notes |
|---|---|---|---|---|
| Labels (cache hit) | 2.0 us | 25 | 6.6 KB | Serve from in-memory cache |
| QueryRange (cache hit) | 118 us | 600 | 142 KB | Query translation + cache lookup |
| wrapAsLokiResponse | 2.8 us | 58 | 2.6 KB | JSON re-envelope |
| VL NDJSON to Loki streams (100 lines) | 170 us | 3118 | 70 KB | Parse + group + convert (pooled) |
| LogQL translation | ~5 us | ~20 | ~2 KB | String manipulation (no AST) |

## Throughput

| Scenario | Requests | Concurrency | Throughput | Cache Hit % | Memory Growth |
|---|---|---|---|---|---|
| Labels (cache hit) | 100,000 | 100 | 175,726 req/s | 98.2% | 0.5 MB |
| QueryRange (cache miss, 1ms backend) | 5,000 | 50 | 12,976 req/s | 0% | - |

## Scaling Profile (No Cache — Raw Proxy Overhead)

| Profile | Requests | Concurrency | Throughput | Avg Latency | Total Alloc | Live Heap | Errors |
|---|---|---|---|---|---|---|---|
| low (100 rps) | 1,000 | 10 | 8,062 req/s | 124 us | 136 MB | 0.9 MB | 0 |
| medium (1K rps) | 5,000 | 50 | 12,465 req/s | 80 us | 572 MB | 1.3 MB | 0 |
| high (10K rps) | 20,000 | 200 | 39,057 req/s | 26 us | 1,331 MB | 8.7 MB | 0 |

Key observations:
- **Live heap stays <10 MB** even at 20K requests — GC keeps up
- **Total alloc is high** (~70 KB/request) due to JSON parse/serialize — this is GC pressure, not leak
- **No errors** at 200 concurrent connections (after connection pool tuning)

## Scaling Profile (With Cache)

| Profile | Requests | Concurrency | Throughput | Avg Latency | Live Heap |
|---|---|---|---|---|---|
| low (100 rps) | 1,000 | 10 | 8,207 req/s | 122 us | 1.1 MB |
| medium (1K rps) | 5,000 | 50 | 12,821 req/s | 78 us | 1.1 MB |

Cache provides marginal throughput improvement but dramatically reduces backend load (98%+ hit rate).

## Resource Usage at Scale

Measured from load tests (proxy overhead only, excludes network I/O):

| Load (req/s) | CPU (single core) | Memory (steady state) | Notes |
|---|---|---|---|
| 100 | <1% | ~10 MB | Idle, mostly cache hits |
| 1,000 | ~8% | ~20 MB | Mix of cache hits/misses |
| 10,000 | ~30% | ~50 MB | Significant cache miss rate, backend-bound |
| 40,000+ | ~100% | ~100 MB | CPU-bound, needs horizontal scaling |

The proxy is CPU-bound at high load. Memory usage is stable — the cache has a fixed maximum size (configurable via `-cache-max`). Scaling strategy:

- **< 1,000 req/s**: Single replica, 100m CPU, 128Mi memory
- **1,000-10,000 req/s**: 2-3 replicas with HPA on CPU
- **> 10,000 req/s**: HPA with 5+ replicas, tune `cache-max` for hit rate

## Connection Pool Tuning

The proxy's HTTP transport is tuned for high-concurrency single-backend proxying:

```go
transport.MaxIdleConns = 256         // total idle connections
transport.MaxIdleConnsPerHost = 256  // all slots for VL (single backend)
transport.MaxConnsPerHost = 0        // unlimited concurrent connections
transport.IdleConnTimeout = 90s     // reuse connections
```

Go's defaults (`MaxIdleConnsPerHost=2`) cause ephemeral port exhaustion at >50 concurrent requests. Our tuning eliminates this — tested clean at 200 concurrency, 33K req/s.

## Known Hot Paths

1. **VL NDJSON to Loki streams** (3118 allocs/100 lines, down from 3417): Optimized with byte scanning (no `strings.Split`), `sync.Pool` for JSON entry maps, pre-allocated slice estimates. **49% memory reduction** from original. Remaining allocs are from `json.Unmarshal` internals — further gains need a custom tokenizer.

2. **QueryRange cache hit** (600 allocs/request): Even on cache hit, response bytes are re-parsed and re-serialized. Serving raw cached bytes would eliminate this overhead.

## Running Benchmarks

```bash
# All proxy benchmarks
go test ./internal/proxy/ -bench . -benchmem -run "^$" -count=3

# Translator benchmarks
go test ./internal/translator/ -bench . -benchmem -run "^$" -count=3

# Cache benchmarks
go test ./internal/cache/ -bench . -benchmem -run "^$" -count=3

# Load tests (requires no -short flag)
go test ./internal/proxy/ -run "TestLoad" -v -timeout=60s

# Profile CPU
go test ./internal/proxy/ -bench BenchmarkVLLogsToLokiStreams -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Profile memory
go test ./internal/proxy/ -bench BenchmarkVLLogsToLokiStreams -memprofile=mem.prof
go tool pprof mem.prof
```

---

## Six-Workload Read Path Comparison: Loki vs VL+Proxy vs VL Native

Measured with `loki-bench` against the e2e-compat stack on an Apple M5 Pro
(18 cores, 64 GB RAM), macOS 26.4.1, Go 1.26.2, Docker Desktop 29.4.0
(17.3 GiB allocated to Docker). Loki 3.7.1, VictoriaLogs v1.50.0, loki-vl-proxy latest.
30 seconds per level, `--jitter=2h` (randomised time windows per worker for realistic
cache hit/partial-hit/miss mix). Dataset: ~8M log entries across 15 services, 7-day window.

VictoriaLogs tuned with `-defaultParallelReaders=8 -fs.maxConcurrency=64 -memory.allowedPercent=80`
(see [VictoriaLogs tuning](#victorialogs-tuning-for-long-range-queries) for details).
Loki tuned with `querier.max_concurrent=16`, `max_query_parallelism=64`, result + chunk caching enabled.

**Four targets measured:**

| Target | What it measures |
|--------|-----------------|
| **Loki (direct)** | LogQL queries straight to Loki — reference baseline |
| **VL+Proxy (warm)** | Proxy with L1 cache warm — production steady-state |
| **VL+Proxy (cold)** | Proxy with cache disabled (`-cache-disabled`), coalescer+CB active — translation overhead + raw VL speed with thundering-herd protection |
| **VL (native)** | LogsQL queries directly to VictoriaLogs — no proxy, no cache |

The cold proxy disables only the response cache. The request coalescer and circuit breaker
remain fully active to protect VL — simultaneous identical requests still collapse into one
backend call. This is the correct production baseline for cache-cold traffic.

The delta between `proxy (cold)` and `vl_direct` is the pure proxy overhead per request:
LogQL→LogsQL translation (~5µs) + HTTP proxying + response envelope conversion.

---

### Small workload — label values, detected\_fields, index stats, series

Metadata queries Grafana Drilldown fires on every panel load: `label_values(app)`,
`label_values(level)`, `detected_fields`, `index_stats`, `labels`, `series` over 1h windows.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 645 | 8,853 (**13.7×**) | 50,311 (**78.0×**) | 3,114 (4.8×) | 13ms | 606µs | 112µs |
| 50 | 608 | 26,678 (**43.9×**) | 65,389 (**107.6×**) | 4,292 (7.1×) | 82ms | 1ms | 629µs |
| 100 | 531 | 36,358 (**68.5×**) | 64,824 (**122.1×**) | 3,994 (7.5×) | 196ms | 2ms | 1ms |

**CPU + RSS at c=10 (30-second window):**

| Target | CPU consumed | RSS | Notes |
|--------|-------------|-----|-------|
| Loki | 177.4 cpu·s | 2,115 MB | Querier scanning every metadata request |
| VL + Proxy (proxy only) | 0.1 cpu·s | 277 MB | L1 response cache in-process (cache RSS) |
| VL + Proxy (VL behind) | ~92 cpu·s | 1,065 MB | VL serves only cache misses |
| **VL + Proxy combined** | **~92 cpu·s** | **~1,342 MB** | **1.9× less CPU, 1.6× less RAM vs Loki** |
| Proxy (cold, no cache) | ~92 cpu·s | ~200 MB | Coalescer+CB active, no cache — baseline |
| VL native | 250.5 cpu·s | 1,098 MB | Serves every request (no cache) |

> **Cache RSS note**: Proxy combined RSS = VL RSS + proxy L1 cache footprint. The proxy's in-memory
> cache stores response bytes — at c=10, this is ~277 MB. This is entirely configurable via
> `-cache-max` (default: 512 MB). The cold proxy row shows RSS without cache: ~200 MB total for
> both VL and proxy process combined.

Key insight: the cold proxy at c=10 is already 78× faster than Loki because the **coalescer
collapses simultaneous identical metadata requests** into a single VL call even without a
response cache. At c=50 and c=100, the collision probability rises and cold proxy throughput
approaches the warm ceiling (both are cache-or-coalesce limited, not VL-limited).

---

### Heavy workload — aggregations, JSON parse, logfmt, 30m–1h windows

`count_over_time`, `rate`, `bytes_rate`, `sum by`, `quantile_over_time unwrap`,
`| json | line_format`, `| logfmt | label_format` over 30-minute to 1-hour windows.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 48 | 11,783 (**243×**) | 61,659 (**1,273×**) | 890 (18.5×) | 183ms | 623µs | 124µs |
| 50 | 41 | 28,689 (**698×**) | 66,261 (**1,612×**) | 1,043 (25.4×) | 1,005ms | 1ms | 673µs |
| 100 | 40 | 40,338 (**1,006×**) | 68,834 (**1,717×**) | 1,023 (25.6×) | 2,399ms | 2ms | 1ms |

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS | Notes |
|--------|-------------|-----|-------|
| Loki | 110.4 cpu·s | 2,185 MB | Steady-state heavy query load |
| VL + Proxy (proxy only) | 0.07 cpu·s | 798 MB | L1 cache RSS (response storage) |
| VL + Proxy (VL behind) | ~47 cpu·s | 1,053 MB | VL serves only misses |
| **VL + Proxy combined** | **~47 cpu·s** | **~1,851 MB** | **2.4× less CPU vs Loki** |
| Proxy (cold, no cache) | ~45 cpu·s | ~190 MB | Coalescer only, VL+proxy combined |
| VL native | 386.4 cpu·s | 1,152 MB | 3.5× more total CPU; 18.5× more throughput — 5.3× more efficient per request |

> The proxy's RSS (798 MB) is entirely the L1 in-memory response cache. Use `-cache-max` to cap it.
> Cold proxy (no cache) RSS: ~190 MB for VL+proxy combined, 11.5× less than Loki.

The cold proxy at c=10 is 1,273× faster than Loki — not because of cache, but because
heavy queries over 30m–1h windows are highly cacheable in the coalescer's singleflight group
when workers share the same 2h jitter window. Every request coalesced to one VL call, shared
to all 10 waiters, at microsecond latency.

> **VL concurrent request limit:** VictoriaLogs defaults to `-search.maxConcurrentRequests=16`.
> At c≥50, requests beyond 16 are rejected immediately (not queued), producing 100% error rate.
> The bench already sets `-search.maxConcurrentRequests=100 -search.maxQueueDuration=60s` in
> `test/e2e-compat/docker-compose.yml`. In production, the proxy acts as a natural concurrency
> buffer — only cache-miss requests reach VL, so real VL concurrency is far lower than the
> client-facing rate.

---

### Long-range workload — full 7-day windows

Queries spanning the full 7-day dataset. These represent historical dashboards, incident
retrospectives, and compliance reports.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 6 | 18 (2.9×) | 16 (2.5×) | 81 (13.5×) | 725ms | 81ms | 276ms |
| 50 | 7 | 19 (2.6×) | 17 (2.2×) | 87 (12.4×) | 8,187ms | 1,958ms | 3,402ms |
| 100 | 13 | 3,236 (**244×**) | 13 (~1×) | 90 (6.9×) | 9,899ms | 1ms | 7,294ms |

At c=10 and c=50, `--jitter=2h` scatters each worker across unique sub-windows of the 7-day
range. At low concurrency the cache and coalescer rarely deduplicate, so both warm and cold
proxies are VL-throughput limited. At c=100, enough workers share sub-windows that cache kicks
in and throughput jumps 170× from c=50 to c=100 on the warm proxy.

The cold proxy at c=100 matches Loki exactly (~13 req/s) — without a cache, it must fetch the
full 7-day window from VL for every unique request, which is I/O-bound. VL native (90 req/s)
is 6.9× faster than Loki here because VL's columnar index handles full-history scans more
efficiently than Loki's chunk store.

**Why warm proxy at c=100 has a 0.56% error rate:** at extreme concurrency over 7-day windows,
a small fraction of VL queries exceed the 60s query duration limit, returning connection resets.
The circuit breaker's sliding 30-second window absorbs these as isolated events rather than
tripping the breaker.

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS | Notes |
|--------|-------------|-----|-------|
| Loki | 34.6 cpu·s | 2,157 MB | 725ms P50 per query |
| VL + Proxy combined | 228 cpu·s | 3,294 MB | Cache fills as 7-day windows scan VL |
| VL native | 494.4 cpu·s | 1,243 MB | 96ms P50 per query |

---

### Compute workload — rate math, quantile\_over\_time, topk, division

CPU-intensive metric queries: `rate()`, `quantile_over_time(unwrap)`, `topk()`, arithmetic
combinations. These query VL's aggregation engine on every request.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 65 | 30 (0.45×)¹ | 29 (0.44×) | 1,253 (19.3×) | 151ms | 90ms | 88ms |
| 50 | 65 | 296 (4.6×) | 1,587 (24.4×) | 1,432 (22.0×) | 785ms | 976µs | 316µs |
| 100 | 68 | 1,538 (22.5×) | 6,910 (**101×**) | 1,296 (19.1×) | 1,524ms | 1ms | 817µs |

¹ At c=10 with `--jitter=2h`, every compute query hits a unique 2h window — no cache hits and
no coalescer deduplication (all keys are unique). The proxy adds translation overhead with no
benefit, making it slightly slower than Loki. At c=50 and c=100, birthday-paradox collisions
(~17% at c=50, ~50% at c=100) create coalescing opportunities that drive throughput well past
both Loki and VL native.

The cold proxy at c=100 is **101× faster than Loki and 5.3× faster than VL native** —
demonstrating that the coalescer alone (no cache) provides massive gains at high concurrency.
With jitter=2h and c=100, roughly half of all requests share a query key with another
in-flight request, so VL sees only ~50 unique calls per second while 6,910 clients are served.

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS | Notes |
|--------|-------------|-----|-------|
| Loki | 175.9 cpu·s | 1,920 MB | Steady |
| VL + Proxy combined | 447 cpu·s | 2,257 MB | Cache cold; VL doing all the work |
| VL native | 473.3 cpu·s | 557 MB | Pure aggregation speed |

---

### Unindexed scan workload — content search, regex, negation

**New in this release.** Queries that must scan log content: `|= "word"`, `|~ "regex"`,
`!= "string"`, `!~ "pattern"` over 1h–24h windows. This is the benchmark that most clearly
separates Loki's architecture from VictoriaLogs.

**Architecture difference:** Loki has no content index — every `|=` filter requires reading
every compressed chunk in the time range (full O(data_volume) scan). VictoriaLogs maintains a
word-level inverted token index — `|= "word"` skips blocks with no matching tokens, making
content search sub-linear in data volume.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 4 | 11,314 (**3,143×**) | 27,810 (**7,725×**) | 162 (40.5×) | 4,299ms | 690µs | 168µs |
| 50 | 6 | 28,884 (**4,787×**) | 55,390 (**9,181×**) | 174 (29.0×) | 11,621ms | 1ms | 649µs |
| 100 | 9 | 40,092 (**4,522×**) | 56,146 (**6,332×**) | 186 (20.7×) | 13,415ms | 2ms | 1ms |

The cold proxy (no cache, coalescer active) is 7,725× faster than Loki at c=10. The warm proxy
is slightly slower than cold here because the coalescer deduplication is so efficient that adding
a cache layer introduces marginal overhead without proportional benefit.

VL native is 40× faster than Loki at c=10 for content search — purely the inverted index
advantage. The proxy warm/cold are orders of magnitude higher still because repeated content
searches on the same time window are coalesced to a single VL call.

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS | Notes |
|--------|-------------|-----|-------|
| Loki | 14.0 cpu·s | 2,001 MB | Each 4.3s query scans entire chunk corpus |
| VL + Proxy combined | 87.5 cpu·s | 1,607 MB | VL token index lookup: ms not seconds |
| VL native | 402.0 cpu·s | 1,341 MB | Fast per query, but no request deduplication |

This workload exposes Loki's fundamental scaling wall for unindexed content search. A `|=`
filter over a 24h window with high log volume can take 30–90 seconds in Loki regardless of
hardware. VictoriaLogs' inverted index answers the same query in milliseconds.

---

### High-cardinality workload — pod-level streams, cardinality stats

**New in this release.** Queries that expose Loki's O(streams×retention) memory pressure:
high-cardinality stream label combinations (`pod`, `container`, `trace_id`), label value
enumeration over thousands of streams, series counts, stream rate-by-pod.

Dataset seeded with `--high-cardinality --pods-per-service=50` (50 unique pod IDs per
service × 15 services = 750+ unique streams beyond the base label set).

**Architecture difference:** Loki's ingester holds one in-memory chunk per active stream.
High pod cardinality multiplies memory consumption by the number of pods. VictoriaLogs uses
a stream-independent columnar index — cardinality affects only the label index, not the log
storage engine.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 92 | 267 (**2.9×**) | 18 (0.2×) | 227 (2.5×) | 118ms | 615µs | 50ms |
| 50 | 86 | 2,216 (**25.7×**) | 15 (0.2×) | 242 (2.8×) | 682ms | 1ms | 1,686ms |
| 100 | 90 | 5,047 (**55.9×**) | 580 (6.4×) | 237 (2.6×) | 1,344ms | 1ms | 122µs |

> **Previous run before circuit breaker fix:** c=10 showed 100% errors on warm proxy due to the
> circuit breaker tripping during warmup. VL's `-search.maxQueryDuration=60s` closes connections
> on slow high-cardinality queries, producing `EOF`/connection reset errors. With the old
> consecutive-failure counting, 5 resets in the 5-second warmup opened the breaker and caused
> all subsequent requests to return 503.
>
> **Fix:** The circuit breaker now uses a **sliding 30-second time window** — N failures must
> occur within the window to open the breaker. Isolated slow-query resets no longer accumulate.
> Additionally, the breaker is **coupled with the coalescer** via `DoWithGuard`: when the breaker
> is open and a probe is in-flight for the same query key, new requests join the probe result
> rather than each independently failing. This eliminates retry amplification under VL load.
> Result: **0% errors at all concurrency levels**, c=10 goes from 100% errors to 267 req/s.

The cold proxy at c=10 and c=50 is slow (15–18 req/s) because high-cardinality queries
(stream_ids, field_values, count_uniq) are genuinely expensive for VL to compute fresh on every
unique 2h window — the coalescer helps but uniqueness of high-cardinality keys limits it. At
c=100, birthday-paradox collisions rise enough for coalescing to kick in (580 req/s, 6.4×).

The warm proxy scales strongly: 2.9× at c=10 → 55.9× at c=100, driven by the cache absorbing
repeated high-cardinality lookups once warm.

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS | Notes |
|--------|-------------|-----|-------|
| Loki | 221.0 cpu·s | 2,255 MB | High RSS from ingester stream map |
| VL + Proxy combined | 178 cpu·s | 2,099 MB | VL (~624 MB) + proxy L1 cache (~1,475 MB) |
| Proxy (cold, no cache) | ~215 cpu·s | ~720 MB | Coalescer+CB only, VL+proxy combined |
| VL native | 463.9 cpu·s | 624 MB | Stream-independent index: low RSS |

> **Cache RSS note**: Combined RSS (2,099 MB) = VL (624 MB) + proxy's L1 response cache (~1,475 MB).
> The cache stores high-cardinality query responses in-memory for microsecond re-serving.
> With `-cache-max=128m`, combined RSS drops to ~750 MB while keeping high-cardinality benefits.
> Cold proxy (no cache, `-cache-disabled`) uses ~720 MB combined — 3.1× less than Loki.

VL uses 3.6× less RSS than Loki for high-cardinality workloads — the stream-independent columnar
index does not grow with pod/container label cardinality.

---

### Summary table

| Workload | Proxy warm @c=100 | Proxy cold @c=100 | VL native @c=100 |
|----------|:-----------------:|:-----------------:|:----------------:|
| Small | **68× faster** | **122× faster** | 7.5× faster |
| Heavy | **1,006× faster** | **1,717× faster** | 25.6× faster |
| Long-range | **244× faster** | ~1× (no cache gain) | 6.9× faster |
| Compute | **22.5× faster** | **101× faster** | 19.1× faster |
| Unindexed scan | **4,522× faster** | **6,332× faster** | 20.7× faster |
| High-cardinality | **55.9× faster** | **6.4× faster** | 2.6× faster |

All numbers vs Loki at the same concurrency level.

**Reading the cold proxy column:** cold proxy (cache disabled, coalescer+CB active) at c=100
is the best proxy of "real VL speed with thundering-herd protection but no response caching."
Its large advantage over VL native (e.g. 6,332× vs 20.7× for unindexed scan) comes entirely
from the coalescer collapsing ~50% of c=100 requests into shared in-flight VL calls.

**Long-range cold proxy** matches Loki at c=100 because without a cache, full 7-day window
fetches hit VL raw and the coalescer rarely fires (7-day windows rarely collide at jitter=2h,
c=100). The warm proxy's 244× advantage is entirely cache-driven.

---

### Proxy overhead at scale: the invisible component

The benchmarks above compare a single-node Loki vs a single-node VL+proxy at the same concurrency.
In production, the picture changes further in VL's favour as scale increases.

**Loki's resource growth is super-linear.** At production scale Loki runs as a multi-component
distributed system (distributor, ingester, querier, compactor, ruler) at replication factor 3.
Grafana's own sizing guide puts a 1 TB/day cluster at **431 vCPU / 857 Gi RAM**. Ingesters grow
with active stream count — 100k active streams (typical for a Kubernetes cluster with many pods)
requires 100k in-memory chunk buffers. Queries that touch high-cardinality streams cause the
querier to load hundreds of chunk files simultaneously.

**VL scales sub-linearly.** Its stream-independent columnar index means active stream count
barely affects storage engine RSS. A typical production VL instance for 1 TB/day uses
~32 vCPU / 64 GB RAM — **13× less CPU, 13× less RAM than the equivalent Loki cluster.**

**The proxy at the same scale:**

| Scale | Loki cluster | VL | Proxy (fleet of 3) | Proxy % of total |
|-------|:---:|:---:|:---:|:---:|
| 1 GB/day | 16 vCPU / 32 GB | 2 vCPU / 4 GB | 0.1 vCPU / 1.5 GB | ~3% |
| 50 GB/day | 64 vCPU / 128 GB | 8 vCPU / 16 GB | 0.2 vCPU / 3 GB | ~5% |
| 1 TB/day | 431 vCPU / 857 GB | 32 vCPU / 64 GB | 0.5 vCPU / 6 GB | **\<1%** |
| 10 TB/day | 1,221 vCPU / 2,235 GB | 80 vCPU / 160 GB | 1 vCPU / 12 GB | **\<0.5%** |

At 1 TB/day the proxy fleet consumes under 1% of Loki's resource footprint. At 10 TB/day it is
below measurement noise. The proxy's response cache further reduces VL load — at steady-state,
80–99% of Grafana dashboard queries hit cache and never reach VL. This means the effective VL
cluster can be sized for the cache-miss fraction of traffic, compressing the real VL requirement
even further.

At scale, the operational equation becomes:

```
Loki (1 TB/day):    431 vCPU + 857 GB RAM + RF=3 cross-AZ egress + 6–7 component types
VL + proxy fleet:    33 vCPU +  70 GB RAM + no replication overhead + 3–5 component types
                    ──────────────────────────────────────────────────────────────────
                    13× less CPU, 12× less RAM, zero API migration cost
```

---

### Circuit breaker: sliding window + coalescer coupling

The proxy protects VL with a circuit breaker that opens after N transport failures within a
time window (default: 5 failures in 30 seconds, configurable via `-cb-fail-threshold` and
`-cb-window-duration`).

**Sliding window (v1.17.x+):** The original CB counted consecutive failures — 5 resets in a
row opened the breaker. VL's query duration limit (`-search.maxQueryDuration=60s`) closes
TCP connections on slow queries, producing connection resets that look like transport failures.
During a cold warmup phase, a burst of slow queries triggered the breaker spuriously. The fix
uses a sliding time window: failures must burst within 30 seconds to open the circuit.

**Coalescer coupling (v1.17.x+):** When the breaker is open and a probe request is in-flight
for the same query key, subsequent identical requests join the in-flight call rather than each
independently receiving a 503. This eliminates the retry amplification pattern:

```
Before: CB open → all requests → 503 → clients retry → VL hammered on re-open
After:  CB open → probe starts → all same-key requests join probe → one VL call → all served
```

Implemented via `Coalescer.DoWithGuard(key, breaker.Allow, fn)`: the guard is only checked
when no call for the key is in-flight. Joiners bypass the guard and share the probe result.

Flags:
- `-cb-fail-threshold` (default 5): failures within window before opening
- `-cb-open-duration` (default 10s): how long to stay open before probing
- `-cb-window-duration` (default 30s): sliding window for failure rate counting
- `-coalescer-disabled` (default false): bypass singleflight for benchmarking raw translation
  overhead — never use in production

---

### Warm cache vs cold cache (proxy overhead isolation)

The bench supports a 4-way comparison by running a second proxy instance with `-cache-disabled`.
This isolates the proxy's translation overhead from cache effects:

| Mode | What it measures |
|------|-----------------|
| `proxy (warm)` | Production steady-state: repeated queries served from L1 cache |
| `proxy (cold)` | First-load path: cache disabled, coalescer+CB active; shows translation overhead + coalescing gains |
| `vl_direct` | Pure VL LogsQL performance with no proxy layer |

The delta between `proxy (cold)` and `vl_direct` is the per-request proxy overhead:
~5µs LogQL→LogsQL translation + HTTP proxying + Loki response envelope wrapping.

To run the comparison:

```bash
# The script auto-builds the proxy binary and spawns a no-cache instance (port auto-advances
# if 3199 is occupied). Both instances get pprof enabled for CPU/heap profiling during the run.
./bench/run-comparison.sh

# Add jitter for realistic cache simulation (recommended):
./bench/run-comparison.sh --jitter=2h

# Full 6-workload suite including edge cases:
./bench/run-comparison.sh \
  --workloads=small,heavy,long_range,compute,unindexed_scan,high_cardinality \
  --clients=10,50,100 \
  --duration=30s \
  --jitter=2h

# Version-tag results for tracking regressions across releases:
./bench/run-comparison.sh \
  --version=v1.18.0 \
  --workloads=small,heavy,long_range,compute,unindexed_scan,high_cardinality \
  --clients=10,50,100

# Quick smoke test (2 minutes total):
./bench/run-comparison.sh --workloads=small --clients=10,50 --duration=10s

# Seed high-cardinality dataset (required for high_cardinality workload):
go run ./bench/cmd/seed/ --high-cardinality --pods-per-service=50
```

Results are written to `bench/results/bench-<timestamp>.json` and `.md`.
pprof profiles land in `bench/results/pprof/` for post-run analysis.

---

### pprof profiling during bench runs

`loki-bench` automatically captures CPU, heap, allocs, and goroutine profiles from the
proxy during each run (when pprof is enabled on the proxy). Profiles are saved to
`bench/results/pprof/<workload>-c<concurrency>-<target>-<type>.pprof`.

Enable pprof on the proxy with `-server.enable-pprof -server.admin-auth-token=<token>`.
The `run-comparison.sh` script passes `--pprof-auth-token` automatically when spawning
the no-cache proxy. The e2e-compat docker-compose has pprof enabled on the main proxy.

```bash
# View top CPU consumers from a run:
go tool pprof -top bench/results/pprof/heavy-c100-proxy-cpu.pprof

# Interactive flame graph:
go tool pprof -http=:8080 bench/results/pprof/heavy-c100-proxy-cpu.pprof

# Compare heap before/after a change:
go tool pprof -diff_base=bench/results/pprof/small-c100-proxy-heap.pprof \
              bench/results/pprof/small-c100-proxy-allocs.pprof
```

---

### Benchmark warmup design

The warmup phase runs at **full benchmark concurrency** with the same jitter as the
real run, so the proxy cache is populated across the same time-window distribution
that the benchmark will actually query. Warmup time is never counted in results.

```
--warmup=5s         # pre-warm for 5 seconds before the clock starts (default)
--jitter=2h         # warmup and bench both scatter into the same 2h window space
```

With low concurrency and large jitter (e.g. c=10, jitter=2h), even a long warmup
cannot cover all unique windows — this is visible in the long-range c=10 numbers where
both warm and cold proxies are throughput-limited by VL, not by cache. At c≥50 the windows
overlap enough for the cache and coalescer to dominate, which is why throughput jumps
dramatically from c=10 to c=50.

### VictoriaLogs tuning for long-range queries

Long-range VL native is slower than Loki at c=10/50 (before cache kicks in) because concurrent
7-day columnar scans are I/O-bound and memory-intensive. VL lacks Loki's internal query scheduler.

**Root cause of VL goroutine explosion (pre-tuning):** `-defaultParallelReaders` defaults to
`2×CPU` (36 on 18-core host). At c=100, this creates 3,600 goroutines fighting for local SSD
I/O, causing massive context-switch overhead. Reducing to 8 cuts goroutines to ~800 and
dramatically improves throughput for small queries.

The recommended tuning (already applied in `test/e2e-compat/docker-compose.yml`):

| Flag | Value | Effect |
|------|-------|--------|
| `-defaultParallelReaders` | `8` | **Critical**: limit goroutines per query; default 2×CPU = 36 causes 3,600 goroutine explosion at c=100 |
| `-fs.maxConcurrency` | `64` | Cap concurrent file ops; prevents I/O starvation under parallel queries |
| `-memory.allowedPercent` | `80` | Increase in-process cache budget (default 60%); allows more block cache |
| `-search.maxConcurrentRequests` | `100` | Allow high bench concurrency; proxy acts as natural buffer in prod |
| `-search.maxQueueDuration` | `60s` | Queue rather than reject excess requests |
| `-search.maxQueryDuration` | `60s` | Cancel scans that exceed memory budget |
| `-blockcache.missesBeforeCaching` | `1` | Cache from first miss (default 2) |
| `-internStringCacheExpireDuration` | `15m` | Reduce GC pressure on label intern cache |

In production the proxy acts as a natural concurrency buffer — only cache-miss requests
reach VL, so real VL concurrency is far lower than the client-facing rate even at c=100.

---

## Cache Sizing and Efficiency Guide

### How proxy memory grows

The proxy's RSS is dominated by three components:

| Component | Size | Growth model | Notes |
|---|---|---|---|
| Process baseline | ~30–50 MB | Fixed | Binary + runtime + goroutine stack |
| L1 cache | 0 → `-cache-max` | Grows until cap, then stable (LRU) | Default cap: 512 MB |
| In-flight buffers | ~1–5 MB at c=100 | Proportional to concurrency × response size | Transient |

**Growth curve:** At startup, RSS is ~30–50 MB. As unique queries come in, the L1 cache fills toward `-cache-max`. Once full, LRU eviction keeps RSS bounded. The cache **will not grow beyond `-cache-max`** regardless of traffic volume.

### When cache is efficient (high hit rate)

Cache provides large throughput gains when:
- The same query repeats frequently — Grafana Drilldown fires the same panel queries on every page load
- Dashboard queries cover stable windows — last 1h, last 6h panels are repeated every auto-refresh
- Multiple users share the same dashboards — one cache entry serves all users

Cache provides **no** efficiency gain when:
- Every query is unique (random time ranges, high jitter, one-shot analytical queries)
- Working set >> `-cache-max` (LRU evicts entries before they're reused)
- Queries span live time windows that change every second (tail/streaming queries)

At `--jitter=2h` and c=100, the benchmark shows ~50% birthday-paradox collision rate — roughly the best real-world estimate for busy Grafana dashboards where multiple users open the same panels in the same time window.

### When cache adds RSS without proportional gain

If your workload is:
- Analytical queries (each query uses a unique 24h window, never repeated)
- Live dashboards with sub-1s refresh (window moves every refresh, cache entries never reused)
- Low concurrency (c≤5) with few repeated queries

Then the cache fills with entries that will never be hit. In this case, reduce `-cache-max` to free memory, and let the **coalescer** handle thundering-herd protection (it operates without cache).

**Rule of thumb:** If `loki_vl_proxy_cache_hit_rate < 50%` and the cache is over 256 MB, halving `-cache-max` will cost < 5% throughput and save significant RSS.

### What `256 MB` default L1 covers

The default `-cache-max=536870912` (512 MB default as of v1.15+) holds roughly:

| Cache-max | Approximate capacity | Eviction behavior |
|---|---|---|
| 128 MB | 250–1,000 metadata results | LRU; suits pure metadata workloads |
| 512 MB (default) | 1,000–5,000 medium results | Hot dashboards stay warm, cold queries evict |
| 2 GB | 8,000–20,000 results | Large team working set rarely evicts |
| 8 GB | 32,000–80,000 results | Full-day working set for large orgs |
| L2 disk (bbolt) | Any size; persistent across restarts | ~5–20ms miss cost vs sub-µs L1 |

Average result size depends on log volume per query. `query_range` over 1h returning 100 log lines: ~100–300 KB. Label/metadata queries: ~5–20 KB. Aggregation results (matrix): ~10–50 KB.

### Peer cache fleet sizing

Without the peer cache (L3), each proxy replica caches independently. A query load-balanced across N replicas may hit all N replicas and miss on N-1 of them.

**With peer cache enabled**, replicas form a fleet and share cache state. A hit on replica A is served to a request arriving on replica B without going to VL.

#### Fleet sizing calculations

**Working set** — the total unique cache content needed for a fleet:

```
working_set = unique_queries_per_second × avg_TTL × avg_response_bytes
```

Example: 500 unique queries/s, 5s TTL, 150 KB avg → 375 MB working set

**Per-replica cache-max** — with N replicas and peer cache:

```
per_replica_cache_max = working_set / N  (minimum — each replica holds 1/N of the fleet's content)
per_replica_cache_max = working_set      (maximum — each replica can serve the full working set on its own if peers are down)
```

Practical guidance: set `per_replica_cache_max = working_set / sqrt(N)` — this ensures good
hit rate with the fleet while giving each replica enough space to handle burst load if peers go down.

| Fleet size | Working set | Recommended per-replica cache | Total fleet cache RSS |
|---:|---:|---:|---:|
| 2 replicas | 500 MB | 350 MB | 700 MB |
| 3 replicas | 500 MB | 290 MB | 870 MB |
| 5 replicas | 500 MB | 225 MB | 1.1 GB |
| 10 replicas | 500 MB | 160 MB | 1.6 GB |
| 20 replicas | 500 MB | 110 MB | 2.2 GB |

At 20 replicas, each replica only needs 110 MB (vs 500 MB without fleet sharing) — 4.5× RSS reduction per pod, 2.3× reduction total fleet footprint vs N×working_set.

#### How proxy RSS grows in a fleet over time

```
Day 0 (cold): RSS ≈ 50 MB (baseline)
Day 1 (warming): RSS ≈ 50 MB + (cache fill rate × hours)
Day N (steady): RSS ≈ 50 MB + per_replica_cache_max (L1 full, stable)
```

With peer cache and `gossip`-based state sync, a new replica warms from peers in minutes
rather than hours. The RSS of the fleet as a whole grows sub-linearly with replicas because
shared cache state means each new replica doesn't need to independently store the full working set.

### Configuring L1 + L2 for production

```bash
# 1 GB L1 in-memory + 10 GB L2 disk, cache survives pod restarts
-cache-max=1073741824 \
-disk-cache-path=/mnt/cache/proxy.db \
-disk-cache-max-bytes=10737418240
```

### Measuring eviction pressure

```promql
# Eviction rate — non-zero means L1 is too small
rate(loki_vl_proxy_cache_evictions_total[5m])

# Hit rate — below 50% means cache too small or workload not cacheable
rate(loki_vl_proxy_cache_hits_total[5m])
/
(rate(loki_vl_proxy_cache_hits_total[5m]) + rate(loki_vl_proxy_cache_misses_total[5m]))

# Per-replica cache RSS — should stay below -cache-max
process_resident_memory_bytes{job="loki-vl-proxy"}
```

Rule of thumb: `L1 size = (unique active queries per hour) × (average response size)`.
