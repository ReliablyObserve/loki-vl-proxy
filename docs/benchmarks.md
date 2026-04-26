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
- **Live heap stays &lt;10 MB** even at 20K requests — GC keeps up
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
| 100 | &lt;1% | ~10 MB | Idle, mostly cache hits |
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

## Four-Way Read Path Comparison: Loki vs VL+Proxy vs VL Native

Measured with `loki-bench` against the e2e-compat stack on an Apple M5 Pro
(18 cores, 64 GB RAM), macOS 26.4.1, Go 1.26.2, Docker Desktop 29.4.0
(17.3 GiB allocated to Docker). Loki 3.7.1, VictoriaLogs v1.50.0, loki-vl-proxy latest.
30 seconds per level, --jitter=2h (randomised time windows per worker for realistic
cache hit/partial-hit/miss mix). Dataset: ~5M log entries across 12 services, 7-day window.

**Four targets measured:**
| Target | What it measures |
|--------|-----------------|
| **Loki (direct)** | LogQL queries straight to Loki — reference baseline |
| **VL+Proxy (warm)** | Proxy with L1 cache warm — production steady-state |
| **VL+Proxy (cold)** | Proxy with cache disabled (`-cache-disabled`) — translation overhead + raw VL speed |
| **VL (native)** | LogsQL queries directly to VictoriaLogs — no proxy, no cache |

The delta between `proxy (cold)` and `vl_direct` is the pure proxy overhead per request:
LogQL→LogsQL translation (~5µs) + HTTP proxying + response envelope conversion.

---

### Small workload — label values, detected\_fields, index stats, series

Metadata queries Grafana Drilldown fires on every panel load: `label_values(app)`,
`label_values(level)`, `detected_fields`, `index_stats`, `labels`, `series` over 1h windows.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 1,943 | 10,287 (**5.3×**) | 53,917 (**27.7×**) | 3,624 (1.9×) | 4ms | 586µs | 117µs |
| 50 | 1,925 | 27,998 (**14.5×**) | 67,535 (**35.1×**) | 5,306 (2.8×) | 25ms | 1ms | 630µs |
| 100 | 1,894 | 37,300 (**19.7×**) | 62,606 (**33.1×**) | 5,654 (3.0×) | 54ms | 2ms | 1ms |

**CPU + RSS at c=10 (30-second window):**

| Target | CPU consumed | RSS delta | Notes |
|--------|-------------|-----------|-------|
| Loki | 148 cpu·s | 788 MB | Querier scanning every metadata request |
| VL + Proxy (proxy only) | 0.1 cpu·s | 209 MB | Cache serving all repeated windows |
| VL + Proxy (VL behind) | 86 cpu·s | 241 MB | VL serves only cache misses |
| **VL + Proxy combined** | **86 cpu·s** | **450 MB** | **1.7× less CPU, 1.8× less RAM vs Loki** |
| VL native | 221 cpu·s | 253 MB | Serves every request (no cache) |

Key insight: VL-native uses more CPU than Loki for small repeated metadata queries because
it lacks a response cache. The proxy cache absorbs repeated requests, so combined CPU is
lower than either Loki or VL alone.

---

### Heavy workload — aggregations, JSON parse, logfmt, 30m–1h windows

`count_over_time`, `rate`, `bytes_rate`, `sum by`, `quantile_over_time unwrap`,
`| json | line_format`, `| logfmt | label_format` over 30-minute to 1-hour windows.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | Proxy cold P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 538 | 12,064 (**22.4×**) | 60,142 (**111.7×**) | 1,035 (1.9×) | 16ms | 605µs | 128µs |
| 50 | 544 | 29,290 (**53.9×**) | 70,525 (**129.7×**) | 1,287 (2.4×) | 88ms | 1ms | 640µs |
| 100 | 542 | 39,131 (**72.1×**) | 66,279 (**122.2×**) | 1,136 (2.1×) | 181ms | 2ms | 1ms |

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS delta | Notes |
|--------|-------------|-----------|-------|
| Loki | 172 cpu·s | 938 MB | Steady-state heavy query load |
| VL + Proxy (proxy only) | 0.064 cpu·s | 765 MB | Cache serving |
| VL + Proxy (VL behind) | 47 cpu·s | 235 MB | VL serves only misses |
| **VL + Proxy combined** | **47 cpu·s** | **1,000 MB** | **3.7× less CPU vs Loki** |
| VL native | 368 cpu·s | 350 MB | 3.4× more CPU than Loki |

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

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy cold P50 | VL native P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 146 | 163 (1.1×) | 16 (0.1×)¹ | 87 (0.6×) | 26ms | 310ms | 88ms |
| 50 | 197 | 938 (**4.8×**) | 1,950 (**9.9×**) | 92 (0.5×) | 103ms | 120µs | 345ms |
| 100 | 186 | 5,479 (**29.5×**) | 14,957 (**80.6×**) | 74 (0.4×) | 201ms | 151µs | 861ms |

¹ At c=10, jitter=2h scatters each worker into a unique sub-window of the 7-day range. At low
concurrency, the cache never warms on any window, so every request is a cold VL read. The cold
proxy at c=10 is slower than VL native because the proxy adds HTTP round-trip overhead on top of
each already-slow VL scan. At c≥50, enough workers share windows that cache dominates.

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS delta | Notes |
|--------|-------------|-----------|-------|
| Loki | 112 cpu·s | 1,044 MB | 26ms P50 per query |
| VL + Proxy combined | 263 cpu·s | 5,909 MB | Cache fills as 7-day windows scan VL |
| VL native | 506 cpu·s | 1,523 MB | 88ms P50 per query |

---

### Compute workload — rate math, quantile\_over\_time, topk, division

CPU-intensive metric queries: `rate()`, `quantile_over_time(unwrap)`, `topk()`, arithmetic
combinations. These query VL's aggregation engine on every request.

| Clients | Loki req/s | Proxy (warm) req/s | Proxy (cold) req/s | VL native req/s | Loki P50 | Proxy warm P50 | VL native P50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 373 | 31 (0.08×)¹ | 28 (0.08×)¹ | 1,352 (**3.6×**) | 23ms | 57ms | 6ms |
| 50 | 366 | 10,564 (**28.9×**) | 24,601 (**67.2×**) | 1,617 (4.4×) | 134ms | 1ms | 18ms |
| 100 | 357 | 36,422 (**102×**) | 36,851 (**103×**) | 1,532 (4.3×) | 278ms | 2ms | 48ms |

¹ At c=10 with jitter=2h, compute queries hit unique 2h windows — the cache has no hits,
so every request goes to VL. VL native is 3.6× faster than Loki for pure aggregation. At
c≥50, the proxy cache takes over and throughput jumps 350×.

**CPU + RSS at c=10:**

| Target | CPU consumed | RSS delta | Notes |
|--------|-------------|-----------|-------|
| Loki | 163 cpu·s | 1,004 MB | Steady |
| VL + Proxy combined | 436 cpu·s | 3,701 MB | Cache cold; VL doing all the work |
| VL native | 387 cpu·s | 404 MB | Pure aggregation speed |

---

### Summary table

| Workload | Proxy warm vs Loki @c=100 | Proxy cold vs Loki @c=100 | VL native vs Loki @c=100 |
|----------|:-------------------------:|:-------------------------:|:------------------------:|
| Small | **19.7× faster** | **33.1× faster** | 3.0× faster |
| Heavy | **72.1× faster** | **122.2× faster** | 2.1× faster |
| Long-range | **29.5× faster** | **80.6× faster** | 0.4× slower |
| Compute | **102× faster** | **103× faster** | 4.3× faster |

Long-range VL native is slower than Loki at c=100 because concurrent 7-day VL scans saturate
memory bandwidth. The proxy cache eliminates this by serving repeated results from RAM.

---

### Warm cache vs cold cache (proxy overhead isolation)

The bench supports a 4-way comparison by running a second proxy instance with `-cache-disabled`.
This isolates the proxy's translation overhead from cache effects:

| Mode | What it measures |
|------|-----------------|
| `proxy (warm)` | Production steady-state: repeated queries served from L1 cache |
| `proxy (cold)` | First-load path: every request hits VL; shows translation overhead + VL speed |
| `vl_direct` | Pure VL LogsQL performance with no proxy layer |

The delta between `proxy (cold)` and `vl_direct` is the per-request proxy overhead:
~5µs LogQL→LogsQL translation + HTTP proxying + Loki response envelope wrapping.

To run the 4-way comparison:

```bash
# The script auto-builds the proxy binary and spawns a no-cache instance on port 3199.
# Both instances get pprof enabled for CPU/heap profiling during the run.
PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh

# Or pre-build for faster restarts across multiple runs:
go build -o /tmp/loki-vl-proxy ./cmd/proxy/
PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh

# Add jitter for realistic cache simulation (recommended):
PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh --jitter=2h

# Quick smoke test (2 minutes total):
./bench/run-comparison.sh --workloads=small --clients=10,50 --duration=10s
```

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


### Benchmark warmup design

The warmup phase runs at **full benchmark concurrency** with the same jitter as the
real run, so the proxy cache is populated across the same time-window distribution
that the benchmark will actually query. Warmup time is never counted in results.

```
--warmup=30s        # pre-warm for 30 seconds before the clock starts (default)
--jitter=2h         # warmup and bench both scatter into the same 2h window space
```

With low concurrency and large jitter (e.g. c=10, jitter=2h), even a long warmup
cannot cover all unique windows — this is visible in the long-range c=10 numbers where
the cold proxy outperforms warm. At c≥50 the windows overlap enough for the cache to
dominate, which is why throughput jumps dramatically.

### VictoriaLogs tuning for long-range queries

Long-range VL native is 0.4× slower than Loki at c=100 because 100 concurrent 7-day
columnar scans each allocate 100–200 MB of read buffers (10–20 GB peak), saturating
memory bandwidth and triggering the 60s query timeout (4.24% errors at c=100).

VL lacks Loki's internal query scheduler that limits concurrency automatically.
The recommended tuning (already applied in `test/e2e-compat/docker-compose.yml`):

| Flag | Value | Effect |
|------|-------|--------|
| `-search.maxConcurrentRequests` | `100` | Allow high bench concurrency; proxy acts as natural buffer in prod |
| `-search.maxQueueDuration` | `60s` | Queue rather than reject excess requests |
| `-search.maxQueryDuration` | `60s` | Cancel scans that exceed memory budget |
| `-blockcache.missesBeforeCaching` | `1` | Cache from first miss (default 2) |
| `-internStringCacheExpireDuration` | `15m` | Reduce GC pressure on label intern cache |
| `-memory.allowedPercent` | `75` | Increase in-process cache budget (default 60%) |

In production the proxy acts as a natural concurrency buffer — only cache-miss requests
reach VL, so real VL concurrency is far lower than the client-facing rate even at c=100.

---

## Cache Size Sizing Guide

### What `256 MB` default L1 covers

The default `-cache-max-bytes=268435456` (256 MB) holds roughly:

| Cache size | Approximate capacity | Eviction behavior |
|---|---|---|
| 256 MB (default) | 500–1,000 medium query results | LRU; hot dashboards stay warm, cold queries evict |
| 1 GB | 4,000–8,000 results | Large working set; multi-team dashboards stay warm |
| 4 GB | 16,000–32,000 results | Full-day working set for large teams rarely evicts |
| L2 disk (bbolt) | Any size; persistent across restarts | `~5–20ms` miss cost vs sub-µs L1, zero VL call on hits |

Average result size depends heavily on log volume per query. For `query_range` over
small time windows returning 100 log lines, expect `~100–300 KB` per result. For
label/series metadata queries, `~5–20 KB` per result.

### Configuring L1 + L2 for production

```bash
# 1 GB L1 in-memory + 10 GB L2 disk, cache survives pod restarts
-cache-max-bytes=1073741824 \
-disk-cache-path=/mnt/cache/proxy.db \
-disk-cache-max-bytes=10737418240
```

### Measuring eviction pressure

```promql
# Eviction rate — non-zero means L1 is too small
rate(loki_vl_proxy_cache_evictions_total[5m])

# If eviction rate > 0 and cache hit rate < 90%, increase -cache-max-bytes
rate(loki_vl_proxy_cache_hits_total[5m])
/
(rate(loki_vl_proxy_cache_hits_total[5m]) + rate(loki_vl_proxy_cache_misses_total[5m]))
```

Rule of thumb: `L1 size = (unique active queries per hour) × (average response size)`.

### Running the full suite

```bash
# Full 4-way suite: small, heavy, long_range, compute — 10/50/100 clients, 30s per level
PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh \
  --workloads=small,heavy,long_range,compute \
  --clients=10,50,100 \
  --duration=30s \
  --jitter=2h

# Version-tag results for tracking regressions across releases
PROXY_BINARY=/tmp/loki-vl-proxy ./bench/run-comparison.sh \
  --version=v1.18.0 \
  --workloads=small,heavy,long_range,compute \
  --clients=10,50,100

# VL with raised concurrency limit (already set in docker-compose)
# -search.maxConcurrentRequests=100 -search.maxQueueDuration=60s
```

Results are written to `bench/results/bench-<timestamp>.json` and `.md`.
pprof profiles land in `bench/results/pprof/` for post-run analysis.
