# Performance

## Architecture Optimizations

### Connection Pool Tuning

The proxy's HTTP transport is tuned for high-concurrency single-backend proxying:

| Setting | Value | Why |
|---|---|---|
| `MaxIdleConns` | 256 | Total idle connection pool size |
| `MaxIdleConnsPerHost` | 256 | All slots for VL (single backend) |
| `MaxConnsPerHost` | 0 (unlimited) | No artificial cap on concurrent VL connections |
| `IdleConnTimeout` | 90s | Reuse warm connections |
| `ResponseHeaderTimeout` | 120s | Allow slow VL queries to complete |
| `DisableCompression` | false | Allow negotiated upstream compression |

Go's default `MaxIdleConnsPerHost=2` causes ephemeral port exhaustion at >50 concurrent requests. Our tuning handles 200+ concurrent with 0 errors.

### Compression Path

The proxy now uses compression on multiple read-path hops:

| Hop | Current behavior |
|---|---|
| Client -> proxy response | Frontend stays on `gzip`/identity; the chart pins `-response-compression=gzip` with `-response-compression-min-bytes=1024` for broad Loki/Grafana compatibility |
| Proxy -> peer-cache owner | `/_cache/get` prefers `zstd`, then `gzip`, then identity for payloads `>=1KiB` |
| Proxy -> VictoriaLogs | `-backend-compression=auto` advertises `zstd, gzip`; the proxy decodes either safely before translation or passthrough |
| Disk cache | gzip-compressed value storage |
| OTLP push | `none`, `gzip`, or `zstd` |

Important: current VictoriaLogs docs clearly describe HTTP response compression, but not a guaranteed `zstd` query-response contract on the normal select path. The proxy can accept `zstd` from upstream when the backend provides it, but stock VictoriaLogs deployments may still return `gzip` or identity today.

Verification note: against Grafana `12.4.2`, the datasource proxy path
advertised `Accept-Encoding: deflate, gzip`, not `zstd`, in local
verification. That means the safe deployment default is frontend `gzip`,
and `-response-compression=auto` now keeps that same gzip path enabled for
legacy configs instead of maintaining a separate client-facing `zstd` mode.

The Tier0 compatibility cache now stores the canonical identity body and lazily
adds compressed variants on hot hits. That removes repeated gzip/zstd work from
the hot cached read path without forcing every cached response to occupy
multiple encodings up front.

### NDJSON Parsing Optimization

VL returns log results as newline-delimited JSON. The proxy converts this to Loki's streams format. Key optimizations:

| Technique | Effect |
|---|---|
| Byte scanning (not `strings.Split`) | Avoids copying entire body to string |
| `sync.Pool` for JSON entry maps | Reuses maps across NDJSON lines, reducing GC pressure |
| Pre-allocated slice capacity | Estimates line count from body size |
| Nanosecond timestamp via `strconv.FormatInt` | Avoids `fmt.Sprintf` allocation |

Result: **49% memory reduction** and **9.6% faster** parsing vs original implementation.

### Request Coalescing

When multiple clients send identical queries simultaneously, only 1 request reaches VL. Others wait for the shared result. Keys include tenant ID to prevent cross-tenant data sharing.

### Cache Topology

The proxy now has two cache layers in front of the backend execution path:

| Layer | Scope | Primary goal |
|---|---|---|
| Tier0 compatibility-edge cache | Final Loki-shaped `GET` responses on safe read endpoints | Bypass translation and backend work for hot repeated reads |
| L1/L2/L3 cache stack | Local memory, optional disk, optional peer fleet reuse | Reduce backend load and share results across replicas |

Tier0 is intentionally small and bounded as a percentage of the primary L1 memory budget. The deeper L1/L2/L3 stack still handles broader reuse, peer sharing, and persistent cache warming.

### What This Means For Operators

The simplest way to understand the cache stack is by operational outcome:

| Layer | Plain-English role | What it buys you |
|---|---|---|
| `Tier0` | Fast answer cache at the Loki-compatible frontend | Repeated Grafana reads can return before most proxy logic runs |
| `L1` memory | Hot cache inside the local process | Best-case latency for repeated dashboards and Explore refreshes |
| `L2` disk | Persistent local cache | Useful cache survives beyond RAM pressure and supports larger working sets |
| `L3` peer cache | Fleet-wide cache reuse between replicas | One warm pod can make the rest of the fleet faster and cheaper |

This is the difference between “it speaks Loki” and “it feels like Loki at runtime.” The project is designed to preserve Loki-compatible UX while reducing repeated backend work aggressively.

## Query-Range Tuning (Long-Range Efficiency)

Default tuning pattern:

- split long `query_range` requests into fixed windows (commonly `1h`)
- avoid caching near-now windows (or keep TTL very short)
- keep historical window cache TTL longer for reuse
- use adaptive bounded parallel fetch to improve range latency under healthy backend conditions

Recommended operator workflow:

1. Start with conservative adaptive bounds (for example min `2`, max `8`).
2. Observe backend latency/error EWMA and window cache hit ratio.
3. Increase max parallel only when backend latency/error stays stable.
4. Increase history TTL together with disk cache budget, not independently.

Key tuning signals:

- `loki_vl_proxy_window_cache_hit_total`
- `loki_vl_proxy_window_cache_miss_total`
- `loki_vl_proxy_window_fetch_seconds`
- `loki_vl_proxy_window_merge_seconds`
- `loki_vl_proxy_window_prefilter_attempt_total`
- `loki_vl_proxy_window_prefilter_error_total`
- `loki_vl_proxy_window_prefilter_kept_total`
- `loki_vl_proxy_window_prefilter_skipped_total`
- `loki_vl_proxy_window_prefilter_duration_seconds`
- `loki_vl_proxy_window_adaptive_parallel_current`
- `loki_vl_proxy_window_adaptive_latency_ewma_seconds`
- `loki_vl_proxy_window_adaptive_error_ewma`

Capacity approximation for history window caching:

`required_disk_bytes ~= unique_windows_per_day * avg_window_bytes * ttl_days`

Always cap disk cache explicitly with `-disk-cache-max-bytes` for predictable retention and node usage.

## Benchmark Results

Measured on Apple M3 Max (14 cores), Go 1.26.1, `-benchmem`.

### Long-Range Phase Program Benchmarks (1h split windows)

Command:

```bash
go test ./internal/proxy -run '^$' -bench 'BenchmarkQueryRangeWindowing_(NoPrefilter|WithPrefilter|StreamAwareBatching_Off|StreamAwareBatching_On)$' -benchmem -benchtime=10x
```

| Benchmark | ns/op | hits_calls/op | query_calls/op | max_inflight | allocs/op | Key result |
|---|---:|---:|---:|---:|---:|---|
| `NoPrefilter` | 39,525,225 | 0 | 49 | n/a | 11,582 | baseline full fanout |
| `WithPrefilter` | 11,672,412 | 48 | 9 | n/a | 10,346 | ~81.6% fewer backend query calls |
| `StreamAwareBatching_Off` | 17,048,771 | n/a | n/a | 4 | 9,490 | higher concurrency under load |
| `StreamAwareBatching_On` | 62,808,025 | n/a | n/a | 1 | 9,656 | lower concurrency spikes, more stable backend pressure |

Notes:

- Prefiltering is the largest direct backend-load reduction lever for sparse long ranges.
- Stream-aware batching intentionally trades raw synthetic throughput for lower backend saturation risk and fewer breaker cascades on real 2d/7d traffic.

### Per-Request Latency

| Operation | Latency | Allocs | Bytes/op |
|---|---|---|---|
| Labels (cache hit) | 2.0 us | 25 | 6.6 KB |
| QueryRange (cache hit) | 118 us | 600 | 142 KB |
| wrapAsLokiResponse | 2.8 us | 58 | 2.6 KB |
| VL NDJSON to Loki streams (100 lines) | 170 us | 3118 | 70 KB |
| LogQL translation | ~5 us | ~20 | ~2 KB |

### Cache Story In One Table

These are the numbers that matter most when you want to judge the value of the cache stack rather than the implementation details:

| Path | Slow path | Fast path | What it means |
|---|---|---|---|
| `query_range` | `4.58 ms` cold miss with delayed backend | `0.64-0.67 us` warm cache hit | Repeated dashboards stop behaving like backend-bound requests |
| `detected_field_values` | `2.76 ms` without Tier0 | `0.71 us` with Tier0 | Drilldown metadata becomes effectively instant after warm-up |
| `L1` memory cache | full handler/backend path | `45 ns` hit | Local hot cache is essentially free |
| `L2` disk cache | backend refill | `0.45 us` uncompressed read, `3.9 us` compressed read | Persistent cache is still cheap enough for hot-path reuse |
| `L3` peer cache | backend or owner re-fetch | `52 ns` warm shadow-copy hit | A warm 3-node fleet can reuse results instead of refetching them |

Tier0 is most valuable on metadata-style and Drilldown-style endpoints where the proxy still has meaningful compatibility work to skip. On `query_range`, the deeper primary cache is already so effective that Tier0 mostly preserves that win rather than multiplying it.

### Throughput

| Scenario | Concurrency | Throughput | Avg Latency | Errors |
|---|---|---|---|---|
| Cache hit (labels) | 100 | 175,726 req/s | 6 us | 0 |
| No cache, 10 concurrent | 10 | 9,823 req/s | 102 us | 0 |
| No cache, 50 concurrent | 50 | 17,791 req/s | 56 us | 0 |
| No cache, 200 concurrent | 200 | 33,659 req/s | 30 us | 0 |
| Cache miss (1ms backend) | 50 | 12,976 req/s | 80 us | 0 |

### Resource Usage at Scale

| Load (req/s) | CPU (est.) | Memory | Notes |
|---|---|---|---|
| 100 | &lt;1% | ~10 MB | Idle, mostly cache hits |
| 1,000 | ~8% | ~20 MB | Mixed cache hit/miss |
| 10,000 | ~30% | ~50 MB | Backend-bound |
| 30,000+ | ~100% | ~100 MB | CPU-bound, scale horizontally |

### Memory Stability

Under sustained load (10K requests, no cache):
- Total allocation: ~70 KB/request (GC reclaims between requests)
- Live heap growth: **&lt;1 MB** (no leak)
- GC handles ~200 cycles per 10K requests

1000-line NDJSON body (700 bytes/line, 700 KB input): **1.2 MB allocated** total.

## Test Coverage

| Test | What it verifies |
|---|---|
| `TestOptimization_VLLogsToLokiStreams_*` (7 tests) | Correctness of byte-scanned NDJSON parser |
| `BenchmarkProxy_Series_CompatCacheHit` / `BenchmarkProxy_Series_NoCompatCache` | Tier0 hit-path cost versus the uncached route-execution path |
| `TestPeerCache_ThreePeers_ShadowCopiesAvoidRepeatedOwnerFetches` | 3-node fleet reuses one owner fetch per non-owner after warm-up |
| `BenchmarkPeerCache_ThreePeers_ShadowCopyHit` | Warm non-owner reads stay local after the first peer shadow copy |
| `TestOptimization_SyncPool_NoStateLeak` | Pool doesn't leak labels between invocations |
| `TestOptimization_SyncPool_ConcurrentSafety` | 50 goroutines x 100 iterations, correct results |
| `TestOptimization_ConnectionPool_HighConcurrency` | 200 concurrent, 0 errors (port exhaustion regression) |
| `TestOptimization_FormatVLStep` | VL step format conversion (8 cases) |
| `TestOptimization_VLLogsToLokiStreams_ValidJSON` | 100-line output produces valid parseable JSON |
| `TestOptimization_NoMemoryLeak_SustainedLoad` | &lt;200 KB/req allocation after 10K requests |
| `TestOptimization_LargeBody_GCPressure` | 1000-line body within allocation budget |
| `TestLoad_NoCache_ScalingProfile` | 3 concurrency tiers, 0 errors |
| `TestLoad_WithCache_ScalingProfile` | Same tiers with cache enabled |

## Running Benchmarks

```bash
# All proxy benchmarks
go test ./internal/proxy/ -bench . -benchmem -run "^$" -count=3

# Focus on the new Tier0 path
go test ./internal/proxy/ -bench 'BenchmarkProxy_Series_(CompatCacheHit|NoCompatCache)$' -benchmem -run "^$" -count=3

# Focus on fleet cache warm shadow-copy behavior
go test ./internal/cache/ -bench 'BenchmarkPeerCache_ThreePeers_ShadowCopyHit$' -benchmem -run "^$" -count=3

# Load tests
go test ./internal/proxy/ -run "TestLoad" -v -timeout=120s

# Optimization regression tests
go test ./internal/proxy/ -run "TestOptimization" -v

# CPU profile
go test ./internal/proxy/ -bench BenchmarkVLLogsToLokiStreams -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Memory profile
go test ./internal/proxy/ -bench BenchmarkVLLogsToLokiStreams -memprofile=mem.prof
go tool pprof mem.prof
```

## Go Runtime Tuning

### GOMEMLIMIT

The Helm chart now injects `GOMEMLIMIT` at runtime. Resolution order:

1. `goMemLimit` when explicitly set
2. otherwise `goMemLimitPercent` of `resources.limits.memory`

In percentage mode, the chart computes bytes and exports that numeric value as `GOMEMLIMIT`.

```yaml
# Default: 70% of memory limit
goMemLimitPercent: 70   # 256Mi * 70% => GOMEMLIMIT=187904819 (bytes)

# Override with explicit value
goMemLimit: "500MiB"    # ignores goMemLimitPercent
```

Supported memory-limit units for percentage mode are integer quantities with `Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E`. If `resources.limits.memory` is missing or unsupported, the chart does not inject a computed `GOMEMLIMIT`.

### GOGC

`GOGC=100` (Go default) is set explicitly. Increase for less CPU overhead at the cost of more memory, or decrease for tighter memory control.

### Go Runtime Metrics Exposed

The `/metrics` endpoint exposes Go runtime and GC statistics:

```
go_memstats_alloc_bytes      # current heap allocation
go_memstats_sys_bytes        # total OS memory
go_memstats_heap_inuse_bytes # heap in use
go_memstats_heap_idle_bytes  # heap idle
go_goroutines                # active goroutines
go_gc_cycles_total           # completed GC cycles
```

## CI Integration

The `bench` job in `.github/workflows/ci.yaml` runs all benchmarks and load tests on every push. It:
1. Runs benchmarks 3x for stability
2. Runs load tests at all concurrency tiers
3. Fails the build if load tests produce errors (regression gate)
4. Uploads results as CI artifacts for historical tracking

The next CI step is to add compose-backed e2e cache/fleet smoke runs for pull requests and post-merge `main` builds, so Tier0 behavior and 3-node peer-cache gains are validated against the full Grafana + proxy + VictoriaLogs stack rather than only unit/load environments.

For `TestLoad_HighConcurrency_MemoryStability`, the throughput expectation is `>10k req/s` in local environments and `>5k req/s` on shared CI runners (`CI=true`) to reduce race-mode noise while still catching major regressions.
