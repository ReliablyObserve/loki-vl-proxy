---
sidebar_label: Benchmarks
description: "Six-workload read-path comparison: Loki vs VL+Proxy (warm/cold) vs VL native. CPU, RSS, P50/P90/P99 latency, and throughput across metadata, heavy, long-range, and compute workloads."
---

# Benchmarks

**Hardware:** Apple M5 Pro, 18 cores, 64 GB RAM, macOS 26.4.1, Go 1.26.2 darwin/arm64, Docker Desktop 29.4.0 (17.3 GiB allocated to Docker).

**Stack:** Loki 3.4.x, VictoriaLogs v1.50.0, loki-vl-proxy latest. ~8 M log entries across 15 services, 7-day window.

**VictoriaLogs flags:** `-defaultParallelReaders=8 -fs.maxConcurrency=64 -memory.allowedPercent=80 -search.maxConcurrentRequests=100 -search.maxQueueDuration=60s`. See [VictoriaLogs tuning](#victorialogs-tuning) for rationale.

**Loki flags:** `querier.max_concurrent=16`, `max_query_parallelism=64`, result + chunk caching enabled.

## Running Benchmarks

```bash
# Warm-cache run (standard — proxy cache pre-warmed at benchmark concurrency)
loki-bench \
  --proxy=http://localhost:3100 \
  --loki=http://localhost:3200 \
  --vl-direct=http://localhost:9428 \
  --workloads=small,heavy,long_range,compute \
  --clients=10,50,100 \
  --duration=30s \
  --warmup=5s \
  --jitter=2h

# Unique-windows run (cache + coalescer both defeated — raw proxy overhead)
loki-bench \
  --proxy=http://localhost:3100 \
  --loki=http://localhost:3200 \
  --vl-direct=http://localhost:9428 \
  --workloads=small,heavy,long_range,compute \
  --clients=10,50,100 \
  --duration=30s \
  --unique-windows
```

## Workload definitions

| Workload | What it covers |
|----------|---------------|
| **small** | Label queries, series, `query_range` 1–5 min, instant queries — Grafana label browser / small panel refreshes |
| **heavy** | Complex LogQL with pipelines (`json`, `line_format`, `label_format`), filters, `label_filter` — content search and alerting queries |
| **long\_range** | 6 h–72 h `query_range`, `rate`/`count`/`bytes_rate` over long windows, metadata over long windows — Drilldown/Explore historical analysis |
| **compute** | Metric aggregations (`sum by`, `rate`, `count_over_time`, `quantile_over_time`, `topk`) — dashboard panels showing metrics derived from logs |

---

## Warm cache — production steady state

Proxy cache is pre-warmed at the same concurrency as the measurement. Repeated queries are served from L1 memory without touching VictoriaLogs. This is the operating mode for Grafana dashboards auto-refreshing every 30 s.

### Throughput (req/s)

| Workload | Concurrency | Loki | Proxy warm | VL native | Proxy / Loki |
|----------|:-----------:|-----:|-----------:|----------:|:------------:|
| small | 10 | 2,011 | 15,626 | 2,342 | **7.8×** |
| small | 50 | 2,297 | 24,958 | 4,484 | **10.9×** |
| small | 100 | 2,290 | 27,513 | 4,380 | **12.0×** |
| heavy | 10 | 407 | 5,944 | 1,064 | **14.6×** |
| heavy | 50 | 302 | 6,403 | 1,203 | **21.2×** |
| heavy | 100 | 162 | 7,134 | 1,245 | **44.1×** |
| long\_range | 10 | 8 | 157 | 88 | **18.7×** |
| long\_range | 50 | 11 | 201 | 87 | **18.0×** |
| long\_range | 100 | 16 | 220 | 95 | **14.0×** |
| compute | 10 | 2,803 | 11,162 | 1,554 | **4.0×** |
| compute | 50 | 2,233 | 13,484 | 1,588 | **6.0×** |
| compute | 100 | 1,611 | 16,456 | 1,465 | **10.2×** |

### P50 latency

| Workload | Concurrency | Loki | Proxy warm | VL native |
|----------|:-----------:|-----:|-----------:|----------:|
| small | 10 | 4 ms | 587 µs | 2 ms |
| small | 50 | 20 ms | 1 ms | 5 ms |
| small | 100 | 42 ms | 3 ms | 8 ms |
| heavy | 10 | 4 ms | 1 ms | 5 ms |
| heavy | 50 | 22 ms | 6 ms | 27 ms |
| heavy | 100 | 3 ms† | 12 ms | 66 ms |
| long\_range | 10 | 481 ms | 1 ms | 102 ms |
| long\_range | 50 | 4,211 ms | 1 ms | 403 ms |
| long\_range | 100 | 4,902 ms | 1 ms | 771 ms |
| compute | 10 | 1 ms | 675 µs | 6 ms |
| compute | 50 | 6 ms | 2 ms | 24 ms |
| compute | 100 | 4 ms | 4 ms | 57 ms |

† Loki heavy c=100: P50=3 ms is misleading — Loki was saturated (P90=1,818 ms, P99=6,950 ms).

### P90 latency

| Workload | Concurrency | Loki | Proxy warm | VL native |
|----------|:-----------:|-----:|-----------:|----------:|
| small | 10 | 10 ms | 917 µs | 8 ms |
| small | 50 | 37 ms | 2 ms | 27 ms |
| small | 100 | 71 ms | 4 ms | 61 ms |
| heavy | 10 | 81 ms | 3 ms | 15 ms |
| heavy | 50 | 471 ms | 11 ms | 59 ms |
| heavy | 100 | 1,818 ms | 18 ms | 106 ms |
| long\_range | 10 | 3,872 ms | 299 ms | 202 ms |
| long\_range | 50 | 12,868 ms | 1,491 ms | 1,317 ms |
| long\_range | 100 | 70,700 ms | 2,788 ms | 2,386 ms |
| compute | 10 | 11 ms | 1 ms | 9 ms |
| compute | 50 | 71 ms | 4 ms | 61 ms |
| compute | 100 | 243 ms | 6 ms | 107 ms |

### P99 latency

| Workload | Concurrency | Loki | Proxy warm | VL native |
|----------|:-----------:|-----:|-----------:|----------:|
| small | 10 | 18 ms | 1 ms | 29 ms |
| small | 50 | 53 ms | 4 ms | 68 ms |
| small | 100 | 92 ms | 7 ms | 175 ms |
| heavy | 10 | 128 ms | 6 ms | 58 ms |
| heavy | 50 | 981 ms | 25 ms | 261 ms |
| heavy | 100 | 6,950 ms | 45 ms | 306 ms |
| long\_range | 10 | 4,923 ms | 751 ms | 252 ms |
| long\_range | 50 | 19,586 ms | 3,189 ms | 1,861 ms |
| long\_range | 100 | 89,306 ms | 5,917 ms | 3,325 ms |
| compute | 10 | 21 ms | 3 ms | 14 ms |
| compute | 50 | 145 ms | 6 ms | 127 ms |
| compute | 100 | 521 ms | 11 ms | 207 ms |

### CPU consumed (cpu·s over 30 s window)

| Workload | Concurrency | Loki | Proxy only | Proxy + VL | Ratio vs Loki |
|----------|:-----------:|-----:|-----------:|-----------:|:-------------:|
| small | 10 | 330.6 | 0.08 | 0.81 | 408× less |
| small | 50 | 415.4 | 0.15 | 2.75 | 151× less |
| small | 100 | 415.3 | 0.16 | 5.31 | 78× less |
| heavy | 10 | 320.9 | 0.05 | 0.91 | 355× less |
| heavy | 50 | 306.3 | 0.07 | 1.68 | 182× less |
| heavy | 100 | 96.8 | 0.06 | 1.19 | 81× less |
| long\_range | 10 | 43.9 | 0.18 | 32.6 | 1.35× less |
| long\_range | 50 | 62.1 | 0.29 | 50.2 | 1.23× less |
| long\_range | 100 | 438.3 | 0.30 | 50.6 | 8.66× less |
| compute | 10 | 315.5 | 0.08 | 10.4 | 30× less |
| compute | 50 | 399.9 | 0.15 | 58.0 | 6.9× less |
| compute | 100 | 379.7 | 0.18 | 63.7 | 6.0× less |

The proxy process itself consumes negligible CPU — the gains come from VL being more efficient than Loki for the same queries, amplified by the cache eliminating most backend calls entirely.

### RSS memory (MB, peak during 30 s window)

| Workload | Concurrency | Loki | Proxy | Proxy + VL | Ratio vs Loki |
|----------|:-----------:|-----:|------:|-----------:|:-------------:|
| small | 10 | 1,910 | 484 | 726 | 2.6× less |
| small | 50 | 2,159 | 454 | 800 | 2.7× less |
| small | 100 | 2,215 | 429 | 782 | 2.8× less |
| heavy | 10 | 2,082 | 418 | 640 | 3.3× less |
| heavy | 50 | 2,269 | 364 | 584 | 3.9× less |
| heavy | 100 | 1,650 | 371 | 636 | 2.6× less |
| long\_range | 10 | 1,957 | 1,072 | 1,373 | 1.4× less |
| long\_range | 50 | 1,737 | 768 | 1,754 | ~parity |
| long\_range | 100 | 2,004 | 1,252 | 2,082 | ~parity |
| compute | 10 | 2,340 | 353 | 733 | 3.2× less |
| compute | 50 | 2,437 | 362 | 766 | 3.2× less |
| compute | 100 | 2,317 | 370 | 720 | 3.2× less |

Long-range memory parity (c=50, c=100) reflects the GOMEMLIMIT=2 GiB fix applied before this run. Without the limit, proxy RSS reached 4,466 MB at c=100 for long-range; with it, VL can scan the same 7-day windows within a bounded footprint.

---

## Cold cache, unique queries — honest worst case

Every worker gets a distinct non-overlapping time window. This defeats both the singleflight coalescer and the response cache. What remains is raw proxy overhead: LogQL→LogsQL translation (~5 µs) + HTTP proxying + response shaping.

### Throughput (req/s)

| Workload | Concurrency | Loki | Proxy cold | VL native | Proxy / Loki |
|----------|:-----------:|-----:|-----------:|----------:|:------------:|
| small | 10 | 1,080 | 1,201 | 2,957 | **1.11×** |
| small | 50 | 1,369 | 1,343 | 3,637 | **0.98× (parity)** |
| heavy | 10 | 133 | 179 | 829 | **1.34×** |
| heavy | 50 | 193† | 182 | 859 | **1.47× on delivered**‡ |
| long\_range | 10 | 9 | 19 | 82 | **2.06×** |
| long\_range | 50 | 9 | 19 | 84 | **2.05×** |
| long\_range | 100 | 13 | 24 | 85 | **1.86×** |
| compute | 10 | 2,281 | 352 | 1,462 | 0.15× |
| compute | 50 | 1,633 | 336 | 1,455 | 0.21× |
| compute | 100 | 899 | 366 | 1,431 | 0.41× |

† Loki heavy c=50: 35.63% error rate — saturated under unique-window load. Successful throughput: ~124 req/s.<br />
‡ 182 proxy req/s (0 errors) vs ~124 Loki successful req/s = 1.47× on delivered traffic.

### P50 latency (unique-windows)

| Workload | Concurrency | Loki | Proxy cold | VL native |
|----------|:-----------:|-----:|-----------:|----------:|
| small | 10 | 7 ms | 4 ms | 1 ms |
| small | 50 | 30 ms | 14 ms | 5 ms |
| heavy | 10 | 22 ms | 21 ms | 6 ms |
| heavy | 50 | 5 ms† | 128 ms | 41 ms |
| long\_range | 10 | 464 ms | 39 ms | 99 ms |
| long\_range | 50 | 5,041 ms | 2,060 ms | 392 ms |
| long\_range | 100 | 4,276 ms | 3,068 ms | 826 ms |
| compute | 10 | 1 ms | 10 ms | 6 ms |
| compute | 50 | 4 ms | 107 ms | 28 ms |
| compute | 100 | 6 ms | 261 ms | 63 ms |

### CPU consumed (unique-windows, cpu·s over 30 s)

| Workload | Concurrency | Loki | Proxy only | Proxy + VL | Ratio vs Loki |
|----------|:-----------:|-----:|-----------:|-----------:|:-------------:|
| small | 100 | 412.1 | 0.21 | 212.8 | 1.9× less |
| heavy | 10 | 251.9 | 0.23 | 230.4 | 1.1× less |
| heavy | 50 | 258.4 | 0.30 | 257.7 | ~parity |
| heavy | 100 | 308.2 | 0.29 | 251.3 | 1.2× less |
| long\_range | 10 | 61.8 | 0.04 | 170.2 | 0.36× (VL scans more in parallel) |
| long\_range | 50 | 72.9 | 0.04 | 188.2 | 0.39× |
| long\_range | 100 | 180.7 | 0.06 | 213.5 | 0.85× |
| compute | 10 | 351.5 | 0.15 | 229.7 | 1.5× less |
| compute | 50 | 377.8 | 0.20 | 274.7 | 1.4× less |
| compute | 100 | 326.6 | 0.18 | 278.0 | 1.2× less |

### RSS memory (unique-windows, MB)

| Workload | Concurrency | Loki | Proxy | Proxy + VL |
|----------|:-----------:|-----:|------:|-----------:|
| small | 100 | 2,380 | 633 | 1,204 |
| heavy | 10 | 2,044 | 886 | 1,236 |
| heavy | 50 | 2,214 | 910 | 1,284 |
| heavy | 100 | 2,456 | 979 | 1,386 |
| long\_range | 10 | 2,174 | 880 | 1,754 |
| long\_range | 50 | 1,923 | 768 | 1,754 |
| long\_range | 100 | 2,004 | 1,252 | 2,082 |
| compute | 10 | 2,471 | 843 | 1,214 |
| compute | 50 | 2,463 | 821 | 1,356 |
| compute | 100 | 2,131 | 671 | 993 |

---

## Cold overhead by workload type

What determines proxy performance when cache and coalescer provide no help:

**Small (metadata):** Proxy **beats Loki at c=10** (1,201 vs 1,080 req/s, **1.11×**) and reaches parity at c=50 (1,343 vs 1,369 req/s). VL native is ~2.7× faster than Loki (2,957 req/s at c=10); the proxy's extra HTTP hop and envelope conversion is the gap between proxy and raw VL. The windowing NDJSON parser was ported to fastjson (eliminating `map[string]interface{}` allocation per entry) to achieve this cold-path parity. With any cache warmth, this reverses strongly (12× warm).

**Heavy (pipeline queries):** Proxy cold outperforms Loki at both measured concurrency levels. At c=10: 179 vs 133 req/s (1.34× faster). At c=50: Loki saturates with 35.63% errors (successful throughput ~124 req/s) while the proxy handles 182 req/s with zero errors — 1.47× more successful traffic delivered. The fastjson NDJSON parser (no `map[string]interface{}` per entry) and background pattern autodetect (offloaded from the request critical path) were the key cold-path improvements; total proxy CPU dropped 22.8%. VL native remains ~4–5× faster than the proxy; the remaining gap is the network round-trip for each sub-window request.

**Long-range (6 h–72 h windows):** Proxy is **1.86–2.06× faster than Loki even cold**. VL's parallel window fetching within the proxy — splitting long ranges into parallel 1 h sub-windows — completes before Loki can scan its chunk store sequentially. This advantage is structural and does not require cache.

**Compute (metric aggregations):** The `stats_query_range` fast path routes `sum by (...) (count_over_time/rate({...}[W]))` and `bytes_over_time/bytes_rate` queries directly to VL's pre-aggregated Prometheus buckets, eliminating the raw NDJSON log scan that was 39% CPU in cold pprof. This delivered the headline improvement in this PR: heavy cold throughput 44→126 req/s (c=10, +2.9×) and 33→139 req/s (c=100, +4.2×). A follow-up round of pprof-guided allocation fixes (pooled fastjson scratch buffers, zero-alloc label map serialization, pre-computed stream keys, direct byte-building replacing `json.Marshal` reflection) eliminated ~20 GB of per-request allocations and raised cold `rate`/`topk` throughput from ~40 req/s to **210 req/s** (+5.25× on the loki-bench compute workload). For complex aggregations without a VL-native equivalent (`quantile_over_time`, `topk`, `sum by` with pipeline stages), the proxy still decomposes the query into N parallel sub-window fetches and aggregates locally. With warm cache (24 h TTL on historical windows), all compute queries hit cache on repeat and the structural overhead disappears.

---

## VictoriaLogs tuning

Long-range columnar scans are I/O-bound and goroutine-heavy. The default `-defaultParallelReaders=2×CPU` (36 on 18 cores) creates 3,600 goroutines at c=100, causing context-switch overhead that degrades throughput for all workloads. Reducing to 8 cuts goroutines to ~800 and improves small query throughput dramatically without harming long-range scan capacity.

| Flag | Value | Effect |
|------|-------|--------|
| `-defaultParallelReaders` | `8` | Critical: limit goroutines per query |
| `-fs.maxConcurrency` | `64` | Cap concurrent file ops |
| `-memory.allowedPercent` | `80` | Increase block cache budget (default 60%) |
| `-search.maxConcurrentRequests` | `100` | Allow high bench concurrency |
| `-search.maxQueueDuration` | `60s` | Queue rather than reject excess requests |
| `-search.maxQueryDuration` | `60s` | Cancel scans that exceed memory budget |
| `-blockcache.missesBeforeCaching` | `1` | Cache from first miss (default 2) |
| `-internStringCacheExpireDuration` | `15m` | Reduce GC pressure on label intern cache |

These flags are already applied in `test/e2e-compat/docker-compose.yml`. In production, the proxy cache further reduces effective VL concurrency — only cache-miss requests reach VL, so real VL concurrency is far lower than the client-facing rate.

---

## Per-request proxy overhead (microbenchmarks)

| Operation | Latency | Allocs | Bytes/op |
|-----------|--------:|-------:|---------:|
| Labels (cache hit) | 2.0 µs | 25 | 6.6 KB |
| QueryRange (cache hit) | 118 µs | 600 | 142 KB |
| LogQL translation | ~5 µs | ~20 | ~2 KB |
| VL NDJSON → Loki streams (100 lines) | 170 µs | 3,118 | 70 KB |
| wrapAsLokiResponse | 2.8 µs | 58 | 2.6 KB |

```bash
# Run microbenchmarks
go test ./internal/proxy/ -bench . -benchmem -run "^$" -count=3
go test ./internal/translator/ -bench . -benchmem -run "^$" -count=3
go test ./internal/cache/ -bench . -benchmem -run "^$" -count=3
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
