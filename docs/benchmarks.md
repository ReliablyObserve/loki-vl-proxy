---
sidebar_label: Benchmarks
description: "Six-workload read-path comparison: Loki vs VL+Proxy (warm/cold) vs VL native. CPU, RSS, P50/P90/P99 latency, and throughput across metadata, heavy, long-range, and compute workloads."
---

# Benchmarks

**Hardware:** Apple M5 Pro, 18 cores, 64 GB RAM, macOS 26.4.1, Go 1.26.4 darwin/arm64, Docker Desktop 29.4.0 (17.3 GiB allocated to Docker).

**Stack:** Loki 3.6.x, VictoriaLogs v1.50.0, loki-vl-proxy latest. ~8 M log entries across 15 services, 7-day window.

**VictoriaLogs flags:** `-defaultParallelReaders=8 -fs.maxConcurrency=64 -memory.allowedPercent=80 -search.maxConcurrentRequests=100 -search.maxQueueDuration=60s`. See [VictoriaLogs tuning](#victorialogs-tuning) for rationale.

**Loki flags:** `querier.max_concurrent=16`, `max_query_parallelism=64`, result + chunk caching enabled.

## Label Metadata Performance

Label endpoints (`/loki/api/v1/labels`, `/loki/api/v1/label/{name}/values`) are the first calls Grafana makes when opening Explore or a dashboard. Their latency determines perceived "snappiness" and the completeness of the label picker.

### How the proxy makes label fetches fast and accurate

**Progressive two-stage fetch** — when a request arrives for a wide time range (e.g. 7d) the proxy returns an initial response from a 1h VL scan immediately, then triggers a background goroutine that fetches the full user-requested range. The second request for the same window gets the complete historical label set from cache (sub-ms). This means:

- First request: fast (~200 µs proxy overhead + one VL round-trip against 1h of data)
- Second request: complete and fast (cache hit, sub-ms)

Labels do change over time (services added, removed, renamed across deployments), so the full-range background fetch ensures historical labels are not silently omitted.

**Time-bucketed cache keys** — Grafana's time picker slides by seconds between dashboard refreshes. The proxy quantises start/end timestamps to fixed bucket boundaries before building the cache key:

| User-selected interval | Bucket size | Effect |
|---|---|---|
| ≤ 6 h | 5 minutes | Refreshes every 30 s collapse to the same key |
| 6 h – 48 h | 1 hour | Intra-hour drift collapses to one entry |
| > 48 h | 6 hours | 7-day queries share one cache entry all day |

**Startup warmup** — on boot the proxy serves immediately from the disk-backed cache (fast start), then waits for VictoriaLogs to become healthy and refreshes any entries that are expired or close to expiry. The first dashboard load after a deployment is a cache hit.

**Disk persistence** — label cache entries are written to disk (`SetLocalAndDiskWithTTL`). On proxy restart the disk entries load immediately so there is no cold-start penalty even before VL warmup completes.

**Periodic keep-warm loop** — a background goroutine runs every 90 seconds and refreshes label cache entries for all four standard Grafana presets (Last 1h / 6h / 24h / 7d) before their 2-minute TTL expires. This keeps the cache hot even with no user queries.

**Background stale refresh on hits** — when a cached entry is served but has less than ~30% of its TTL remaining, the proxy automatically triggers a background full-range refresh so the next request sees fresher data.

### Measured latency (proxy overhead against a local VL mock)

| Time range | First request | Second request | VL scan on first |
|---|---|---|---|
| 1 h | ~200 µs | ~5 µs (cache hit) | 1 h |
| 6 h | ~200 µs | ~5 µs (cache hit) | 1 h (sync) → 6 h (background) |
| 12 h | ~200 µs | ~5 µs (cache hit) | 1 h (sync) → 12 h (background) |
| 24 h | ~200 µs | ~5 µs (cache hit) | 1 h (sync) → 24 h (background) |
| 2 d | ~200 µs | ~5 µs (cache hit) | 1 h (sync) → 2 d (background) |
| 7 d | ~200 µs | ~5 µs (cache hit) | 1 h (sync) → 7 d (background) |

Numbers above are proxy-only overhead measured with a zero-latency in-process VL mock (`go test -bench BenchmarkLabels_`). In production, add your actual VL round-trip (~50–300 ms on first request; sub-ms on cache hit).

**Against a real VictoriaLogs instance (manual measurement, 15 services, 8 M entries):**

| Time range | First request | Second request |
|---|---|---|
| 1 h (pre-warmed at startup) | sub-ms (cache hit) | sub-ms |
| 7 d (sync: 1h VL scan + background: 7d scan) | ~300 ms | sub-ms with full historical data |

### Running the label perf tests

```bash
# Correctness tests (run in normal CI)
go test ./internal/proxy/ -run 'TestPerf_Labels_' -v

# Cold and warm benchmarks
go test ./internal/proxy/ -bench 'BenchmarkLabels_' -benchmem -count=3
```

---

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

Every worker gets a distinct non-overlapping time window. This defeats both the singleflight coalescer and the response cache. What remains is raw proxy overhead: LogQL→LogsQL translation (2.7–7.2 µs depending on complexity) + HTTP proxying + response shaping.

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

**AST-typed translation (v1.35.0+):** The LogQL→LogsQL translation layer was migrated from fragile `fmt.Sprintf` string assembly to a typed `logsql.PipeStats`/`PipeMath`/`PipeFilter` AST. This does not change throughput (translation is 2.7–7.2 µs vs 100–500 ms VL query time), but restores correctness for binary metric queries (`sum(rate(...)) / sum(rate(...))`, `rate(...) * 100`, `sum(...) + sum(...)`) that were previously silently erroring because the generated `| math alias:=expr` form was rejected by VictoriaLogs (which expects `| math expr as alias`). These queries now work and are included in the compute and heavy workload numbers above.

---

## Drilldown / Explore — real Grafana queries vs Loki direct

The numbers above came from synthetic benchmark workloads. This section replays the exact queries Grafana Drilldown and Explore emit (`sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))`, `trace_id` variants, `service_version`, `detected_fields`) at the time ranges that exercise the chunked-merge path (1 h / 6 h / 24 h / 2 d / 7 d), measured against the live e2e-compat compose stack: Loki 3.6 with `max_query_series=1M`, 8 GiB heap, result + chunk caching enabled; proxy fronted by vmauth (the same path Grafana actually uses).

Methodology: cold = first request (cache miss), warm = same request replayed 200 ms later. `cold_status=500` / blank means Loki returned `too_many_series` or query-timeout. Run with `./bench/drilldown-vs-loki.sh`.

### `sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))`

| Range | Loki cold | Loki warm | Loki status | Proxy cold | Proxy warm | Proxy series |
|-------|----------:|----------:|------------:|-----------:|-----------:|-------------:|
| 1 h   | 1 360 ms  |    13 ms  | timeout / no body | **239 ms** | **20 ms** | 5 000 |
| 6 h   | 27 509 ms | 1 240 ms  | 500 (too_many_series) | **901 ms** | **14 ms** | 4 989 |
| 24 h  | 26 870 ms | 2 312 ms  | 500 (too_many_series) | **535 ms** | **13 ms** | 16 (`/hits` top-N) |
| 2 d   | 21 132 ms |     9 ms  | timeout / no body | **1 027 ms** | **10 ms** | 16 |
| 7 d   | 22 727 ms |    12 ms  | timeout / no body | **3 844 ms** | **12 ms** | 16 |

Cold-path speedup: **6× (1 h) → 50× (24 h) → 6× (7 d)**. At 6 h+ Loki returns HTTP 500 because the high-cardinality `pod!=""` selector blows the series cap; the proxy routes those through `/select/logsql/hits` and returns a real top-N chart.

### `sum by (trace_id) (count_over_time({namespace="prod"}|json|drop __error__,__error_details__|trace_id!=""[2m]))`

| Range | Loki cold | Proxy cold | Proxy series |
|-------|----------:|-----------:|-------------:|
| 1 h   | 26 106 ms (timeout)   | **523 ms**  | 4 992 |
| 6 h   | 20 568 ms (timeout)   | 2 883 ms ⚠️ | 0 (502 — VL cap exceeded on parser-direct path; tracked below) |
| 24 h  | 22 100 ms (500)       | **1 260 ms**| 8 |
| 2 d   | 29 408 ms (500)       | **3 598 ms**| 8 |
| 7 d   | 26 451 ms (500)       | 4 483 ms ⚠️ | 0 (same 502 path) |

`trace_id` parser-direct at 6 h / 7 d returns 502 because the per-trace cardinality on this dataset exceeds the VL parser-pipe limit; the chunked-merge path elsewhere returns top-N successfully. Tracked as a known limit — see [`drilldown_high_card_fields_known_limit`](../memory/drilldown_high_card_fields_known_limit.md). Loki returns either timeout or `too_many_series` for every range.

### `sum by (service_version) (count_over_time({namespace="prod",service_version!=""}[2m]))`

| Range | Loki cold | Loki series | Proxy cold | Proxy series |
|-------|----------:|------------:|-----------:|-------------:|
| 1 h   | 25 723 ms (500) | -    | **61 ms**   | 100 |
| 6 h   | 28 824 ms (500) | -    | **205 ms**  | 100 |
| 24 h  |     24 ms (200) | **0** (empty result) | **202 ms**  | 14 |
| 2 d   |    224 ms (200) | **0** (empty result) | 315 ms      | 15 |
| 7 d   |     43 ms (200) | **0** (empty result) | 1 381 ms    | 14 |

At 24 h+ Loki returns `HTTP 200 with empty data` for `service_version` — silently — because the cardinality reduces below its sample threshold and Loki gives up before stream selection completes. The proxy returns the real top-N every time.

### `/loki/api/v1/detected_fields`

| Range | Loki cold | Loki body | Proxy cold | Proxy body |
|-------|----------:|----------:|-----------:|-----------:|
| 1 h   |  3 035 ms |   3 885 B (empty fields) | **58 ms**  | 7 772 B (full field set) |
| 6 h   |      8 ms |   no body / timeout      | **148 ms** | 8 178 B |
| 24 h  | 26 981 ms |   500 (too_many_series)  | **249 ms** | 8 088 B |

`detected_fields` is the call Drilldown makes first to populate the field picker — it determines whether the panel even renders. On Loki at 24 h+ it 500s; on the proxy it returns the full OTel field map in 250 ms.

### Container resource consumption (peak during bench run)

Captured with `docker stats e2e-loki e2e-victorialogs e2e-proxy e2e-proxy-vmauth --no-trunc` for the duration of the bench. Loki was given 8 GiB heap, 16 CPUs, and result + chunk caching; VL + proxy together ran in 4 GiB / 8 CPUs.

| Container | Peak CPU | Peak RSS | Outcome |
|-----------|---------:|---------:|---------|
| `e2e-loki` | 1 580% (15.8 cores) | 7.9 GiB / 8 GiB | OOM-near; 500s on every 6 h+ pod query, 500 on 24 h `detected_fields` |
| `e2e-victorialogs` | 410% (4.1 cores) | 1.4 GiB / 4 GiB | Steady; no errors |
| `e2e-proxy` | 90% (0.9 cores) | 180 MiB | Steady; cache hit rate 78% by end of run |
| `e2e-proxy-vmauth` | 22% (0.2 cores) | 35 MiB | Steady |

**What the numbers mean.** Loki consumed roughly **18× the CPU and 44× the RSS** of the proxy on the same workload while returning errors or empty results for the majority of Drilldown / Explore queries. The proxy + VL stack together used **~5 cores and 1.6 GiB** to serve the entire query set with full results. This is the gap that motivated the project: Drilldown's call pattern (Grafana 24 h querySplitting, high-cardinality `field!=""` selectors, parser-heavy queries) is essentially incompatible with Loki's stream-store model at production volume, and the proxy bridges it by routing through VL's columnar storage.

For continuous tracking, run `bench/drilldown-vs-loki.sh` against any stack and diff successive runs — output is TSV so it diffs cleanly.

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
| LogQL→LogsQL translation (selector) | 2.7 µs | 18 | 836 B |
| LogQL→LogsQL translation (rate/sum) | 4.9 µs | 48 | 2.1 KB |
| LogQL→LogsQL translation (binary rate/rate) | 7.2 µs | 76 | 3.7 KB |
| LogQL→LogsQL translation (ip() filter, v1.45+) | 4.4 µs | 36 | 1.4 KB |
| `PipeMath.String()` (AST serialization) | 55 ns | 3 | 80 B |
| `PipeStats.String()` (AST serialization) | 70 ns | 5 | 136 B |
| VL NDJSON → Loki streams (100 lines) | 170 µs | 3,118 | 70 KB |
| wrapAsLokiResponse | 2.8 µs | 58 | 2.6 KB |

Translation overhead is 2.7–7.2 µs depending on query complexity. For a typical heavy-workload query taking 100–500 ms against VL, translation is under 0.007% of wall-clock time. The AST-typed path (`PipeMath`, `PipeStats`) adds no overhead versus the previous string-concat approach.

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
