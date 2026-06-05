---
sidebar_label: Honest TLDR
description: "Full bench numbers behind the README one-glance comparison — workload, dataset, outcome counts, latency, resource consumption, heap-pool deep dive, and the queries Loki structurally cannot serve."
---

# Honest TLDR — measured 2026-06-05 on the included e2e stack

Real numbers from `bench/drilldown-vs-loki.sh` against the live `test/e2e-compat` compose stack. This is the long-form companion to the [README one-glance comparison](https://github.com/ReliablyObserve/loki-vl-proxy#one-glance-comparison); the README has the headline ratios, this page has the per-query breakdown, methodology, and tuning we tried.

## Workload + dataset

**Workload:** 14 minutes of Grafana Logs Drilldown traffic against `namespace=prod`. **36 cold queries** at 30 m / 1 h / 2 h / 3 h / 6 h / 24 h ranges, covering the six call shapes Drilldown actually emits (`sum by pod`, `sum by trace_id`, `sum by k8s_pod_name`, `sum by service_version`, `/detected_fields`, `/labels`). Each query sent with the real Grafana headers (`X-Query-Tags: Source=grafana-lokiexplore-app`, `User-Agent: Grafana/11.5.0`) so Loki's [own partial-results carve-out](https://github.com/grafana/loki/blob/release-3.6.x/pkg/util/httpreq/tags.go) is active.

**Dataset:** 8 M log entries across 15 services over 7 days. `namespace=prod` carries ~5 000 active pods, ~50 000 unique `trace_id` values per hour, plus the usual app / level / app_kind / k8s.* labels. Host: Apple M5 Pro, 64 GB system RAM, Docker Desktop allocated 17.3 GiB.

**Important caveat about Loki's container budget.** The first runs of this bench gave Loki the historical 8 GiB container limit and Loki OOM-looped continuously — 35 restarts in one session, 0 successful queries. That was a *setup* artifact: 8 GiB is too little for a 7-day dataset with 6.7 GiB of stored bigParts (Loki just idling consumed 7.6 GiB). After bumping to **12 GiB with `GOMEMLIMIT=10GiB` + `GOGC=80`** Loki actually serves queries. All numbers below are from the fair (12 GiB) run — committed at `test/e2e-compat/results/drilldown-vs-loki-fair-*.tsv`.

## One-glance comparison (full table)

|  | Loki direct | VictoriaLogs | Proxy | vmauth (optional gateway) |
|---|---:|---:|---:|---:|
| **Cold queries returning real data** | **9 / 36** (25 %) | n/a (proxy-fronted) | **35 / 36** (97 %) | – |
| **Cold queries silently returning empty** | **13 / 36** ← blank panels in Grafana | – | 0 | – |
| **Cold queries returning HTTP 500 / 502** | 9 / 36 | – | 1 (known VL parser-pipe limit on `trace_id` 6 h) | – |
| **Cold queries that timed out (60 s)** | 5 / 36 | – | 0 | – |
| **Median cold latency on shared-success queries** (`pod` 30 m – 2 h, `labels` 30 m – 24 h) | 9 ms – **6 302 ms** | – | 8 ms – **436 ms** (10 – 25× faster on metric paths; tied on `labels`) | – |
| **Total CPU·s consumed across the 17.5-min bench** | **2 712.6 cpu·s** (45 cpu·min) | 117.2 cpu·s | **5.2 cpu·s** (~0.09 cpu·min) | 15.1 cpu·s |
| **Avg CPU during the bench** | **2.58 cores** | 0.11 cores | **0.005 cores** | 0.014 cores |
| **Peak CPU during the bench** | **14.44 cores** | 6.99 cores | 0.14 cores | 0.90 cores |
| **Steady-state RSS (`process_resident_memory_bytes`)** | 8 466 MiB idle, **10 445 MiB peak** | 459 MiB idle, 2 129 MiB peak | **38 MiB idle, ~44 MiB peak** | 55 MiB idle, ~60 MiB peak |
| **Go heap (`go_memstats_heap_inuse_bytes`)** | – | – | **43 MiB** | 62 MiB |
| **Cache contents observed via `/metrics`** | – | – | L1: **10 objects / 1 976 bytes** (most queries had unique timestamps → cache miss) | request-buffer pool, drops to idle between bursts |

**Headline ratios.** Loki burned **~520× more CPU than the proxy** (2 712 vs 5.2 cpu·s) and **~190× more peak RSS** (10 445 vs 44 MiB) — to serve **fewer than a quarter of the successful queries**. Combined proxy + VL + vmauth: **~140 cpu·s and ~2.2 GiB peak** (steady-state ≈ 550 MiB) for **97 %** of the workload.

> **Methodology — both numbers come from each program's own `/metrics`, not docker stats.** The two RSS sources can disagree by orders of magnitude. `docker stats MemUsage` includes Go's `sys_bytes` (mmap regions reserved from the OS but not actively dirtied) — for a Go program that can show 2.4 GiB while the actual working set is 60 MiB. The RSS column above is `process_resident_memory_bytes` from each program's own `/metrics`, which reflects real anonymous RSS. CPU numbers are integrated from the docker-stats stream captured during the bench (`test/e2e-compat/results/docker-stats-fair-*.tsv`) — `process_cpu_seconds_total` from each program's `/metrics` corroborates within ~10 %.

## Outcomes — 36 cold queries each (6 query shapes × 6 ranges from 30 m to 24 h)

| Outcome | Loki direct | Proxy |
|---|---:|---:|
| 200 OK with data | **9** | **35** |
| 200 OK but empty result (silent fail) | 13 ← Grafana renders blank panel | 0 |
| HTTP 500 / 502 | 9 | 1 (known VL parser-pipe limit on `trace_id` 6 h) |
| Timeout / no response | 5 | 0 |

The proxy serves **35 / 36** of Drilldown's actual call shapes; Loki direct serves **9 / 36**. The most user-hostile failure mode is the silent-empty bucket — Loki returns `HTTP 200 + result:[]` and Grafana shows a blank panel with no error, so the operator assumes there's no data when in fact Loki gave up.

## Side-by-side cold latency where both succeed with real data

| Query | Range | Loki | Proxy | Speedup |
|---|---|---:|---:|---:|
| `sum by (pod)` | 30 m | 6 302 ms (9 274 series) | **436 ms** (5 000 series via /hits) | **14.5×** |
| `sum by (pod)` | 1 h | 5 412 ms (17 342 series) | **223 ms** (5 000 series) | **24.3×** |
| `sum by (pod)` | 2 h | 3 502 ms (35 388 series) | **354 ms** (5 000 series) | **9.9×** |
| `/loki/api/v1/labels` | 30 m – 24 h | 9 – 27 ms | 8 – 31 ms | parity |

Loki returns the full unbounded series set on `pod` (9 k – 35 k unique values); the proxy routes through VL's `/select/logsql/hits` and returns top-N + remainder. Both render the same chart shape in Grafana; the proxy uses an order of magnitude less wall-time and bandwidth.

## Where only the proxy returns a usable answer

These are the queries Grafana Logs Drilldown emits when a user clicks a field on `namespace=prod` — and where Loki direct silently breaks the UX:

- `sum by (trace_id) (...)` 30 m – 24 h → Loki HTTP 500 every range; proxy 406 ms – 1 232 ms.
- `sum by (service_version) (...)` 1 h – 24 h → Loki returns `HTTP 200` + 0 series (silent); proxy 76 – 240 ms with 100 versions.
- `sum by (k8s_pod_name) (...)` 30 m – 24 h → Loki silent empty; proxy 116 – 257 ms with 2 956 – 4 994 series.
- `/loki/api/v1/detected_fields` 1 h – 24 h → Loki silent empty or 500; proxy 36 – 323 ms with the OTel field map populated.
- `sum by (pod) (...)` 24 h → Loki HTTP 500 (`too_many_series`); proxy 549 ms (16-series chart via /hits top-N).

## Resource consumption (14 min bench window)

| Container | Peak CPU | Peak RSS | Notes |
|---|---:|---:|---|
| `e2e-loki` | 1 444 % (≈ 14 cores) | 10 445 MiB | within 12 GiB budget, no OOM |
| `e2e-victorialogs` | 699 % (≈ 7 cores) | 2 129 MiB | served everything the proxy asked for |
| `e2e-proxy` | 14 % (≈ 0.1 cores) | 44 MiB | negligible |
| `e2e-proxy-vmauth` | 90 % (≈ 0.9 cores) | 2 254 MiB | cache layer |

Combined VL + proxy stack: **~7 cores and ~2.2 GiB** to serve 35/36 queries. Loki standalone: **14 cores and 10.4 GiB** to serve 9/36. Roughly half the CPU and one-fifth the RAM to serve four times more of the workload.

## Proxy heap behaviour — why it spikes, and what we did about it

During the bench the proxy's process RSS climbed from a **38 MiB idle** baseline to a transient peak of **~1.4 GiB** under unbounded load. A pprof snapshot (`test/e2e-compat/results/pprof/heap-*.pb.gz`) attributed **96 MB (61 %) of live heap to `bytes.growSlice` from `compatCacheMiddleware → CompressionHandlerWithOptions`** — the response buffer being grown to hold the gzipped cache entry for each large `query_range` response (5 000 series × ~25 KB ≈ 5 MB per response, ~20 concurrent in flight). Cumulative allocations since process start were **33.2 GB**, with **23.4 GB (70 %)** in `fastjson.(*cache).getValue` and **3.99 GB** in the same compression growSlice path.

The post-fix proxy ([commit `2340928`](https://github.com/ReliablyObserve/loki-vl-proxy/commit/2340928)) pools both buffers:

- `EncodeResponseBody` uses a pooled `bytes.Buffer` with a 4 MiB cap-trim — kills the per-cache-write allocation cascade.
- `compressedResponseWriter.buf` switched from value to pooled `*bytes.Buffer` with an explicit acquire/release lifecycle — caps the per-request hold-buffer cost.
- `buildHitsRangeMetricMatrix` pre-sizes the per-series `values` slice to the actual bucket count instead of starting at cap 16 — eliminates the 1.31 GB cumulative growSlice cascade for that one function.

Both pools are guarded by **heap-bounded regression tests**: `TestEncodeResponseBody_PoolKeepsHeapBounded` (1 000 sequential 288 KiB encodes, asserts heap delta < 32 MiB; current run measures **-0.28 MiB** — pool freed memory mid-test), `TestEncodeResponseBody_PoolStableUnderConcurrency` (32 × 100 concurrent, asserts < 128 MiB), `TestCompressedResponseWriter_HeapStableUnderRepeatedRequests` (1 000 handler calls, < 32 MiB), `TestBuildHitsRangeMetricMatrix_HeapBoundedAcrossManyCalls` (100 × 20-series builds, < 16 MiB). Plus an e2e lock: `TestE2ELock_ProxyHeapBoundedUnderDrilldownLoad` drives **30 concurrent workers × 60 s of Drilldown traffic** against the live proxy and asserts `heap_inuse < 500 MiB` and `process_resident_memory_bytes < 800 MiB`. Any future PR that removes a pool or replaces it with per-request allocation fails CI with a named test pointing back at this work.

What we did **not** fix in this round: fastjson's 23 GB cumulative is intrinsic to its per-`Parse` cache reset pattern; the existing three `fj.ParserPool` instances (`statsQRFJPool`, `statsTranslateFJPool`, `vlFJParserPool`) already amortize Parser allocation, but the cache rebuilds per call. Reducing it further requires replacing the parser — a deeper refactor, separate PR. Also pending separate PR: NDJSON line-by-line translation (eliminates the residual buffer-then-translate pattern) and a size-threshold cache skip (Drilldown queries with unique timestamps barely cache-hit, so skipping cache for >2 MiB responses saves a pure-overhead buffering pass).

## What this is and isn't

- **This is honest.** Real numbers from real queries on the live stack. Setup, raw data, and reproduction command are in `test/e2e-compat/results/` and `bench/drilldown-vs-loki.sh`.
- **It is NOT** "Loki is always broken." Loki holds up on the `labels` endpoint (tied with the proxy) and on `pod` at short ranges. It legitimately works for many workloads — just not for the high-cardinality per-stream aggregations Drilldown emits.
- **Loki's structural issue.** Loki's read path requires materializing one series per unique label combination per step bucket. For `sum by (FIELD) (count_over_time({namespace="prod", FIELD!=""}[2m]))` on a 5 000-pod namespace, the working set blows past `max_query_series` and any chunk-store budget before the result is ready. No Loki config we tested (max_query_series up to 1 M, cardinality_limit up to 1 M, max_query_parallelism up to 256, GOMEMLIMIT up to 20 GiB) made these succeed at full cardinality — it's algorithmic, not a tuning gap. The proxy bypasses it by routing through VL's columnar `/select/logsql/hits` which computes top-N server-side.
- **Errors are converted, not leaked.** When VL legitimately fails (parser-pipe row scan limit on shapes like `|json|trace_id!=""` at long ranges), the proxy converts the upstream 4xx/5xx into Loki's `HTTP 200 + warnings:[...]` partial-results envelope — the same shape Loki itself emits for `X-Query-Tags: Source=grafana-lokiexplore-app` traffic. Grafana renders a warning badge, not an error toast. Non-Grafana clients (curl, internal tooling) still see the real upstream error. Locked by `TestLock_VLErrorsConvertedToPartialResults` (5 subtests).

Reproduce: `./bench/drilldown-vs-loki.sh --ranges=30m,1h,2h,3h,6h,24h --queries=pod,trace_id,k8s_pod_name,service_version,detected_fields,labels`. All numbers above are cold-path (cache miss, first request).
