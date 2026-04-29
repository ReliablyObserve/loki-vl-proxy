# loki-bench — Read-Path Comparison Benchmark

Measures **Loki (direct)** vs **VictoriaLogs via loki-vl-proxy** across:

- **Throughput** (req/s)
- **Latency** (P50/P90/P99/P99.9/max)
- **Error rate**
- **CPU consumed** (seconds of CPU time during the run)
- **Memory** (RSS, heap in-use at end of run)
- **Network I/O** (bytes received/transmitted)
- **Per-query breakdown** (each query type individually)

## Prerequisites

Both Loki and the proxy must be running with data ingested.
The easiest path is the e2e compose stack:

```bash
cd test/e2e-compat
docker compose up -d
# Wait ~30s for stack to be healthy and log generator to push data
docker compose ps          # all green?
docker logs e2e-log-gen    # data flowing?
```

This gives you:
- Loki at `http://localhost:3101`
- loki-vl-proxy at `http://localhost:3100` (backed by VictoriaLogs)
- loki-vl-proxy /metrics at `http://localhost:3100/metrics`

## Quick Start

```bash
# Full suite: all workloads, 10/50/100/500 clients, 30s per level
./bench/run-comparison.sh

# Quick smoke test (small workload, 10 and 50 clients, 10s)
./bench/run-comparison.sh --workloads=small --clients=10,50 --duration=10s

# Proxy only (no Loki to compare against)
./bench/run-comparison.sh --skip-loki --workloads=small,heavy --clients=10,50,100

# Tag results for version tracking
./bench/run-comparison.sh --version=v1.17.1

# Long-range only (exercises proxy windowing, prefilter, cache warm)
./bench/run-comparison.sh --workloads=long_range --clients=10,50,100 --duration=60s
```

## Workloads

| Workload | Query types | Time window | What it exercises |
|---|---|---|---|
| `small` | Labels, label values, series, simple log select, instant query, detected_fields | ≤5 min | Metadata cache (T0/L1), simple VL selects, label browsing |
| `heavy` | JSON parse+filter, logfmt, multi-stage pipeline, rate/count_over_time/bytes_rate, patterns | 30m–1h | Proxy translation overhead, VL full-field search, metric aggregation |
| `long_range` | Simple log select 6h/24h, rate metric 6h/24h, count_over_time 48h | 6h–48h | **Proxy window splitting** (1h windows), prefilter, adaptive parallelism, historical window cache reuse |

## Flags

```
--loki=URL           Loki direct API base URL (default: http://localhost:3101)
--proxy=URL          loki-vl-proxy base URL (default: http://localhost:3100)
--loki-metrics=URL   Loki /metrics for resource tracking (optional; auto-detected)
--proxy-metrics=URL  Proxy /metrics for resource tracking (default: proxy-url/metrics)
--workloads=LIST     Comma-separated: small,heavy,long_range (default: all three)
--clients=LIST       Comma-separated concurrency levels (default: 10,50,100,500)
--duration=DURATION  Test duration per level (default: 30s)
--warmup=DURATION    Warmup phase duration before each run (default: 5s; warms cache)
--skip-loki          Skip Loki target
--skip-proxy         Skip proxy target
--version=TAG        Version tag attached to JSON results for trend tracking
--output=DIR         Output directory (default: bench/results/)
```

## Output

Each run writes two files to `--output`:

- `bench-<timestamp>.json` — full machine-readable results (all records, per-query stats)
- `bench-<timestamp>.md`   — markdown summary table for PR comments or wiki

## Understanding the Results

### Proxy overhead on small/heavy workloads

For small metadata queries (labels, label values), the proxy adds:
- **Cache hit**: near-zero overhead — served from L1/T0 memory cache
- **Cache miss**: translation time (~5µs) + VL roundtrip

For heavy metric queries, expect slightly higher proxy latency vs Loki because:
- Some metric queries use proxy-side compatibility evaluation (not native VL stats)
- JSON parse+filter pipelines translate well (VL has native JSON support)

### Proxy advantage on long-range workloads

The `long_range` workload shows where the proxy **outperforms Loki** on repeat runs:
- 24h window requests hit historical window cache (24h TTL) — subsequent clients pay ~0 backend cost
- Prefilter via `/select/logsql/hits` skips empty windows — reduces VL calls up to 81%
- Adaptive parallelism (2–8 windows in flight) saturates VL throughput efficiently

Run `long_range` twice: first pass cold, second pass warm. The warm-pass latency drop shows the window cache ROI.

### Resource comparison

The resource deltas (CPU seconds, RSS memory) show what each system consumes **for the same query volume**. Key signals:
- **Loki RSS** at scale includes ingesters with in-memory chunks + querier pools + distributor
- **VL RSS** is typically the single vlselect/standalone process — much lower baseline
- **Proxy RSS** adds to VL: baseline ~50–100MB for cache + translation, scales with cache size (`-cache-max-bytes`)

### Network efficiency

The proxy compresses client responses (`gzip`/`zstd`) and can negotiate compressed upstream responses from VL. For the same log data:
- Loki: uncompressed or gzip responses depending on client `Accept-Encoding`
- Proxy: compressed responses to clients that accept them; compressed peer-cache hops

Check `response Bytes/s` in results — lower bytes/s at same req/s = more network-efficient.

## Comparing Across Versions

Use `--version` to tag results, then compare JSON files:

```bash
# v1.17.0
./bench/run-comparison.sh --version=v1.17.0 --output=bench/results/v1.17.0

# v1.17.1
./bench/run-comparison.sh --version=v1.17.1 --output=bench/results/v1.17.1

# diff (jq or any JSON diff tool)
jq '[.[] | {target,workload,concurrency,p99: .Result.Overall.P99}]' \
  bench/results/v1.17.0/bench-*.json \
  bench/results/v1.17.1/bench-*.json
```

## Architecture

```
bench/
  cmd/loki-bench/main.go          # CLI entry point, orchestrates runs
  internal/
    histogram/histogram.go        # Concurrent-safe percentile tracker (sorted slice)
    workload/workload.go          # Query definitions: small / heavy / long_range
    runner/runner.go              # Worker pool (goroutines), per-request timing
    metricscrape/scraper.go       # Prometheus text scraper for resource deltas
    report/report.go              # Text table, JSON, Markdown output
  run-comparison.sh               # Convenience wrapper for full comparison
  results/                        # Default output directory (gitignored)
```

No external dependencies beyond the Go standard library and the project's existing `go.mod`.
