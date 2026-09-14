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
# docker-compose.bench.yml layers the VictoriaLogs reader/fs tuning used for the
# published numbers on top of the version-agnostic base stack (requires VictoriaLogs >= v1.37.0).
# The continuous log generator only starts with the `ui` profile.
docker compose -f docker-compose.yml -f docker-compose.bench.yml --profile ui up -d --build
../../scripts/ci/wait_e2e_stack.sh 180
docker compose ps                  # all services up?
docker logs e2e-log-generator      # data flowing?
cd ../..
```

This gives you:
- Loki at `http://localhost:13101`
- loki-vl-proxy at `http://localhost:13100` (backed by VictoriaLogs; pprof enabled with admin token `bench-pprof-token`)
- loki-vl-proxy /metrics at `http://localhost:13100/metrics`
- VictoriaLogs at `http://localhost:19428`

### Seeding identical historical data

The log generator only writes live data. For long-range workloads, load the same historical streams into both backends with the seed tool (it pushes to Loki `/loki/api/v1/push` and VictoriaLogs `/insert/loki/api/v1/push`). The `bench/` directory is its own Go module, so run it from there:

```bash
cd bench
go run ./cmd/seed/ --loki=http://localhost:13101 --vl=http://localhost:19428 --days=3
cd ..
```

| Flag | Default | Meaning |
|---|---|---|
| `--loki` | `http://localhost:3101` | Loki push base URL |
| `--vl` | `http://localhost:9428` | VictoriaLogs push base URL |
| `--days` | `7` | days of history, back-filled from now |
| `--services` | `12` | service streams (cycles through the built-in pool) |
| `--rate` | `0` | target lines/sec per service; overrides `--lines-per-batch` when set |
| `--lines-per-batch` | `21` | lines per service per time step |
| `--batch-interval` | `30s` | simulated time step between batches |
| `--skip-loki` / `--skip-vl` | `false` | ingest into one backend only |
| `--high-cardinality` | `false` | add `pod` as a stream label (needed by the `high_cardinality` workload) |
| `--pods-per-service` | `50` | unique pod IDs per service with `--high-cardinality` |

The compose Loki config rejects samples older than 168h (`reject_old_samples_max_age`) and VictoriaLogs keeps 7 days (`-retentionPeriod=7d`), so keep `--days` at 7 or below, and check that both backends return data for the seeded range (for example with `--verify`, below) before comparing results.

## Quick Start

`run-comparison.sh` defaults to `http://localhost:3101` (Loki), `http://localhost:3100` (proxy) and `http://localhost:9428` (VictoriaLogs). Against the compose stack, export the compose ports first (run from the repository root):

```bash
export LOKI_URL=http://localhost:13101
export PROXY_URL=http://localhost:13100
export VL_URL=http://localhost:19428
export PROXY_METRICS=http://localhost:13100/metrics
```

```bash
# Full suite: small, heavy and long_range workloads, 10/50/100/500 clients, 30s per level
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
| `compute` | Multi-level aggregations, arithmetic on rates, parse pipelines, unwrap aggregations | 5m–1h | Proxy translation and query-engine CPU for rate/quantile/division computations |
| `unindexed_scan` | Substring and regex content searches | varies | Content-search scaling on a large dataset |
| `high_cardinality` | Queries over `pod`-labelled streams | varies | High stream cardinality; seed with `--high-cardinality` |
| `machinery` | Curated mix of representative proxy operations | varies | Raw proxy overhead; run with `--unique-windows` against a proxy started with `-cache-disabled -label-values-indexed-cache=false` |

`loki-bench` runs `small,heavy,long_range` unless `--workloads` names other workloads; `compute`, `unindexed_scan`, `high_cardinality` and `machinery` only run when requested.

## Flags

`loki-bench` flags (`bench/cmd/loki-bench/main.go`). `run-comparison.sh` forwards any extra arguments to `loki-bench`.

```
Targets
--loki=URL                    Loki direct API base URL (default: http://localhost:3101)
--proxy=URL                   loki-vl-proxy base URL (default: http://localhost:3100)
--vl=URL                      VictoriaLogs API base URL, for VL resource tracking (optional)
--vl-direct=URL               VictoriaLogs native LogsQL API URL; adds the vl_direct target (optional)
--proxy-no-cache=URL          proxy started with the cache disabled; adds the proxy_nocache target (optional)
--proxy-partial=URL           proxy with a short cache TTL and coalescing; adds the proxy_partial target (optional)
--proxy-coalescer=URL         proxy with coalescer but no cache; adds the proxy_coalescer target (optional)
--skip-loki                   Skip Loki target
--skip-proxy                  Skip proxy target
--skip-vl-direct              Skip VL-direct target
--skip-proxy-no-cache         Skip no-cache proxy target
--skip-proxy-partial          Skip partial-cache proxy target

Resource tracking
--loki-metrics=URL            Loki /metrics URL (optional)
--proxy-metrics=URL           Proxy /metrics URL (optional)
--proxy-no-cache-metrics=URL  No-cache proxy /metrics URL (optional)
--proxy-partial-metrics=URL   Partial-cache proxy /metrics URL (optional)
--vl-metrics=URL              VictoriaLogs /metrics URL (optional)

Load shape
--workloads=LIST              Comma-separated workloads (default: small,heavy,long_range)
--clients=LIST                Comma-separated concurrency levels (default: 10,50,100,500)
--duration=DURATION           Test duration per concurrency level per workload (default: 30s)
--warmup=DURATION             Warmup before each run (default: 5s; warms proxy cache)
--jitter=DURATION             Shift each query window back by a random amount in [0, jitter] (default: 0)
--unique-windows              Give each worker a distinct, non-overlapping time window (defeats cache and coalescer)

Verification
--verify                      Before benchmarking, check Loki and proxy return equivalent data per query
--verify-strict               Exit non-zero if any verification diff is found

Profiling
--pprof-proxy=URL             Proxy base URL to capture CPU/heap/alloc profiles from during proxy runs
--pprof-no-cache=URL          Same for the no-cache proxy
--pprof-partial=URL           Same for the partial-cache proxy
--pprof-duration=DURATION     CPU profile duration (default: 30s)
--pprof-auth-token=TOKEN      Bearer token for proxy admin/pprof endpoints (-server.admin-auth-token)

Output
--version=TAG                 Version tag attached to JSON results for trend tracking
--output=DIR                  Output directory (default: results, relative to the working directory)
--verbose                     Print per-request errors
```

### run-comparison.sh environment

| Variable | Default | Meaning |
|---|---|---|
| `LOKI_URL` | `http://localhost:3101` | Loki target (compose: `http://localhost:13101`) |
| `PROXY_URL` | `http://localhost:3100` | warm proxy target (compose: `http://localhost:13100`) |
| `VL_URL` | `http://localhost:9428` | VictoriaLogs for spawned proxies, VL metrics and native LogsQL auto-detection (compose: `http://localhost:19428`) |
| `PROXY_METRICS` | `http://localhost:3100/metrics` | proxy /metrics (compose: `http://localhost:13100/metrics`) |
| `LOKI_METRICS`, `VL_METRICS`, `VL_DIRECT_URL` | auto-detected | set explicitly to override detection from `LOKI_URL` / `VL_URL` |
| `OUTPUT_DIR` | `bench/results` | output directory for both passes |
| `PROXY_NO_CACHE_URL`, `PROXY_PARTIAL_URL` | empty | use pre-started no-cache / partial-cache proxies instead of spawning them |
| `PROXY_BINARY` | built to `/tmp/loki-vl-proxy` | proxy binary used for the spawned no-cache / partial-cache instances |
| `NO_CACHE_PORT`, `PARTIAL_PORT` | `3199`, `3198` | first ports tried for spawned proxies |
| `PPROF_AUTH_TOKEN` | `bench-pprof-token` | admin token for pprof capture (matches the compose proxy) |
| `SKIP_MACHINERY` | `false` | set `true` to skip the second, unique-windows machinery pass |

The script always builds `loki-bench` to `/tmp/loki-bench`. Unless `PROXY_NO_CACHE_URL` is set, it builds the proxy to `/tmp/loki-vl-proxy` (or uses `PROXY_BINARY`) and starts a no-cache proxy (`-cache-disabled`) against `VL_URL`; unless `PROXY_PARTIAL_URL` is set, it starts a partial-cache proxy (`-cache-ttl=6s`) from that binary when it exists. Spawned proxies are stopped on exit. After the main pass it runs a machinery pass with `--unique-windows` into `$OUTPUT_DIR/machinery`.

## Output

Each run writes two files to `--output`:

- `bench-<timestamp>.json` — full machine-readable results (all records, per-query stats)
- `bench-<timestamp>.md`   — markdown summary table for PR comments or wiki

With `--pprof-*` set, profiles are written to `<output>/pprof/`.

## Drilldown Scripts

Shell harnesses for Drilldown endpoints against the compose stack (see each script header for options):

- `drilldown-vs-loki.sh` — cold/warm latency, status, body size and series count for Drilldown/Explore query shapes, Loki (`LOKI_URL`, default `http://localhost:13101`) vs proxy (`PROXY_URL`, default `http://localhost:13109`)
- `drilldown-filter-matrix.sh` — Drilldown endpoint × filter timing matrix against the proxy (`PROXY_URL`, default `http://localhost:13200`, the `vmauth-ring` load balancer)
- `drilldown-equivalence.sh capture|verify <dir>` — captures Drilldown responses and diffs them after a change (`PROXY_URL`, default `http://localhost:13200`)

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
OUTPUT_DIR=bench/results/v1.17.0 ./bench/run-comparison.sh --version=v1.17.0

# v1.17.1
OUTPUT_DIR=bench/results/v1.17.1 ./bench/run-comparison.sh --version=v1.17.1

# diff (jq or any JSON diff tool); JSON keys are the Go field names
jq '[.[] | {Target, WorkloadName, Concurrency, p99: .Result.Overall.P99}]' \
  bench/results/v1.17.0/bench-*.json \
  bench/results/v1.17.1/bench-*.json
```

## Architecture

```
bench/
  go.mod                          # Separate Go module (github.com/ReliablyObserve/Loki-VL-proxy/bench)
  cmd/
    loki-bench/main.go            # CLI entry point, orchestrates runs
    seed/main.go                  # Historical data seeder for Loki + VictoriaLogs
  internal/
    histogram/histogram.go        # Concurrent-safe percentile tracker (sorted slice)
    workload/workload.go          # LogQL query definitions for every workload
    workload/vl.go                # Native LogsQL equivalents for the vl_direct target
    runner/runner.go              # Worker pool (goroutines), per-request timing
    metricscrape/scraper.go       # Prometheus text scraper for resource deltas
    pprof/capture.go              # CPU/heap/alloc profile capture from /debug/pprof
    verify/verify.go              # Loki vs proxy response equivalence check (--verify)
    report/report.go              # Text table, JSON, Markdown output
  run-comparison.sh               # Convenience wrapper for full comparison
  drilldown-vs-loki.sh            # Drilldown/Explore query latency, Loki vs proxy
  drilldown-filter-matrix.sh      # Drilldown endpoint × filter timing matrix
  drilldown-equivalence.sh        # Drilldown response capture/diff
  results/                        # Default output directory (gitignored)
```

The `bench` module has no external dependencies beyond the Go standard library.
