# loki-bench — Read-Path Comparison Benchmark

Measures **Loki (direct)** vs **VictoriaLogs via loki-vl-proxy** across:

- **Throughput** (req/s)
- **Latency** (P50/P90/P99/P99.9/max)
- **Error rate** and **degraded-answer rate** (both gate the run, see [Error gate](#error-gate-and-publishable-results))
- **CPU consumed** (seconds of CPU time during the run)
- **Memory** (RSS, heap in-use at end of run)
- **Network I/O** (bytes received/transmitted)
- **Per-query breakdown** (each query type individually)

## Prerequisites

Both Loki and the proxy must be running on a fresh stack with identical data seeded into Loki and VictoriaLogs.
The easiest path is the e2e compose stack with the benchmark override:

```bash
cd test/e2e-compat
# docker-compose.bench.yml layers benchmark-only settings on the base stack:
#   - VictoriaLogs reader/fs tuning used for the published numbers (requires VictoriaLogs >= v1.37.0)
#   - Loki ingestion parity: loki-bench-config.yaml (the CI e2e config plus stream sharding disabled)
#   - LOKI_BENCH_CONFIG selects the Loki config: loki-bench-config.yaml (default, warm
#     comparison) or loki-bench-nocache-config.yaml (every results cache off, cold comparison)
# Do not enable the `ui` profile: its live log generator keeps writing to both
# backends, which moves the data end and breaks the entry check.
docker compose -f docker-compose.yml -f docker-compose.bench.yml up -d --build
../../scripts/ci/wait_e2e_stack.sh 180
docker compose ps                  # all services up?
cd ../..
```

This gives you:
- Loki at `http://localhost:13101`
- loki-vl-proxy at `http://localhost:13100` (backed by VictoriaLogs; pprof enabled with admin token `bench-pprof-token`)
- loki-vl-proxy /metrics at `http://localhost:13100/metrics`
- VictoriaLogs at `http://localhost:19428`

### Seeding identical historical data

Load the same historical streams into both backends with the seed tool. The `bench/` directory is its own Go module, so run it from there:

```bash
cd bench
go run ./cmd/seed/ --days=4
cd ..
```

The seed makes both backends store the same data:

- **Identical line bytes.** Loki receives each line on `/loki/api/v1/push`; VictoriaLogs receives the same bytes as `_msg` on `/insert/jsonline`. Nothing is wrapped or injected and VictoriaLogs does no ingest-time JSON parsing, so `| json`, `| logfmt`, `bytes_rate` and `detected_fields` parse and count the same bytes on both sides.
- **Identical streams.** The stream labels become the VictoriaLogs stream fields (`_stream_fields`). With `--services` above the 12-entry pool, repeats get a distinct region and cluster, so every service index is its own stream.
- **Identical level information.** Each line's level is attached as structured metadata `level` in Loki and as a regular (non-stream) `level` field in VictoriaLogs. Loki derives `detected_level` from it (`discover_log_levels` stays at its default), and the proxy derives `detected_level` from the same field. Access-log lines carry no level on either side.

| Flag | Default | Meaning |
|---|---|---|
| `--loki` | `http://localhost:13101` | Loki push base URL |
| `--vl` | `http://localhost:19428` | VictoriaLogs push base URL |
| `--days` | `4` | days of history, back-filled from now (6 or below) |
| `--services` | `12` | service streams (cycles through the built-in pool; repeats get distinct region/cluster labels) |
| `--rate` | `0` | target lines/sec per service; overrides `--lines-per-batch` when set |
| `--lines-per-batch` | `21` | lines per service per time step |
| `--batch-interval` | `30s` | simulated time step between batches |
| `--skip-loki` / `--skip-vl` | `false` | ingest into one backend only |
| `--high-cardinality` | `false` | add `pod` as a stream label (needed by the `high_cardinality` workload) |
| `--pods-per-service` | `50` | unique pod IDs per service with `--high-cardinality` |
| `--force` | `false` | seed even when a backend already holds data (see below) |
| `--ready-timeout` | `3m` | retry the existing-data check while a backend is still starting |

The seed refuses to start when either backend already holds lines in the target span, or streams whose `app` label matches a seeded service in the last 7 days: pushing over existing data leaves the backends with different contents (VictoriaLogs keeps duplicate rows, Loki drops duplicate entries). Seed a fresh stack (`docker compose down -v`), or pass `--force` when both backends are known to hold the same prior data. The seed stops at the first failed push and exits non-zero: one backend then holds a batch the other does not, and retrying could duplicate rows in VictoriaLogs, so the only recovery is a re-seed on a fresh stack.

Choose `--days` from the longest window a workload reads: `long_range` reaches back 72h, flooring its start to a 1h step can add up to 1h, and `bytes_rate_72h` adds a 1h range-vector lookback, so it reads up to 74h before the data end. `--days=4` (96h) covers that and leaves about 22h for `--jitter`; `--days=3` (72h) does not. `loki-bench` reports every query whose evaluated window (start minus lookback) begins before the seeded data and aborts under `--verify-strict`. Keep `--days` at 6 or below: the compose Loki config rejects samples older than 168h (`reject_old_samples_max_age`) and VictoriaLogs keeps 7 days (`-retentionPeriod=7d`), and both apply their own clocks, so the oldest batches of a 7-day seed can be dropped on one side only.

The seed also interleaves the lines of all services within each batch, so no two streams share a timestamp and limit-capped log queries cut at the same line on both backends.

## Methodology: equivalent work

A comparison is only meaningful when every target answers the same question over the same data. `run-comparison.sh` enforces this before any timing:

1. **Pinned reference time.** Workload windows end at a reference time instead of the wall clock. `DATA_END=auto` (the default when VictoriaLogs is reachable) passes `--data-end=auto --data-start=auto`: `loki-bench` reads `max(_time)` and `min(_time)` of the seeded streams from VictoriaLogs (`app:* | stats min(_time), max(_time)`). Seeded data has a fixed end, so wall-clock windows (especially those of 2h or less) drift past the data as time passes and time empty ranges. `DATA_END` also accepts RFC3339 or a Unix timestamp.
2. **Step alignment.** Range queries have `start` and `end` floored to their `step`, as Grafana does, so Loki (`align_queries_with_step`) and the proxy (epoch-aligned buckets) evaluate the same axis. Instant queries keep their `time`. `--jitter` shifts windows backward only, by whole steps for range queries, and never so far that the evaluated window (start or time minus the largest range-vector lookback, including subquery ranges and `offset`) begins before the seeded data start. A query whose unshifted window already reads before the data start aborts the run under `--verify-strict`. `--unique-windows` (the machinery pass) shifts request *n* of worker *w* back by `n × clients + w` whole steps (whole seconds for queries without a step), so step alignment cannot map two requests onto the same window; the shifts stay inside the data and wrap once a query runs out of distinct windows. It requires `--cache-mode=cold`, because a results cache answers shifted windows from overlapping cached extents.
3. **Identical data.** See [Seeding identical historical data](#seeding-identical-historical-data): same line bytes, same streams, same level information.
4. **Loki ingestion parity.** `docker-compose.bench.yml` mounts `test/e2e-compat/loki-bench-config.yaml`, which is the CI e2e Loki config plus `limits_config.shard_streams.enabled: false`. Loki otherwise splits the seed's burst-pushed streams into `__stream_shard__` sub-streams and holds 2-3x the streams VictoriaLogs holds. `discover_log_levels` and `discover_service_name` stay at Loki's defaults: Loki derives `detected_level` from the seeded `level` metadata and `service_name` from `app`, and the proxy synthesises both from the same fields and label order. Verification compares label names, series label sets and detected field names, so a difference fails the run before timing. A bench-module test, run in CI, fails if the benchmark config drifts from the CI config outside the marked block or the block leaves `limits_config`, or if the cold-mode config (`loki-bench-nocache-config.yaml`) differs from the benchmark config anywhere but its block of disabled results caches. The CI e2e config itself is unchanged.
5. **Entry check.** `--wait-ingested` (`ENTRY_CHECK_TIMEOUT`, default 45m) polls until Loki and VictoriaLogs hold the same number of lines for every service (`app`, `cluster`, `region`) in every 1h window of the seeded span: Loki `sum by (app, cluster, region) (count_over_time({app=~".+"}[3600s]))` at a 1h step (sent with `Cache-Control: no-cache`, so Loki's results cache cannot return counts from before its ingesters flushed), and VictoriaLogs `stats by (_time:1h offset 1h-1ns, app, cluster, region) count()`, whose offset turns VictoriaLogs' start-labelled `[T, T+1h)` buckets into Loki's `(T-1h, T]` windows. Equal totals are not enough: a line missing in one window and duplicated in another fails the check. Loki keeps recently pushed chunks in ingesters that its read path does not consult for historical timestamps until they are flushed, so counts converge some time after seeding; the script first calls Loki's `/flush` (`LOKI_FLUSH=true`). The run fails, listing the differing service windows, if the counts do not converge in time. `--data-end=auto` also fails when a 1h window inside the seeded span holds no line at all (an incomplete seed).
6. **Strict verification.** `--verify-strict` (`VERIFY_STRICT=true`) requests every compared query once from Loki and once from **every timed proxy target** (`proxy` and `proxy_partial` in warm mode, `proxy_nocache` and `proxy_coalescer` in cold mode) and fails the run, listing each mismatch per target, unless:
   - both return the same HTTP status (200);
   - neither answer is degraded: no `Warning`, `X-Proxy-Stale-Response`, `X-Proxy-Drilldown-Hits-Fallback`, `X-Proxy-Upstream-Status`, `X-Proxy-Upstream-Error`, `X-Loki-VL-Partial-Response` or `X-Multi-Tenant-Partial-Failures` header, and no non-empty `warnings` array in the body (Loki's own partial answers fail too);
   - both have the same shape: resultType, series or stream count, and total points (metric queries) or lines (log queries); for metadata endpoints the item count;
   - both have the same content: label names and values, series label sets, stream and metric label sets, detected field names, and the first 20 log entries (timestamp and line) ordered by timestamp.

   A query where both targets return no data also fails, because it would time an empty range. A query may carry a documented `workload.Tolerance` (relative series/points allowance, allow-empty, or skip) where Loki semantics legitimately differ; content is compared only where counts must match exactly. The only skip today is `patterns_prod`, because pattern mining is backend-specific; it still has to answer 200 without degradation.
7. **Comparable queries only.** Queries marked `Excluded` are removed from the Loki and proxy runs (so both run the same query mix) and from verification, and the reason is printed:
   - `index/volume`, `index/volume_range`: the proxy reports line hits where Loki reports bytes.
   - `index/stats`: Loki answers from TSDB index chunk statistics while the proxy counts entries with a VictoriaLogs query and reports one stream.
8. **Equal caching.** `--cache-mode` (`CACHE_MODE`) makes the cache state explicit and the same on both sides; see [Cache modes](#cache-modes).
9. **Error gate.** Every timed run is checked against `--max-error-rate`; see [Error gate](#error-gate-and-publishable-results).

Level queries use what both backends expose: grouping is `by (detected_level)` (the Logs Drilldown volume shape), because `level` is structured metadata rather than an index label and `by (level)` collapses to one series in Loki; label-value browsing uses the `cluster` stream label instead of `level`.

The `vl_direct` target runs native LogsQL mirrors of the workloads: parsing with `unpack_json` / `unpack_logfmt`, range stats on `/select/logsql/stats_query_range` (the instant `stats_query` takes only `time` and would scan all data), and level breakdowns on `/select/logsql/hits` with `field=level`, the equivalent of `sum by (detected_level) (count_over_time(...))`. They are not shape-compared with Loki, but under `--verify`/`--verify-strict` each one is requested once and must return HTTP 200 with data: NDJSON rows, stats series, non-zero hits buckets, or returned values for metadata endpoints. Failures are listed per query and abort the run under `--verify-strict`.

## Cache modes

Loki and the proxy both cache responses, so a comparison is only equal when both run with the same kind of caching and the same warm-up. `--cache-mode` selects one of two comparisons; `loki-bench` reads Loki's `/config` and refuses to start (under `--verify-strict`) when Loki's `query_range` results caches do not match the mode.

| | `warm` (default) | `cold` |
|---|---|---|
| Loki | results caches on (`loki-bench-config.yaml`); `cache_results`, `cache_series_results`, `cache_label_results`, `cache_index_stats_results` and `cache_volume_results` must be on | every results cache off (`loki-bench-nocache-config.yaml`) |
| Proxy targets | `proxy` (the compose proxy with its response and `query_range` window caches), `proxy_partial` (short-TTL variant) | `proxy_nocache` (`-cache-disabled`), `proxy_coalescer` when given |
| Warm-up | the verification pass requests every query from Loki and each proxy target, then `--warmup` runs before every timed run for every target; no cache is flushed on either side | none on any target |
| Queries timed | metric queries, `/series`, `/labels`, `/label/{name}/values` | log queries, metric queries, `/series`, detected fields, patterns |
| `--unique-windows` | refused | allowed (machinery pass) |

Some queries do different work on Loki and the proxy in one mode, so that mode excludes them from verification and timing and prints the reason:

- **Warm mode excludes log queries, detected fields and patterns.** Loki 3.7 caches metric, series and label results, but its log results cache stores only empty answers and it does not cache detected fields or patterns. The cached proxy serves all of them from its response and window caches. These queries are compared in cold mode.
- **Cold mode excludes `/labels` and `/label/{name}/values`.** A proxy started with `-cache-disabled` still keeps a 30s in-process stream-field-names cache behind them, while cold Loki runs with `cache_label_results: false`. They are compared in warm mode.
- **`--unique-windows` excludes queries that have fewer distinct whole-step windows inside the data than the highest `--clients` value.** For example, the 1h-step `long_range` queries at 50 clients. For these queries the shifts would repeat windows and coalescing could answer one request from another.

Storage-level caches stay on in both modes on both sides: Loki's chunk cache, VictoriaLogs' block cache and the OS page cache. Cache TTLs differ even in warm mode:

- **Loki** keeps results-cache entries for 1h.
- **The compose proxy** keeps `query_range` and `query` responses in its compatibility cache for 5 minutes, and historical `query_range` windows for 24h (`-query-range-history-cache-ttl`). It keeps no near-now windows (`-query-range-recent-cache-ttl` defaults to 0). It caches metadata with built-in endpoint TTLs, for example 5m for labels and 30s for series.

The warm comparison therefore measures both systems as deployed, after the same warm-up, not with identical TTLs.

The cold comparison needs Loki restarted with the no-cache config. Loki's data volume and WAL are kept, and the entry check runs again before timing:

```bash
cd test/e2e-compat
LOKI_BENCH_CONFIG=./loki-bench-nocache-config.yaml \
  docker compose -f docker-compose.yml -f docker-compose.bench.yml up -d loki
cd ../..
CACHE_MODE=cold ./bench/run-comparison.sh --workloads=small,heavy,long_range,compute --clients=10,50
```

Switch back with the same `up -d loki` command without `LOKI_BENCH_CONFIG`.

## Error gate and publishable results

A number is published only when both backends answered without errors, limits or fallbacks, on identical, non-empty, verified data doing equivalent work. `loki-bench` enforces this:

- **During timing** every response is classified: transport errors, timeouts and HTTP status >= 400 count as errors; a successful response with a degraded-response header or a non-empty `warnings` array counts as degraded. When a timed run's error rate or degraded rate exceeds `--max-error-rate` (default `0`, `MAX_ERROR_RATE`), the benchmark stops, writes the results collected so far as `bench-<timestamp>-NOT-PUBLISHABLE.json` / `.md` for debugging, and exits non-zero. The threshold is printed in the report header.
- **Before timing** a run is marked not publishable, with a loud banner, when it cannot back a number: `--skip-loki`, no timed proxy target, no `--verify-strict`, no `--wait-ingested`, no `--data-end`, or a Loki cache configuration that could not be confirmed. Its output files carry the `-NOT-PUBLISHABLE` suffix, the markdown and text reports start with the reasons, and every JSON record has `"Publishable": false` and `NotPublishable` reasons.

## Quick Start

`run-comparison.sh` defaults match the compose stack: `http://localhost:13101` (Loki), `http://localhost:13100` (proxy), `http://localhost:19428` (VictoriaLogs) and `http://localhost:13100/metrics` (proxy metrics). Override with `LOKI_URL`, `PROXY_URL`, `VL_URL` and `PROXY_METRICS`. Run from the repository root:

```bash
# Full suite: small, heavy and long_range workloads, 10/50/100/500 clients, 30s per level
./bench/run-comparison.sh

# Quick smoke test (small workload, 10 and 50 clients, 10s)
./bench/run-comparison.sh --workloads=small --clients=10,50 --duration=10s

# Proxy only (no Loki to compare against; marked NOT PUBLISHABLE)
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
--loki=URL                    Loki direct API base URL (default: http://localhost:13101)
--proxy=URL                   loki-vl-proxy base URL, timed in warm mode (default: http://localhost:13100)
--vl=URL                      VictoriaLogs API base URL, for VL resource tracking (optional)
--vl-direct=URL               VictoriaLogs native LogsQL API URL; adds the vl_direct target (optional)
--proxy-no-cache=URL          proxy started with -cache-disabled; the proxy_nocache target in cold mode (optional)
--proxy-partial=URL           proxy with a short cache TTL and coalescing; the proxy_partial target in warm mode (optional)
--proxy-coalescer=URL         proxy with coalescer but no cache; the proxy_coalescer target in cold mode (optional)
--skip-loki                   Skip Loki target (marks the run NOT PUBLISHABLE)
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
--warmup=DURATION             Warm-up before each run in warm mode, identical for every target (default: 5s)
--jitter=DURATION             Shift each query window back by a random amount in [0, jitter): whole steps for range queries, never before --data-start (default: 0)
--unique-windows              Shift every request back by distinct whole steps inside the data (defeats caches and coalescer; requires --cache-mode=cold)
--cache-mode=warm|cold        warm: Loki results caches on vs cached proxy; cold: every Loki results cache off vs proxies without a response cache (default: warm)

Reference time
--data-end=TIME|auto          Every window ends here: RFC3339, Unix s/ms/ns, or auto (max(_time) from VictoriaLogs via --vl-direct or --vl); empty = wall clock
--now=TIME|auto               Alias for --data-end
--data-start=TIME|auto        Seeded data start, bounds --jitter (auto = min(_time) from VictoriaLogs; implied by --data-end=auto)

Entry check and verification
--wait-ingested=DURATION      Wait up to DURATION until Loki and VictoriaLogs hold the same lines per service and 1h window (default: 0 = off)
--wait-poll=DURATION          Entry check poll interval (default: 15s)
--verify                      Before benchmarking, compare each query on Loki and every timed proxy target (status, degradation, shape, content)
--verify-strict               Verify, and exit non-zero before any timing on a mismatch, a degraded answer, a failed or empty native LogsQL query, a window before the seeded data, or a Loki cache config that does not match --cache-mode
--max-error-rate=FRACTION     Largest error and degraded-answer rate of a timed run; above it the benchmark stops with a non-zero exit and NOT-PUBLISHABLE output (default: 0)
--verify-timeout=DURATION     Per-request verification timeout (default: 5m, matching Loki query_timeout)
--verify-only                 Stop after the entry check and verification

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
| `LOKI_URL` | `http://localhost:13101` | Loki target |
| `PROXY_URL` | `http://localhost:13100` | warm proxy target |
| `VL_URL` | `http://localhost:19428` | VictoriaLogs for spawned proxies, VL metrics, native LogsQL and data-end auto-detection |
| `PROXY_METRICS` | `$PROXY_URL/metrics` | proxy /metrics |
| `DATA_END` | `auto` when VictoriaLogs is reachable | workload reference time (`auto`, RFC3339 or Unix timestamp); `auto` also bounds jitter to the data start |
| `ENTRY_CHECK_TIMEOUT` | `45m` | wait for Loki/VictoriaLogs line-count parity before the main pass; `0` disables (also disabled without `DATA_END`) |
| `LOKI_FLUSH` | `true` | call Loki `/flush` before the entry check |
| `VERIFY_STRICT` | `true` | strict verification of every timed proxy target before each pass; `false` disables (results not publishable) |
| `CACHE_MODE` | `warm` | `warm` or `cold` (see [Cache modes](#cache-modes)); decides which proxies are spawned and whether the machinery pass runs |
| `MAX_ERROR_RATE` | `0` | `--max-error-rate` for both passes |
| `LOKI_METRICS`, `VL_METRICS`, `VL_DIRECT_URL` | auto-detected | set explicitly to override detection from `LOKI_URL` / `VL_URL` |
| `OUTPUT_DIR` | `bench/results` | output directory for both passes |
| `PROXY_NO_CACHE_URL`, `PROXY_PARTIAL_URL` | empty | use pre-started no-cache / partial-cache proxies instead of spawning them |
| `PROXY_BINARY` | built to `/tmp/loki-vl-proxy` | proxy binary used for the spawned no-cache / partial-cache instances |
| `NO_CACHE_PORT`, `PARTIAL_PORT` | `3199`, `3198` | first ports tried for spawned proxies |
| `PPROF_AUTH_TOKEN` | `bench-pprof-token` | admin token for pprof capture (matches the compose proxy) |
| `SKIP_MACHINERY` | `false` | set `true` to skip the second, unique-windows machinery pass (cold mode only) |

The script always builds `loki-bench` to `/tmp/loki-bench`. It builds the proxy to `/tmp/loki-vl-proxy` (or uses `PROXY_BINARY`) and, in cold mode unless `PROXY_NO_CACHE_URL` is set, starts a no-cache proxy (`-cache-disabled`) against `VL_URL`; in warm mode unless `PROXY_PARTIAL_URL` is set, it starts a partial-cache proxy (`-cache-ttl=6s`). Spawned proxies are stopped on exit. In cold mode, after the main pass it runs a machinery pass with `--unique-windows` into `$OUTPUT_DIR/machinery`; that pass repeats the entry check and strict verification with the same `DATA_END`.

## Output

Each run writes two files to `--output`:

- `bench-<timestamp>.json` — full machine-readable results (all records, per-query stats, `CacheMode`, `MaxErrorRate`, `Publishable`)
- `bench-<timestamp>.md`   — markdown summary table for PR comments or wiki, headed by the cache mode, the error threshold and publishability

Runs that are not publishable (see [Error gate](#error-gate-and-publishable-results)) write `bench-<timestamp>-NOT-PUBLISHABLE.json` and `.md` instead.

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

Run `long_range` in both cache modes (`CACHE_MODE=cold`, then `CACHE_MODE=warm`). The warm-mode latency drop, against Loki with its own results cache, shows the window cache ROI.

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
    loki-bench/reference.go       # --data-end / --data-start resolution
    loki-bench/modes.go           # Cache modes, timed and verified targets, publishability, Loki /config check
    seed/main.go                  # Historical data seeder for Loki + VictoriaLogs
  internal/
    histogram/histogram.go        # Concurrent-safe percentile tracker (sorted slice)
    workload/workload.go          # LogQL query definitions, exclusions and shape tolerances
    workload/align.go             # Step alignment of range queries
    workload/vl.go                # Native LogsQL equivalents for the vl_direct target
    runner/runner.go              # Worker pool (goroutines), per-request timing
    metricscrape/scraper.go       # Prometheus text scraper for resource deltas
    pprof/capture.go              # CPU/heap/alloc profile capture from /debug/pprof
    verify/verify.go              # Loki vs proxy targets: status, degradation, shape and content (--verify / --verify-strict)
    degraded/degraded.go          # Degraded-response headers and warnings detection (verification and timed runs)
    dataspan/dataspan.go          # Seeded data span and gap detection, per-service/window entry check, existing-data check
    report/report.go              # Text table, JSON, Markdown output
  run-comparison.sh               # Convenience wrapper for full comparison
  drilldown-vs-loki.sh            # Drilldown/Explore query latency, Loki vs proxy
  drilldown-filter-matrix.sh      # Drilldown endpoint × filter timing matrix
  drilldown-equivalence.sh        # Drilldown response capture/diff
  results/                        # Default output directory (gitignored)
```

The `bench` module has no external dependencies beyond the Go standard library.
