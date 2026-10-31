# Focused DLV cache baseline — 2026-10-31

One proxy revision: `9956043537323d3bc30742fff509f7cb6edce5be`
(v2.50.0), Go 1.27.3, Darwin arm64. This characterizes existing behavior;
it is not a comparison between software versions or a production speedup claim.

All **80 main cells / 1,600 timed responses** and **10 independent same-source
control cells / 200 responses** returned matching, nonempty HTTP 200 matrices.
Full canonical comparison ignores execution statistics and series ordering,
normalizes equivalent numeric formatting, and retains result labels, points,
warnings and status. Analytical expected-row tests are a separate regression
corpus; reference equality alone is not an independent proof of every fixture
row's meaning.

[Machine-readable results](dlv-baseline.json) include configuration, runtime
image IDs, fixture/binary/harness hashes, per-cell p50/p95, phase ledger ranges
and hashes, body counts/bytes, HTTP message bytes and resource observations.
[All 1,800 timed samples](dlv-baseline.samples.csv) retain response hashes and
reference Loki result/chunk cache counters. Unsupported proxy semantic-layer
metrics remain null. The bounded results retain the fields needed to interpret
the measurements.

## Latency

RF=2, 20 samples per cell. Each entry is **proxy / Loki p50 in milliseconds**.
Tier0 is disabled except in the explicitly marked column. p95 and RF=1 are in
the JSON. Do not combine normal warm requests with restart first waves.

| Shape | Exact, Tier0 on | Exact, Tier0 off | Shifted | Forward refresh | Disk restart | Node 0 absent |
|---|---:|---:|---:|---:|---:|---:|
| DLV1 | 0.27 / 2.15 | 9.07 / 3.14 | 8.50 / 6.60 | 9.36 / 7.55 | 7.76 / 2.33 | 7.85 / 2.61 |
| DLV2 | 0.20 / 1.54 | 18.68 / 1.87 | 20.56 / 7.31 | 18.50 / 7.30 | 21.32 / 3.77 | 20.85 / 3.95 |
| DLV3 | 0.20 / 1.35 | 8.15 / 1.76 | 8.42 / 5.69 | 7.07 / 5.54 | 5.93 / 1.47 | 7.06 / 1.67 |
| native | 0.14 / 1.20 | 0.18 / 1.50 | 7.76 / 5.13 | 7.22 / 4.71 | 1.17 / 2.31 | 1.38 / 2.23 |

The project's practical latency classification requires both 25% and 50 ms.
These small fixture timings do not establish a practical production speedup or
a universal warm-query advantage. Same-source selected-control proxy p50
variation reached **4.57 ms** across the ten matched configurations;
the repeated selectors use independent balanced shard groups. This is a
variation check, not a statistical-significance test.
Work counts also varied: DLV1's RF=2/Tier0-off node-loss cell made 100 then
108 VL calls and 265 then 303 peer calls. Controls remain separate; neither
their timing nor scheduling variation isolates a causal software change.

## Backend reuse

RF=2, Tier0 off, 20 requests per cell. Each call/byte entry is **VL HTTP calls /
response-body MiB**, not scanned storage bytes, CPU or physical device I/O.
All backend endpoints are counted, including any capacity/health checks;
their groups are identified separately in JSON.

| Shape | Exact calls / MiB | Shifted calls / MiB | Largest shifted backend spans (rounded) |
|---|---:|---:|---|
| DLV1 | 40 / 2.157 | 40 / 2.194 | 3660s |
| DLV2 | 20 / 25.655 | 20 / 25.730 | 21660s |
| DLV3 | 40 / 2.166 | 40 / 2.204 | 3660s |
| native | 0 / 0.000 | 40 / 0.011 | 180s per endpoint |

With Tier0 enabled, each shape's RF=2 exact-repeat cell made zero VL calls.
Without Tier0, DLV1–DLV3 continued fetching raw rows over the full required
range. The native control reused settled buckets: shifted requests fetched
edge data plus edge level-presence probes. Its RF=2 restart and node-loss cells
also made zero VL calls, with peer contacts retained in the phase evidence.
This does not imply that every requested key lost its primary owner: node 0
is one specified unavailable ring member, and RF=1 has no backup replica.

The JSON paths and native control use different execution routes. Their byte
totals identify cache coverage differences; they are not an A/B measurement of
one optimization. Caching native buckets does not establish reusable coverage
for ordered JSON raw-row reads.

## Method and limits

- See [reproduction commands](../../README.md#focused-dlv-baseline). The fixture
  contains 768,000 rows in 1,280 shard
  identities, with separate groups for every cell and control. Setup's exact
  historical count must converge on Loki. Seed requests are bounded to 4 MiB;
  backend server limits are unchanged.
- Pinned reference: Loki 3.7.9 with `align_queries_with_step=false` and
  `max_cache_freshness_per_query=10m`; VL 1.52.0. Runtime image IDs and config
  hashes are included. Backend limits are 4 GiB and 2 GiB respectively.
- Historical windows, one-minute lookback/step, one-hour request spans (six
  hours for DLV2). Forward refresh means advancing that historical window,
  not fresh ingestion or near-now tail behavior. Eight balanced shards per
  cell; no production distribution, large-cardinality or capacity claim.
- Each cell has fresh proxy processes/files. Restart/loss repetitions restore
  closed immutable primed files before each first wave. A later request in a
  wave can benefit from peer promotions caused by an earlier request. Loki
  stays running, with its own cache counters recorded per response.
- Per node: primary RAM 64 MiB, additional Tier0 10% when enabled, logical SSD
  cap 64 MiB, disk index/write queue 16 MiB each. These settings are not a total
  cache/process-memory bound or recommended production defaults.
- Timed CPU is proxy-only and includes phase sampling/drain overhead. It
  excludes observer/backend CPU and separate startup/priming phases. On this
  host the timed process gauge is Go MemStats.Sys, not RSS. Individual lifetime
  kernel RSS peaks include startup/priming/shutdown and are never added into a
  concurrent fleet RSS total.
- Sampled file length/allocated blocks cover live process directories, excluding
  closed priming snapshots and older experimental wave copies. They are not
  logical payload occupancy, full experimental disk usage or device IOPS.
- Priming, timed phases and untimed Tier0-pressure replay have separate hashed
  ledger ranges. Churn attempts do not prove a Tier0 eviction: its dedicated
  miss/occupancy counters are unavailable, and tier `l0` is the hotness sidecar.
- Instrumented loopback on a shared host. Other user workloads remain active;
  no quiet-host, linear scaling, total-resource or all-warm-faster claim.

## Separate profiling diagnostic

[Profile provenance](profiles.json) and [CPU/allocation top tables](profiles.txt)
come from a later, untimed single-proxy run: Tier0 disabled, two concurrent
request loops, ten-second CPU captures for DLV1, DLV2 and the native control.
Every diagnostic response matched its nonempty reference matrix. These request
counts are not a comparative throughput benchmark, and the native profile does
not have the same backend work as the JSON profiles.

In DLV2's sampled allocation-space delta, `reflect.mapassign_faststr0` accounts
for 36.77% and `cloneStringMap` for 27.46%. The raw-row collector is the
cumulative parent of most allocations. CPU samples also include substantial
system-call/I/O work. These identify row decoding and map construction/cloning
as allocation targets. Cumulative allocation churn is not live heap or RSS;
the profile percentages do not estimate a speedup from changing those sites.

To reproduce a separate diagnostic, enable the authenticated instrumentation
and pprof endpoints (`-server.enable-pprof=true`), disable compatibility Tier0,
run a fixed fixture query, and collect `/debug/pprof/profile?seconds=10` plus
`/debug/pprof/allocs` before/after the workload. Compare allocation profiles
with `go tool pprof -alloc_space -base before.pprof BINARY after.pprof`.
Keep profiling traffic outside timed baseline phases. Raw profiles remain on
the measurement host; their hashes and decoded top tables are retained here.

## Regression verification

The deterministic proxy corpus checks independent window counts, real route
middleware, Tier0 reuse, tenant/auth/profile isolation, cold failure/retry and
typed Loki/VL payload separation. Focused race selection passed 12 cases.
The nested bench module passed 102 race tests across 12 packages and vet;
these include malformed response/counter handling, snapshot isolation, ledger
readability, exclusive namespace/setup claims and bounded seed batching.

Existing CI jobs run these tests. This test/tooling change does not alter
production Go code, so Visual smoke and product A/B have `evidence=false`.
The live baseline is opt-in and was executed locally, not represented as a
hosted CI measurement.
