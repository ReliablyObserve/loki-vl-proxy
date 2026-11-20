# Verification after the numeric-filter change reached main

Measured 2026-11-20. Base: `100e63e530fd08a985ad26ccf11aafeabaa1fb41`;
candidate product: `7cbff1ad5d9d7f5aac0177486926d30a5764eca9`.
The parent directory retains the earlier measurements against `eef311db6`.
They are historical; this directory is the comparison with the updated main.

## Request-path costs

Apple M5 Pro, arm64, Go 1.27.4, `GOMAXPROCS=2`, `GOMEMLIMIT=2GiB`.
Eight samples per side, 300ms per sample, four main/PR/PR/main cycles with
one sample per invocation. The benchmark file is identical on both sides.

| Path | Main | Candidate | Comparison |
|---|---:|---:|---|
| Native response miss | 94.42 µs | 93.82 µs | Stable, p=0.645 |
| Native response hit | 4.438 µs | 4.435 µs | Stable |
| JSON response miss | 175.9 µs | 168.1 µs | Stable; noisy baseline |
| JSON response hit | 5.707 µs | 5.725 µs | Stable |
| Drilldown JSON response miss | 168.6 µs | 170.9 µs | Stable |
| Drilldown JSON response hit | 7.195 µs | 6.931 µs | 3.67% lower, p=0.005 |
| Primary query-range hit | 1.426 µs | 1.423 µs | Stable |
| Compat query-range hit | 4.044 µs | 4.043 µs | Stable |

All eight paths allocate less, with allocated bytes equal or lower. The
Drilldown repeat uses 48 allocations instead of 53 and 8.860 KiB instead of
9.048 KiB. A primary query-range hit uses 464 bytes and 10 allocations,
against 496 bytes and 11 allocations. These are per-operation allocation
costs, not retained RAM, whole-process CPU utilisation or fleet RSS.

The profile builder writes into the final key and reuses the client surface
already classified by `grafanaRuntimeProfile`, avoiding another Source-tag
parse. Existing recorded browser-header tests also check the cache mode.

The `cold` benchmark names mean a Loki response miss: the benchmark removes
the inner answer before each iteration. Its 240-row fake backend and other
caches have already been primed. It does not simulate cold VictoriaLogs
storage, disk restart, a peer loss or a first Grafana request.

The initial updated-base run, grouped as four samples per invocation, reported
a 48.40% native response-miss increase and two other slower paths. Both raw
sides and its benchstat are retained as `initial-*`. A balanced one-sample
repeat before the final optimization measured 94.29 µs versus 93.40 µs
(p=0.721), with no significant timing regression among the eight paths;
these are `paired-before-*`. The final comparison above uses `optimized-*`.
The initial slowdown was not reproduced; it is not counted as an optimization
gain. The CPU profile is diagnostic and is dominated by runtime wait samples;
it is not used to infer whole-process CPU savings.

## Correctness and playground

The final focused cache/limit/error-flight suite, race checks, captured-client
headers and full route parity are retained under `optimized-*`. The route gate
checks 2593 rows and 25,930 executions, with zero mismatches and unchanged
main's route/backend-read baselines. The existing 126-case live query matrix
also passed after the main merge; that run preceded the redundant-tag-parse
removal. Final browser checks use the rebuilt optimized product.

The same existing Compose project is reused, with all 13 proxy variants and
their cache volumes; backend and Grafana container IDs are preserved. The
patterns proxy and reference Loki use the same 1,000,000-series limit, while
vmauth intentionally retains 1000. No product default changes.

After the first merged-code rebuild, the saved three-hour request returned
37,830 series and 37,870 points on each side, with identical normalized
labels/timestamps/values and no partial notices. Individual request times
were proxy 1.388s and Loki 0.780s; the cache histories differ, and these are
not a controlled main/PR or p50/p95 comparison. This observation does not
prove that every warm query is faster than Loki.

Larger complete answers can cost more than truncated answers. Raising the
playground limit is a completeness setting, not a RAM or SSD optimization;
configured cache admission and storage bounds remain unchanged. Physical disk
IOPS/throughput, retained occupancy, sustained load, whole-process CPU/RSS and
new peer-loss/pressure performance were not measured for this change.

The earlier unsharded local run's ten-minute timeout remains disclosed in the
parent report. It is not converted into a pass; standard CI shards supply
full-package validation. No test timeout, compatibility expectation, route
ratchet or performance threshold is weakened.

## Browser captures

Final optimized image, existing Fields suite: [15m](complete-0_25h.png), [3h](complete-3h.png), [24h](complete-24h.png), [48h](complete-48h.png). Deliberately bounded controls: [24h](partial-24h.png), [48h](partial-48h.png). Exact field identities and notices are in [browser-summary.json](browser-summary.json) and [browser-identities.json.gz](browser-identities.json.gz).
