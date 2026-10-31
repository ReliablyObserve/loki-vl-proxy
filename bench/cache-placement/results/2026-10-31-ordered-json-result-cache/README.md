# Ordered JSON exact-response reuse

Measured on 2026-10-31 with main `0f9d8a6fa16da11a6f124ec3388a8da85caba035`
and implementation `3e00b32a1be0e455c6ed94c7705ff675cf7e0ee3`.
The later test/evidence follow-up does not change production Go source.
This fixes a missing write to the existing Loki-facing local response cache;
it does not add a VictoriaLogs result cache or change fleet placement.

## Exact-repeat result

RF=2, Tier0 disabled, twenty measured requests per cell across three already
primed proxy processes. Latencies are milliseconds. Every answer is nonempty
and canonically matches Loki, including labels, timestamps and values.

| Workload | Main VL calls | Candidate VL calls | Main VL body bytes | Candidate VL body bytes | Main p50 | Candidate p50 / p95 | Loki p50 / p95 during candidate |
|---|---:|---:|---:|---:|---:|---:|---:|
| DLV1 | 40 | 0 | 563,220 | 0 | 2.441 | 0.207 / 0.270 | 1.378 / 2.409 |
| DLV2 | 20 | 0 | 6,698,200 | 0 | 5.989 | 0.220 / 0.291 | 1.634 / 2.595 |
| DLV3 | 40 | 0 | 565,620 | 0 | 3.344 | 0.148 / 0.180 | 1.504 / 2.482 |

DLV1/DLV3 counts include a parser probe as well as raw-row reads; forty calls
are not forty bulk scans. Each candidate row records twenty Loki-primary L1
hits. These are **Loki-facing response hits**, not VL-layer cache hits.
Aggregate primary Loki key/value charges after these cells are respectively
4,725, 1,935 and 4,695 bytes across the three processes. Full responses retain
their existing local-only policy: no new SSD or peer copy is created for them.

All sixteen candidate exact-repeat cells have observed p50 and p95 below their
Loki reference. Tier0-on and native exact controls already avoid VL reads on
main; this change does not establish a new improvement for those controls.

## Remaining window gap

The thirty-two shifted/refresh cells still perform the same backend call
counts as main. They move the bounds on every request and therefore do not
reuse an exact response. For RF=2/Tier0-off, DLV1/DLV3 still make forty VL calls
per twenty requests; DLV2 makes twenty. This fix does not reuse JSON row
fragments or compute only missing edges. Their measured timings can vary with
the host; no speedup is attributed to a cache-policy change in those cells.

## Corpus, method and limits

- **1,920/1,920** nonempty canonical matches: 48 cells per executable ×20
  requests. Four shapes ×exact/shifted/refresh ×RF1/RF2 ×Tier0 on/off.
- All 96 measured phases have valid before/after resource observations.
  Recorded backend-request counts and response bytes reconcile with every
  phase's independent request list; raw cell and ledger hashes are retained.
- One 192,000-row fixture, 320 shard identities, two balanced disjoint shards
  per cell. Main and candidate use separate reserved namespaces and fresh
  process/cache directories. Namespace-label lengths differ slightly; raw
  moving-window byte differences must not be treated as software savings.
- Loki 3.7.9, VictoriaLogs 1.52.0; actual container image IDs and Loki build
  information are recorded. Loki uses its defaults profile. Loki cache
  statistics are retained per response, separately from both proxy layers.
- Each proxy has a 64 MiB primary logical RAM budget and 64 MiB logical disk
  budget, plus the existing index/queue and auxiliary limits. Tier0-on adds
  10% compatibility capacity. These are not total RSS or physical-file caps.
  This is a fit workload, not a capacity-pressure or eviction-policy proof.
- Go1.27.3, Apple M5 Pro/arm64. Runs are sequential on a shared host. Full
  unit suites, profiles and microbenchmarks run outside timed live phases;
  brief focused validation and ordinary host work can still overlap. Timings
  are descriptive observations over twenty samples, not universal guarantees
  or a linear fleet-scaling result.
- Logical layer charges, per-process lifetime kernel RSS high-water and
  aggregate bbolt file length are separate quantities in the comparison.
  A maximum per-process RSS is not a simultaneous fleet peak. Encoded writes
  may be delayed priming flushes; they do not prove repeat-caused device I/O.
- Default five-minute response expiry, original identity and streaming/offset
  exclusions remain. Near-now lookups use stored age and the actual response
  TTL. Errors, oversized captures and observed cancellation are excluded.
  This does not add durable distributed deletion invalidation.

[All 48 comparisons](comparison.json), [all recorded samples](samples.csv)
and [source/fixture/raw-evidence provenance](provenance.json) retain the
individual outcomes rather than hiding misses in aggregate timing.

## Automated regression

`TestDrilldown_OrderedJSONExactResponseCache` is selected by the existing CI
Drilldown shard. It uses actual Loki/VL, an independently counted fixture and
an embedded proxy with Tier0 disabled. All three JSON cases fail against main
because the repeat contacts VL; the native control passes. All four pass with
the fix. Unit tests additionally cover identity, expiry, near-now refresh,
cancellation after evaluation, warnings, pool ownership and admission limits.

## Reproduce

Build main and the implementation into separate binaries with Go1.27.3.
Run these from `bench/`, replacing `REPO` with the repository root. The output
directory/project must be fresh and the selected ports unused:

```sh
DOCKER_CONTEXT=desktop-linux go test ./cache-placement \
  -run '^TestDLVCacheBaseline$' -count=1 -timeout=30m -args \
  -dlv-baseline=setup -dlv-root="$REPO" \
  -dlv-output=/tmp/json-cache-proof -dlv-project=json-cache-proof \
  -dlv-loki-port=63101 -dlv-vl-port=63428 -dlv-proxy-port-base=64200 \
  -dlv-shards-per-cell=2 -dlv-control-repeats=2
```

Repeat the same command with `-dlv-baseline=measure`, adding
`-dlv-binary=/tmp/main-proxy -dlv-source-ref=MAIN_SHA -dlv-repeat-index=0
-dlv-states=exact,shifted,refresh -dlv-samples=20
-dlv-eviction-diagnostic=false`. Then measure the candidate with its binary,
source SHA and `-dlv-repeat-index=1`. Reserved namespace claims prevent
accidental remeasurement of contaminated cells. Finish with
`-dlv-baseline=cleanup` and the same project/output/ports to remove only this
stack and its volumes.

The reproduction harness emits complete per-cell reports, latency summaries,
logical layer observations and request lists. Full raw artifacts from this run
remain on the measurement host; the committed comparison retains their hashes.

## Handler allocation and CPU checks

Six sequential runs per variant use `GOMAXPROCS=4`, `-benchtime=150ms`
and the same benchmark source. Main control A versus main control B has
identical allocations and no statistically significant latency differences.
The candidate comparison uses control B; these synthetic handler numbers
include an in-process HTTP fake backend and request/recorder construction.
They are separate from the real-backend latency measurements above.

| Case | Main median | Candidate median | Main allocations | Candidate allocations |
|---|---:|---:|---:|---:|
| Cold native | 102.6 µs | 103.9 µs | 602 | 602 |
| Warm native | 4.13 µs | 4.09 µs | 54 | 54 |
| Cold JSON | 167.9 µs | 171.8 µs | 2,184 | 2,187 |
| Warm JSON | 160.95 µs | 4.56 µs | 2,184 | 54 |

Warm JSON allocation falls from about 242.7 KiB to 8.8 KiB per request.
Cold JSON pays about 0.6 KiB and three allocations for response admission.
Only warm JSON has a statistically significant timing change in this sample
(p=0.002, about 97% lower); native and cold JSON timings remain within the
measured noise. This is not a whole-server throughput or RSS measurement.

[Same-source control](microbench-control.txt) and
[candidate comparison](microbench-comparison.txt) include all benchstat
statistics; the three raw variant inputs retain the samples.
Run with `GOMAXPROCS=4`:

```sh
go test ./internal/proxy -run '^$' \
  -bench '^BenchmarkOrderedJSONResultCache$' -benchmem -benchtime=150ms -count=6
```

Add the same benchmark file to the baseline via a Go `-overlay` file;
do not modify baseline production source.

Separate two-second warm/JSON CPU and allocation profiles retain their raw
`*.pprof`, benchmark counts and top tables. They include benchmark calibration
and setup and execute different operation counts; total allocated GB are not
comparable per request. Main allocation samples include backend body reads
and JSON map decoding. In the candidate, `httptest.NewRequestWithContext`
accounts for about 60% of sampled allocation cumulatively, with additional
recorder/header allocation. CPU profiles are dominated by Darwin runtime and
socket/scheduling samples, so they do not justify a precise production CPU
attribution. Request construction is a harness cost, not evidence that a new
cache engine is needed. Inspect, for example:

```sh
go tool pprof -top -sample_index=alloc_space candidate.mem.pprof
```

## Local verification

The final root Go suite passes uncached, including fuzz seeds. Focused race
tests cover the changed handler, fixture compatibility and identity/freshness
edges; the near-now freshness regression also passes 100 race repetitions.
The real-backend regression passes all four cases against the candidate;
its three JSON cases fail against the base while the native control passes.
The generated conformance gate passes using the fresh full-suite JSON
results. Lint reports zero issues and the configured gosec scan reports zero
findings. The two existing tests that mutate fixtures or in-process series
limits now explicitly invalidate the corresponding exact-response prefix;
all their original route, limit and result assertions remain.
