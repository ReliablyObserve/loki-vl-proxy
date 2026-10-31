# Semantic cache layer observability proof

Measured 2026-10-31 against merged main `27ee67a14c1b56ec4bf7a3bd8dd869e42ba6ad25`
and the observability implementation. The final proxy executable SHA-256 is
`e3ed638f41d6920ede40c29549d0a418b2bad329a4917064adf9e17daa57eea1`.
[Production file hashes](production-source-files.json) identify its working-tree
source; the build disables VCS embedding, so the source-ref field in the report
is an operator claim, not a VCS attestation from the executable.

## Correctness and attribution

All **80 independent cells / 240 timed responses** returned nonempty HTTP 200
matrices matching Loki 3.7.9 canonically. Four shapes (DLV1–DLV3 and native
count) cover exact repeat, shifted window, forward refresh, disk restart and
one unavailable node, with RF=1/RF=2 and compatibility Tier0 on/off. There are
three samples per cell; this is a correctness/attribution proof, not a
production latency comparison.

[Structured proof](layer-proof.json) retains per-cell layer/component/tier
counter deltas, last measured gauges, cell hashes, runtime image IDs and ledger
section hashes. [Individual samples](samples.csv) retain canonical-match
provenance and descriptive timings. All **176 phase sections**, their request
arrays and seven wire totals reconcile with the hashed raw ledgers; exported
layer counter/gauge values reconcile with raw scrapes and per-node monotonicity.
Unknown families remain separate. Missing/reset/nonfinite measurements remain
null in the harness instead of becoming zero; none occurred in this run.

Examples from RF=2, three timed requests per cell:

| Case | Observed cache evidence | VL calls / response-body bytes |
|---|---|---:|
| DLV2 exact, Tier0 on | Three Loki compatibility L1 hits | 0 / 0 |
| DLV2 exact, Tier0 off | Three translation-cache hits; no reusable VL raw-row answer | 3 / 1,004,730 |
| Native exact, Tier0 off | Three Loki primary L1 hits | 0 / 0 |
| Native disk restart, Tier0 off | Two VL L2 hits and one VL L3 hit; 562 encoded record bytes read | 0 / 0 |
| Native node 0 absent, Tier0 off | Two VL L2 hits and one VL L3 hit | 0 / 0 |

The restart/node-loss rows demonstrate local disk and peer reuse. Node 0 is one
ring member; its absence does not prove every key lost its primary. Logical
peer lookups count callers, not network contacts. Delayed priming flushes can
finish during a measured phase: an encoded-write delta beside an exact cache
hit does not establish a new backend read or a write caused by that repeat.

Fixture: 96,000 rows / 160 shard identities, two balanced shards per cell;
historical windows with separate selectors per cell. VL 1.52.0 has a 2 GiB
container limit; Loki 3.7.9 has 4 GiB and its defaults-profile query cache.
Each cell starts fresh proxy processes/files. Restart/loss waves restore
closed primed disk snapshots before their first requests. The existing DLV
harness records its exact flags, fixture, codec, budget and source hashes.
Other host workloads were active. Raw ledgers and process artifacts remain on
the measurement host; bounded summaries are retained here.

## Instrumentation cost

[Method and binary hashes](method.json), [baseline](baseline.txt),
[candidate](candidate.txt), [same-source control A](control-a.txt) and
[control B](control-b.txt) retain six benchmark blocks with `GOMAXPROCS=4`
and 200 ms per workload. The new namespace benchmarks use the same source
file on both revisions. [Comparison](comparison-benchstat.txt) and
[control](control-benchstat.txt) are decoded with the recorded benchstat version.

| Tiny lookup | Baseline median | Candidate median | Allocations |
|---|---:|---:|---:|
| Loki L1 hit | 328.4 ns | 347.0 ns | 0 → 0 |
| VL L1 hit | 333.8 ns | 319.8 ns | 0 → 0 |
| Unknown L1 hit | 326.6 ns | 332.3 ns | 0 → 0 |
| L1 misses, Loki / VL / unknown | 9.32 / 9.31 / 9.82 ns | 10.70 / 10.69 / 13.12 ns | 0 → 0 |
| Compressed L2 hit | 400.1 ns | 443.2 ns | 9 → 9 |
| Raw L2 hit | 351.8 ns | 414.1 ns | 9 → 9 |

The added counters/timing have an observed CPU cost, including approximately
43–62 ns on these tiny L2 hits. Allocation bytes/counts are identical in all
eight workloads. Same-source control uncertainty bands reach ±34% on this
shared host; the table is not evidence that a lower individual candidate
median is a speedup, nor a prediction of end-to-end request overhead. The
live proofs were active during these microbenchmarks.

## Regression and visual evidence

The full root-module suite passed. Final affected checks include 597 cache
race tests, 87 metrics race tests, 115 bench race tests across 12 packages,
proxy/middleware race regressions, vet and lint. All eight memory lookup APIs
and queued-disk delivery retain zero allocations in their dedicated tests.
The RF1/RF2 randomized fleet model checks coherent layer sums after operations,
including Loki, VL and unknown namespaces; operation-pair/seed cases pass under
race, and a separate ten-second fuzz smoke completed successfully.

[Visual provenance](visual-proof.json): nine general core cases, 27
main/PR/Loki screenshots and nine montages used this exact final proxy binary.
All pages settled; **79/79 captured responses matched main vs PR**. Existing
Loki reference differences remain unchanged, so this is not a zero-gap Loki
claim. Local capture files are identified by hashes; current-head CI separately
publishes its general regression images in the Visual smoke comment.

These metrics expose application charges and logical traffic. They do not
measure total cache-owned memory, unique heap/RSS, physical disk shares,
device throughput/IOPS or peer write/occupancy statistics. No resource-budget
controller or query-reuse improvement is delivered by this measurement.

## Reproduce

Build the implementation with the repository Go version, then run from
`bench/` using a fresh output directory/project. The selected ports must be
unused. Keep the same arguments across setup, measure and cleanup:

```sh
go build -trimpath -buildvcs=false -o /tmp/layer-proxy ./cmd/proxy
# Run the commands below from bench/ (set REPO to the repository root).
go test ./cache-placement -run '^TestDLVCacheBaseline$' -count=1 -timeout=25m -args \
  -dlv-baseline=setup -dlv-root="$REPO" -dlv-output=/tmp/layer-proof \
  -dlv-project=layerproof -dlv-loki-port=62101 -dlv-vl-port=62428 \
  -dlv-proxy-port-base=63200 -dlv-shards-per-cell=2
```

Repeat with `-dlv-baseline=measure`, adding
`-dlv-binary=/tmp/layer-proxy -dlv-source-ref=YOUR_REVISION -dlv-samples=3
-dlv-eviction-diagnostic=false`; finally run `-dlv-baseline=cleanup` with the
same project/output/ports. The generated raw reports already contain the
typed layer observations and provenance. The pressure replay is disabled here;
capacity/expiry accounting is covered by deterministic and randomized tests.
