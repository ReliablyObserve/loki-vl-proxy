# Real API cache measurements — 2026-10-30

These results verify cache behavior and expose limits. They do **not** establish
a material query-latency improvement, less VictoriaLogs scan work, or linear
throughput scaling from these changes.

Baseline main is `7c59fafa`, foundation is `2a2e882b`, and placement candidate
production code is `55d3f7fb`. Executable digests and full source refs are in
each JSON manifest. Baseline-to-candidate comparisons include both foundation
and placement changes. The isolated foundation-to-placement pair separates
those change sets.

The isolated fixture has 90 shard streams, three levels, 54,000 log entries
and historical/recent JSON data. Exact source, fixture and wire-ledger hashes
are retained in the reports. All observations are from one arm64 macOS host,
with real proxy processes and pinned Loki 3.7.9 / VictoriaLogs 1.52.0.

## Coverage and latency

| Run | Compared responses | Equal successful HTTP 200 | Scope |
|---|---:|---:|---|
| Same-source fit ABBA control | 2,028 | 2,028 | Both source labels use the baseline executable |
| Main vs candidate fit ABBA | 2,028 | 2,028 | Eight shards; fixed, shifted, unseen forward, age-recorded setup-time recent, restart and owner-loss |
| Main vs candidate pooled ABBA | 14,820 | 14,820 | 90 shards; same modes |
| Main vs candidate tiny-budget probe | 3,420 | 3,420 | 64 L1 slots / 32 KiB logical SSD per node |
| Foundation vs placement tiny-budget probe | 3,420 | 3,420 | Same fixture and addresses; isolated follow-up |
| Foundation vs placement finite reader-pool pass | 5,440 | 5,440 | Tier0 disabled; 90 metadata identities; 1/3/6 nonmember readers |

All above resource samples were valid. The same-source fit control and actual
fit/pooled comparisons had no workload-median change exceeding **both** 25%
and 50 ms. Relative changes in the small loopback timings are not significant
speedup evidence. p50/p95 and individual block aggregates remain in the JSON.

A separate local frontend-hot QPS control was unstable despite identical
executables: one reader ranged from approximately 5.2k to 10.8k correct req/s;
six readers ranged from 11.4k to 19.7k. Every warm cell had zero peer/backend
calls. Candidate QPS measurement was therefore skipped; there is no throughput
or production-linearity claim.

## Backend work and resource tradeoff

Pooled ABBA has 7,410 requests per source (two blocks each):

| Measurement | Main | Candidate |
|---|---:|---:|
| VictoriaLogs requests | 20,987 | 19,825 |
| VictoriaLogs response-body bytes | 1,474,590,654 | 1,474,482,266 |
| Peer GET contacts | 63,297 | 66,452 |
| Peer GET response-body bytes | 1,073,257 | 1,648,203 |
| Replica SET request-body bytes | 1,964,661 | 1,986,195 |
| Proxy request-phase CPU | 31.261 s | 31.136 s |
| Largest observed child kernel peak RSS | 189,677,568 B | 171,114,496 B |
| Peak fleet bbolt file lengths | 16,547,840 B | 16,465,920 B |
| Peak allocated filesystem blocks | 18,972,672 B | 18,972,672 B |

Requests fell 5.54%, but response bytes fell only **108,388 B (0.00735%)**.
Most avoided calls were small metadata responses. This is not proof of less
internal scan CPU or scanned rows: VictoriaLogs CPU and internal scanned bytes
were not measured. Proxy CPU changed about 0.4%, within control variation;
peak RSS is an observation, not a demonstrated memory saving.

In the pooled exact phase, cross-frontend backend requests fell from 194 to 68
over 950 API requests per source. Unseen forward refresh fell from 3,128 to
2,960 over 950 requests. Primed repeat still issued **50 backend requests on
both sources** over 950 API requests, transferring about 201 MB on each.
These are the ordered JSON/DLV paths after Tier0 churn. Candidate repeat also
needed 326 owner GETs versus zero on main. Shifted and refresh raw JSON scans
remain a large gap.

Each phase includes per-backend-path counts, body bytes and sums of explicit
requested start/end bounds. Range sums include overlaps/repeats and are not
actual rows scanned. Peer contact statuses and both directions of measured
HTTP message bytes are separate from encoded payload bytes.

## Finite reader-pool pass

This isolated foundation-to-placement case uses three hot owners, RF=2,
Tier0 disabled, 90 historical metadata identities, and three passes per reader.
Cold readers are not ring members and have no SSD configured.

| Per reader | Foundation | Placement |
|---|---:|---:|
| Successful requests in finite pass | 270 | 270 |
| VictoriaLogs requests | 0 | 0 |
| Resident primary-L1 objects afterward | 90 | 25 |
| Peer GETs | 88 | 268 |
| Peer GET response-body bytes | 3,564 | 10,854 |
| Peer HTTP message bytes, both directions | 73,340 | 223,350 |

At six readers the pass had 1,620 correct responses, zero backend calls,
528 versus 1,608 GETs, and 440,040 versus 1,340,100 HTTP message bytes.
Object counts prove bounded frontend duplication; they do not imply a
corresponding RSS reduction. The extra owner traffic is the cost of retaining
25 short RAM shadows rather than all 90 values. This is independent-request
cache reuse, not distributed execution of one query.

## Tiny-budget regression

A deliberately undersized 32 KiB SSD / 64-slot profile exposes increased
backend reads in foundation. The isolated placement pair did not add this
increase:

| Phase | Original main | Foundation | Placement paired with foundation |
|---|---:|---:|---:|
| Exact cross-frontend | 2,801 | 3,935 | 3,935 |
| Primed repeat | 1,652 | 2,356 | 2,356 |
| Shifted historical | 3,400 | 4,246 | 4,246 |
| Unseen forward refresh | 5,146 | 6,136 | 6,136 |
| Retained-disk restart | 3,321 | 4,718 | 4,718 |
| Owner loss | 3,854 | 4,715 | 4,715 |

Main and foundation columns come from separately named pair runs on the same
fixture; raw manifests identify them. Placement's isolated frontend-prime
phase did fewer reads (4,314 to 4,178); the other listed phases matched.
The change set is isolated, but a specific admission/LRU cause has not been
proven. This budget-pressure regression is retained rather than hidden by the
fit results. These probes do not support timing claims.

## Reproduction and remaining limits

See [the harness guide](../../README.md) for setup, pinned executable builds,
same-source control, ABBA, finite reader counts and cleanup commands.
The helper/parser/observer suite runs in the nested bench module's regular
tests; process/Compose measurements are opt-in.

Historical forward windows are previously unrequested, but phases within a
block share earlier cache state. Older preparation results that reversed
shifted windows describe replay and are not used here as forward-refresh
evidence. The recent fixture is immutable setup-time data; each response's
age and freshness-window eligibility are recorded, so it is not assumed to be
near-now and it proves no new-ingestion behavior.

HTTP byte counters cover the client-to-observer message hop, including headers,
framing and encoded bodies. They exclude TCP/IP overhead, retransmission and
the observer-to-target second hop. Proxy CPU excludes observers and backends.
Physical bbolt file high-water is not logical admitted cache occupancy.

The fixed-owner reader matrix does not verify three-to-six-owner ring growth,
handoff, historical-record migration or convergence after membership changes.
Independent-host throughput scaling remains unverified.
