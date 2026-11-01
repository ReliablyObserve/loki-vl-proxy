# Settled raw JSON windows: main versus candidate

Measured 2026-11-01 against main `3daa8a543841de9d0a2608ddda6bf439ed194525`
(v2.55.0) and candidate `0e2963a2d8abc02628057b2ed9994abbd37f73c3`.
Later documentation/evidence changes do not change the measured runtime source.
The candidate includes body-read diagnostics; its production decoded-byte
counter independently agrees with the HTTP observer for the example below.

The fix avoids reading settled history again for complete raw JSON count/rate
evaluations. It adds Loki-facing retained points using existing RAM, SSD and
peers. It does not replace the separate VL-facing backend cache.

## Result and cost

Example: DLV2, 48h shifted window, RF=2, 20 measured requests across three
proxy processes. These are totals or end snapshots for that cell, not per-query
resource costs. [All 28 paired cells](matrix.md) include unchanged controls,
restart and node-loss cases; [comparison.json](comparison.json) retains the
derived resource details and priming separately.

| Signal | Main | Candidate |
|---|---:|---:|
| Successful, nonempty answers equal to Loki | 20/20 | 20/20 |
| VL requests | 20 | 20 |
| Decoded VL body bytes | 53,673,364 | 36,372 |
| Largest requested raw-read span, including lookback | 172,860 s | 180 s |
| Proxy CPU counter delta, three processes | 0.690 s | 0.022 s |
| Loki-facing logical L1 bytes at end | 31,700 | 50,924 |
| Loki-facing logical L2 bytes at end | 0 | 14,016 |
| VL-facing logical L1 / L2 bytes at end | 11,976 / 7,890 | 11,024 / 7,476 |
| Loki-facing lookup / hit counter deltas, all tiers | 40 / 20 | 532 / 508 |
| VL-facing lookup / hit counter deltas | 0 / 0 | 0 / 0 |
| Loki-facing encoded SSD write bytes during phase | 0 | 6,214 |
| VL-facing encoded SSD write bytes during phase | 2,070 | 1,932 |
| Peer GETs / replica SETs during phase | 0 / 0 | 0 / 27 |
| Replica SET payload bytes during phase | 0 | 3,024 |
| Layer eviction / rejection counter deltas | 0 / 0 | 0 / 0 |
| Aggregate bbolt file length at end | 294,912 B | 344,064 B |
| Aggregate filesystem allocated blocks at end | 294,912 B | 491,520 B |
| Largest individual process lifetime kernel RSS peak | 99,680,256 B | 60,735,488 B |
| Aggregate Go `MemStats.Sys` gauge at end | 325,819,224 B | 206,781,240 B |

VL calls remain because each new edge requires evaluation. Bytes fall by
99.93%; more cache lookups and replica writes buy the narrower reads. Lookups
are internal tier operations, not a count of client requests. A bypass is not
a miss; the zero VL-layer lookup counts are not a measured zero-percent hit
rate. Per-tier samples and fixed eligibility/admission reasons are in the raw
captures. Whole-query exact hits bypass evaluator work on both sources.

Logical occupancy is not allocator retention or process RSS. The last row is
the existing macOS fallback gauge, despite its legacy resident-memory name.
Kernel RSS is the largest **individual lifetime** peak, not simultaneous fleet
peak or measured-request-only RSS. These memory figures include priming and
are sensitive to runtime/host state. File length and allocated blocks are not
physical throughput, IOPS or hard disk caps. Encoded writes can include delayed
priming/background work. Per-operation allocations and attributable VL-server
CPU/RSS were not measured; no savings are claimed for them.

## Alternating-order timing control

The full matrix ran main first, then candidate, on a busy host. Its timings
also reflect different reference-cache/OS-cache states. Use these fresh-process
main/candidate/candidate/main blocks for the DLV2 shifted RF=2 latency check:

| Block | Proxy p50 | Proxy p95 | Loki p50 | Proxy phase CPU | VL bytes |
|---|---:|---:|---:|---:|---:|
| Main A | 21.19 ms | 23.39 ms | 1.48 ms | 0.437 s | 53,673,364 |
| Candidate B | 1.32 ms | 1.88 ms | 1.20 ms | 0.020 s | 36,372 |
| Candidate B | 1.57 ms | 2.22 ms | 1.08 ms | 0.023 s | 36,372 |
| Main A | 20.87 ms | 21.74 ms | 1.44 ms | 0.430 s | 53,673,364 |

Each block has 20 nonempty parity-checked answers. [Control data](control.json)
links the compressed captures. These are observed loopback timings, below the
project's 50 ms absolute guard for classifying latency differences; they are
not a full performance-gate win or a production scaling claim. The candidate
still trails Loki's warm p50 in these blocks. Native field controls retain
the same VL byte counts; their timing changes are not attributed to this fix.
The full matrix also retains candidate exact-hit p95 outliers (for example
DLV1 RF=2 at 8.03 ms), rather than hiding them in the headline.

Each cell has a separate cold process/cache priming phase, recorded in the
captures. The example's first priming request took 32.03 ms on main and
26.75 ms on candidate, one observation each: this does not establish cold
latency improvement. Priming also performs replication and metadata work;
do not combine its resource totals with the measured warm phase.

## Setup, scope and remaining boundaries

- Same immutable 499,200-row fixture, 160 shards, two disjoint shards per cell;
  historical requests span 48h, with additional history/edge data. Both sources
  use identical fixture namespaces and query bounds. Loki 3.7.9 uses its defaults
  profile (alignment off, cache freshness 10m); VictoriaLogs 1.52.0 has 3d retention.
  [Run context](context.json) identifies actual backend images and binaries.
- Three local arm64 Go 1.27.3 proxy processes, RF=1 or RF=2, compatibility Tier0
  off. Per node: primary L1 64 MiB/1,024 entries, logical SSD 64 MiB, index 16 MiB,
  queue 16 MiB, S2, flush 4 MiB/1s/1,000 entries. Extent TTL 1h, freshness 10m;
  exact-response TTL 5m. These are component budgets, not an aggregate RSS or
  physical bbolt-size guarantee. Actual launch flags are retained in each cell.
- 24 ordinary cells ×20 samples, plus four DLV2 recovery cells ×3 samples:
  **492/492 answers match Loki per source**, with valid resource observations.
  The alternating blocks add 80 successful comparisons. Separate real-backend
  regression tests cover 24h and 48h exact, forward and backward moves, including
  native field breakdown controls.
- Recovery restarts the fleet or stops node0 after priming; node0 is not claimed
  to own every key. RF=2 avoids the full-history raw transfer in this fixture.
  RF=1 losing the sole cached copy still reads full history. Recovery sparse
  probes can request 30m even when consuming only a small body; the 3m span
  claim applies to ordinary moving cells, not all backend operations.
- Complete raw JSON count/rate only. Truncated answers, incompatible routes or
  merged series/byte/sample bounds fall back to full evaluation. Native field
  histograms and unsafe transforms keep existing exclusions. New raw-step keys
  are versioned separately from older step payloads; mixed-version fleets were
  not live-tested here.
- Original read age, tenant/auth/backend/profile scope, grid phase, freshness
  and TTL remain. Recently writable points revalidate; settled late backfills
  need existing expiry/revalidation. This does not implement deletion
  invalidation, cold-miss coordination, membership migration or fleet scaling.
- Tier0-on, sustained pressure, allocator microbenchmarks, physical device I/O,
  high-cardinality field fan-out and production network scaling were not measured.
  This isolated fixture is not a claim that every warm query beats Loki.

## Reproduce and verify

Use the existing [cache-placement harness](../../README.md), building a binary
for each source and retaining separate output directories. The existing
`TestDLVCacheBaseline` supports `-dlv-range-hours=48` (or 24); its default zero
preserves the earlier short corpus. Seed once in `-dlv-baseline=setup`, then
measure both sources against that same fixture with separate claim/output dirs.
Use `-dlv-shards-per-cell=2 -dlv-control-repeats=1 -dlv-tier0=off`, shapes
`DLV1,DLV2,DLV3,native`, states `exact,shifted,refresh`, RF `1,2`, 20 samples.
Run DLV2 `restart,owner-loss` separately with three samples. Do not reuse a
claimed cell/output directory for another measurement or re-ingest duplicates.

Every compressed cell preserves queries/bounds, parity, phase resources,
backend requests and per-layer counters. Run indexes preserve source/binary
and harness hashes; local temporary paths in those indexes are historical
locations, not prerequisites for reading the retained files.

```sh
python3 bench/cache-placement/results/2026-11-01-ordered-json-window-reuse/verify.py
```

The standard-library verifier checks retained hashes, successful nonempty parity,
valid resource samples and exact reproduction of the derived comparison.
Production investigation uses the same [metrics and structured diagnostics](../../../../docs/cache-troubleshooting.md),
including `loki_vl_proxy_backend_read_bytes_total`; the laboratory observer is
not required to see reduced transfer in production.
