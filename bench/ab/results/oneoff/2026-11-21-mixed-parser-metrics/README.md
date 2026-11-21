# Mixed parser metrics: scoped runtime evidence

Measured 2026-11-21 on an Apple M5 Pro, Go 1.27.4. This is historical evidence for the product Go source digest
`ec3c812280d920d691ac3f48287d5d17d35f3b77854b5af5b7d90d7218844779`.
The base is `51b84d819f6a1e3cd2200796cf6f87646c0fe6ed` (the pending field-discovery change), **not main**.
Loki 3.7.10 and VictoriaLogs 1.54.0 share the existing dual-written dataset.

`cohort-final.raw.json` is the existing API A/B collector output; `api-summary.json` summarizes six native Fields
queries. Each has one first request and five one-minute forward updates over 15 minutes at a two-second step.
The collector used two existing proxy variants with peer discovery disabled and separate initially empty L2
files. These are attribution measurements, not a fleet scaling test. No second stack or datasource was added.
The primary playground was preserved and diagnostic variants were restored after each run.

The base returns incorrect results for all six mixed fields. Its lower cold resource cost is not equivalent work,
but the candidate's higher cold cost remains a performance blocker. The correct candidate first requests take
209–292 ms versus Loki's 306–807 ms and the incorrect base's 77–142 ms. Forward updates have per-field p50
24–35 ms, p95 25–36 ms (only five samples per field), one request-scoped VL call each. Consumed decoded VL
body bytes fall from about 29.2 MiB on the first request to about 1.95 MiB per update. Seven exact-repeat
requests make zero VL calls. After a process restart with peers disabled and the same SSD, 14 complete answers
still match Loki and make zero VL calls.

CPU process-counter deltas on the six first field requests are 218–302 ms versus base 4–15 ms. API process RSS
is sampled, at most 331 MiB versus base 236 MiB; this is not a true peak or per-query retained memory.
Decoded consumed bytes are not encoded traffic or physical disk throughput/IOPS. Per-layer occupancy,
evictions, encoded disk traffic and physical IOPS were not measured by this collection. Cross-field raw VL
read reuse is not implemented. No whole Fields/Labels 12h–48h, pressure, owner-loss or pixel-equivalence
acceptance is claimed.

`interleaved-*.txt` contains six alternating benchmark pairs, GOMAXPROCS=2. The comparison is the final
candidate against the **same source with only narrow-row scratch reuse removed**, reconstructed using
`scratch-only.patch.gz (decompress before applying)`; it is not a base/PR speedup. The committed benchmark is
`BenchmarkOrderedMixedParserMetric` in `internal/proxy/ordered_json_logs_volume_bench_test.go`.
Median cost is 27.71 → 26.64 ms (-3.86%), allocated bytes 36.00 → 30.09 MiB (-16.42%), and
allocations 529.7k → 475.7k (-10.19%), p=.002, n=6. Allocation savings do not establish production RSS savings.

Functional checks retained separately: 208 recorded Loki engine replies (including runtime errors), 14 complete
instant/range API comparisons, 42 complete forward-window comparisons, and six native Fields tuple comparisons
with the stock app against Loki and the candidate. These do not establish full visual parity or large-window UX.

The full existing typed matrix subsequently passed against this deployed product image: 151/151 subtests,
zero skipped or failed, 6.560 seconds. Five cases retain explicit known-deviation contracts; this is not a
claim that those differences are closed. The full log and source/deployment receipt are retained here.
