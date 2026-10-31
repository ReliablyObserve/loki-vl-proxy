# Long-range Fields memory proof

Measured on 2026-10-31 with Go 1.27.3, Linux/arm64 proxy binaries, Loki 3.7.9
and VictoriaLogs 1.52.0 in an isolated Docker stack. The baseline is main
`27ee67a14c1b56ec4bf7a3bd8dd869e42ba6ad25`. Candidate binary SHA-256:
`f32086ca03fc7d752d880eb873d6e63797a5df7ff756730a686fbedbcc38bc58`.
Both binaries use `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath
-ldflags="-s -w" ./cmd/proxy`; their hashes are recorded in the summaries.

The immutable 360,000-row fixture spans 48 hours, with 80% in its final three
hours. Six concurrent requests use the reported `60000ms` step and a fractional
end. Each proxy starts fresh under the same 2 GiB Docker limit and 2 GiB
`GOMEMLIMIT`; Loki and VL remain running between measurements. The series cap
is deliberately raised to 1,000,000 for this high-cardinality stress case; the
shipped default remains 500. Loki's result cache is disabled. This is memory
and correctness evidence, not a controlled latency comparison.

| Build and range | Requests with complete Loki-matching answers | Sampled peak RSS | Outcome |
|---|---:|---:|---|
| Main, 12h | 3 / 6 | 2,035 MiB | OOM flag; three connections lost |
| Candidate, 12h | 6 / 6 | 554 MiB | Running, no OOM |
| Candidate, 48h | 6 / 6 | 661 MiB | Running, no OOM |

The initial main reproduction also ended with Docker `OOMKilled=true` and
exit 137; its state is retained in `main-initial-oom-state.json`. The final public-harness
main control above reproduced the OOM flag and failed requests with already
warm backends. Every successful main response also matched Loki.

Each candidate matrix is compared completely with Loki: labels, timestamps and
values. Before comparison the fixture oracle checks reference cardinality,
aggregate event counts and axis. At 48h, trace/span have 360,000 series each,
request/total 180,000 each, user_id five and version two. A flush/completeness
gate prevents an incompletely exposed Loki head chunk from passing as a valid
reference. Raw sampled memory, container state, exact queries and response
hashes are included beside this document. Sampling is 4 Hz, so peaks are
observations rather than continuous high-water marks.

The default Go suite separately locks large-response allocation limits and pool
boundaries. Same-binary full-tree/per-series benchmarks are included:

| Fixture | Full-tree allocated bytes/op | Per-series allocated bytes/op | Allocations/op, full → per-series |
|---|---:|---:|---:|
| 1,000 series × 60 points | 89.73 MiB | 2.56 MiB | ~135,100 → ~3,038 |
| 12,000 series × 2 points | 63.21 MiB | 2.80 MiB | ~168,100 → ~36,030 |

This reference includes the new parser retention cutoff; it is **not** a claim
about main's warm pool allocations. The original-source small-path control and
candidate both use 54 allocations/op. Shared-host timing controls varied
substantially. Per-series CPU time was higher in these large microbenchmarks;
the change trades parsing CPU for lower memory, and makes no speedup claim.

The real-backend `TestDrilldown_LongRangeFieldsBurst` passes 28 comparisons:
seven field shapes × 12h/48h × first/repeated request, with 6,960 fixture rows,
288 high-cardinality series and twelve populated points per series. Non-version
count replies exceed 64 KiB. That test runs in the existing CI Drilldown shard;
the larger pressure harness exercises the >1 MiB production dispatch. Race,
differential fuzz, full Go tests, lint and the configured gosec rules also pass.

Reproduce with the [portable harness](../../README.md). Its fresh setup, seed,
reference, start, measurement and cleanup lifecycle was separately exercised
with 2,000 rows. The proof does not establish a hard process-memory bound:
input/output buffers, one very large series, and unsupported-shape fallbacks
can still require substantial memory. It covers the reported whole-second
start and fractional end; fractional-start timestamp normalization remains a
separate compatibility difference.
