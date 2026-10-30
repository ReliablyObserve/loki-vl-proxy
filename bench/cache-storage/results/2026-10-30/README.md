# Cache storage evidence

The matched `committed-*` fixtures commit exactly 100 distinct records per
operation. The admission benchmark in `read-matrix-*` coalesces overwrites and
must not be interpreted as physical disk throughput or per-record commit cost.
Do not combine the different operations into a meaningful aggregate speedup.

Initial read timings include host contention. `quiet-*` retains the focused
repeat checks; allocation counts were stable. The `*.pprof` files are the
batch-100 allocation/CPU profiles, not the constant-work benchmark profiles.
The profile totals are cumulative allocation volume over a run, not RSS.

`fleet-proof.json` records requests, resources, backend calls and contract
bounds across restarts, cache loss and peer failures. `visual-*` records the
general main/PR/Loki browser comparison; no new Loki difference was introduced.
The existing error-rate series discrepancy remains visible rather than hidden.

The fixture and reproduction commands are documented in
`docs/benchmarks/cache-storage-2026-10-30.md`. `SHA256SUMS` covers the raw evidence.
