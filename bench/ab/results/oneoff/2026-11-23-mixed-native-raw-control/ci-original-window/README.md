# Original-window CI evidence

These are CI collector, comparator and seeder mechanism checks. Product Go code
is unchanged from the measured `fb2b09872464` source. The persistent playground
uses `playground-3a808613394b`; its two existing peers are **not** distinct
main/PR product builds. These checks do not establish a cold-query speedup,
browser acceptance, current-image disk/fleet behavior or merge readiness.

## Complete answers and independent windows

The maintained collector, summary and merge-proof validator ran against the
two existing peers and Loki 3.7.10. Matrix, refresh and confirmation each retained
six timed replies and one additional complete Loki correctness reply at that
phase's original window: **21 bodies** in total, plus six reference prewarm
reads. Each candidate comparison checks full labels, timestamps and values;
an equal aggregate signature alone is insufficient.

The measured reference deliberately exercised Loki's result cache. A separate
128-character space/tab suffix obtained a computed reference without changing
the measured query or replacing its time/cache labels. Counter evidence must
confirm computation: the header alone does not prove a bypass. Each phase has
its own receipt; a matching later window cannot repair an earlier answer.

Required cells retain at most 32 MiB per target and 96 MiB per cell. Missing,
oversized, malformed, ambiguous-source or incomplete evidence stays incomplete.
Errors compare status, errorType and normalized error text. A wrong candidate
blocks; a valid candidate that corrects main's answer can pass while main's
full difference facets remain visible.

See [retained original-window replies](original-window/receipt.json),
[flushed-resample mechanics](cold-resample.json),
[source hashes](source-hashes.json) and
[full unit-test logs and counts](verification.json).

## Metadata API observations

Loki's labels, label-values, detected-labels, detected-fields and patterns
responses can omit cache counters. Their complete measured response is retained
as an **API observation, cache unavailable**, never labelled a computed cache
miss. Present malformed counters fail; a volume result with valid cache-hit
counters still needs its separate computed reference.

The existing catalog's 20 metadata shapes replay recorded Loki envelopes, with
an additional index/stats schema control. Pattern comparison includes identity,
level and every timestamp/count. Field comparison checks paths and attributes;
duplicate identities and malformed volumes fail. Only the registry's existing
narrowly documented deviations remain explicitly `documented`; an OPEN
candidate difference blocks. Actual API observations and bodies are retained
under [metadata reference evidence](metadata-reference/receipt.json).

## Backfill and scheduling

The prior CI seed failed before measurement: Loki was OOM-killed with a 12 GiB
limit while preparing approximately 72.58 hours of normal-generator data.
VictoriaLogs remained healthy. See [backend failure](seed-failure.json).

The A/B seeder now triggers `/flush` after 10,000 acknowledged stream objects,
at existing dual-write payload boundaries. Rows, labels, timestamps, random
sequence and backend limits are unchanged. Flush failure stops the seed without
replay. A flush trigger is **not** a memory bound or completion acknowledgment;
the final flush and per-slice equal-count readiness gate remain required.

A bounded normal-generator check wrote 36 rows to each existing backend and
observed one checkpoint after both writes succeeded. The first VL read was
empty; a later read of the same input, without reingestion, returned all 36
timestamp/line pairs. Both the [initial failed read](seed-first-read.json) and
[after-visibility proof](seed-after-visibility.json) remain retained. This does
not prove that the full CI seed avoids OOM.

Required runtime selections permit up to eight shards; ordinary selections
retain four, and the existing cost heuristic can use fewer. Replaying the
actual workflow preserved all **309 shapes, 791 cells and 1,345 profile/query/
range contracts**, exactly once. Timeouts, runs, profiles, ranges and gates are
unchanged. The largest shard cost falls from 339 to 171, but aggregate seeded
history grows about 59% and the longest individual seed remains about 72.38 h.
This trades runner/ingestion work for a shorter query schedule; it does not
resolve the seed memory failure by itself. See [partition evidence](partition.json).

## Acceptance remains open

At `ca22cb1e09e5`, CI finished with 122 successful checks, three failures and
four cancellations. One A/B ingestion shard recorded Loki OOM; the other three
reached the 60-minute job limit. Visual smoke retained 14 runtime regressions
and seven unproven comparisons. The scoped positive rate proof passed, but
24-hour rate settlement and broader Fields evidence still fail acceptance.

The existing main-versus-candidate cold CPU/latency/RSS regressions remain
blocking. No result here relabels them, relaxes a gate or claims the next CI run
has passed.
