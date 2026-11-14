# Conformance

Everything that defines and proves Loki compatibility lives here.

```
conformance/
  registry/   the inventory, one file per item:
              loki/endpoints/   Loki HTTP endpoints
              loki/errors/      failure classes: one item per status and one per Loki error message
                                (generated: error compatibility, see below)
              loki/logql/       the LogQL surface: every operator, range function,
                                parser, filter and conversion Loki defines
              behaviours/       semantics, severity, identity and data-quality behaviour
                                (detected_level derivation, service_name discovery,
                                window bounds, parse errors, chart density, probes)
              vl/capabilities/  version-gated VictoriaLogs capabilities
              vl/logsql/        every VictoriaLogs pipe and stats function, the release it
                                appeared in, and whether the proxy emits it
              translations/     per feature, end to end: the Loki layer, the VictoriaLogs
                                layer, every difference between them, and the layer the
                                proxy inserts to close each one, with cost and proof
              cases/            edge cases per consumer, with real requests
              state/            the state of each item, owner decisions, waivers
  scripts/    generators and checks that keep the registry true to the code
  reports/    generated output: the coverage map, the translation map and the open gaps
```

- **Registry** — see [registry/README.md](registry/README.md) for the file layout and the fields
  each item carries. Curated prose and owner state survive regeneration; `registry/generated/`
  is extraction output and is never edited by hand.
- **Reports** — [reports/coverage-map.md](reports/coverage-map.md) shows every Loki endpoint with
  its state, whether VictoriaLogs answers it natively or the proxy computes it, which clients
  depend on it, and what evidence exists. [reports/translation-map.md](reports/translation-map.md) shows, per feature, what Loki gives,
  what VictoriaLogs gives, and what the proxy adds between them.
  [reports/gaps.md](reports/gaps.md) ranks what is missing.
  [reports/parity-gaps.md](reports/parity-gaps.md) ranks every known difference from Loki
  (registry cases with a `gap:` block), with the corpus queries of the last differential run
  (`bench/parity`) that hit each one.
  [reports/performance.md](reports/performance.md) joins the saved A/B runs in `bench/ab/results/`
  with the registry items each measured shape covers, and lists every item the proxy answers
  slower than Loki on first load.
- **Gate** — `python3 scripts/ci/check_conformance.py` fails when a test claims an unknown registry
  id, a proxy route has no registry item, an item points at code that no longer exists, or the
  coverage map is stale.

## What the gate enforces

`scripts/ci/check_conformance.py` runs in CI on every pull request and fails when:

- a test claims a registry id that does not exist, a proxy route has no registry
  item, or an item points at code that no longer exists;
- a generated report is stale (performance, coverage map, gaps, translation map,
  roadmap, compatibility matrix, ranked parity gaps);
- a cluster of the last differential run (`registry/generated/parity-discovery.json`)
  is accounted for by no registry case's `gap.clusters`;
- a `bench/ab/shapes.json` shape covers a registry id that does not exist, covers
  nothing, or a saved A/B result does not match the shape sets;
- a flag is missing from the places that list every flag;
- a score or an item's state regressed against `registry/baseline.json`, or a
  waiver expired;
- an item's state claims more than the evidence supports: `proven` without a
  passing test that declares it.

Scores may rise, never fall. `python3 conformance/scripts/ratchet.py --accept`
records an improvement as the new baseline.

The full registry retains optional deletion management for visibility and its
ratchet. Public read-API scores use the separate `read_api` population in
`registry/generated/summary.json`: deletion management, write-only endpoints and
the five explicitly excluded deprecated read aliases do not enter that
denominator. The optional planner endpoint `index/shards` still does. Routed
endpoints, endpoints with differential tests and declared `proven` endpoints
are separate counts, not interchangeable claims of full parity.

The API reference, Loki compatibility page, Known Issues and roadmap also have
generated `api-status` blocks. Refresh a block with
`python3 conformance/scripts/coverage_map.py --api-status docs/api-reference.md`
(substitute the target page). The existing conformance gate checks these pages
along with the README summary, so adding a route must update all affected views.

## Proving a case against the real stack

`live_proof.py` sends each case to Loki and to the proxy on the same data and
compares them under the case's contract:

```bash
python3 conformance/scripts/live_proof.py \
  --loki http://127.0.0.1:13101 --proxy http://127.0.0.1:13100
```

Verdicts are `holds`, `gap` (the answers differ) or `blocked` (one side errored,
or Loki returned nothing, so nothing is proven). Results land in
`registry/generated/live-evidence.json`, and the matrix report shows them beside
the versions under test.

### Per-case verdicts (Go)

`test/caseverify` runs every case whose request is an HTTP request against Loki
and the proxy, through the strict loader (`test/registry`), and writes
`registry/generated/verification/cases.json`: a verdict per case (`matches`,
`differs` with both measured values, `unstable`, `both_fail`, `blocked`,
`not_executable`), the run's date, commit, Loki and VictoriaLogs versions and
proxy flags.

```bash
go run ./test/caseverify -loki http://127.0.0.1:13101 -proxy http://127.0.0.1:13100 \
  -vl http://127.0.0.1:19428 -record /tmp/answers.jsonl -report /tmp/cases.md
go run ./test/caseverify -replay /tmp/answers.jsonl   # the same verdicts, no request sent
```

Relative times (`now`, `now-1h`) are resolved once per run against a settled
end (the run's start less `-settle`, at least 5 minutes, aligned down to the
case's step), so both sides get the same bytes and no window reads data still
arriving. A case compares the facets its `compare.fields` name (or its mode's
default), and always the HTTP status; a difference is asked again before it is
reported.
Each field name reads only its own part of an answer: `timestamps` the sample or entry
timestamps (never the values at them), `structured_metadata_keys` the keys of the structured
metadata (never their values), `fields.label`, `fields.parsers`, `fields.type` and `limit` the
parts of a detected_fields answer, `detectedLabels.cardinality` the cardinalities, and on
`/index/stats` `streams`, `entries`, `bytes` and `chunks` the one number each names. A table
test (`TestEveryFacetReadsOnlyWhatItNames`) pins that a change elsewhere in the answer is not a
difference of the facet.
`ranked_values` compares a `topk` or `bottomk` answer where Loki's choice among series tied at the cutoff is
its Go map order: per grouping key of by()/without(), the ranked values of every step must equal
(`test/metricparity.RankedValuesDifference`) and every series ranked strictly above the cutoff of a full step must
be in both answers with the same value; only the series tied at the cutoff may differ.

No answer is compared on data that is not proven equal. Before a comparison
that reads data, the case's stream selectors (every stream for a request without
a query) are counted over its window on Loki (`sum(count_over_time(...))`) and on
VictoriaLogs (`-vl`, the same matchers as field filters and `stats count()`).
Unequal counts are `data_differs`, no line on either side `empty`, no answer from
Loki's metric path while VictoriaLogs holds the lines `loki_not_ready`, a Loki
answer with warnings or a stale proxy answer `not_comparable`, and no count at
all `data_unchecked`; none of them is a difference of the proxy. Every count is
recorded with the answers, so `-replay` needs no stack. The recording's first line holds the settled end every window was resolved against and the proxy build, so a replay at any later time gives the live run's verdicts.

Log entries compare as Loki defines their equality: per stream, ordered by timestamp with ties ordered by line. An answer that reaches its line limit is compared up to the timestamp both sides hold whole (the timestamp the limit cut at may hold any of its lines), and a patterns answer without its first and last sample buckets. `-proxy-revision` (and `-proxy-image`) record the proxy's build: a revision other than `-commit` marks the run stale and lists its differences apart, never as regressions; `-require-current-proxy` refuses such a run.

Every answer the proxy served is asked once more, with the proxy's `/metrics` read around the first ask and
the repeat: the VictoriaLogs calls (in all and per VictoriaLogs endpoint) and bytes, the cache lookups and hits
per semantic layer (`loki`, `vl`) and tier, CPU and translations of each. A repeat that answers differently or
calls VictoriaLogs again on the endpoints its first ask used is `cache_not_reused` (calls on other endpoints, such
as a health check or the stream field inventory, are counted apart as the proxy's background work), a finding about the cache and never a regression of the case;
the counters are recorded, so a replay gives the same reuse verdicts. Not measured yet, for want of a metric:
allocations per request, plan and translation cache hits, the reuse of proxy-evaluated operators, and a per-request
attribution of the counters (background work counts too). `-no-reuse` skips the repeats. A case whose request
names `proxy_flags` runs against the proxy variant of `-variants` that has them
(`[{"name","url","flags"}]`), or is blocked. Each verdict is set against the case's `gap.status`: a fixed case (or
one without a gap) that differs is a regression (`-check` exits 1), an open case
that matches is a proposed close, and a documented, owner-kept or upstream case
that matches has changed behaviour. The runner never edits a case file: a
proposed close is closed by hand with its evidence.

A case whose answers show an owner-kept deviation names it in `owner_kept` (a list of owner-kept case ids, for
example `metric-identity/structured-metadata-splits-series`). A fixed case that differs from Loki on equal data and
names one is reported as `differs_owner_kept` with the deviation, listed in the report and in `cases.json`, never
passed and never counted as a regression; `-check` prints it and does not fail on it. A name that is not an owner-kept
case blocks the case.

A request the runner does not run says why in `request.verify`: `met_by: stack` with its `evidence` (the generator line
or compose flag that meets the case's `precondition`, so the runner sends it); `blocked_by` one of `load`, `fault`,
`volume`, `version`, `fixture`, `proxy_only`, `moved_window` or `write`, with a `reason`; or `empty_is_answer` for a
fixed case whose point is that a query the proxy once mishandled is answered like Loki, with nothing and no error (two
empty answers then match; never for an open case). Every blocked verdict in `cases.json` and the report carries its
class, so the daily report counts what is blocked by what it needs: also `websocket`, `grafana`, `placeholder_time`,
`empty_answer`, `unread_fields`, `no_answer`, `loki_error` and `no_variant`. `-compose
test/e2e-compat/docker-compose.yml` reads the proxy variants (URL by published port, flags by command) and the default
proxy's flags from the stack's compose file; `-list` sends nothing and prints how many cases go to which proxy and which
are blocked, by class. Redirects are answers, not followed. Loki's `/index/stats` counts flushed chunks only: an answer
whose entries lag behind its own `count_over_time` of the window is `not_comparable`.

### The daily report

`.github/workflows/daily-report.yaml` runs every day on `main`: the per-case
verdicts on a fresh e2e-compat stack for VictoriaLogs v1.49.0 and the pinned
version, the recorded Loki error answers asked again (`test/errorrecord -check`)
and replayed (`TestLokiErrorAnswersReplay`), a slice of the differential parity
corpus, the perf smoke (`bench/ab/daily.py --smoke`) and the health of every
scheduled workflow (`docs/ci-workflows.md`). `test/dailyreport` writes
`registry/generated/verification/summary.json` (every number with the date,
commit and versions it is verified on, and why the day is red) and `cases.json`
(the verdicts of both versions), and the workflow delivers them, the smoke's
history and the inventory's last-run columns through one rolling pull request
on `bench/daily-report`. The grammar sweep and the Loki differentials are
re-verified live by `query-corpus.yaml` each night; the report reads that run's
result. The day is red when a fixed case differs from Loki, a recording drifted,
a parity cluster is claimed by no case, a perf row is above 2x Loki or lost the
weekly ratchet, a scheduled workflow is missing or red, or a job produced no
result.

## Reporting what a change touches

Before opening a pull request that changes client-visible behaviour, run:

```bash
python3 conformance/scripts/pr_report.py           # against origin/main
```

It prints a table of the registry items behind the files you changed: their
state, whether VictoriaLogs serves them natively, and how many tests declare
them. Paste it into the PR description so a reviewer sees which compatibility
contracts the change stands on, and what is still unproven.

## Wiring tests

Every test declares what it proves, so coverage is measured rather than assumed:

```go
// conformance: loki_api_v1_index_volume, drilldown/index_volume/single-sample-returns-vector
func TestVolume_RangeWithOneSamplePerSeriesIsVector(t *testing.T) {
```

```ts
test('landing volume renders services @cov:loki_api_v1_index_volume', async ({ page }) => {
```

The existing evidence collector reads terminal Go JSON events and Playwright results:

```bash
python3 conformance/scripts/evidence.py --go unit-results.json --check
```

`registry/generated/evidence.json` keeps each declaring test's parent summary and an
optional `subtests` array containing full child paths, outcomes and durations. Match Go
tests by package and test name; a same-named test in another package cannot supply proof.
Child durations do not replace the parent's duration. Nested grouping subtests and leaf
cases both remain visible, so the number of child paths is not a count of independent
contracts or a compatibility percentage.

Across multiple inputs, failures remain failures and a skip cannot erase a passing run.
A failed child fails its parent evidence even if a later input reports the parent passing.
Children without a terminal parent result are `incomplete`, which cannot prove an item.
Skipped combinations remain visible; a passing parent does not establish that every child
ran. These records extend the existing registry evidence and do not replace differential
Loki comparisons, retained fixtures or response proofs.

## Refresh

```bash
python3 conformance/scripts/sync_loki.py --version v3.7.7
python3 conformance/scripts/sync_proxy.py --endpoints conformance/registry/generated/loki/v3.7.7/endpoints.json
python3 conformance/scripts/sync_impl.py --coverage conformance/registry/generated/proxy/coverage.json
python3 conformance/scripts/sync_errors.py
python3 conformance/scripts/error_registry.py   # message items, error compatibility matrix and scores, docs/error-compatibility-coverage.md
python3 conformance/scripts/sync_vl.py
python3 conformance/scripts/seed_registry.py \
  --endpoints conformance/registry/generated/loki/v3.7.7/endpoints.json \
  --coverage conformance/registry/generated/proxy/coverage.json
python3 conformance/scripts/coverage_map.py
python3 conformance/scripts/wire.py
python3 conformance/scripts/translation_map.py
python3 conformance/scripts/gaps.py
python3 conformance/scripts/parity_gaps.py   # after bench/parity/publish.py records a run (--prune drops the cluster ids it no longer shows)
```
