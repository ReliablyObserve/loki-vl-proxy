# Differential parity run: the proxy against Loki

Finds every way the proxy's answers differ from Loki's on identical data, and
clusters the differences into gap signatures that the conformance registry
must account for. It discovers and registers gaps; it fixes nothing.

| piece | what |
|---|---|
| `corpus.py` | builds the request corpus from five sources: the LogQL examples of Loki's published query documentation (`--loki-docs`, the documentation version such as `v3.7.x`, fetched from grafana.com; selectors are moved onto the seeded data; no Loki source is read), the repository's grammar sweep (`--sweep`: the queries `internal/querycorpus` enumerates from the LogQL grammar, with Loki's recorded accept or reject for each; per query shape and outer operation a few accepted ones moved onto the seeded data, per Loki error text a few rejected ones as written, and our own compositions of the sweep's pipelines and of the stages Explore and Logs Drilldown build inside the range and vector aggregations they use, with and without `unwrap`), the repository's parity cases (`test/e2e-compat`, `query-semantics-matrix.json`, `bench/ab/shapes.json`, every registry case's request), a seeded sweep generated from what Loki reports for the window (every label and value, every service's detected fields with the stages Explore and Logs Drilldown build on them, the metadata endpoints, patterns, format_query and tail), and every request Grafana Explore and Logs Drilldown sent on the `bench/visual` pages (`--captures`). |
| `run.py` | proves both sides healthy and the data equal, then sends every request to Loki and the proxy (query endpoints twice: with `X-Loki-Response-Encoding-Flags: categorize-labels`, as Grafana sends them, and without), replays the Grafana requests through Grafana's API against the Loki and proxy datasources, and diffs each pair (`differ.py`). |
| `differ.py` | the semantic diff: status and error text, result type, log entries matched by timestamp and line (stream order ignored, entry order inside a stream checked against the query's direction; ties at the line limit's boundary timestamp ignored, Loki breaks them arbitrarily), per-entry labels and label categories, metric series sets (including one label set returned as several series) and values (relative tolerance 1e-6; scalars compared as numbers), name/value sets of the metadata endpoints, detected_fields types/parsers/cardinality (5 % for Loki's sketch), index/stats streams and entries. Stats blocks and execution metadata are ignored. Label names are generalised to their class (`__error__`, `*_extracted`, `<stream-label>`, `<line-or-metadata-key>`, ...) so one cause clusters once. Recorded deviations carry their registry case id and are never ranked. |
| `cluster.py` | one cluster per differing request: endpoint class, query shape (outer operation, parser, stages and the kinds of filter used: line, label, pattern), the request's most fundamental differing facet (status before series before labels before values) with error texts reduced to a template. Ranked by user impact (explore- and drilldown-visible first), then distinct corpus queries. Writes `clusters.json` and `report.md`. |
| `refresh.py` | the whole flow below in one entrypoint, as CI runs it (stack, captures, corpus, run, record, prune, regenerate, summary, tear down). |
| `publish.py` | records a run in `conformance/registry/generated/parity-discovery.json`; `conformance/scripts/parity_gaps.py` ranks the registry's gap cases with those counts into `conformance/reports/parity-gaps.md`, and the conformance gate fails while a discovered cluster is accounted for by no registry case. |

## Run

On an isolated stack (own compose project and ports; never the shared e2e stack):

```bash
OUT=<scratch dir>
python3 bench/visual/stack.py up --out $OUT --project parity --port-offset 1500 \
  --hours 1.5 --loki-hours 1.5 --overlay bench/parity/docker-compose.parity.yml
END=$(python3 -c "import json;print(json.load(open('$OUT/state.json'))['end'])")
# Grafana's own requests (optional source): the bench/visual capture of the default pages
(cd bench/visual && npm install && GRAFANA_URL=http://127.0.0.1:34502 VP_OUT=$OUT/cap VP_END=$END \
  VP_PAGES=$(python3 -c "import json;print(','.join(p['id'] for p in json.load(open('spec.json'))['pages'] if p['kind']!='tail'))") \
  VP_RANGES=15m,1h npx playwright test)
python3 bench/parity/corpus.py --out $OUT/corpus.json --loki-docs v3.7.x --sweep \
  --seeded http://127.0.0.1:34601 --end $END --captures $OUT/cap/data
python3 bench/parity/run.py --state $OUT/state.json --corpus $OUT/corpus.json --out $OUT/run
python3 bench/parity/publish.py $OUT/run --ref <the commit the proxy was built from>
python3 conformance/scripts/parity_gaps.py --check   # every cluster registered?
python3 bench/visual/stack.py down --out $OUT
```

`docker-compose.parity.yml` raises Loki's gRPC message limits from the e2e
config's 8 MiB to 100 MiB: a per-stream metric answer over an hour of
generator data is 13-26 MiB between Loki's querier and frontend, and with
8 MiB Loki drops the answer and the client waits for `query_timeout`. Ports:
`--port-offset 1500` puts Loki on 34601, VictoriaLogs on 34928, the proxies
on 34700/34702 and Grafana on 34502.

## Weekly refresh (CI)

`.github/workflows/parity-refresh.yaml` runs `refresh.py --mode full` from `main` every Saturday (and on
dispatch), at an hour clear of `perf-daily.yaml`, on a fresh stack with the whole corpus. The queries come
from the published query documentation of the release the stack runs and the repository's own grammar sweep (the committed
recording `internal/querycorpus/testdata/grammar-sweep-loki.jsonl.gz`), next to the repository's cases, what Loki reports
for the seeded window and Grafana's own requests; no Loki source is checked out or read. The
run also asks the stack's Loki every route and method of the registry's Loki surface
(`conformance/scripts/sync_loki.py --record-live`, which re-records `live-routes.json` and regenerates
`endpoints.json`); when that changes a route's methods, the pull request carries the change and the job ends red
after opening it. The run fetches the documentation from grafana.com; a page that does not load leaves its
examples out (and a corpus source with none refuses pruning). The result goes to the bot branch
`bench/parity-refresh` as a pull request (never to `main`): the recorded run, the registry cases that lost cluster
ids, the regenerated reports, and a summary with every cluster no registry case claims yet. A run that cannot
prove both sides healthy, or whose corpus sources did not all send requests, fails the job and opens nothing. New
signatures need a registry case by hand: the pull request is opened anyway and the job ends red.

A pull request that edits the pipeline runs `refresh.py --mode validate`: an evenly spaced slice of every corpus
source (25 each) through the same steps, recorded as partial. Pruning refuses a partial run, and the result is only
shown in the job summary.

`conformance/scripts/parity_gaps.py --prune` edits the cases for the newest recorded run: a cluster id a case
claims that the run no longer holds is deleted from its `clusters:` list, whatever the case's status, so `--check`
passes after a rerun without a hand edit. Every case that lost ids is listed (`--prune-report FILE` writes it as
markdown); an open, owner-kept, documented or upstream case left with no cluster is called out as undecided and its
status is never changed by the script. It refuses a run with no cluster, a partial run or a corpus source that sent
no request.

## Corpus breadth

The corpus is the same size or larger than the one that read Loki's test files (the run of 2026-11-07, kept as
`tests/fixtures/run-2026-11-07-breadth.json`). "Requests" are what `run.py` counts: a query endpoint is sent twice
(with and without Grafana's response-encoding flag), the other endpoints once; each source is counted on its own, and a request two sources both send is sent once in a run.

| source | 2026-11-07 run | now |
|---|---|---|
| Loki's test files (retired) | 3,592 | 0 |
| Loki's documentation examples | 0 | 162 (81 entries) |
| grammar sweep | 0 | 4,946 (2,473 entries) |
| repository cases | 3,362 | 3,952 (1,976 entries) |
| seeded sweep from Loki's own labels, fields and metadata | 5,484 | built live, the same code |
| Grafana's own requests | 493 | captured live, the same code |

The sweep and repository rows are what `python3 bench/parity/corpus.py --out corpus.json --loki-docs v3.7.x --sweep` builds from the committed sources (the documentation rows need grafana.com, the seeded and Grafana rows a stack); `tests/test_sweep.py` rebuilds the sweep and repository figures and fails when this table disagrees with them. The sweep source keeps one accepted query per query shape (moved onto the seeded data), one rejected query per Loki error text (as written), and 1,473 entries composed from the sweep's own pipelines and the Explore and Logs Drilldown stages, where sending all 25,824 queries (about 56,000 requests, four times the whole earlier run) would add the same shapes many times over. The corpus figures are those of 2026-11-20. `tests/test_sweep.py` also holds what can be held without a stack: the sweep source alone sends at least as many requests
as the retired source did, the three offline sources together at least what the retired source and the repository
cases sent, and every query shape (`cluster.py shape()`: the outer operation, the parser and the stages) of the 154
clusters the retired source reached is reached again by the sweep and the repository cases (the sweep's compositions were built to reach them, and a shape is a necessary condition for finding a cluster, not proof of it). Which clusters a run finds
depends on the stack; the first weekly run after this change shows them, and `parity_gaps.py` lists every one that no
registry case claims.

## Proof that both sides are healthy

`run.py` refuses to compare (exit 2) unless Loki, VictoriaLogs and the proxy
are ready, Loki and VictoriaLogs count the same lines in every 10-minute slice
of the window, Loki's range-metric path is out of its fresh-stack blank window
(one series per service it lists), and the Loki and VictoriaLogs restart
counts can be read. It fails (exit 3) when a restart count changed during the
run, or when the proxy log shows VictoriaLogs failing during it: 5xx answers,
or a fallback or partial answer after one. A 4xx from VictoriaLogs is a query
the proxy translated wrongly, so it is counted in `health.json` but does not
invalidate the comparison: it is the difference being measured.

A request whose Loki answer carries warnings, fails with 5xx or times out, or
whose proxy answer is marked partial is `blocked`, never a difference; two
empty answers are `vacuous` and prove nothing. After the corpus, up to three
requests of every difference signature are sent again (`--no-confirm` skips
it, `--confirm-only` reconfirms an existing run): a signature none of whose
samples differs again is `blocked` as not reproducible, so a Loki answer that
moves between identical requests is never registered as a gap. An index/stats
difference counts as Loki's chunk accounting only when Loki's own
`count_over_time` (entries) or `/series` (streams) over the window equals the
proxy's number.

## Recorded deviations and offline re-diffs

A difference the diff marks as a recorded deviation carries the id of the
registry case that records it (`differ.DOCUMENTED`): index/stats bytes and
chunks, volume byte values of matched series, detected_labels cardinality, and
detected_fields `service` / `service.name` instead of Loki's `_extracted`
(only those two). `parity_gaps.py --check` fails when the id is not a registry
case whose status is documented, owner-kept, upstream or fixed.

`--keep-bodies` stores both answers of every request in
`RUN_DIR/bodies.jsonl.gz`; `--rediff` recomputes `results.jsonl` from them
under the current diff rules without sending a request, so a change to
`differ.py` can be applied to a recorded run.

A fix PR retires a gap without a full rerun: set the case's `gap.status` to
`fixed` with `fixed_by:` (the PR number or commit); its clusters stay claimed
and the report lists it apart until the next recorded run no longer shows them; the weekly refresh then drops them
(`--prune`).

## Tests

```bash
python3 -m unittest discover -s bench/parity/tests
```
