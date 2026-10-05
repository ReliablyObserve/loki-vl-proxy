# Differential parity run: the proxy against Loki

Finds every way the proxy's answers differ from Loki's on identical data, and
clusters the differences into gap signatures that the conformance registry
must account for. It discovers and registers gaps; it fixes nothing.

| piece | what |
|---|---|
| `corpus.py` | builds the request corpus from four sources: LogQL from Loki's own test suites (`--loki-src`, a checkout of `grafana/loki` at the reference version; selectors are moved onto the seeded data, sharding/template fixtures dropped), the repository's parity cases (`test/e2e-compat`, `query-semantics-matrix.json`, `bench/ab/shapes.json`, every registry case's request), a seeded sweep generated from what Loki reports for the window (every label and value, every service's detected fields with the stages Explore and Logs Drilldown build on them, the metadata endpoints, patterns, format_query and tail), and every request Grafana Explore and Logs Drilldown sent on the `bench/visual` pages (`--captures`). |
| `run.py` | proves both sides healthy and the data equal, then sends every request to Loki and the proxy (query endpoints twice: with `X-Loki-Response-Encoding-Flags: categorize-labels`, as Grafana sends them, and without), replays the Grafana requests through Grafana's API against the Loki and proxy datasources, and diffs each pair (`differ.py`). |
| `differ.py` | the semantic diff: status and error text, result type, log entries matched by timestamp and line (stream order ignored, entry order inside a stream checked against the query's direction; ties at the line limit's boundary timestamp ignored, Loki breaks them arbitrarily), per-entry labels and label categories, metric series sets (including one label set returned as several series) and values (relative tolerance 1e-6; scalars compared as numbers), name/value sets of the metadata endpoints, detected_fields types/parsers/cardinality (5 % for Loki's sketch), index/stats streams and entries. Stats blocks and execution metadata are ignored. Label names are generalised to their class (`__error__`, `*_extracted`, `<stream-label>`, `<line-or-metadata-key>`, ...) so one cause clusters once. Recorded deviations carry their registry case id and are never ranked. |
| `cluster.py` | one cluster per differing request: endpoint class, query shape (outer operation, parser, stages and the kinds of filter used: line, label, pattern), the request's most fundamental differing facet (status before series before labels before values) with error texts reduced to a template. Ranked by user impact (explore- and drilldown-visible first), then distinct corpus queries. Writes `clusters.json` and `report.md`. |
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
python3 bench/parity/corpus.py --out $OUT/corpus.json --loki-src <loki checkout> \
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
and the report lists it apart until the next recorded run no longer shows them.

## Tests

```bash
python3 -m unittest discover -s bench/parity/tests
```
