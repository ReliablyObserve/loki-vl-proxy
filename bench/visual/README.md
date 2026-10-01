# Visual proof for a pull request

Every PR carries before/after Grafana screenshots of the panels it affects,
with Loki as the reference, plus a comparison of the panel data Grafana
receives. This folder is the reusable tooling.

What runs, on an isolated stack (own compose project `vp`, own ports, never
the e2e stack):

| piece | what |
|---|---|
| `stack.py up` | Loki + VictoriaLogs in Docker (via `bench/ab/stack.py`), the **main** proxy (built from `--main-ref`, default `origin/main`) and the **PR** proxy (built from the working tree) as host processes with the flags of the e2e `loki-vl-proxy-patterns-autodetect` service (the profile Logs Drilldown opens by default), and Grafana with the pinned plugins of the e2e compose. Datasources: `vp-main` "Loki (via VL proxy main)", `vp-pr` "Loki (via VL proxy)", `vp-loki` "Loki (direct)". |
| seeding | the e2e log generator's backfill, once, ending at a fixed minute (`state.json: end`); no live generator, so the data is static and all datasources see the same window. VictoriaLogs gets `--hours` (default 24; use 168 for 7d), Loki only the last `--loki-hours` (default 1.5: Loki on a laptop stalls on longer backfills), so ranges beyond that compare main vs PR only. |
| `spec.json` | the page list (profile `drilldown+explore`): Explore graphs and log view for the query shapes under test, the label browser, and the Logs Drilldown landing, service logs, labels, label values, fields, field values and patterns pages, and Explore Live tail (plain and `| json` filtered) through the proxies as Loki-type datasources. Edit `pages` for a PR; query shapes come from `bench/ab/shapes.json`. |
| `capture.spec.ts` | Playwright: per page, range and datasource, opens the page with an absolute window, waits for the backend traffic to settle, saves a screenshot and every `/api/ds/query` body and response plus datasource resource calls (so Drilldown's internal queries are covered). |
| `stack.py live-start` / `live-stop` | the e2e generator in live mode, writing every line to both Loki and VictoriaLogs (mirrored) so Live tail sees the same stream on all three datasources. Start it only around the tail capture; static ranges are unaffected (they end at the seed end). |
| `compare.py` | matches the captured requests of main, PR and Loki and compares series sets, point values (rel. 1e-9) and timestamps; main vs PR must be identical. Loki is compared for the ranges it holds. Writes `compare.md` / `compare.json`. |
| `montage.py` | `montage/<page>-<range>.png` (main, PR, Loki side by side, downscaled, at most 300 KB each) and `pixeldiff.json` (share of pixels that differ main vs PR). |
| `plan.py`, `ci_run.py`, `comment.py`, `publish.py` | the per-PR CI run, see [In CI](#in-ci). |

## Run

```bash
OUT=/tmp/vp-out
python3 bench/visual/stack.py up --out $OUT --hours 168      # ~40 min for 7d of data; --hours 24 is quick
cd bench/visual && npm install                               # once
END=$(python3 -c "import json;print(json.load(open('$OUT/state.json'))['end'])")
PAGES=$(python3 -c "import json;print(','.join(p['id'] for p in json.load(open('spec.json'))['pages'] if p['kind']!='tail'))")
GRAFANA_URL=http://127.0.0.1:33002 VP_OUT=$OUT VP_END=$END VP_PAGES=$PAGES npx playwright test   # static ranges
python3 stack.py live-start --out $OUT                       # Live tail: ~25 s of mirrored streaming
GRAFANA_URL=http://127.0.0.1:33002 VP_OUT=$OUT VP_PAGES=tail-env,tail-json-filter VP_RANGES=live npx playwright test
python3 stack.py live-stop --out $OUT
python3 compare.py $OUT          # exit 1 when main and PR differ
python3 montage.py $OUT
python3 stack.py down --out $OUT
```

`stack.py up --skip-seed` restarts the proxies and Grafana on data seeded earlier. Filters: `VP_PAGES=explore-B,dd-fields VP_RANGES=15m,1h`, `WORKERS=2`.
Outputs: `$OUT/shots`, `$OUT/data`, `$OUT/montage`, `$OUT/compare.md`,
`$OUT/pixeldiff.json`. Before pushing images, keep each montage at or below
about 300 KB and host them on the `pr-visuals` branch under `pr-<number>/`,
then embed them from the PR description with
`https://raw.githubusercontent.com/ReliablyObserve/loki-vl-proxy/pr-visuals/pr-<number>/<file>.png`.

The e2e-ui spec `explore-json-filter-panels.spec.ts` (`@explore-core`) runs on
this stack too (`GRAFANA_URL=... EXPLORE_WINDOW_END_MS=$((END*1000)) npx
playwright test --grep @explore-core` from `test/e2e-ui`).

Reading the results: the pixel-diff score counts every changed pixel, including
the datasource name in the page header (the three datasources have different
names, and a longer name moves the time picker), so a small non-zero score on
an otherwise identical page is expected; `compare.md` is the proof, the images
are for the eye. A Grafana whose Loki plugin process dies under memory pressure
(very large breakdowns over 7d) answers `plugin unavailable`; the capture
repeats such a page load up to three times, and a request that still fails is
listed by `compare.py` as one-sided.

## In CI

`.github/workflows/visual-smoke.yaml` runs this tooling on every pull request,
like the A/B performance smoke does, and posts one sticky comment.

**What runs.** `plan.py` reads the changed files (base = the merge commit's first
parent) and picks captures in two tiers:

- *Core set*: the `spec.json` entries marked `"core": true` (Explore logs and a
  metric graph, Drilldown landing, a service's Logs and Fields), 5 captures at
  `core_range` (1h). They run on every relevant change, including a change to
  this tooling.
- *Detailed set*: entries whose `covers` (conformance registry ids, as in
  `bench/ab/shapes.json`) point at a source file the change touches, at
  `ci_ranges` (15m, 1h, 6h), plus the entries a change to `spec.json` adds or
  edits. The code-to-registry mapping is `bench/ab/selection.py`'s (imported, not
  copied): registry implementation sites, `// conformance:` tags and `covers`
  links, so a new registry item or a moved implementation changes the selection
  with no path list here. The `loki_api_v1_query_range` and `loki_api_v1_query`
  handlers are not followed (every page reads them); every other endpoint
  handler selects the entries that show that endpoint. `scripts/ci/check_conformance.py`
  runs `plan.py --check`: every entry's `covers` must resolve, and a non-core entry
  must reach code. A change that touches nothing visual (docs, CI, Helm, tests,
  registry text) runs nothing, and an existing comment is updated to say so.
  A capture budget (36 page x range captures) first narrows the detailed entries
  to 1h, then drops the last ones, and the comment says which.

Preview a selection: `python3 bench/visual/plan.py --base origin/main` or
`--files internal/proxy/ordered_json_metric.go`.

**Stack and capture.** `ci_run.py` brings up the same isolated stack as the manual
run (`stack.py up`: base build from the first parent, PR build from the checked-out
merge commit, Loki and VictoriaLogs seeded once through the generator backfill, ending
at a fixed minute, so base, PR and Loki see the same static window). VictoriaLogs
holds 7h, Loki 1.5h: 15m and 1h are compared with Loki, 6h is base vs PR only. A
warm-up page load of each kind comes first (a fresh Grafana loads the Drilldown app
slowly), then the static captures, then, only when a Live tail entry was selected,
the mirrored live generator and the tail captures. 24h and 7d stay out of PR CI
(use the manual run).

**Gate.** The `visual-smoke` check fails when, on the PR side and not on the base:

- the data Grafana receives differs from the base's (series, values, timestamps,
  resource responses; patterns compared as a set), or a request exists on one side only;
- a panel that showed data on the base is empty (no non-zero points or rows), or the
  page shows more "No data" panels than the base;
- an error banner (plugin unavailable, failed to load, something went wrong), a panel
  error icon or an error boundary appears that the base does not show;
- a planned capture is missing, or the run died (exit 3, "did not complete").

Warnings only: a pixel diff above 3% (header and time-picker noise stays below),
a page that did not settle, a capture with no data on either side. Differences from
Loki never fail by themselves: the comment counts them and says how many the base
has too (pre-existing); a difference the base does not have would also be a base-vs-PR
difference.

**Images.** Three jobs, so that no code of the pull request runs with a write token:
`visual-smoke` (read-only: stack, capture, compare, gate), `visual-publish`
(`contents: write`, same-repo PRs only, runs repository code and git only) and
`visual-comment` (`pull-requests: write`, same-repo PRs only). The publish job
replaces `pr-<number>/` on the orphan `pr-visuals` branch with the run's montages in
one commit by the github-actions identity (CI cannot sign; every push-triggered
workflow of the repository runs for `main` only, and a push with the workflow token
starts none), refusing files that are not plain PNG names within 300 KB. The comment
embeds an image only for a row that is not clean and for one collapsed block with the
core montages; clean detailed rows are linked on the branch. A fork PR has no write
token: the montages are an artifact, the comment text has no images, and the table is
in the job summary.

**Reproduce a comment locally** (next to another stack: its own project and ports):

```bash
python3 bench/visual/plan.py --base origin/main --out /tmp/vs/plan.json
python3 bench/visual/ci_run.py --base origin/main --plan /tmp/vs/plan.json --out /tmp/vs --project vs --port-offset 1000
python3 bench/visual/comment.py --out /tmp/vs --mode artifact --md /tmp/vs/comment.md   # verdict in /tmp/vs/verdict.json
python3 -m unittest discover -s bench/visual/tests
```

Timing, measured locally on a busy laptop (images pulled, Go cache warm): the core
set took 6 minutes (stack 2.5, warm-up 2, 5 captures 1, compare and teardown 0.5); a
selection of 35 captures (34 static at two workers, one Live tail) took 4.5 minutes of
static captures, 0.5 of Live tail and the same stack and warm-up, about 11 minutes in
all. On `ubuntu-latest`, add the runner setup (checkout, Go and npm caches, Docker
pulls, about 2-3 minutes): about 6-9 minutes for the core set and 13-17 for a full
detailed selection, inside the job's 30 minute limit.
