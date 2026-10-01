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
| `montage.py` | `montage/<page>-<range>.png` (main, PR, Loki side by side, downscaled) and `pixeldiff.json` (share of pixels that differ main vs PR). |

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
