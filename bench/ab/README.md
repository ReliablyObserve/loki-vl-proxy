# A/B performance and parity reports

Every performance-relevant pull request carries two proofs: the change is
visible (the shapes it targets got faster or stopped failing) and nothing else
degraded (a fixed control set is unchanged). This directory holds the tooling
and the saved results, so each run can be compared with the ones before it.

Both proofs also run on their own: a small A/B smoke on every pull request
that touches runtime code (a sticky comment with the table), and a full run
from `main` every day whose results and history are committed through a bot
pull request. See [Automation](#automation).

| file | purpose |
|---|---|
| `shapes.json` | Named query sets and ranges. `control` is the shared no-degradation set; add a set per change. Shapes marked `"smoke": true` run on every code change. |
| `perf_matrix.py` | Runs a set against two or more proxy builds and Loki direct, interleaved, and writes raw rows. No target gets a `Cache-Control` header; `--refresh-s` runs the windows forward like a dashboard refresh; `--flush-before-run0 http://HOST:ADMIN_PORT/admin/cache/flush` (repeatable, token from `$AB_ADMIN_TOKEN`) empties a proxy's cache tiers (memory and disk: responses, kept stats buckets, parse-risk verdicts) before run 0 of every shape and range, so run 0 is that proxy's cold first request. A request does not wait for the VictoriaLogs CPU sampler (`docker stats` runs in its own thread; each row's `vl_cpu_*` is read from its samples after the loop), and Loki is asked once about a shape and range it times out on: its later runs are not sent and their rows repeat the timeout marked `"skipped": "loki_timeout"` (the raw file lists the first ones under `timeouts`; `--known-timeouts RAW.json` carries them into the refresh and confirm passes). `--pause SECONDS` (default 0) waits after every request. |
| `report.py` | `summarize` turns raw rows into a compact table and a summary JSON; `compare` diffs two summaries. |
| `selection.py` | Picks the shapes a change needs from the files it touches, through the registry. `--check` is part of the conformance gate. |
| `stack.py` | A fresh Loki + VictoriaLogs stack under its own compose project and ports, seeded with a fixed window of the log generator's data; proxy builds run as host processes with the stack proxy's flags. |
| `pr_smoke.py` | The per-PR run: select, stack, seed, build base and PR, measure, re-measure what looks slower, render the comment. |
| `comment.py` | Renders the PR comment and decides the `perf-smoke` check. |
| `daily.py` | The daily run: last release vs `main` vs Loki, every set and range up to 24h. |
| `history.py` | Appends the daily results to `history/<set>.jsonl` and renders `history/trend.md`. |
| `results/` | Saved summaries, `<date>-<label>.json` from pull requests and `daily-<set>.json` from the daily run. Commit these; keep raw rows out of the repository. |
| `history/` | Written by the daily run only: append-only JSONL per set and the generated trend report. |

## Registry

Every shape declares the conformance registry items it measures in `covers`
(cases, behaviours, translations, LogQL constructs, endpoints).
`conformance/scripts/perf_evidence.py` joins the latest saved result for each
shape and range with those items and writes
`conformance/registry/generated/perf-evidence.json` and
[`conformance/reports/performance.md`](../../conformance/reports/performance.md),
which lists every item the proxy answers slower than Loki on first load; the
same items appear in `conformance/reports/gaps.md`. The conformance gate fails
when a shape covers an id the registry does not have, a shape covers nothing, a
saved result names a set or shape `shapes.json` does not define, or the
generated report is stale. Each measurement also records whether the proxy was
slower than Loki warm (and, when a run measured it, on refresh), next to the
cold flag; a row Loki's own cache counters (or the stale-answer check) say it
answered from its results cache is excluded from all of them. Fields an older
result never recorded (Loki's cache counters, refresh timings) are absent in the evidence: unknown, never false. After saving a result, run
`python3 conformance/scripts/perf_evidence.py` and `python3 conformance/scripts/gaps.py`.

## Running

On the e2e compose stack, run the baseline build and the candidate build as
local binaries next to the stack (the stack itself is not restarted):

```bash
# each build: its own port AND its own admin port, the stack proxy's flags,
# the stack's VictoriaLogs, and no rate limit (the stack proxy has none)
./proxy-main   -listen=127.0.0.1:13190 -admin-listen=127.0.0.1:13191 -backend=http://127.0.0.1:19428 \
               -rate-limit-per-second=0 -rate-limit-burst=0 <flags from `docker inspect e2e-proxy-underscore`> &
./proxy-branch -listen=127.0.0.1:13192 -admin-listen=127.0.0.1:13193 -backend=http://127.0.0.1:19428 \
               -rate-limit-per-second=0 -rate-limit-burst=0 <same flags> &

# prove each port belongs to the process you started: a stale proxy on the
# port keeps answering when a new one fails to bind, and measures the wrong build
[ "$(lsof -nP -iTCP:13192 -sTCP:LISTEN -t)" = "$BRANCH_PID" ] || echo "port 13192 is not ours"

python3 bench/ab/perf_matrix.py --set control --runs 5 \
  --target main=http://127.0.0.1:13190 --target branch=http://127.0.0.1:13192 \
  --target loki=http://127.0.0.1:13101 --long-runs main=1 --out /tmp/control.raw.json

python3 bench/ab/report.py summarize /tmp/control.raw.json \
  --baseline main --candidate branch --label pr123-control --save bench/ab/results/
```

`summarize` prints the table for the pull request description:

```
| shape | range | main | branch | loki | change | result vs loki |
| B grouped sum, filter, no drop | 3h | **502** | 0.13s | 0.01s | fixed | same series, sum ±0.06% |
| B grouped sum, filter, no drop | 1h | 3.64s | 0.04s | 0.00s | 88.85× | same series, sum ±0.17% |
...
Verdict: 36 shape×range — 15 fixed, 8 faster, 13 same; results: 0 new difference(s) from loki, 2 pre-existing (also on main).
```

To see what moved between two saved runs (a release against the previous one,
or a pull request against the last result for the same set):

```bash
python3 bench/ab/report.py compare bench/ab/results/2026-09-23-pr611-control.json \
  bench/ab/results/<new>.json --target branch
```

It lists only the shapes that changed beyond noise and prints one summary line.

### What a cold, warm and refresh number mean

Every number is what a client sees, so the cold time must be computed, not
answered from a cache the harness itself filled.

| timing | windows | Loki | proxy builds |
|---|---|---|---|
| cold | run 0 of a shape x range, on a window no earlier request of that query covered | no header; whether its results cache answered is read from its own counters | no header |
| warm | runs 1.. , each window shifted back by the shape's `shift` (a minute by default) | no header; its cache serves the overlapping splits, as for a user | no header |
| refresh (opt-in) | run k ends at `end + k*refresh_s`, the last run at `end` (nothing in the future); the p50 of runs 1.. | no header, so run 0 fills its cache the way a first dashboard load does | no header |

A strictly cold comparison, where neither side answers run 0 from an extent
another set or range filled: start the stack with `--loki-uncached` (`stack.py`
and `pr_smoke.py`; it adds `docker-compose.ab-loki-uncached.yml`, the stack's
Loki with every query results cache off through Loki's
`-querier.cache-*-results=false` flags) and pass each proxy's
`/admin/cache/flush` to `perf_matrix.py --flush-before-run0`. Loki then computes
every request, so in such a run its warm and refresh numbers are cold numbers
too: use it for the cold column and a normal run for warm and refresh.

- No target gets a `Cache-Control` header. Loki 3.7.7 does not honour
  `Cache-Control: no-cache` on `query_range` or index `volume_range`: it still
  looks up and stores results-cache entries with it (the query frontend decodes the
  request without caching options), so sending it would only claim a bypass that
  does not happen. Instant metric queries have no results cache and log queries
  never use it. Loki's own counters are the ground truth for whether a cache
  answered (below).
- Sets do not share windows. `pr_smoke.py` gives each selected set its own block
  of windows (`perf_matrix.plan_windows`): the first pass, the confirm pass (which
  uses the set's real `shift`, not a minute) and the refresh pass, stacked downwards
  from the first set's end, each block as long as the set's largest `shift` needs.
  No request is sent twice across sets, so a later set (the control set after a
  change set, say) never meets an exact-URL cache an earlier one filled. The seeded window
  grows with the number of sets to hold the blocks; `meta.json` records the plan
  (`windows`, `seed_s`, `end`). Shapes of one set may still share a URL on purpose
  (a refresh twin of a plain shape). `--end` pins the first set's end, so a run is
  reproducible window for window.
- Blocks are disjoint in `end`, not in data: a 1h window overlaps the data of the
  neighbouring block's 1h windows. The proxy's window cache and Loki's split cache
  can reuse overlapping data on warm runs; that is what a user refreshing a panel
  gets. Loki's results cache also reuses overlapping time extents of the same query
  string, so a query an earlier set already ran is answered by Loki's cache from
  those extents even on another block of windows. Loki's counters detect it (below),
  and `pr_smoke.py` then takes the first set's computed Loki cold for that query as
  the later row's reference (`loki_cold_reused_from`: the first set and shape; the
  row's other fields are unchanged), shown with `↺` in the cold table. Only a later
  row whose Loki cold was cache-served is changed, and only when the first
  occurrence was a 200 Loki computed. Across `shapes.json` this applies to the
  control set's `json volume no filters` (also `D volume, no filters` of
  `json-filter-pushdown`), to
  `sum(count_over_time({service_name="nginx-ingress"} |> "<_> \"GET <_>" [1m]))`
  (`pattern-line-filter` and `pattern-line-filter-anchoring`) and to the metadata
  endpoints the `metadata-inventory` and `metadata-freshness` sets share.
- Loki's own counters (`data.stats.cache.result`, and the other `*Result` sub-caches
  `volumeResult`, `seriesResult`, `labelResult`, `instantMetricResult`) are stored on
  every Loki request as `cache` (entriesFound, entriesRequested, bytesReceived). A cold
  run with entriesFound or bytesReceived above zero in any of them is marked
  `cold_cache_served` and shown with a dagger in the cold table: Loki's cache answered
  it. `statsResult` is the index-stats cache Loki uses to plan a query; it is hit on
  almost every request, is not a cached answer and is ignored. That adds to the older
  check (identical answers for windows a minute apart, the `†` after a warm number).
  Warm runs finding entries is expected and does not mark the row.
- Refresh is a tracking figure: it is shown next to cold and warm and recorded in
  the evidence (`slower_than_loki_refresh`); it never changes the check's verdict.
- The evidence flags `slower_than_loki_warm` and `slower_than_loki_refresh` are recorded
  only for rows of a run that gave each set its own windows (`isolated_windows`):
  older results shared windows between sets, so their Loki warm figures may come
  from a cache an earlier set primed. For those rows the flags are absent
  (unknown) while the recorded warm p50s still show.

Local stack asymmetry: the stack's Loki runs with `max_cache_freshness_per_query: 0s`
(`test/e2e-compat/loki-local-config.yaml`), so it caches results up to the present
and its warm and refresh answers are as cache-assisted as they can be. Production
Loki defaults to 10m, so the newest 10 minutes are always recomputed there and a
warm or refresh view of a near-now window is slower than on this stack. Read the
Loki warm and refresh columns as a best case for Loki.

### Rows with no stored level

The generator puts a `level` stream label on every row, so a run answers
`detected_level` from the stored fields. To measure the routes that read the
level from the line (JSON `level`, logfmt, plain text), seed a stack whose
rows have no stored level and run the same sets on it:

```bash
python3 bench/ab/pr_smoke.py --base origin/main --no-level-label --out /tmp/ab-body-only
```

(`stack.py seed --no-level-label` does the same for a hand-run stack; the
generator option is `LOG_NO_LEVEL_LABEL=1`.)

The level-filter sets follow the two seeds. `level-filter` (detected_level
filters, Drilldown's level pill and volumes, level filters after a parser,
metrics by detected_level, keep and drop of the level labels) answers with data
on both seeds, so run it on each; its unknown forms (`=~"unknown|error"`) put the
rows without a level under data on the line-only seed. `level-filter-stored` (the
`level` stream label and `level_extracted` filters) answers with data only on the
default seed and is empty on the other (Loki too). The per-PR smoke and the daily
run seed the default stack only, and an empty answer on both sides compares as
the same, so a set that is only meaningful on the line-only seed is not added to
`shapes.json` (`detected_level="unknown"` alone is such a query). Any change to the
code that derives, filters or exposes the level selects these sets through the
registry like any other.

### The first view of a window

A proxy remembers what it learned about a window (which level fields and keys it
holds, that no line needs the slower path), so a warm run does not measure the
first request after a restart, which is what a dashboard opened after a deploy or
a new time range costs. `cold_first_view.py` measures it: a stack seeded with rows
that have no stored level (the data the detected_level pipes read), VictoriaLogs
capped at 2 CPUs, and every build started fresh before each request.
`cold_first_view.py cold --build before=054d1706 --build main=d12a062c --build
head=HEAD --runs 1h=7,24h=5 --out <dir>` writes `coldrestart.json`: wall time and
VictoriaLogs' cgroup CPU (Docker's container stats) of every sample, the builds
interleaved round by round, and the medians; `probes` mode writes `probes.json`:
the requests one first view sends to VictoriaLogs and what each costs when replayed.
The results of a run are committed as `results/oneoff/<date>-<name>-coldrestart.json`
and `-probes.json`.

## Reading the results

- Timings are warm p50 over the runs after the first; the first run is kept as
  `cold` in the summary JSON. A shape is `faster` or `slower` only when the p50
  moves by more than 25% and 50 ms (`--noise`, `--min-delta`).
- `fixed` and `broken` compare HTTP status: the baseline failed and the
  candidate answers, or the other way round.
- `result vs loki` compares each run's answer with Loki's for the same window.
  Differences the baseline also had are counted as pre-existing, not as new.
  Near the live edge Loki can answer from its results cache or lag on the
  newest samples, so small sum differences at 1h are expected; the series set
  must match.
- Warm timings compare cache-assisted answers on both sides: Loki's frontend
  splits a range into 1h pieces and caches each one, so moving the window by a
  minute recomputes only the newest piece; the proxy has its own window cache.
  Use `summarize --cold` for first-run timings, which is what a user opening a
  new panel sees (see
  [What a cold, warm and refresh number mean](#what-a-cold-warm-and-refresh-number-mean)). A cell marked `†` returned identical answers for windows a
  minute apart — a cache or a stale view answered, not a like-for-like timing.
- The VictoriaLogs container's restart count is recorded before and after. A
  run that spans a restart is marked `INVALID` and must be repeated; never
  attribute its errors to either build.
- Repeated 24h raw-row scans can exhaust the shared VictoriaLogs. When a
  baseline still takes a raw-row path, cap its long ranges with `--long-runs`.

## Automation

### Every pull request: the A/B smoke

`.github/workflows/perf-ab.yaml` (check `perf-smoke`) runs
`pr_smoke.py` on each push to a pull request:

1. `selection.py` maps the changed files to shapes (below). A change to docs,
   CI, tests, the Helm chart or registry text selects nothing: the job ends in
   seconds, and an existing comment is updated to say the run was skipped.
2. A fresh Loki and VictoriaLogs start from the e2e compose file with
   `docker-compose.ab.yml` (own project, container names and ports). While
   the log generator seeds a fixed 86-minute window into both (about 75 lines/s,
   seeded random, lines on half-second timestamps) and waits until both count
   the same lines in every 10-minute slice (the window is the longest range plus
   one block of windows per selected set, see above), the base (the merge commit's first
   parent, built in a detached worktree) and the PR build compile.
3. Each build runs as a host process with its own tree's flags of the stack's
   `loki-vl-proxy-underscore` service. A port that is not free fails the run
   instead of measuring someone else's process.
4. `perf_matrix.py` runs the selected shapes over 1h (instant shapes at one
   instant), 4 runs each, base, PR and Loki interleaved per request on the
   same step-aligned windows, each set in its own block of windows ending at or
   below the seeded data's end. No target gets a `Cache-Control` header; a query
   an earlier set already ran takes that set's computed Loki cold.
5. A shape the first pass calls slower or faster is measured again with 7
   runs on windows the first pass did not use, inside the set's block, so the
   proxies' exact-URL caches cannot answer them; only a move that holds is
   reported. Loki's results cache can still answer the re-measured cold run from
   the first pass's overlapping extents: its counters mark that run cache-served,
   so a confirmed row usually has no Loki cold reference.
6. `comment.py` updates one sticky comment and sets the check.

Reproduce it locally (it never touches the e2e stack; smaller memory limits
keep it friendly to a laptop that also runs that stack):

```bash
python3 bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke --loki-mem 3g --vl-mem 4g
python3 bench/ab/pr_smoke.py --same-build --out /tmp/ab-aa ...   # A/A: the noise floor
python3 bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke --refresh-s 60 --refresh-runs 5
                                  # + the refresh pass (60 s is a multiple of every range's step); adds requests,
                                  # so it is off in CI
python3 bench/ab/pr_smoke.py ... --end 1790000000   # pin the first set's end for a reproducible run
```

What to look at on the result (`comment.md`, `summary-<set>.json`, `meta.json`):

- Loki's cold column: a dagger means its own counters say a cache answered. If many
  rows carry one, Loki's cache answered them (a window an earlier request of the
  query covered) or the stack is not fresh.
- Loki's `cache` counters on the warm runs (`raw-<set>.json`, `summary-<set>.json` rows
  `cache.<target>.warm`): entries found there are expected, since run 0 stores its
  result and the next window overlaps it, as for a user refreshing a panel.
- `summary-<set>.json` is the set's final summary (confirmed rows included) and
  carries `isolated_windows: true`. Save that file to `results/`: `perf_evidence.py`
  records warm and refresh gaps only from summaries with this marker, because the
  daily run's sets share windows.
- The cold, warm and refresh columns together: a proxy that is close to Loki cold
  but far behind on warm or refresh is a cache problem, not a query-cost one.
- `meta.json` `windows`: one block per set; no two sets share an `end`.
- The refresh columns exist only with `--refresh-s`; `perf_matrix.py --refresh-s 60
  --runs 5 --end <e> --set <set> --target ... --out refresh.raw.json`, then
  `report.py summarize raw.json ... --refresh refresh.raw.json`, does the same by hand.

### Reading the comment

| column | meaning |
|---|---|
| icon | ❌ broken (base answered 200, PR does not) · 🔴 slower, confirmed by the re-run · ✅ fixed (base failed, PR answers) · 🟢 faster · ⚪ within noise |
| base p50 / PR p50 | warm p50 over runs 2..n; a bold status is an HTTP error |
| change | PR p50 against base p50 |
| Loki p50 | Loki direct, same window |
| PR ÷ Loki | the PR's time as a multiple of Loki's; bold when the PR is more than 25% and 50 ms slower |
| result vs Loki | ✅ same answer (or the same series with a sum within 1%) · ➖ differs, as the base already did · ⚠️ differs, new in this PR |

Rows that did not move are folded into "unchanged", so the visible table is
what the change did. The collapsed sections hold cold (first-run) timings —
what a user opening a panel sees, since warm timings on both sides are
helped by caches — and the reason each shape was selected.

A row the pull request adds above 2× Loki, or makes worse, is listed in a
⚠️ section right under the title (see "Above 2× Loki" below); a collapsed
section lists the rows that are above 2× already. A locked row (see
"Acceptance tiers and the registry lock" below) the pull request takes above
its bound comes first, in its own 🔒 section, and a collapsed 🟡 section lists
the rows that lost their place below Loki.

The check fails on ❌, on 🔴 and on ⚠️ (a new difference from Loki). A run in
which VictoriaLogs restarted is invalid and fails with a request to re-run; a
run that could not finish posts what stopped it. A shape counts as slower only
beyond 30% and 100 ms of the base's p50, and only if the 7-run re-measure
agrees. The thresholds come from A/A runs (`--same-build`: one build as both
targets) of 29 shapes on the seeded stack. On the 4-run first pass the median
move is about 25% and single shapes move up to +73% (+327 ms); two runs had 7
and 2 shapes cross both thresholds. Every shape the 7-run re-measure on fresh
windows took again (6 across both runs) came back within noise (largest
remaining move 15%); the run that re-measured both directions ended with 29
of 29 unchanged. The absolute floor keeps a 20 ms query
from flapping, and the re-measure keeps a single slow request from failing a
pull request. The check is not a required status yet: make it
one once the daily history confirms the noise floor on hosted runners.

### How shapes are selected

Nothing maps paths to shapes by hand. Each shape lists the registry items it
measures (`covers`); registry items name their implementation (`where`,
`seen_in`, `implementation`, and each endpoint's handler in
`conformance/registry/generated/proxy/implementation.json`), and a case links
to the behaviours it proves. A changed file selects every shape whose items,
or the items those link to, name it:

| changed path | runs |
|---|---|
| `internal/`, `pkg/` Go (not tests) | the shapes whose items name the file, plus the smoke subset of `control` |
| an endpoint handler only (for example `proxy.go` for `loki_api_v1_query_range`) | the smoke shapes of that endpoint — nearly every change touches the handler |
| `cmd/`, `go.mod`, `go.sum` | the whole `control` set |
| `bench/ab/shapes.json` | the shapes added or edited, plus smoke |
| the harness (`bench/ab/*.py`, the compose files, Loki config, log generator, this workflow) | the smoke subset |
| anything else, including the `Dockerfile` (the runs build host binaries) | nothing |

`python3 bench/ab/selection.py --base origin/main` prints the selection and
the reason for each shape. `selection.py --check` runs in the conformance gate
and fails when a shape outside `control` cannot be reached from any code —
the fix is an implementation site on the registry item it covers, not a path
list here.

### Adding a shape

1. Add it to the set for your change in `shapes.json` (`name`, `query`, and
   `headers` / `instant` / `logs` / `limit` (a log query's entry limit, default 1000) as needed) with `covers`: the registry
   cases, behaviours or translations it measures. Add `"smoke": true` only to
   a `control` shape that should run on every code change.
2. `python3 bench/ab/selection.py --check` and
   `python3 conformance/scripts/perf_evidence.py --check` must pass.
3. The pull request that adds it runs it (it is "added or edited"), and every
   later change to the code its items name runs it again.

### Every day: the full run and its history

`.github/workflows/perf-daily.yaml` runs `daily.py` at 03:17 UTC (and on
manual dispatch) on a fresh runner: the newest release tag vs `main` vs Loki,
every set over its own ranges up to 24h, 7 runs, on a seeded window of about
24.4h (24h plus the runs' shifts and margin; about 25 lines/s, a batch every
30 s, about 2.2 M lines; fixed so days are comparable). The
release build's 24h ranges are capped at 3 runs. It writes:

| path | content |
|---|---|
| `results/daily-<set>.json` | the day's summary per set (`report.py` format), overwritten daily; git history keeps every day, and `perf_evidence.py` reads it as the latest evidence |
| `history/<set>.jsonl` | one line per set per day, append-only: date, commit, baseline, runs, validity, and one row per shape × range in the column order its `cols` names (proxy status, warm p50, cold, Loki p50, cold and status, result vs Loki, release p50, verdict vs release, and whether Loki computed its cold run or its results cache answered it) |
| `history/trend.md` | per set: proxy p50, day-over-day and week-over-week change (🟢/🔴 beyond 25% and 50 ms), Loki p50, proxy ÷ Loki warm and cold, result vs Loki and a 14-day sparkline |
| `history/ratchet.json` | the rows that were as good as Loki, one entry per set × shape × range; the run only adds to it |
| `history/over-2x-loki.json` | the rows above 2× Loki, one entry per set × shape × range × mode; the run only removes from it (see "Above 2× Loki" below) |
| `history/acceptance.json` | per day of the history, cold and warm: the rows compared and how many were below Loki, close to it, between 1× and 2× and above 2×, with the share below Loki (see "Acceptance tiers and the registry lock" below); regenerated from the history lines every run (`history.py acceptance`) |
| `conformance/registry/generated/perf-evidence.json`, `conformance/reports/performance.md`, `gaps.md` | regenerated from the new results |

The files reach `main` through a bot pull request on the rolling branch
`bench/daily-perf` (never a push to `main`); a day that was not merged yet is
carried into the next day's pull request, so history has no gaps. Closing a
daily pull request without merging does not drop its days while the branch
exists; delete `bench/daily-perf` to discard them. A run in which
VictoriaLogs restarted writes nothing and fails.

A scheduled run is skipped, with no new or updated pull request, when nothing
it measures changed on `main` since the commit the history's newest full
measurement was taken at (`history.py last-commit`; an open daily pull
request's days count). A full measurement is a line with 7 runs and every
range of its set up to 24h, so a cut-down manual run does not count; only the
sets in `shapes.json` count, and a set without a full measurement, or sets
measured at different commits, run the measurement. What it measures is the
proxy's code (`cmd/`, `internal/`, `pkg/`, `go.mod`, `go.sum`, without Go test
files), `bench/ab/` without its results, history, tests and README, the
stack's `test/e2e-compat/docker-compose.yml`, `loki-local-config.yaml` and
`log-generator.py`, and the workflow itself. A manual dispatch always runs, and
a history that cannot be read runs the measurement.

Each run also compares every set's newest history line with the earlier ones
(`history.py alerts`) and reports transitions, not the state many rows are
already in (rows that differ from Loki today do not alert again). Only full
measurements (7 runs, every range of the set up to 24h) count, on either side:
a cut-down manual run is recorded but neither compared nor a baseline, and the
scheduled full run replaces a cut-down line of the same day. Each row is
compared with the newest earlier full line that has it, and only sets in
`shapes.json` are looked at. A regression is a result that went `same` →
`differs` (only when Loki answered 200 in both lines: a Loki failure says
nothing about the proxy), a status that went 200 → an error Loki does not give
or to none, or a warm p50 slower beyond 25% and 50 ms that held for two runs
(both worse than the run before them) or that the same run's verdict against
the release newly calls `slower`. An improvement is the reverse of the first
two (a status that now answers 200 while Loki still gives that error is not
one), or a warm p50 faster beyond the same margin (one run is enough). Each
alert names the commit range to bisect, `<previous line's commit>..<newest
commit>` (a slowdown that held for two runs starts at the line before it
began). Rows where Loki did not answer 200 are listed apart as "Loki failed",
for information only.

A row enters `history/ratchet.json` when its newest line has the same result as
Loki, answers 200 and its cold run is no slower than Loki's cold run, and only
when Loki computed that cold run (`loki_cold_computed`, from Loki's cache
counters): never on warm p50, which for Loki is its results cache, and never on
lines written before the column existed. A ratchet loss is a ratcheted row that
differs from Loki (Loki answering 200) or fails (at once), or whose cold run is
slower than Loki's computed cold run beyond the margin in the newest line and
the previous full one (a cold run is one sample); it repeats every run until
fixed, with the range starting at the newest line the row was still as good as
Loki (or the commit it entered at). Rows leave the list only by hand; the run
counts the rows it did not measure (renamed or removed shapes), and an
unreadable `ratchet.json` is reported and left untouched. The daily run takes
`ratchet.json` from `main` and adds to it: it carries an open daily pull
request's history lines, not its ratchet file, so a row removed on `main`
stays removed.

The run writes the alerts to `<out>/alerts.md` and `alerts.json` (not
committed: they describe one run) and the job summary and artifact hold all of
them. The daily pull request body gets the first 50 entries (`alerts-body.md`,
regressions first) and a count of the rest, since a body is limited to 65,536
characters. A final step fails the job when there is a regression or a ratchet
loss, even when the pull request step failed, so the red run is the
notification. A failure of the alerts themselves is logged and the day is still
delivered. A pull request that edits the pipeline runs a cut-down measurement,
so it has nothing to compare; it never fails on alerts.

### Above 2× Loki

Any shape × range where the proxy is slower than 2× Loki, cold, warm or
refresh, is a defect, as serious as a wrong answer. `over2x.py` holds the one
definition the pull request smoke, the daily run and `perf_evidence.py` share:

- above 2× Loki: proxy time more than 2 × Loki's time and more than 50 ms
  slower (the absolute noise floor of the daily report and `perf_evidence.py`,
  so 10 ms against 3 ms is not counted);
- only when the pair is comparable: both answer 200, and for the cold run,
  Loki's results cache did not answer it (its counters, or identical answers
  for moved windows). Loki's warm and refresh timings come from its results
  cache by design (that is what a repeat view of Loki costs), so they are
  compared as measured. A Loki error or timeout is not a proxy defect: it is
  recorded apart ("Loki failed") and never counted.

`history/over-2x-loki.json` is the list of rows above 2× Loki, seeded from the
daily results of 2026-10-27 (`over2x.py seed`): 165 rows, 55 cold and 110
warm. Each daily run (`history.py alerts`, full measurements only) keeps it:

- a listed row is removed once two consecutive full runs measure it
  comparable and within 2× (a run where Loki failed, or its cache answered the
  cold run, does not count either way). The state comes from the history
  lines, so an unmerged daily pull request does not reset it;
- a listed row's ratio is tightened when a run measures it lower, never raised;
- a row above 2× that is not listed is a warning at the top of the daily pull
  request body and a `::warning::` annotation. It is never added: the list only
  shrinks. The body starts with `above-2× baseline: N rows ...; the gate
  becomes blocking at 0`.

In each pull request, `over2x.gate` compares the PR build's rows with the list
as committed on the base (so a pull request cannot list its own rows) and with
the base build in the same run, which decides first because the smoke's data
and ranges are not the daily run's:

| kind | when |
|---|---|
| known | above 2× on the PR and on the base build within noise (±30% and 100 ms), listed or not; or listed and not worsened; or the base is comparable, just under 2×, and the PR is within noise of it |
| new | above 2× on the PR, not listed, the base not above 2× and the PR slower than the base beyond noise (or the base did not answer comparably) |
| worsened | listed, and the PR is worse beyond noise than both the listed ratio and the base build in the same run; or not listed, above 2× on the base too, and the PR worse than the base beyond noise |

A new or worsened cold or warm row is re-measured with the 7-run pass, like a
slower row, before it is reported. The confirmation pass has no refresh run,
so a refresh finding stands on the first pass. A cold one does too when Loki's
cache answered the re-run's cold run from the first pass's extents (each pass
has one cold sample): the row keeps the first pass's cold timings
(`cold_from_first_pass`).
The mode is set in `perf-ab.yaml` by `--over-2x-loki`:

- `warn` (now): new and worsened rows are a ⚠️ section at the top of the
  comment and `::warning::` annotations; the check's verdict and exit status
  are what they would be without them.
- `fail`: the same rows fail `perf-smoke` (state `failed`, exit 1, title
  "above 2× Loki"). Once the list is empty, switching is the one-line change
  of `--over-2x-loki warn` to `--over-2x-loki fail` in
  `.github/workflows/perf-ab.yaml`. The daily run only ever warns.

To clear a flagged row: fix the path so the shape is within 2× Loki (target 1×)
on cold, warm and refresh, and check the comment's PR ÷ Loki column; a row the
re-measure does not confirm is gone on the next push. A row that is above 2×
on the base build too and not made worse is already known and does not need
clearing in this pull request. Adding a row to the list by hand needs the
owner's approval. `python3 bench/ab/over2x.py replay summary-<set>.json ...`
shows what the gate says about saved summaries; `conformance/reports/performance.md`
lists the current rows under "Above 2× Loki (must fix)".

For docs, read `history/<set>.jsonl` (one JSON object per line; zip `cols`
with each row) or reuse `history/trend.md` as is. A pull request that edits
the daily pipeline runs a short version of it (control set, 1h, 3 runs) that
writes the files on the runner and shows the trend in the job summary.

### Acceptance tiers and the registry lock

The owner's rule (2026-10-28): "our rule's acceptance is max 2× Loki worst
case, but in general acceptance should be close to Loki — same or worse in a
minority of cases — and below Loki for most queries", and up to 2× is accepted
only where going lower is high effort now. Each comparable row (the
definitions of "Above 2× Loki" above) falls in one band, beyond 25% and 50 ms
(`over2x.band`):

| band | meaning | acceptance |
|---|---|---|
| below Loki | Loki is slower beyond noise | the target for most rows |
| close to Loki | within 25% or 50 ms of Loki | accepted |
| 1–2× Loki | slower beyond noise, not above 2× | accepted only with an `effort_exception` on a registry item the shape covers; otherwise listed, flagged for review |
| above 2× Loki | the definition above | a defect (the list above) |

Every registry item a shape covers carries a `perf:` block in its own file
(the form is in `conformance/scripts/registry_io.py`; the conformance gate
checks it):

```yaml
perf:
  max_ratio: 2.0                 # the hard bound, never above 2.0
  target_ratio: 1.0              # below Loki; between it and max_ratio needs an effort_exception
  modes: [cold, warm, refresh]
  locked: true
  locked_since: '2026-10-27'
  effort_exception: null         # or reason: '...' and follow_up: '...' on two indented lines
```

An item is locked once every comparable row of its shapes measured within its
`max_ratio` in every mode on the daily run. The locks were seeded from the
daily results of 2026-10-27 (`over2x.py locks seed`): 29 of the 68 covered
items, which lock 110 cold and 201 warm rows (the daily run has no refresh
pass, so refresh is bounded only in `perf-smoke`); the 39 others have a row
above 2× (they stay on the above-2× list) or no comparable row. A row
(set × shape × range × mode) is locked when any item that covers its shape is
locked for that mode, and its bound is the smallest `max_ratio` of those
items. Locks are set by people, in a pull request: the daily run lists the
unlocked items whose rows were within their bound on its run and the previous
one ("Ready to lock"), and `python3 bench/ab/over2x.py locks lock <id>` locks
one after checking the latest daily results (it refuses an item with a row
above its bound). The daily pull request never edits the registry. The
conformance gate fails a `perf:` block that is malformed, a `max_ratio` above
2.0, a covered item without a block, and a locked item that covers a row on the
above-2× list.

What the lock does:

- **Daily run** (`history.py alerts`): a locked row above its bound is a
  regression, so the run turns red. A warm row (a p50 of seven runs) counts
  after one run. A cold row (one sample) counts when the previous full run was
  above too; the first time it is listed as pending. The noise floor and the
  comparability rules are those of "Above 2× Loki", so a row where Loki
  failed, or where Loki's cache answered the cold run, never counts.
- **Pull requests** (`perf-smoke`): the locks as committed on the base
  (`over2x.bounds_at`, so a pull request cannot unlock its own rows) judge a
  locked row at its bound in the same gate as above. A new or worsened locked
  row is re-measured and then listed in the 🔒 section. Whether it fails the
  check follows `--over-2x-loki` (`warn` today).
- **Lost below-Loki**: a row that was below Loki (the previous daily run, or
  the base build in the same pull request run) and is above Loki now, both
  beyond noise, is flagged, never failing.
- **1–2× rows**: the daily alerts and `conformance/reports/performance.md` list
  them with the items whose exception accepts them, or as flagged with none.
  No exception is seeded: the registry names no high-effort cause for today's
  18 rows (two of them, "A explore volume, 2 filters, drop" over 6h and 24h
  cold, are on locked items).

The distribution is ratcheted on the daily run: on the rows comparable on both
runs, against the previous full run and the full run a week earlier, the share
below Loki may not fall by more than 3 points and 5 rows, and the count above
2× may not rise by more than 2 points and 5 rows; a move beyond both margins is
a regression. A row moves to a worse band there (and is flagged as lost
below-Loki) only when the proxy's own time got slower beyond 25% and 50 ms, so
Loki's cache timings and rows that cross a band edge by a few milliseconds do
not count. Because the week-earlier comparison holds for seven days, a real
move keeps the daily run red for up to a week unless it is fixed: that is the
ratchet, not a flaky run.

Replayed on the history of 2026-10-21 to 2026-10-27, the share below Loki moved
by at most 2 points (6 rows) from one day to the next and no day trips the
distribution ratchet (on 2026-10-22, 30 warm rows crossed the 2× line, 22 of
them with the proxy's own time within noise). The same replay with today's
locks flags 36, 53 and 54 locked rows on 2026-10-22, 10-23 and 10-24 (rows that
became fast by 2026-10-27, so a revert of those speedups is a regression) and 0
on 2026-10-27; going from the 2026-10-27 run back to the 2026-10-24 one trips
the ratchet (warm rows above 2× Loki 98 → 195 of 462). In pull requests,
reverting #758 trips 5 locked rows (J1 to J5 of `json-error-split`).

The daily pull request shows the distribution with the owner's targets (most
rows below Loki; no row above 2×) under "Acceptance versus Loki", and
`conformance/reports/performance.md` shows it over the latest run of every
shape × range, with the lock state of every item. `python3 bench/ab/over2x.py
locks status` prints the locks, the locked rows above their bound, the 1–2×
rows and the distribution for the saved daily results.

To clear a flagged row: make it faster than Loki; when that is high effort now,
add an `effort_exception` to the item, quoting the evidence (what VictoriaLogs
cannot do, the measured cost) and the follow-up that would bring it below Loki,
in a pull request the owner reviews.
