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
| `perf_matrix.py` | Runs a set against two or more proxy builds and Loki direct, interleaved, and writes raw rows. No target gets a `Cache-Control` header; `--refresh-s` runs the windows forward like a dashboard refresh. |
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
| `history/<set>.jsonl` | one line per set per day, append-only: date, commit, baseline, runs, validity, and one row per shape × range in the column order its `cols` names (proxy status, warm p50, cold, Loki p50, cold and status, result vs Loki, release p50, verdict vs release) |
| `history/trend.md` | per set: proxy p50, day-over-day and week-over-week change (🟢/🔴 beyond 25% and 50 ms), Loki p50, proxy ÷ Loki warm and cold, result vs Loki and a 14-day sparkline |
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

For docs, read `history/<set>.jsonl` (one JSON object per line; zip `cols`
with each row) or reuse `history/trend.md` as is. A pull request that edits
the daily pipeline runs a short version of it (control set, 1h, 3 runs) that
writes the files on the runner and shows the trend in the job summary.
