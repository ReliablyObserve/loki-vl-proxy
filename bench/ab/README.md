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
| `perf_matrix.py` | Runs a set against two or more proxy builds and Loki direct, interleaved, and writes raw rows. |
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
generated report is stale. After saving a result, run
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
  new panel sees. A cell marked `†` returned identical answers for windows a
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
   the same lines in every 10-minute slice, the base (the merge commit's first
   parent, built in a detached worktree) and the PR build compile.
3. Each build runs as a host process with its own tree's flags of the stack's
   `loki-vl-proxy-underscore` service. A port that is not free fails the run
   instead of measuring someone else's process.
4. `perf_matrix.py` runs the selected shapes over 1h (instant shapes at one
   instant), 4 runs each, base, PR and Loki interleaved per request on the
   same step-aligned windows ending at the seeded data's end.
5. A shape the first pass calls slower is measured again with 7 runs; only a
   slowdown that holds is reported.
6. `comment.py` updates one sticky comment and sets the check.

Reproduce it locally (it never touches the e2e stack; smaller memory limits
keep it friendly to a laptop that also runs that stack):

```bash
python3 bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke --loki-mem 3g --vl-mem 4g
python3 bench/ab/pr_smoke.py --same-build --out /tmp/ab-aa ...   # A/A: the noise floor
```

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
agrees. Those thresholds come from A/A runs (the same build as both targets)
on the seeded stack, whose largest per-shape move was NOISE_AA; hosted runners
are noisier than a laptop, so the absolute floor is what keeps a 20 ms query
from flapping. The check is not a required status yet: make it one once the
daily history confirms the noise floor on hosted runners.

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
| `cmd/`, `go.mod`, `go.sum`, `Dockerfile` | the whole `control` set |
| `bench/ab/shapes.json` | the shapes added or edited, plus smoke |
| the harness (`bench/ab/*.py`, the compose files, Loki config, log generator, this workflow) | the smoke subset |
| anything else | nothing |

`python3 bench/ab/selection.py --base origin/main` prints the selection and
the reason for each shape. `selection.py --check` runs in the conformance gate
and fails when a shape outside `control` cannot be reached from any code —
the fix is an implementation site on the registry item it covers, not a path
list here.

### Adding a shape

1. Add it to the set for your change in `shapes.json` (`name`, `query`, and
   `headers` / `instant` / `logs` as needed) with `covers`: the registry
   cases, behaviours or translations it measures. Add `"smoke": true` only to
   a `control` shape that should run on every code change.
2. `python3 bench/ab/selection.py --check` and
   `python3 conformance/scripts/perf_evidence.py --check` must pass.
3. The pull request that adds it runs it (it is "added or edited"), and every
   later change to the code its items name runs it again.

### Every day: the full run and its history

`.github/workflows/perf-daily.yaml` runs `daily.py` at 03:17 UTC (and on
manual dispatch) on a fresh runner: the newest release tag vs `main` vs Loki,
every set over its own ranges up to 24h, 7 runs, on a seeded 25h window
(about 25 lines/s, a batch every 30 s; fixed so days are comparable). The
release build's 24h ranges are capped at 3 runs. It writes:

| path | content |
|---|---|
| `results/daily-<set>.json` | the day's summary per set (`report.py` format), overwritten daily; git history keeps every day, and `perf_evidence.py` reads it as the latest evidence |
| `history/<set>.jsonl` | one line per set per day, append-only: date, commit, baseline, runs, validity, and one row per shape × range in the column order its `cols` names (proxy status, warm p50, cold, Loki p50, cold and status, result vs Loki, release p50, verdict vs release) |
| `history/trend.md` | per set: proxy p50, day-over-day and week-over-week change (🟢/🔴 beyond 25% and 50 ms), Loki p50, proxy ÷ Loki warm and cold, result vs Loki and a 14-day sparkline |
| `conformance/registry/generated/perf-evidence.json`, `conformance/reports/performance.md`, `gaps.md` | regenerated from the new results |

The files reach `main` through a bot pull request on the rolling branch
`bench/daily-perf` (never a push to `main`); a day that was not merged yet is
carried into the next day's pull request, so history has no gaps. A run in
which VictoriaLogs restarted writes nothing and fails.

For docs, read `history/<set>.jsonl` (one JSON object per line; zip `cols`
with each row) or reuse `history/trend.md` as is. A pull request that edits
the daily pipeline runs a short version of it (control set, 1h, 3 runs) that
writes the files on the runner and shows the trend in the job summary.
