# A/B performance and parity reports

Every performance-relevant pull request carries two proofs: the change is
visible (the shapes it targets got faster or stopped failing) and nothing else
degraded (a fixed control set is unchanged). This directory holds the tooling
and the saved results, so each run can be compared with the ones before it.

| file | purpose |
|---|---|
| `shapes.json` | Named query sets and ranges. `control` is the shared no-degradation set; add a set per change. |
| `perf_matrix.py` | Runs a set against two or more proxy builds and Loki direct, interleaved, and writes raw rows. |
| `report.py` | `summarize` turns raw rows into a compact table and a summary JSON; `compare` diffs two summaries. |
| `results/` | Saved summaries, `<date>-<label>.json`. Commit these; keep raw rows out of the repository. |

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
