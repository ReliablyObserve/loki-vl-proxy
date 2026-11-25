#!/usr/bin/env python3
"""The visual-smoke run of one pull request: stack, capture, compare, montage.

The CI job (.github/workflows/visual-smoke.yaml) runs exactly this; run it
locally to reproduce a comment:

  python3 bench/visual/plan.py --base origin/main --out /tmp/vs/plan.json
  python3 bench/visual/ci_run.py --base origin/main --plan /tmp/vs/plan.json --out /tmp/vs \\
      --project vs --port-offset 200          # next to another stack
  python3 bench/visual/visual_comment.py --out /tmp/vs --mode artifact --md /tmp/vs/comment.md

Steps (stack.py does the stack, shared with the manual tooling):
  1. a fresh Loki + VictoriaLogs stack under its own compose project, seeded once
     with a fixed window ending at a fixed minute (state.json: end), the base
     proxy (--base) and the PR proxy (the working tree), and Grafana;
  2. capture.spec.ts over the static plan (page x range, base | PR | Loki in sequence);
     compare.py; the static captures whose data differed (or never settled) once more;
     the captures whose only finding is a first-pass runtime regression measured again cold (repeat_runtime);
  3. the Live tail pages with the mirrored live generator (after the recapture: the generator
     writes into the streams the static windows read), compare.py again over everything,
     montage.py (images, at most 300 KB each);
  4. the stack is torn down, whatever happened.

Outputs in --out: plan.json, meta.json, compare.json/.md, pixeldiff.json,
montage/*.png, shots/ and data/ (the raw captures), error.json when the run
died, and the logs. visual_comment.py renders the comment and the verdict from them.
"""
import argparse
import glob
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import tempfile
import traceback
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from compare import RUNTIME_GUARDS, runtime_problems, runtime_regressions  # noqa: E402
from shards import fixture_hours
from vio import dump_json, load_json  # noqa: E402
from visual_comment import classify, loki_ui_for_gate, new_empty_panel, ui_problems  # noqa: E402

# bench/ab/stack.py under its own name, as stack.py loads it: `stack` on sys.path may be either of the two files.
_spec = importlib.util.spec_from_file_location("ab_stack", os.path.join(HERE, "..", "ab", "stack.py"))
ab_stack = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ab_stack)

PY = sys.executable
REGRESSION = "runtime regression:"


def run(cmd, cwd=None, env=None, log=None, check=True):
    """Run a step; its output goes to the log file (and stays out of the job log unless it fails)."""
    print("+ " + " ".join(cmd), flush=True)
    if log:
        with open(log, "a", encoding="utf-8") as f:
            return subprocess.run(cmd, cwd=cwd, env=env, stdout=f, stderr=subprocess.STDOUT, check=check)
    return subprocess.run(cmd, cwd=cwd, env=env, check=check)


def git(*args):
    return subprocess.run(["git", *args], cwd=HERE, check=True, capture_output=True, text=True).stdout.strip()


def wait_loki_metric(loki_port, end, deadline_s=360):
    """Loki answers range METRIC queries with empty 200s for a couple of minutes after a fresh stack starts (the
    query frontend has no shards yet): wait until it returns data before any capture compares with it."""
    query = urllib.parse.urlencode({"query": 'sum(count_over_time({env="production"}[5m]))', "start": end - 3600, "end": end, "step": 300})
    url = f"http://127.0.0.1:{loki_port}/loki/api/v1/query_range?{query}"
    t0, last = time.time(), "no answer"
    while time.time() - t0 < deadline_s:
        try:
            req = urllib.request.Request(url, headers={"X-Scope-OrgID": "0", "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read())["data"]["result"]
            if any(float(v[1]) > 0 for series in result for v in series["values"]):
                return round(time.time() - t0)
            last = "empty metric answer"
        except (urllib.error.URLError, OSError, ValueError, KeyError) as e:
            last = str(e)[:80]
        time.sleep(5)
    print(f"Loki metric data did not appear in {deadline_s}s ({last}); the Loki columns may be blank", flush=True)
    return None


def seed_fingerprint(state, loki_port, vl_port):
    """Line counts of the seeded window on VictoriaLogs and on Loki: the shards of one run must hold the same data
    (merge.py compares them). None for a backend that does not answer; the comparison then skips it."""
    end, hours = state["end"], state.get("hours", 7)
    out = {"end": end, "hours": hours, "loki_hours": state.get("loki_hours", 1.5)}
    try:
        lo, hi = (time.strftime("%Y-%m-%dT%H:%M:%S.000000000Z", time.gmtime(x)) for x in (end - int(hours * 3600), end))
        query = urllib.parse.urlencode({"query": f'_time:({lo}, {hi}] | stats count() c'})
        with urllib.request.urlopen(f"http://127.0.0.1:{vl_port}/select/logsql/query?{query}", timeout=60) as resp:
            out["vl_lines"] = int(json.loads(resp.read().decode().strip().splitlines()[0])["c"])
    except (urllib.error.URLError, OSError, ValueError, KeyError, IndexError):
        out["vl_lines"] = None
    try:
        window = int(state.get("loki_hours", 1.5) * 3600)
        query = urllib.parse.urlencode({"query": f'sum(count_over_time({{env="production"}}[{window}s]))', "time": end})
        req = urllib.request.Request(f"http://127.0.0.1:{loki_port}/loki/api/v1/query?{query}", headers={"X-Scope-OrgID": "0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())["data"]["result"]
        out["loki_lines"] = int(float(result[0]["value"][1])) if result else None
    except (urllib.error.URLError, OSError, ValueError, KeyError, IndexError):
        out["loki_lines"] = None
    return out


def phase(meta, name, t0):
    meta.setdefault("phases_s", {})[name] = round(time.time() - t0)


def differing(out):
    """'page range' of the captures whose base-vs-PR data differs, from compare.json."""
    path = os.path.join(out, "compare.json")
    rows = load_json(path) if os.path.exists(path) else []
    return {f"{r['page']} {r['range']}" for r in rows if r["main_pr_diffs"]}


def recapture_flips(first, second):
    """Separate vanished data differences from a first pass that missed a request wave."""
    final_diffs = {f"{r['page']} {r['range']}" for r in second if r["main_pr_diffs"]}
    flipped, late = [], []
    for row in first:
        diffs = row["main_pr_diffs"]
        name = f"{row['page']} {row['range']}"
        if not diffs or name in final_diffs:
            continue
        (late if all("request only on one side" in d for d in diffs) else flipped).append(name)
    return sorted(flipped), sorted(late)


def keep_first_pass(out, again, montages=True):
    """Set aside the first-pass rows, scores and montages of the captures about to be loaded again.

    A run split into shards cannot know alone whether the whole run may reload (see recapture): merge.py reads
    these when the shards together found more differences than the cap and the run must stay as a single job
    would have left it, first pass only.
    """
    first = os.path.join(out, "first-pass")
    keep = {(p, r) for p, e in again.items() for r in e["ranges"]}
    path = os.path.join(out, "compare.json")
    os.makedirs(first, exist_ok=True)
    dump_json(os.path.join(first, "compare.json"),
              [r for r in (load_json(path) if os.path.exists(path) else []) if (r["page"], r["range"]) in keep])
    for page, rng in keep:
        for tree in ("shots", "data"):
            src = os.path.join(out, tree, page, rng)
            if os.path.isdir(src):
                shutil.copytree(src, os.path.join(first, tree, page, rng), dirs_exist_ok=True)
    if montages:
        run([PY, os.path.join(HERE, "montage.py"), first], log=os.path.join(out, "montage.log"), check=False)


def needs_second_look(r):
    """A static capture whose base-vs-PR data differed, that never settled, or whose PR side shows a "No data" panel
    the base and Loki do not.

    A side counts as settled once no request is in flight for a few seconds, so a side whose panel query was still
    loading then (the run button's spinner, a blank panel) is captured before its panel says anything: an empty
    answer shows "No data" only on the side that finished first, with the data every side received the same
    (explore-unwrap-missing-label at 15m on #1639 and #1650)."""
    if r["range"] == "live":
        return False
    # A resource regression is kept from the first pass (keep_runtime_observations): a warmer second look cannot
    # clear it, so it is no reason to load the page again and must not use up the cap of recaptures (--max-recapture)
    # that the rows that can flip need (an extra request wave, a one-sided answer).
    return bool(r["main_pr_diffs"] or not r.get("settled", True)
                or any(not p.startswith("runtime regression:") for p in r.get("runtime_problems") or ())
                or new_empty_panel(r.get("ui_main") or {}, r.get("ui_pr") or {}, loki_ui_for_gate(r)))


def keep_runtime_observations(first, second, recaptured=None):
    """A warmed retry with a longer wait cannot clear an earlier resource regression."""
    by = {(r["page"], r["range"]): r for r in first}
    for row in second:
        prior = by.get((row["page"], row["range"]), {})
        problems = [p for p in prior.get("runtime_problems", ()) if p.startswith("runtime regression:")]
        if not prior or (recaptured is not None and f"{row['page']} {row['range']}" not in recaptured):
            continue
        row["runtime_first_pass"] = {k: prior.get(k) for k in ("runtime_main", "runtime_pr", "runtime_loki", "settle_s")}
        for problem in problems:
            message = problem + "; first pass retained: exact-window recapture has warmer caches and a longer wait"
            if message not in row.setdefault("runtime_problems", []):
                row["runtime_problems"].append(message)
    return second


def recapture(a, out, plan, end, port, meta):
    """Load again, once and more patiently, the static captures whose base-vs-PR data differed or never settled, or
    whose PR side shows a "No data" panel the others do not (needs_second_look).

    A page that requests in waves can look settled between two waves on a busy host, and a half-loaded side
    differs from a fully loaded one. A difference that survives the second look is the one reported.

    The cap (--max-recapture) is on the whole run: more differences than that are a real change, not a flake, and
    nothing is loaded again. A shard sees its share only, so it records its count (`recapture_candidates`) and,
    when it loads again because its own count is within the cap, keeps the first pass (keep_first_pass); merge.py
    sums the counts and falls back to the first pass when the run as a whole is over the cap.
    """
    rows = load_json(os.path.join(out, "compare.json")) if os.path.exists(os.path.join(out, "compare.json")) else []
    again, n = {}, 0
    for r in rows:
        if needs_second_look(r):
            again.setdefault(r["page"], {"kind": plan["entries"][r["page"]]["kind"], "ranges": []})["ranges"].append(r["range"])
            # A difference Loki judged an improvement is expected (fix-proof captures differ by design): still
            # loaded again, not counted against the cap.
            n += 0 if classify(r) == "improved" else 1
    meta["recapture_candidates"], meta["max_recapture"] = n, a.max_recapture
    if not again or n > a.max_recapture:
        meta["recaptured"] = []
        return False
    keep_first_pass(out, again, montages=bool(a.shard))
    retry = os.path.join(out, "plan-recapture.json")
    dump_json(retry, {"entries": again})
    stage = tempfile.mkdtemp(prefix="recapture-", dir=out)
    env = dict(capture_env(a, stage, end, port), VP_PLAN=retry, VP_QUIET_MS="10000", VP_SETTLE_MS="150000")
    t0 = time.time()
    result = run(["npx", "playwright", "test"], cwd=HERE, env=env, log=os.path.join(out, "capture.log"), check=False)
    phase(meta, "recapture", t0)
    spec = load_json(os.path.join(HERE, "spec.json"))
    files = [os.path.join(tree, page, rng, f"{ds['id']}.{ext}")
             for page, entry in again.items() for rng in entry["ranges"] for ds in spec["datasources"]
             if ds["id"] != "loki" or spec["ranges"][rng] <= spec.get("loki_capture_max_s", float("inf"))
             for tree, ext in (("shots", "png"), ("data", "json"))]
    missing = [name for name in files if not os.path.isfile(os.path.join(stage, name))
               or os.path.getsize(os.path.join(stage, name)) == 0]
    meta["playwright_recapture_exit"] = result.returncode
    if result.returncode or missing:
        meta["recaptured"] = []
        raise RuntimeError(f"incomplete recapture: Playwright exit {result.returncode}; missing {missing[:6]}")
    for name in files:
        dest = os.path.join(out, name)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copyfile(os.path.join(stage, name), dest)
    meta["recaptured"] = [f"{p} {r}" for p, e in again.items() for r in e["ranges"]]
    return True


def capture_env(a, out, end, port):
    return dict(os.environ, GRAFANA_URL=f"http://127.0.0.1:{port}", VP_OUT=out, VP_END=str(end),
                VP_PLAN=os.path.join(out, "plan.json"), WORKERS=str(a.workers))


# Cold repeats of a first-pass runtime regression (README, "Runtime regressions are measured again cold").
#
# A capture measures each build once, the base first, so a settlement that moves by a second or two from one page load
# to the next (the quiet window ends a few seconds after the last answer, and a late answer moves it) can fail a page
# the pull request does not touch. A warm recapture cannot settle that: the proxies cache, so it would hide a real cold
# cost. Instead, each such page is loaded RUNTIME_REPEATS more times through both builds, each time after both proxies'
# caches were flushed and their logs went quiet, the PR first in the first repeat and the base first in the second.
# The rule: per build, the median of every measured value over the first pass and the repeats, judged by the same
# thresholds as one capture (compare.runtime_regressions). A regression the median shows stands; one it does not show
# is cleared and listed as not reproduced. Any repeat that is not a clean cold sample (not settled, loaded more than
# once, a runtime-proof problem, another API request count than the first pass, a refused flush, Playwright failed,
# over the time budget) leaves the first-pass finding standing, as does a row past the cap.
RUNTIME_REPEATS = 2
MAX_RUNTIME_REPEAT_ROWS = 4
RUNTIME_REPEAT_BUDGET_S = 300
REPEAT_ORDERS = (("pr", "main"), ("main", "pr"))  # the first pass asked main first
FLUSH_QUIET_S, FLUSH_QUIET_CAP_S = 2.0, 10.0


def runtime_repeat_candidate(r, recaptured=()):
    """A static capture whose only finding is a base-vs-PR runtime regression of the first pass.

    Not a recaptured row (its observations stay under keep_runtime_observations), not a fix page (its findings are
    against Loki, `runtime regression vs Loki:`, and Loki's results cache ignores no-cache), and nothing else that fails
    the row: no data difference, settled, no "No data" panel or error banner only the PR shows, no error answer."""
    problems = r.get("runtime_problems") or []
    return (r["range"] != "live" and f"{r['page']} {r['range']}" not in set(recaptured) and bool(problems)
            and all(p.startswith(REGRESSION) for p in problems) and not needs_second_look(r) and not r.get("errors_pr")
            and not ui_problems(r.get("ui_main") or {}, r.get("ui_pr") or {}, loki_ui_for_gate(r)))


def first_pass_observation(row):
    """The row's own capture, as one observation (Loki's side kept for the report; the rule reads main and PR)."""
    return {"order": ["main", "pr", "loki"], "settle_s": dict(row.get("settle_s") or {}),
            **{f"runtime_{side}": row.get(f"runtime_{side}") for side in ("main", "pr", "loki")}}


def repeat_observation(order, captures, first):
    """One cold pair as the row keeps it, with why it cannot count (None when it can)."""
    obs = {"order": list(order), "settle_s": {}, "attempts": {}}
    why = []
    for side in ("main", "pr"):
        cap = captures.get(side)
        if not isinstance(cap, dict):
            why.append(f"{side}: no capture")
            continue
        settle = cap.get("settle_ms")
        obs["settle_s"][side] = settle / 1000 if isinstance(settle, (int, float)) and not isinstance(settle, bool) else None
        obs["runtime_" + side] = cap.get("runtime")
        obs["attempts"][side] = cap.get("attempts")
        if cap.get("attempts") != 1:
            why.append(f"{side}: loaded {cap.get('attempts')} times (a retry meets a warm proxy)")
        proof = runtime_problems(cap, cap, required=True, comparable=False)
        if proof:
            why.append(f"{side}: {proof[0]}")
        requests = (cap.get("runtime") or {}).get("api_requests")
        wanted = (first.get("runtime_" + side) or {}).get("api_requests")
        if requests != wanted:
            why.append(f"{side}: {requests} API requests, the first pass {wanted}")
    if why:
        obs["why"] = "; ".join(why)
    return obs


def median(values):
    values = sorted(values)
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


def median_observation(observations):
    """Per build, the median of each judged value over the observations (shaped like a row: settle_s in seconds)."""
    out = {"settle_s": {}}
    for side in ("main", "pr"):
        out["settle_s"][side] = median([o["settle_s"][side] for o in observations])
        out["runtime_" + side] = {key: median([o["runtime_" + side][key] for o in observations]) for key, _, _ in RUNTIME_GUARDS}
    return out


def judge_runtime_repeat(row, repeat, n=RUNTIME_REPEATS):
    """Apply the rule of the cold repeats to a candidate row of the final comparison; changes the row in place.

    repeat: {"pairs": [observation, ...]} or {"why": reason} for a row that was not measured again. All observations
    stay on the row (`runtime_repeat`); only a median within the thresholds over n clean repeats clears the finding."""
    first = first_pass_observation(row)
    record = row["runtime_repeat"] = {"first_pass": first, "first_pass_findings": list(row.get("runtime_problems") or []),
                                      "pairs": repeat.get("pairs") or [], "rule": f"median of the first pass and {n} cold repeats per build"}
    bad = [p["why"] for p in record["pairs"] if p.get("why")]
    if repeat.get("why") or bad or len(record["pairs"]) < n:
        record["verdict"] = "stands"
        record["why"] = repeat.get("why") or (bad[0] if bad else f"{len(record['pairs'])} of {n} cold repeats taken")
        return row
    observations = [first, *record["pairs"]]
    medians = median_observation(observations)
    record["median"] = medians
    found = runtime_regressions(
        {"settle_ms": medians["settle_s"]["main"] * 1000, "runtime": medians["runtime_main"]},
        {"settle_ms": medians["settle_s"]["pr"] * 1000, "runtime": medians["runtime_pr"]})
    if found:
        record["verdict"] = "reproduced"
        row["runtime_problems"] = [f"{p} (median of {len(observations)} cold observations per build)" for p in found]
    else:
        record["verdict"] = "not reproduced"
        row["runtime_unconfirmed"] = record["first_pass_findings"]
        row["runtime_problems"] = []
    return row


def flush_proxies(state):
    """POST /admin/cache/flush to both proxies (their admin listener, port + 1, as bench/ab/stack.proxy_command starts
    them); then wait until both proxies' logs are quiet, so no background work a flush started runs into a capture."""
    for side in ("main", "pr"):
        url = f"http://127.0.0.1:{state['ports'][side] + 1}/admin/cache/flush"
        req = urllib.request.Request(url, data=b"", method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status
        except urllib.error.HTTPError as e:
            status = e.code
            e.close()
        if status != 200:
            raise OSError(f"cache flush {url}: HTTP {status}")
    logs = [p for side in ("main", "pr") for p in sorted(glob.glob(os.path.join(state["out"], f"proxy-{side}-*.log")))]
    return ab_stack.logs_quiet(logs, quiet=FLUSH_QUIET_S, cap=FLUSH_QUIET_CAP_S)


def bounded(cmd, cwd, env, log, timeout):
    """Run a step that must end by `timeout` seconds; the whole process group (npx, node, Chrome) is stopped then."""
    print("+ " + " ".join(cmd), flush=True)
    with open(log, "a", encoding="utf-8") as f:
        proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=f, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return proc.wait(timeout=max(timeout, 1))
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            return None


def repeat_pair(a, out, page, rng, kind, order, end, port, state, first, deadline):
    """One cold pair of a page: flush both proxies, then load the page through both builds in `order`, once each."""
    try:
        quiet = flush_proxies(state)
    except (OSError, ValueError) as e:  # urllib's errors are OSErrors
        return {"order": list(order), "why": f"cache flush refused: {str(e)[:120]}"}
    stage = tempfile.mkdtemp(prefix="runtime-repeat-", dir=out)
    plan = os.path.join(stage, "plan.json")
    dump_json(plan, {"entries": {page: {"kind": kind, "ranges": [rng]}}})
    # The first pass's settle rule (no VP_QUIET_MS / VP_SETTLE_MS of the patient recapture), one page at a time: two
    # workers would load the other build while one is measured, and a flush would land in the middle of a capture.
    env = dict(capture_env(a, stage, end, port), VP_PLAN=plan, VP_DATASOURCES=",".join(order), WORKERS="1")
    code = bounded(["npx", "playwright", "test"], HERE, env, os.path.join(out, "capture.log"), deadline - time.time())
    if code is None:
        return {"order": list(order), "why": "the time budget of the cold repeats ran out during the pair"}
    captures = {}
    for side in order:
        path = os.path.join(stage, "data", page, rng, f"{side}.json")
        if os.path.isfile(path) and os.path.getsize(path):
            captures[side] = load_json(path)
    obs = repeat_observation(order, captures, first)
    obs["logs_quiet"] = quiet
    if code:
        obs["why"] = "; ".join(x for x in (f"Playwright exit {code}", obs.get("why")) if x)
    return obs


def repeat_runtime(a, out, plan, state, meta):
    """Measure again, cold, the captures whose only finding is a first-pass runtime regression (see RUNTIME_REPEATS).

    Runs before the Live tail pass (its generator writes into the streams the static windows read). Returns
    {"page range": {"pairs": [...]} or {"why": ...}} for judge_runtime_repeat; also written to runtime-repeat.json."""
    path = os.path.join(out, "compare.json")
    rows = load_json(path) if os.path.exists(path) else []
    names = sorted(f"{r['page']} {r['range']}" for r in rows if runtime_repeat_candidate(r, meta.get("recaptured") or ()))
    by = {f"{r['page']} {r['range']}": r for r in rows}
    t0 = time.time()
    deadline = t0 + a.runtime_repeat_budget
    repeats = {}
    for i, name in enumerate(names):
        if i >= a.max_runtime_repeat:
            repeats[name] = {"why": f"over the cap of {a.max_runtime_repeat} cold-repeated captures per shard"}
            continue
        if time.time() >= deadline:
            repeats[name] = {"why": f"the {a.runtime_repeat_budget} s budget of the cold repeats was used up"}
            continue
        page, rng = name.rsplit(" ", 1)
        first = first_pass_observation(by[name])
        pairs = []
        for j in range(a.runtime_repeats):
            pairs.append(repeat_pair(a, out, page, rng, plan["entries"][page]["kind"], REPEAT_ORDERS[j % 2],
                                     state["end"], state["ports"]["grafana"], dict(state, out=out), first, deadline))
            if pairs[-1].get("why"):
                break  # the finding stands: no use in loading the page again
        repeats[name] = {"pairs": pairs}
    if names:
        dump_json(os.path.join(out, "runtime-repeat.json"), repeats)
        phase(meta, "runtime_repeat", t0)
    meta["runtime_repeat"] = {"candidates": names, "repeats": a.runtime_repeats, "max_rows": a.max_runtime_repeat,
                              "budget_s": a.runtime_repeat_budget, "elapsed_s": round(time.time() - t0)}
    return repeats


def apply_runtime_repeats(rows, repeats, n=RUNTIME_REPEATS):
    """Judge every row measured again; a row whose final comparison is no longer a candidate keeps its findings."""
    for row in rows:
        name = f"{row['page']} {row['range']}"
        if name in repeats and runtime_repeat_candidate(row):
            judge_runtime_repeat(row, repeats[name], n)
    return rows


def capture(a, out, plan, end, port, meta, loki_port):
    """The static captures; a failed Playwright test leaves a capture missing, which the verdict reports."""
    static = [p for p, e in plan["entries"].items() if e["kind"] != "tail"]
    env = capture_env(a, out, end, port)
    log = os.path.join(out, "capture.log")
    if not os.path.isdir(os.path.join(HERE, "node_modules")):
        run(["npm", "ci", "--no-audit", "--no-fund"], cwd=HERE, log=log)
    t0 = time.time()
    meta["loki_metric_wait_s"] = wait_loki_metric(loki_port, end)
    state = load_json(os.path.join(out, "state.json"))
    meta["seed"] = seed_fingerprint(state, loki_port, state["ports"]["vl"])
    # The first drilldown load of a fresh Grafana is slow: load it once, unrecorded, before any capture.
    run(["npx", "playwright", "test"], cwd=HERE, env=dict(env, VP_WARMUP="1"), log=log, check=False)
    phase(meta, "warmup", t0)
    t0 = time.time()
    if static:
        r = run(["npx", "playwright", "test"], cwd=HERE, env=env, log=log, check=False)
        meta["playwright_static_exit"] = r.returncode
    phase(meta, "capture", t0)


def capture_live(a, out, plan, end, port, meta):
    """The Live tail pass with the mirrored live generator. It runs after the static captures and their recapture:
    the generator writes into the streams the static windows read, and Loki's index/stats is an estimate from chunk
    metadata that moves while it writes, so a recapture taken after it would compare a different Loki answer."""
    if not any(e["kind"] == "tail" for e in plan["entries"].values()):
        return False
    t0 = time.time()
    log = os.path.join(out, "capture.log")
    stack = [PY, os.path.join(HERE, "stack.py"), "--out", out, "--project", a.project, "--port-offset", str(a.port_offset)]
    run([*stack, "live-start"], log=log)
    try:
        time.sleep(8)  # the generator has written a few batches to both backends
        r = run(["npx", "playwright", "test"], cwd=HERE, env=dict(capture_env(a, out, end, port), VP_LIVE="1"), log=log, check=False)
        meta["playwright_live_exit"] = r.returncode
    finally:
        run([*stack, "live-stop"], log=log, check=False)
    phase(meta, "capture_live", t0)
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", required=True, help="git ref of the base build (CI: the merge commit's first parent)")
    ap.add_argument("--plan", required=True, help="plan.json from plan.py")
    ap.add_argument("--out", required=True)
    ap.add_argument("--head-label", default="", help="PR head sha for the comment (default: HEAD)")
    ap.add_argument("--project", default="vp")
    ap.add_argument("--port-offset", type=int, default=0)
    ap.add_argument("--hours", type=float, default=7, help="minimum VictoriaLogs history, expanded to cover the whole plan")
    ap.add_argument("--loki-hours", type=float, default=1.5)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--max-recapture", type=int, default=6, help="re-capture at most this many differing captures once")
    ap.add_argument("--runtime-repeats", type=int, default=RUNTIME_REPEATS,
                    help="cold base/PR pairs that measure again a capture whose only finding is a runtime regression")
    ap.add_argument("--max-runtime-repeat", type=int, default=MAX_RUNTIME_REPEAT_ROWS,
                    help="measure again at most this many such captures (past it the first-pass finding stands)")
    ap.add_argument("--runtime-repeat-budget", type=int, default=RUNTIME_REPEAT_BUDGET_S,
                    help="seconds all cold repeats of this run may take (past it the first-pass finding stands)")
    ap.add_argument("--end", type=int, default=0, help="seed end (unix seconds); the shards of one run pass the same")
    ap.add_argument("--shard", default="", help="'i/k': this run captures shard i of k (the plan is already that shard's; recorded in meta.json)")
    ap.add_argument("--keep-stack", action="store_true")
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    plan = load_json(a.plan)
    a.hours = max(a.hours, plan.get("fixture_hours", 0), fixture_hours(plan))
    dump_json(os.path.join(out, "plan.json"), plan)
    build_head = git("rev-parse", "HEAD")
    meta = {"base": git("rev-parse", a.base), "head": a.head_label or build_head,
            "build_head": build_head, "plan_captures": plan["captures"]}
    if a.shard:
        meta["shard"] = a.shard
    t_all = time.time()
    stack = [PY, os.path.join(HERE, "stack.py"), "--out", out, "--project", a.project, "--port-offset", str(a.port_offset)]
    ok = False
    try:
        if not plan["run"]:
            return 0
        t0 = time.time()
        for attempt in (1, 2):
            try:
                run([*stack, "up", "--main-ref", a.base, "--hours", str(a.hours), "--loki-hours", str(a.loki_hours),
                     *(["--end", str(a.end)] if a.end else [])], log=os.path.join(out, "stack.log"))
                break
            except subprocess.CalledProcessError:
                if attempt == 2:
                    raise
                # A seed the backend refused (Loki answers 500 while it warms up) or a slow start: begin again on a clean stack.
                run([*stack, "down"], log=os.path.join(out, "stack.log"), check=False)
        phase(meta, "stack_up", t0)
        state = load_json(os.path.join(out, "state.json"))
        meta["stack_up_phases_s"] = state.get("phases_s", {})
        capture(a, out, plan, state["end"], state["ports"]["grafana"], meta, state["ports"]["loki"])
        t0 = time.time()
        compare = [PY, os.path.join(HERE, "compare.py"), out]
        run(compare, log=os.path.join(out, "compare.log"), check=False)  # exit 1 = a difference; the verdict decides
        first = load_json(os.path.join(out, "compare.json")) if os.path.exists(os.path.join(out, "compare.json")) else []
        recaptured = recapture(a, out, plan, state["end"], state["ports"]["grafana"], meta)
        repeats = repeat_runtime(a, out, plan, state, meta)
        live = capture_live(a, out, plan, state["end"], state["ports"]["grafana"], meta)
        if recaptured or live:
            run(compare, log=os.path.join(out, "compare.log"), check=False)
        compared = os.path.join(out, "compare.json")
        if recaptured:
            # A one-sided request that appears on the patient second look was an incomplete first capture.
            # A changed value that disappears remains a non-deterministic data failure.
            second = load_json(compared) if os.path.exists(compared) else []
            dump_json(compared, keep_runtime_observations(first, second, set(meta.get("recaptured", []))))
            meta["flipped"], meta["late_requests"] = recapture_flips(first, second)
        if repeats and os.path.exists(compared):
            dump_json(compared, apply_runtime_repeats(load_json(compared), repeats, a.runtime_repeats))
        run([PY, os.path.join(HERE, "montage.py"), out], log=os.path.join(out, "montage.log"))
        phase(meta, "compare", t0)
        ok = True
    except Exception as e:  # noqa: BLE001 - any failure must still produce a comment and tear down
        traceback.print_exc()
        dump_json(os.path.join(out, "error.json"), {"error": f"{type(e).__name__}: {e}"[:300]})
    finally:
        if plan["run"] and not a.keep_stack:
            t0 = time.time()
            run([*stack, "down"], log=os.path.join(out, "stack.log"), check=False)
            phase(meta, "stack_down", t0)
        meta["elapsed_s"] = round(time.time() - t_all)
        dump_json(os.path.join(out, "meta.json"), meta)
    return 0 if ok or not plan["run"] else 1


if __name__ == "__main__":
    sys.exit(main())
