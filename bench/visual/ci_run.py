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
  2. capture.spec.ts over the plan (page x range, base | PR | Loki in sequence);
     the Live tail pages in a second pass with the mirrored live generator;
  3. compare.py (panel data and UI state), montage.py (images, at most 300 KB each);
  4. the stack is torn down, whatever happened.

Outputs in --out: plan.json, meta.json, compare.json/.md, pixeldiff.json,
montage/*.png, shots/ and data/ (the raw captures), error.json when the run
died, and the logs. visual_comment.py renders the comment and the verdict from them.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from vio import dump_json, load_json  # noqa: E402

PY = sys.executable


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


def phase(meta, name, t0):
    meta.setdefault("phases_s", {})[name] = round(time.time() - t0)


def differing(out):
    """'page range' of the captures whose base-vs-PR data differs, from compare.json."""
    path = os.path.join(out, "compare.json")
    rows = load_json(path) if os.path.exists(path) else []
    return {f"{r['page']} {r['range']}" for r in rows if r["main_pr_diffs"]}


def recapture(a, out, plan, end, port, meta):
    """Load again, once and more patiently, the static captures whose base-vs-PR data differed or never settled.

    A page that requests in waves can look settled between two waves on a busy host, and a half-loaded side
    differs from a fully loaded one. A difference that survives the second look is the one reported.
    """
    rows = load_json(os.path.join(out, "compare.json")) if os.path.exists(os.path.join(out, "compare.json")) else []
    again = {}
    for r in rows:
        if r["range"] != "live" and (r["main_pr_diffs"] or not r.get("settled", True)):
            again.setdefault(r["page"], {"kind": plan["entries"][r["page"]]["kind"], "ranges": []})["ranges"].append(r["range"])
    n = sum(len(e["ranges"]) for e in again.values())
    if not n or n > a.max_recapture:
        meta["recaptured"] = []
        return False
    retry = os.path.join(out, "plan-recapture.json")
    dump_json(retry, {"entries": again})
    env = dict(os.environ, GRAFANA_URL=f"http://127.0.0.1:{port}", VP_OUT=out, VP_END=str(end), VP_PLAN=retry,
               WORKERS=str(a.workers), VP_QUIET_MS="10000", VP_SETTLE_MS="150000")
    t0 = time.time()
    run(["npx", "playwright", "test"], cwd=HERE, env=env, log=os.path.join(out, "capture.log"), check=False)
    phase(meta, "recapture", t0)
    meta["recaptured"] = [f"{p} {r}" for p, e in again.items() for r in e["ranges"]]
    return True


def capture(a, out, plan, end, port, meta, loki_port):
    """Static captures, then the Live tail pass; a failed Playwright test leaves a capture missing, which the verdict reports."""
    entries = plan["entries"]
    static = [p for p, e in entries.items() if e["kind"] != "tail"]
    tail = [p for p, e in entries.items() if e["kind"] == "tail"]
    env = dict(os.environ, GRAFANA_URL=f"http://127.0.0.1:{port}", VP_OUT=out, VP_END=str(end),
               VP_PLAN=os.path.join(out, "plan.json"), WORKERS=str(a.workers))
    log = os.path.join(out, "capture.log")
    if not os.path.isdir(os.path.join(HERE, "node_modules")):
        run(["npm", "ci", "--no-audit", "--no-fund"], cwd=HERE, log=log)
    t0 = time.time()
    meta["loki_metric_wait_s"] = wait_loki_metric(loki_port, end)
    # The first drilldown load of a fresh Grafana is slow: load it once, unrecorded, before any capture.
    run(["npx", "playwright", "test"], cwd=HERE, env=dict(env, VP_WARMUP="1"), log=log, check=False)
    phase(meta, "warmup", t0)
    t0 = time.time()
    if static:
        r = run(["npx", "playwright", "test"], cwd=HERE, env=env, log=log, check=False)
        meta["playwright_static_exit"] = r.returncode
    phase(meta, "capture", t0)
    if tail:
        t0 = time.time()
        stack = [PY, os.path.join(HERE, "stack.py"), "--out", out, "--project", a.project, "--port-offset", str(a.port_offset)]
        run([*stack, "live-start"], log=log)
        try:
            time.sleep(8)  # the generator has written a few batches to both backends
            r = run(["npx", "playwright", "test"], cwd=HERE, env=dict(env, VP_LIVE="1"), log=log, check=False)
            meta["playwright_live_exit"] = r.returncode
        finally:
            run([*stack, "live-stop"], log=log, check=False)
        phase(meta, "capture_live", t0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", required=True, help="git ref of the base build (CI: the merge commit's first parent)")
    ap.add_argument("--plan", required=True, help="plan.json from plan.py")
    ap.add_argument("--out", required=True)
    ap.add_argument("--head-label", default="", help="PR head sha for the comment (default: HEAD)")
    ap.add_argument("--project", default="vp")
    ap.add_argument("--port-offset", type=int, default=0)
    ap.add_argument("--hours", type=float, default=7, help="VictoriaLogs history; the widest CI range is 6h")
    ap.add_argument("--loki-hours", type=float, default=1.5)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--max-recapture", type=int, default=6, help="re-capture at most this many differing captures once")
    ap.add_argument("--keep-stack", action="store_true")
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    plan = load_json(a.plan)
    dump_json(os.path.join(out, "plan.json"), plan)
    meta = {"base": git("rev-parse", a.base), "head": a.head_label or git("rev-parse", "HEAD"), "plan_captures": plan["captures"]}
    t_all = time.time()
    stack = [PY, os.path.join(HERE, "stack.py"), "--out", out, "--project", a.project, "--port-offset", str(a.port_offset)]
    ok = False
    try:
        if not plan["run"]:
            return 0
        t0 = time.time()
        for attempt in (1, 2):
            try:
                run([*stack, "up", "--main-ref", a.base, "--hours", str(a.hours), "--loki-hours", str(a.loki_hours)],
                    log=os.path.join(out, "stack.log"))
                break
            except subprocess.CalledProcessError:
                if attempt == 2:
                    raise
                # A seed the backend refused (Loki answers 500 while it warms up) or a slow start: begin again on a clean stack.
                run([*stack, "down"], log=os.path.join(out, "stack.log"), check=False)
        phase(meta, "stack_up", t0)
        state = load_json(os.path.join(out, "state.json"))
        capture(a, out, plan, state["end"], state["ports"]["grafana"], meta, state["ports"]["loki"])
        t0 = time.time()
        compare = [PY, os.path.join(HERE, "compare.py"), out]
        run(compare, log=os.path.join(out, "compare.log"), check=False)  # exit 1 = a difference; the verdict decides
        differed = differing(out)
        if recapture(a, out, plan, state["end"], state["ports"]["grafana"], meta):
            run(compare, log=os.path.join(out, "compare.log"), check=False)
            # A difference that was gone on the second look is not a difference of the PR: the run is not deterministic.
            meta["flipped"] = sorted(differed - differing(out))
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
