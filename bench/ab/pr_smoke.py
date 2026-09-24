#!/usr/bin/env python3
"""Per-pull-request A/B smoke: base build vs PR build vs Loki, on one fresh stack.

The CI job (.github/workflows/perf-ab.yaml) runs exactly this; run it locally
to reproduce a comment:

  python3 bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke
  python3 bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke \\
      --loki-mem 3g --vl-mem 4g          # on a laptop next to the e2e stack

Steps:
  1. selection.py picks the shapes the change needs (nothing for docs/CI-only
     changes: the comment says so and the job ends).
  2. A fresh Loki + VictoriaLogs stack starts under its own compose project
     and ports, and is seeded with a fixed window of the log generator's data
     while the base and PR proxies build (the base in a detached worktree).
  3. Both builds run as host processes with their own tree's stack flags.
  4. perf_matrix.py runs each selected set over the short ranges, the three
     targets interleaved per request on the same pinned windows.
  5. Shapes the first pass flags slower are re-measured with more runs; only a
     slowdown that holds on the re-run is reported slower.
  6. comment.py renders the sticky comment and sets the exit status.

Outputs in --out: selection.json, raw-<set>.json, summary-<set>.json,
comment.md, verdict.json, meta.json and the proxies' logs.
"""
import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import comment  # noqa: E402
import selection  # noqa: E402
import stack  # noqa: E402

ROOT = stack.ROOT
PY = sys.executable


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


def run_matrix(set_name, shapes, targets, args, end, st, out, runs, tag="", ranges=None, label="pr-smoke",
               extra=()):
    """perf_matrix.py for one set, then report.py summarize; returns the summary.

    targets are (name, url) pairs: baseline, candidate, then Loki as 'loki'.
    """
    raw = os.path.join(out, f"raw-{set_name}{tag}.json")
    cmd = [PY, os.path.join(HERE, "perf_matrix.py"), "--set", set_name, "--runs", str(runs),
           "--ranges", ",".join(ranges or args.ranges), "--end", str(end), "--timeout", str(args.timeout),
           "--container", st.vl_container, "--vl-health", f"{st.vl}/health", "--out", raw, *extra]
    for name in shapes or []:
        cmd += ["--shape", name]
    for name, url in targets:
        cmd += ["--target", f"{name}={url}"]
    subprocess.run(cmd, cwd=ROOT, check=True, stdout=sys.stderr)
    summary = os.path.join(out, f"summary-{set_name}{tag}.json")
    subprocess.run([PY, os.path.join(HERE, "report.py"), "summarize", raw, "--baseline", targets[0][0],
                    "--candidate", targets[1][0], "--label", f"{label}-{set_name}{tag}", "--save", summary,
                    "--noise", str(args.noise), "--min-delta", str(args.min_delta)],
                   cwd=ROOT, check=True, stdout=sys.stderr)
    with open(summary) as f:
        return json.load(f)


def confirm(summary, args, end, st, out, targets):
    """Re-measure the shapes the first pass flagged slower; keep the re-run's verdict."""
    slow = sorted({row["shape"] for row in summary["rows"] if row["verdict"] == "slower"})
    if not slow:
        return summary
    stack.log(f"re-measuring {len(slow)} shape(s) flagged slower in {summary['set']}")
    # Windows the first pass did not use: a re-run over the same windows would
    # time the caches the first pass filled on every target.
    again = run_matrix(summary["set"], slow, targets, args, end - 60 * args.runs, st, out, args.confirm_runs,
                       tag="-confirm")
    redo = {(row["shape"], row["range"]): row for row in again["rows"]}
    for i, row in enumerate(summary["rows"]):
        if (row["shape"], row["range"]) in redo and row["verdict"] == "slower":
            summary["rows"][i] = dict(redo[(row["shape"], row["range"])], confirmed=True)
    summary["valid"] = summary.get("valid", True) and again.get("valid", True)
    summary["restart_after"] = again.get("restart_after")
    counts = {}
    for row in summary["rows"]:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
    summary["verdicts"] = counts
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="", help="base git ref (default: merge-base with origin/main)")
    ap.add_argument("--head", default="", help="PR git ref to build (default: the working tree)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--selection", default="", help="precomputed selection.json (default: compute it)")
    ap.add_argument("--ranges", default="1h", help="comma-separated ranges (instant shapes always run instant)")
    ap.add_argument("--runs", type=int, default=4, help="per shape x range x target; the first is cold")
    ap.add_argument("--confirm-runs", type=int, default=7)
    ap.add_argument("--noise", type=float, default=0.30, help="relative p50 change still counted as noise")
    ap.add_argument("--min-delta", type=float, default=0.10, help="absolute p50 change (s) still counted as noise")
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--service", default="loki-vl-proxy-underscore", help="stack proxy service whose flags to use")
    ap.add_argument("--proxy-port", type=int, default=23190, help="base listens here, PR on +2 (admin +1)")
    ap.add_argument("--keep-stack", action="store_true", help="leave the stack running (for a re-run)")
    ap.add_argument("--same-build", action="store_true", help="A/A: run the PR build as both targets (noise check)")
    stack.stack_args(ap)
    args = ap.parse_args()
    args.ranges = [r for r in args.ranges.split(",") if r]
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    t0 = time.time()

    base = args.base or git("merge-base", "origin/main", "HEAD")
    base_sha = git("rev-parse", base)
    head_sha = git("rev-parse", args.head or "HEAD")
    meta = {"base": base_sha, "head": head_sha, "runs": args.runs, "ranges": args.ranges, "noise": args.noise,
            "min_delta": args.min_delta, "confirm_runs": args.confirm_runs, "same_build": args.same_build}

    if args.selection:
        with open(args.selection) as f:
            sel = json.load(f)
    else:
        sel = selection.select(selection.changed_files(base_sha, args.head or "HEAD"), base=base_sha,
                               head=args.head or None)
    with open(os.path.join(out, "selection.json"), "w") as f:
        json.dump(sel, f, indent=1)
    if not sel["run"]:
        text, verdict = comment.render([], sel, meta)
        finish(out, text, verdict, meta)
        return 0

    try:
        summaries = measure(args, sel, out, base_sha)
    except Exception as e:  # noqa: BLE001 - any failure must still produce a comment
        meta["elapsed_s"] = round(time.time() - t0)
        text, verdict = comment.errored(sel, meta, f"{type(e).__name__}: {e}")
        finish(out, text, verdict, meta)
        traceback.print_exc()
        return verdict["exit"]
    meta["elapsed_s"] = round(time.time() - t0)
    text, verdict = comment.render(summaries, sel, meta)
    finish(out, text, verdict, meta)
    return verdict["exit"]


def measure(args, sel, out, base_sha):
    """Stack up, seed, build, run every selected set; returns the summaries."""
    st = stack.stack_from(args)
    proxies, trees, summaries = [], [], []
    try:
        st.up()
        # Window: the longest range, the runs' one-minute shifts and the
        # largest range-vector lookback ([5m]) plus margin, ending at the
        # last whole minute.
        range_secs = max(1 if r == "instant" else stack_range_seconds(r) for r in args.ranges)
        seed_s = range_secs + 60 * (args.runs + args.confirm_runs) + 900
        end = int(time.time()) // 60 * 60
        bins = {name: os.path.join(out, f"proxy-{name}") for name in ("base", "pr")}
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            seeding = pool.submit(st.seed, seed_s, end)
            builds = [pool.submit(stack.build, args.head or None, bins["pr"])]
            if not args.same_build:
                builds.append(pool.submit(stack.build, base_sha, bins["base"]))
        for build in builds:
            if build.exception() is None:
                trees.append(build.result())
        for future in (*builds, seeding):
            future.result()  # re-raise the first failure, after every worktree is recorded for removal
        base_tree = trees[0] if args.same_build else trees[1]
        base_bin = bins["pr"] if args.same_build else bins["base"]
        for name, binary, tree, port in (("base", base_bin, base_tree, args.proxy_port),
                                         ("pr", bins["pr"], trees[0], args.proxy_port + 2)):
            work = os.path.join(out, f"work-{name}")
            os.makedirs(work, exist_ok=True)
            cmd, env = stack.proxy_command(tree, args.service, port, st.vl, work)
            proxy = stack.Proxy(name, binary, cmd, env, port, out)
            proxy.start()
            proxies.append(proxy)
        targets = [("base", proxies[0].url), ("pr", proxies[1].url), ("loki", st.loki)]
        for set_name, shapes in sel["sets"].items():
            summary = run_matrix(set_name, shapes, targets, args, end, st, out, args.runs)
            summaries.append(confirm(summary, args, end, st, out, targets))
            for p in proxies:
                if not p.alive():
                    raise RuntimeError(f"proxy '{p.name}' died during the run; see {p.log_path}")
    finally:
        for p in proxies:
            p.stop()
        for tree in trees:
            stack.remove_tree(tree)
        if not args.keep_stack:
            st.down()
    return summaries


def stack_range_seconds(name):
    with open(os.path.join(HERE, "shapes.json")) as f:
        return json.load(f)["ranges"][name][0]


def finish(out, text, verdict, meta):
    for name, payload in (("comment.md", text), ("verdict.json", json.dumps(verdict, indent=1)),
                          ("meta.json", json.dumps(meta, indent=1))):
        with open(os.path.join(out, name), "w") as f:
            f.write(payload)
    print(text)


if __name__ == "__main__":
    sys.exit(main())
