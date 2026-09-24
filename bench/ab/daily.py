#!/usr/bin/env python3
"""Daily full A/B run from main: the last release vs main vs Loki, every set, every range.

The scheduled workflow (.github/workflows/perf-daily.yaml) runs this on a
fresh runner and opens a bot pull request with what it writes:

  bench/ab/results/daily-<set>.json   the day's summary per set (report.py
                                      format; overwritten daily, the git
                                      history keeps every day)
  bench/ab/history/<set>.jsonl        one appended line per set per day
  bench/ab/history/trend.md           day-over-day and week-over-week report
  conformance/registry/generated/perf-evidence.json, conformance/reports/
  performance.md and gaps.md          regenerated from the new results

  python3 bench/ab/daily.py --out /tmp/ab-daily                 # everything
  python3 bench/ab/daily.py --out /tmp/ab-daily --sets control --max-range 3h --runs 3 --no-write

The baseline is the newest release tag reachable from HEAD (--baseline-ref
to override), so each day also shows what main has gained since the release.
Ranges are each set's own, up to --max-range (the seeded window is sized from
it); 7d is not seeded.
"""
import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import history  # noqa: E402
import pr_smoke  # noqa: E402
import stack  # noqa: E402

ROOT = stack.ROOT
RESULTS = os.path.join(HERE, "results")
PY = sys.executable
# Daily data profile: a batch per 30 s at a third of the PR smoke's line rate
# (about 25 lines/s), so a 25 h seed stays near 2.3 M lines and 120 k streams
# on a hosted runner. Fixed: every day of history measures the same data.
DAILY_BATCH, DAILY_INTERVAL = 60, 30


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="scratch directory for raw rows, logs and binaries")
    ap.add_argument("--baseline-ref", default="", help="default: newest v* tag reachable from HEAD")
    ap.add_argument("--sets", default="", help="comma-separated sets (default: all)")
    ap.add_argument("--max-range", default="24h")
    ap.add_argument("--runs", type=int, default=7)
    ap.add_argument("--long-runs", type=int, default=3, help="runs of ranges >= 24h for the baseline build")
    ap.add_argument("--noise", type=float, default=0.25)
    ap.add_argument("--min-delta", type=float, default=0.05)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--service", default="loki-vl-proxy-underscore")
    ap.add_argument("--proxy-port", type=int, default=23190)
    ap.add_argument("--seed-timeout", type=int, default=3600)
    ap.add_argument("--no-write", action="store_true", help="do not touch results/, history/ or conformance reports")
    ap.add_argument("--keep-stack", action="store_true")
    stack.stack_args(ap)
    args = ap.parse_args()
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    t0 = time.time()

    with open(os.path.join(HERE, "shapes.json")) as f:
        spec = json.load(f)
    range_defs = spec["ranges"]
    max_secs = range_defs[args.max_range][0]
    sets = [s for s in args.sets.split(",") if s] or list(spec["sets"])
    baseline = args.baseline_ref or pr_smoke.git("describe", "--tags", "--abbrev=0", "--match", "v*", "HEAD")
    head = pr_smoke.git("rev-parse", "HEAD")
    stack.log(f"daily: {baseline} vs main {head[:10]} vs Loki; sets {sets}; ranges up to {args.max_range}")

    st = stack.stack_from(args)
    proxies, trees, summaries = [], [], []
    try:
        st.up()
        seed_s = max_secs + 60 * args.runs + 900
        end = int(time.time()) // 60 * 60
        bins = {name: os.path.join(out, f"proxy-{name}") for name in ("release", "main")}
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            seeding = pool.submit(st.seed, seed_s, end, args.seed_timeout, DAILY_BATCH, DAILY_INTERVAL)
            main_build = pool.submit(stack.build, None, bins["main"])
            release_build = pool.submit(stack.build, baseline, bins["release"])
            trees += [main_build.result(), release_build.result()]
            seeding.result()
        for name, tree, port in (("release", trees[1], args.proxy_port), ("main", trees[0], args.proxy_port + 2)):
            work = os.path.join(out, f"work-{name}")
            os.makedirs(work, exist_ok=True)
            cmd, env = stack.proxy_command(tree, args.service, port, st.vl, work)
            proxy = stack.Proxy(name, bins[name], cmd, env, port, out)
            proxy.start()
            proxies.append(proxy)
        targets = [("release", proxies[0].url), ("main", proxies[1].url), ("loki", st.loki)]
        for set_name in sets:
            ranges = [r for r in spec["sets"][set_name]["ranges"] if range_defs[r][0] <= max_secs]
            summary = pr_smoke.run_matrix(set_name, None, targets, args, end, st, out, args.runs, ranges=ranges,
                                          label="daily", extra=["--long-runs", f"release={args.long_runs}"])
            summary["baseline_ref"], summary["commit"] = baseline, head
            summaries.append(summary)
            if not all(p.alive() for p in proxies):
                raise RuntimeError("a proxy died during the run; see the proxy logs in " + out)
    finally:
        for p in proxies:
            p.stop()
        for tree in trees:
            stack.remove_tree(tree)
        if not args.keep_stack:
            st.down()

    meta = {"baseline": baseline, "commit": head, "runs": args.runs, "max_range": args.max_range,
            "elapsed_s": round(time.time() - t0), "sets": sets,
            "valid": all(s.get("valid", True) for s in summaries)}
    with open(os.path.join(out, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    if args.no_write:
        stack.log(f"--no-write: summaries left in {out}")
        return 0 if meta["valid"] else 2
    if not meta["valid"]:
        # An invalid run (VictoriaLogs restarted) is not evidence: keep it out
        # of results and history, fail the job so the next day retries.
        stack.log("run invalid (VictoriaLogs restarted); nothing written")
        return 2
    for summary in summaries:
        with open(os.path.join(RESULTS, f"daily-{summary['set']}.json"), "w") as f:
            json.dump(summary, f, indent=1)
        history.append(summary, commit=head)
    with open(os.path.join(history.HISTORY, "trend.md"), "w") as f:
        f.write(history.trend())
    for script in ("perf_evidence.py", "gaps.py"):
        subprocess.run([PY, os.path.join(ROOT, "conformance/scripts", script)], cwd=ROOT, check=True)
    stack.log(f"daily run written in {(time.time() - t0) / 60:.1f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())
