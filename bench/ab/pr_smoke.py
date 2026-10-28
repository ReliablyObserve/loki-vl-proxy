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
     targets interleaved per request on the same pinned windows. Every set has
     its own block of windows (perf_matrix.plan_windows): no request repeats
     across sets. No target gets a Cache-Control header (Loki 3.7.7 ignores
     no-cache on query_range). Loki's results cache can still answer a query two
     sets share from the earlier set's overlapping extents; Loki's own cache
     counters detect that, and the later row's Loki cold reference becomes the
     first set's computed run of the same query (loki_cold_reused_from).
  5. Shapes the first pass calls slower or faster are re-measured with more runs
     on windows it did not use; only a move that holds is reported.
     With --refresh-s N each set is also run in refresh mode (windows moving
     forward N seconds per run, like a dashboard refresh) and its p50 is
     reported next to cold and warm. Off by default: it adds requests.
     A row above 2× Loki that the PR adds or makes worse (over2x.gate, against
     bench/ab/history/over-2x-loki.json as committed on the base) is re-measured
     the same way before it is reported; so is a locked row (the registry's
     perf: blocks as committed on the base) above its bound.
  6. comment.py renders the sticky comment and sets the exit status. With
     --over-2x-loki warn (the default) an above-2× finding is a warning at the
     top of the comment and a ::warning:: annotation; with fail it fails the job.

Outputs in --out: selection.json, raw-<set>.json, summary-<set>.json,
comment.md, verdict.json, meta.json and the proxies' logs.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import comment  # noqa: E402
import over2x  # noqa: E402
import perf_matrix  # noqa: E402
import selection  # noqa: E402
import stack  # noqa: E402

ROOT = stack.ROOT
PY = sys.executable


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


def matrix_cmd(set_name, shapes, targets, args, end, st, runs, ranges, raw, extra=()):
    """The perf_matrix.py command line for one set over the given windows."""
    cmd = [PY, os.path.join(HERE, "perf_matrix.py"), "--set", set_name, "--runs", str(runs),
           "--ranges", ",".join(ranges or args.ranges), "--end", str(end), "--timeout", str(args.timeout),
           "--container", st.vl_container, "--vl-health", f"{st.vl}/health", "--out", raw, *extra]
    for name in shapes or []:
        cmd += ["--shape", name]
    for name, url in targets:
        cmd += ["--target", f"{name}={url}"]
    return cmd


def run_matrix(set_name, shapes, targets, args, end, st, out, runs, tag="", ranges=None, label="pr-smoke",
               extra=(), refresh=None):
    """perf_matrix.py for one set, then report.py summarize; returns the summary.

    targets are (name, url) pairs: baseline, candidate, then Loki as 'loki'.
    refresh is (end, seconds per run, runs) for an extra refresh-mode pass whose
    p50 goes into the summary, or None.
    """
    raw = os.path.join(out, f"raw-{set_name}{tag}.json")
    subprocess.run(matrix_cmd(set_name, shapes, targets, args, end, st, runs, ranges, raw, extra),
                   cwd=ROOT, check=True, stdout=sys.stderr)
    summary = os.path.join(out, f"summary-{set_name}{tag}.json")
    report_cmd = [PY, os.path.join(HERE, "report.py"), "summarize", raw, "--baseline", targets[0][0],
                  "--candidate", targets[1][0], "--label", f"{label}-{set_name}{tag}", "--save", summary,
                  "--noise", str(args.noise), "--min-delta", str(args.min_delta)]
    if refresh:
        refresh_end, refresh_s, refresh_runs = refresh
        raw_refresh = os.path.join(out, f"raw-{set_name}{tag}-refresh.json")
        # Same shapes and targets; the windows move forward inside the set's own block.
        # A shape and range Loki timed out on in the first pass is not sent to Loki again.
        subprocess.run(matrix_cmd(set_name, shapes, targets, args, refresh_end, st, refresh_runs, ranges,
                                  raw_refresh, [*extra, "--refresh-s", str(refresh_s), "--known-timeouts", raw]),
                       cwd=ROOT, check=True, stdout=sys.stderr)
        report_cmd += ["--refresh", raw_refresh]
    subprocess.run(report_cmd, cwd=ROOT, check=True, stdout=sys.stderr)
    with open(summary) as f:
        return json.load(f)


# Keys a re-measured row would lose: the refresh pass is not repeated in the confirm pass.
KEEP_ON_CONFIRM = ("refresh", "refresh_status")


def confirm(summary, args, plan, st, out, targets, baseline=None, bounds=None):
    """Re-measure the shapes the first pass called slower or faster, or found newly or further above 2× Loki;
    keep the re-run's rows.

    A/A runs (the same build as both targets) move single shapes by up to
    ~70% on a first pass of 4 runs; a 7-run pass on fresh windows settles them.
    """
    moved = ("slower", "faster")
    over = (set(over2x.remeasure(summary, baseline, args.noise, args.min_delta, bounds)) if baseline is not None
            else set())
    slow = sorted({row["shape"] for row in summary["rows"] if row["verdict"] in moved} | over)
    if not slow:
        return summary
    stack.log(f"re-measuring {len(slow)} shape(s) the first pass moved in {summary['set']}")
    # Windows the first pass did not use (plan_windows reserves them, shifts included):
    # a re-run over the same windows would time the caches the first pass filled.
    # Loki is not asked again about a shape and range it timed out on in the first pass: the re-run's Loki rows
    # repeat that timeout.
    first_raw = os.path.join(out, f"raw-{summary['set']}.json")
    again = run_matrix(summary["set"], slow, targets, args, plan["confirm_end"], st, out, args.confirm_runs,
                       tag="-confirm", extra=("--known-timeouts", first_raw))
    redo = {(row["shape"], row["range"]): row for row in again["rows"]}
    for i, row in enumerate(summary["rows"]):
        if (row["shape"], row["range"]) in redo and (row["verdict"] in moved or row["shape"] in over):
            new = dict(redo[(row["shape"], row["range"])], confirmed=True,
                       **{k: row[k] for k in KEEP_ON_CONFIRM if k in row})
            ref = summary.get("reference", "loki")
            if (row["shape"] in over and ref in new.get("cold_cache_served", [])
                    and ref not in row.get("cold_cache_served", [])):
                # Loki's cache answered the re-run's cold from the first pass's overlapping extents: for the
                # above-2× check the first pass's computed cold stands (each pass has one cold sample).
                new.pop("cold_cache_served")
                new.update(cold=row["cold"], cold_from_first_pass=True)
            summary["rows"][i] = new
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
    ap.add_argument("--refresh-s", type=int, default=0, metavar="N",
                    help="also run each set in refresh mode: windows move forward N seconds per run (a multiple of "
                         "every range's step), p50 reported next to cold and warm; 0 = off (default)")
    ap.add_argument("--refresh-runs", type=int, default=5, help="runs of the refresh pass; the first fills the caches")
    ap.add_argument("--end", type=int, default=0,
                    help="unix seconds the first set's windows end at, floored to the minute (default: now); "
                         "with the same value a run is reproducible window for window")
    ap.add_argument("--noise", type=float, default=0.30, help="relative p50 change still counted as noise")
    ap.add_argument("--min-delta", type=float, default=0.10, help="absolute p50 change (s) still counted as noise")
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--service", default="loki-vl-proxy-underscore", help="stack proxy service whose flags to use")
    ap.add_argument("--proxy-port", type=int, default=23190, help="base listens here, PR on +2 (admin +1)")
    ap.add_argument("--keep-stack", action="store_true", help="leave the stack running (for a re-run)")
    ap.add_argument("--same-build", action="store_true", help="A/A: run the PR build as both targets (noise check)")
    ap.add_argument("--over-2x-loki", choices=over2x.GATE_MODES, default=over2x.DEFAULT_GATE,
                    help="a row the PR adds above 2x Loki or makes worse: warn (comment + annotation, the verdict is "
                         "unchanged) or fail (fails the check)")
    ap.add_argument("--over-2x-baseline", default="",
                    help="the above-2x Loki list to compare with (default: bench/ab/history/over-2x-loki.json as "
                         "committed on the base ref, so a PR cannot list its own rows)")
    stack.stack_args(ap)
    args = ap.parse_args()
    args.ranges = [r for r in args.ranges.split(",") if r]
    with open(os.path.join(HERE, "shapes.json")) as f:
        steps = [v[1] for k, v in json.load(f)["ranges"].items() if k in args.ranges]
    problem = perf_matrix.refresh_problem(args.refresh_s, args.refresh_runs, steps)
    if problem:
        ap.error(problem)  # before the stack starts, not after the first pass
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    t0 = time.time()

    meta = {"runs": args.runs, "ranges": args.ranges, "noise": args.noise, "min_delta": args.min_delta,
            "confirm_runs": args.confirm_runs, "same_build": args.same_build,
            "refresh_s": args.refresh_s, "refresh_runs": args.refresh_runs, "over_2x_loki": args.over_2x_loki}
    sel = {"run": True, "sets": {}}
    try:
        base = args.base or git("merge-base", "origin/main", "HEAD")
        meta["base"] = base_sha = git("rev-parse", base)
        meta["head"] = git("rev-parse", args.head or "HEAD")
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
        else:
            if args.over_2x_baseline:
                baseline, problem = over2x.read_baseline(args.over_2x_baseline)
                if problem:
                    raise ValueError(problem)
            else:
                baseline = over2x.baseline_at(base_sha)
            # The locks as committed on the base: a pull request cannot unlock its own rows.
            bounds = over2x.bounds_at(base_sha)
            if bounds is None:
                stack.log("the registry's perf: blocks at the base cannot be read; no row is locked in this run")
            summaries = measure(args, sel, out, base_sha, meta, baseline, bounds)
            meta["elapsed_s"] = round(time.time() - t0)
            text, verdict = comment.render(summaries, sel, meta, baseline, bounds)
    except Exception as e:  # noqa: BLE001 - any failure must still produce a comment and exit 3
        traceback.print_exc()
        meta["elapsed_s"] = round(time.time() - t0)
        text, verdict = comment.errored(sel, meta, f"{type(e).__name__}: {e}")
    finish(out, text, verdict, meta)
    for line in over2x.annotations(verdict.get("over_2x", {}).get("rows", []), args.over_2x_loki):
        print(line)
    return verdict["exit"]


# The seed's wait for Loki and VictoriaLogs to agree took well under 900 s for the 86-minute window
# the smoke used to seed; a longer window gets a proportionally longer wait.
SEED_TIMEOUT_PER_SECOND = 900 / (86 * 60)


def window_plan(args, sel, end):
    """(per-set window plans, seconds to seed ending at `end`).

    Each selected set gets its own block of windows below the previous set's
    (perf_matrix.plan_windows), so no request is sent twice across sets; the
    seed covers the longest range, every block, and the [5m] lookback plus margin.
    """
    with open(os.path.join(HERE, "shapes.json")) as f:
        spec = json.load(f)
    refresh_runs = args.refresh_runs if args.refresh_s else 0
    plans = perf_matrix.plan_windows(spec, list(sel["sets"].items()), args.ranges, args.runs, args.confirm_runs,
                                     end, args.refresh_s, refresh_runs)
    range_secs = max(1 if r == "instant" else stack_range_seconds(r) for r in args.ranges)
    blocks = plans[-1]["offset"] + plans[-1]["span"] if plans else 0
    return plans, range_secs + blocks + 900


def reuse_first_cold(summaries):
    """Take a repeated query's Loki cold from the first set that computed it.

    Loki's results cache reuses overlapping extents of the same query, so the
    cold run of a query an earlier set already ran can be answered from the
    extents that set stored (Loki's own counters say so: cold_cache_served). In
    run order, per (path, query) and range, the first occurrence is the
    reference; a later row whose Loki cold was cache-served, when the first
    occurrence was a 200 that Loki computed, gets the first one's Loki cold, loses
    the cache-served mark and records loki_cold_reused_from. Nothing else of the
    row changes. Instant queries have no results cache and are skipped.
    """
    with open(os.path.join(HERE, "shapes.json")) as f:
        spec = json.load(f)
    first = {}
    for summary in summaries:
        shapes = {sh["name"]: sh for sh in spec["sets"].get(summary["set"], {}).get("shapes", [])}
        ref = summary.get("reference", "loki")
        for row in summary["rows"]:
            key = perf_matrix.query_key(shapes.get(row["shape"], {"instant": True}))
            if key is None:
                continue
            key = (key, row["range"])
            if key not in first:
                served = ref in row.get("cold_cache_served", [])
                first[key] = (summary["set"], row["shape"], None if served or row["status"].get(ref) != "200"
                              else row["cold"].get(ref))
                continue
            origin_set, origin_shape, cold = first[key]
            if cold is None or ref not in row.get("cold_cache_served", []):
                continue
            row["cold"][ref] = cold
            left = [t for t in row["cold_cache_served"] if t != ref]
            if left:
                row["cold_cache_served"] = left
            else:
                del row["cold_cache_served"]
            row["loki_cold_reused_from"] = {"set": origin_set, "shape": origin_shape}
    return summaries


def write_summaries(summaries, out):
    for summary in summaries:
        with open(os.path.join(out, f"summary-{summary['set']}.json"), "w") as f:
            json.dump(summary, f, indent=1)


def measure(args, sel, out, base_sha, meta=None, baseline=None, bounds=None):
    """Stack up, seed, build, run every selected set; returns the summaries."""
    st = stack.stack_from(args)
    proxies, trees, summaries = [], {}, []
    try:
        st.up()
        end = (args.end or int(time.time())) // 60 * 60
        plans, seed_s = window_plan(args, sel, end)
        if meta is not None:
            meta["end"], meta["seed_s"], meta["windows"] = end, seed_s, plans
        bins = {name: os.path.join(out, f"proxy-{name}") for name in ("base", "pr")}
        builds = [("pr", args.head or None, bins["pr"])]
        if not args.same_build:
            builds.append(("base", base_sha, bins["base"]))
        try:
            trees.update(stack.build_and_seed(st, builds, (seed_s, end, max(900, int(seed_s * SEED_TIMEOUT_PER_SECOND)))))
        except BaseException as e:
            trees.update(getattr(e, "trees", {}))
            raise
        base_name = "pr" if args.same_build else "base"
        stack.start_proxies([("base", bins[base_name], trees[base_name], args.proxy_port),
                             ("pr", bins["pr"], trees["pr"], args.proxy_port + 2)], out, st, args.service, proxies)
        targets = [("base", proxies[0].url), ("pr", proxies[1].url), ("loki", st.loki)]
        for plan, (set_name, shapes) in zip(plans, sel["sets"].items()):
            refresh = (plan["refresh_end"], args.refresh_s, args.refresh_runs) if args.refresh_s else None
            summary = run_matrix(set_name, shapes, targets, args, plan["end"], st, out, args.runs, refresh=refresh)
            summary = confirm(summary, args, plan, st, out, targets, baseline, bounds)
            # The set ran on its own block of windows: perf_evidence trusts its warm and refresh figures.
            # summary-<set>.json is rewritten with the marker and the confirmed rows, as the comment shows them.
            summary["isolated_windows"] = True
            write_summaries([summary], out)
            summaries.append(summary)
            for p in proxies:
                if not p.alive():
                    raise RuntimeError(f"proxy '{p.name}' died during the run; see {p.log_path}")
        # Every set is measured and confirmed: a repeated query's Loki cold now has its reference.
        write_summaries(reuse_first_cold(summaries), out)
    finally:
        stack.teardown(proxies, trees.values(), st, args.keep_stack)
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
