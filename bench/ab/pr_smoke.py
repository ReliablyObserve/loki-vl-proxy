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
  3. Both builds run as host processes with their own tree's stack flags,
     set per Loki profile (below).
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
     Before a cold request (run 0) perf_matrix.py waits, at most --quiet-cap-s, for the other build's log to
     go quiet, and the two builds swap places on every other shape x range: neither meets the other's
     post-request work as a rule. Before its first request each build is warmed on windows no shape uses (cold.warm_up). A flagged
     above-2× row whose finding rests on its cold cell gets the cold re-measured, not the confirm pass:
     the confirm windows share the first pass's split buckets, so the proxies answer its run 0 from their
     window cache. cold.recold takes --cold-samples more cold samples of each build, caches flushed, both
     logs quiet, the build asked first alternating, below every block; the cell becomes the median of the
     first pass's sample and the new ones, and the gate judges it as before (bench/ab/README.md, "Cold
     cells that one sample cannot fail").
  6. comment.py renders the sticky comment and sets the exit status. With
     --over-2x-loki warn (the default) an above-2× finding is a warning at the
     top of the comment and a ::warning:: annotation; with fail it fails the job.

  7. A baseline run (selection.py --all, with --baseline-candidate DIR) also writes
     what could become the new baseline into DIR and ends the comment with a "new
     baseline candidate" section (baseline_candidate.py). Nothing is committed.

Like for like. The stack's Loki is configured with align_queries_with_step on and
max_cache_freshness_per_query 0s (test/e2e-compat/loki-local-config.yaml); Loki's own defaults are
off and 10m, and a Loki with the first two answers a refresh as an exact results-cache hit. Each
build is therefore compared with Loki in two profiles (stack.PROFILES):
  aligned        the stack's Loki; builds run with -align-queries-with-step=true. A build that has no
                 such flag (probe_align_support) runs without it and its rows are marked "not like for
                 like" instead of being compared
  loki-defaults  Loki with its defaults (docker-compose.ab-loki-defaults.yml, applied to the same
                 seeded data by recreating Loki); builds run with their defaults
--profiles chooses what runs: auto (the default) runs loki-defaults for the whole selection and
aligned for its range-metric shapes (the only queries the flag changes) when a build supports the
flag; both runs the whole selection in both; aligned or loki-defaults run one. Every profile has its
own block of windows, proxy processes and work directories. A row is above 2x Loki when it is in
either profile, and the comment says which.

Outputs in --out: selection.json, raw-<set>-<profile>.json, summary-<set>-<profile>.json,
comment.md, verdict.json, meta.json and the proxies' logs.

Shards. A large selection runs on several runners: --split N writes up to N shard selections
(shard-<i>.json) of about equal cost into --out and prints their numbers; each shard runs this script
with --selection shard-<i>.json on its own stack, and --merge DIR... renders the one comment and verdict
from the shards' outputs, as one run would. A selection below SHARD_MIN_COST stays one shard. Loki's
cold reuse across sets (step 4) only applies within one Loki process, so each shard does it for its own
sets and the merge leaves it as it is.
"""
import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import answers  # noqa: E402
import baseline_candidate  # noqa: E402
import cold  # noqa: E402
import comment  # noqa: E402
import over2x  # noqa: E402
import perf_matrix  # noqa: E402
import report  # noqa: E402
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
    if getattr(args, "runtime_range_plan", ""):
        cmd += ["--proxy-metrics", "--range-plan", args.runtime_range_plan]
    elif getattr(args, "cells_plan", ""):
        cmd += ["--cells-plan", args.cells_plan]
    for name in shapes or []:
        cmd += ["--shape", name]
    for name, url in targets:
        cmd += ["--target", f"{name}={url}"]
    # The builds' logs (set by measure): a cold request waits for the other build's background work to end.
    for name, path in getattr(args, "quiet_logs", {}).items():
        cmd += ["--quiet-log", f"{name}={path}"]
    if getattr(args, "quiet_cap_s", None) is not None:
        cmd += ["--quiet-cap-s", str(args.quiet_cap_s)]
    return cmd


def raw_path(out, set_name, profile="", suffix=""):
    """The raw file of a set's pass: raw-<set>[-<profile>][<suffix>].json."""
    return os.path.join(out, f"raw-{set_name}{'-' + profile if profile else ''}{suffix}.json")


def run_matrix(set_name, shapes, targets, args, end, st, out, runs, tag="", ranges=None, label="pr-smoke",
               extra=(), refresh=None, profile="", unlike=()):
    """perf_matrix.py for one set, then report.py summarize; returns the summary.

    targets are (name, url) pairs: baseline, candidate, then Loki as 'loki'.
    refresh is (end, seconds per run, runs) for an extra refresh-mode pass whose
    p50 goes into the summary, or None. profile is the Loki profile the pass runs against and unlike the
    targets that are not like-for-like with it (both go into the raw file and the summary).
    """
    if profile:
        tag = f"-{profile}{tag}"
        extra = [*extra, "--profile", profile]
    for name in unlike:
        extra = [*extra, "--not-like-for-like", name]
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


def confirm(summary, args, plan, st, out, targets, baseline=None, bounds=None, others=()):
    """Re-measure the shapes the first pass called slower or faster, or found newly or further above 2× Loki;
    keep the re-run's rows.

    A/A runs (the same build as both targets) move single shapes by up to
    ~70% on a first pass of 4 runs; a 7-run pass on fresh windows settles them.
    """
    moved = ("slower", "faster")
    over = (set(over2x.remeasure(summary, baseline, args.noise, args.min_delta, bounds, others)) if baseline is not None
            else set())
    slow = sorted({row["shape"] for row in summary["rows"] if row["verdict"] in moved} | over)
    if not slow:
        return summary
    stack.log(f"re-measuring {len(slow)} shape(s) the first pass moved in {summary['set']}")
    # Windows the first pass did not use (plan_windows reserves them, shifts included):
    # a re-run over the same windows would time the caches the first pass filled.
    # Loki is not asked again about a shape and range it timed out on in the first pass: the re-run's Loki rows
    # repeat that timeout.
    profile = summary.get("profile") or ""
    first_raw = raw_path(out, summary["set"], profile)
    again = run_matrix(summary["set"], slow, targets, args, plan["confirm_end"], st, out, args.confirm_runs,
                       tag="-confirm", extra=("--known-timeouts", first_raw), profile=profile,
                       unlike=summary.get("not_like_for_like") or ())
    redo = {(row["shape"], row["range"]): row for row in again["rows"]}
    for i, row in enumerate(summary["rows"]):
        if (row["shape"], row["range"]) in redo and (row["verdict"] in moved or row["shape"] in over):
            new = dict(redo[(row["shape"], row["range"])], confirmed=True,
                       **{k: row[k] for k in KEEP_ON_CONFIRM if k in row})
            if row.get("runtime_samples"):
                new["runtime_samples"] = {**row["runtime_samples"], "confirm": new.get("runtime_samples", {}).get("matrix", {})}
            if row.get("original_window_correctness"):
                new["original_window_correctness"] = {**row["original_window_correctness"], "confirm": new.get("original_window_correctness", {}).get("matrix")}
            ref = summary.get("reference", "loki")
            if ref in new.get("cold_cache_served", []):
                # Loki's cache answered the re-run's cold from the first pass's overlapping extents (the windows
                # are new, the data under them is not: Loki's results cache reuses overlapping extents of a
                # query). Every cold column then stays the first pass's, with its cache mark, so a row never
                # carries a cache-served Loki cold next to proxy colds of another pass.
                new.pop("cold_cache_served")
                new["cold"] = row["cold"]
                for target in set(new.get("reuse", {})) | set(row.get("reuse", {})):
                    if "cold" in row.get("reuse", {}).get(target, {}):
                        new.setdefault("reuse", {}).setdefault(target, {})["cold"] = row["reuse"][target]["cold"]
                    elif target in new.get("reuse", {}):
                        new["reuse"][target].pop("cold", None)
                if "cold_cache_served" in row:
                    new["cold_cache_served"] = row["cold_cache_served"]
                new["cold_from_first_pass"] = True
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
    ap.add_argument("--cold-samples", type=int, default=cold.COLD_SAMPLES, metavar="N",
                    help="new cold samples of each build for a row whose above-2x finding rests on its cold cell "
                         f"(0 = keep the single first-pass sample; at most {cold.SPARE_WINDOWS})")
    ap.add_argument("--quiet-cap-s", type=float, default=4.0, metavar="S",
                    help="before a cold request (run 0) wait at most this long for the other build's log to go quiet, "
                         "so it does not meet the previous request's background work (0 = no wait)")
    ap.add_argument("--cold-budget-s", type=int, default=cold.BUDGET_S, metavar="S",
                    help="most seconds the cold re-measures of the whole run take; a row past the budget keeps its "
                         "first-pass cold and its finding")
    ap.add_argument("--warm-rounds", type=int, default=cold.WARM_ROUNDS, metavar="N",
                    help="most rounds of warm-up requests per build before its first measured request (0 = none)")
    ap.add_argument("--refresh-s", type=int, default=0, metavar="N",
                    help="also run each set in refresh mode: windows move forward N seconds per run (a multiple of "
                         "every range's step), p50 reported next to cold and warm; 0 = off (default)")
    ap.add_argument("--refresh-runs", type=int, default=5, help="runs of the refresh pass; the first fills the caches")
    ap.add_argument("--end", type=int, default=0,
                    help="unix seconds the first set's windows end at, floored to the minute (default: now); "
                         "with the same value a run is reproducible window for window")
    ap.add_argument("--noise", type=float, default=0.30, help="relative p50 change still counted as noise")
    ap.add_argument("--min-delta", type=float, default=0.10, help="absolute p50 change (s) still counted as noise")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--service", default="loki-vl-proxy-underscore", help="stack proxy service whose flags to use")
    ap.add_argument("--proxy-port", type=int, default=23190, help="base listens here, PR on +2 (admin +1)")
    ap.add_argument("--keep-stack", action="store_true", help="leave the stack running (for a re-run)")
    ap.add_argument("--same-build", action="store_true", help="A/A: run the PR build as both targets (noise check)")
    ap.add_argument("--profiles", choices=PROFILE_MODES, default="auto",
                    help="Loki profiles to compare with (see the module docstring): auto = loki-defaults for the "
                         "selection plus aligned for its range-metric shapes when a build supports "
                         "-align-queries-with-step; both = the whole selection in both; or one profile")
    ap.add_argument("--over-2x-loki", choices=over2x.GATE_MODES, default=over2x.DEFAULT_GATE,
                    help="a row the PR adds above 2x Loki or makes worse: warn (comment + annotation, the verdict is "
                         "unchanged) or fail (fails the check)")
    ap.add_argument("--over-2x-baseline", default="",
                    help="the above-2x Loki list to compare with (default: bench/ab/history/over-2x-loki.json as "
                         "committed on the base ref, so a PR cannot list its own rows)")
    ap.add_argument("--baseline-candidate", default="", metavar="DIR",
                    help="a baseline run: write the above-2x list and the acceptance distribution this run would "
                         "make the baseline into DIR, and add the section to the comment (nothing is committed)")
    ap.add_argument("--split", type=int, default=0, metavar="N",
                    help="write up to N shard selections of --selection into --out, print their numbers, and exit")
    ap.add_argument("--merge", nargs="+", default=[], metavar="DIR",
                    help="render the comment and verdict of a sharded run from the shards' --out directories")
    stack.stack_args(ap, profile=False)
    args = ap.parse_args()
    if args.split:
        if not args.selection:
            ap.error("--split needs --selection")
        return split_main(args)
    if not 0 <= args.cold_samples <= cold.SPARE_WINDOWS:
        ap.error(f"--cold-samples must be between 0 and {cold.SPARE_WINDOWS}: the seeded margin holds that many windows")
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
            "refresh_s": args.refresh_s, "refresh_runs": args.refresh_runs, "over_2x_loki": args.over_2x_loki,
            "quiet_cap_s": args.quiet_cap_s, "cold_samples": args.cold_samples, "warm_rounds": args.warm_rounds, "cold_budget_s": args.cold_budget_s}
    sel = {"run": True, "sets": {}}
    if args.merge:
        return merge_main(args, out, meta, t0)
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
        if sel.get("runtime_proof", {}).get("required"):
            args.runtime_range_plan = os.path.join(out, "selection.json")
            args.ranges = sel["runtime_proof"]["ranges"]
            meta["ranges"] = args.ranges
            with open(os.path.join(HERE, "shapes.json")) as f:
                ranges = json.load(f)["ranges"]
            problem = perf_matrix.refresh_problem(args.refresh_s, args.refresh_runs,
                                                   [ranges[r][1] for r in args.ranges if r in ranges])
            if problem:
                raise ValueError(problem)
        elif sel.get("cells"):
            # Shapes with their own ranges (selection.range_cells) run at them; the others at args.ranges.
            args.cells_plan = os.path.join(out, "selection.json")
            cell_ranges = sorted({r for values in sel["cells"].values() for r in values},
                                 key=lambda r: (stack_range_seconds(r), r))
            meta["cell_ranges"] = args.cell_ranges = cell_ranges
            with open(os.path.join(HERE, "shapes.json")) as f:
                ranges = json.load(f)["ranges"]
            problem = perf_matrix.refresh_problem(args.refresh_s, args.refresh_runs,
                                                   [ranges[r][1] for r in cell_ranges if r in ranges])
            if problem:
                raise ValueError(problem)
        if not sel["run"]:
            text, verdict = comment.render([], sel, meta)
        else:
            baseline, bounds = baseline_and_bounds(args, base_sha)
            summaries = measure(args, sel, out, base_sha, meta, baseline, bounds)
            meta["elapsed_s"] = round(time.time() - t0)
            text, verdict = render_run(args, summaries, sel, meta, baseline, bounds, base_sha)
    except Exception as e:  # noqa: BLE001 - any failure must still produce a comment and exit 3
        traceback.print_exc()
        meta["elapsed_s"] = round(time.time() - t0)
        text, verdict = comment.errored(sel, meta, f"{type(e).__name__}: {e}")
    return finish_run(args, out, text, verdict, meta)


def baseline_and_bounds(args, base_sha):
    """The above-2x Loki list and the registry's locks, both as committed on the base."""
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
    return baseline, bounds


def original_window_problems(wrapper, shape, rng, sample, meta, phase="matrix"):
    """Full same-original-window proof, separate from measured cache/timing/signature verdicts."""
    if not isinstance(wrapper, dict) or not isinstance(wrapper.get("proof"), dict):
        return ["missing original-window computed proof"]
    proof, measured = wrapper["proof"], wrapper.get("measured") or {}
    secs, step = selection.load_shapes()["ranges"].get(rng, (0, 0))
    query, end = perf_matrix.shape_query(shape, step), proof.get("end")
    expected_end = sample.get("end") if phase != "refresh" else (sample.get("start") or 0)+secs+perf_matrix.query_lookback(query)
    if (proof.get("complete") is not True or proof.get("run") != 0 or proof.get("range") != rng
            or proof.get("query") != query or type(end) is not int or end != expected_end
            or proof.get("history_start") != end-secs-perf_matrix.query_lookback(query)
            or not meta["end"]-meta["seed_s"] <= proof["history_start"] <= end <= meta["end"]
            or set(measured) != {"base", "pr", "loki"} or set(proof.get("targets", {})) != {"base", "pr"}):
        return ["invalid original-window source"]
    ref, old = proof.get("reference") or {}, proof.get("measured_reference") or {}
    target_urls = wrapper.get("target_urls") or {}
    # Loki's whole request timed out and its answer was stitched from pieces (perf_matrix.stitched_reference):
    # the measured Loki receipt is that timeout, and the reference is checked piece by piece below.
    stitched = ref.get("reference_kind") == perf_matrix.STITCHED_KIND
    endpoints = {t: answers.source_endpoint(measured[t].get("request_url")) for t in measured}
    if not isinstance(target_urls, dict) or set(target_urls) != set(measured) or any(v is None for v in endpoints.values()) or endpoints["loki"] in (endpoints["base"], endpoints["pr"]):
        return ["invalid or proxy-as-Loki measured source"]
    for target in ("base", "pr", "loki"):
        receipt = old if target == "loki" else proof["targets"][target]
        timeout = stitched and target == "loki"
        if (any(measured[target].get(k) != receipt.get(k) for k in ("status", "response_bytes", "body_sha256", "answer_proof"))
                or measured[target].get("end") != end or measured[target].get("history_start") != proof["history_start"]
                or (not timeout or receipt.get("skipped") != "loki_timeout") and (
                    type(receipt.get("response_bytes")) is not int or not 0 < receipt["response_bytes"] <= perf_matrix.CORRECTNESS_BODY_CAP
                    or not re.fullmatch(r"[0-9a-f]{64}", receipt.get("body_sha256") or ""))):
            return ["original-window body provenance mismatch"]
        if (answers.source_endpoint(target_urls[target]) is None or measured[target]["request_url"] !=
                perf_matrix.request_url(target_urls[target], shape, query, secs, step, end)):
            return ["measured request does not match declared target"]
        if timeout:
            # A whole request that timed out, sent in this pass or (skipped) in an earlier one: nothing else.
            if (not perf_matrix.timed_out_row(receipt) or receipt.get("answer_proof") is not None
                    or receipt.get("skipped") not in (None, "loki_timeout")
                    or receipt.get("skipped") and (receipt.get("response_bytes"), receipt.get("body_sha256")) != (None, None)):
                return ["invalid original-window query contract"]
        elif not (receipt.get("status") == 200 and receipt.get("answer_proof") == "answer" or
                receipt.get("status") in (400, 422, 500) and str(receipt.get("answer_proof", "")).startswith("rejection:")):
            return ["invalid original-window query contract"]
    if stitched:
        problems = stitched_reference_problems(ref, shape, query, secs, step, end, target_urls["loki"])
        if problems:
            return problems
    extra, actual = proof.get("extra_reads"), ref.get("actual_query")
    if (type(extra) is not int or extra not in (0, 1) or ref.get("complete") is not True or ref.get("phase") != "original-window"
            or ref.get("original_query") != query or ref.get("end") != end or ref.get("history_start") != proof["history_start"]
            or not isinstance(actual, str) or (extra == 0 and (actual != query or (ref.get("correctness_query_mode") != "measured"
                                                                                   if stitched else ref != old)))
            or extra == 1 and (ref.get("correctness_query_mode") != "space-tab-suffix-128"
                              or not actual.startswith(query) or len(actual) != len(query)+128 or set(actual[len(query):]) - {" ", "\t"})):
        return ["invalid original-window reference spelling"]
    if (answers.source_endpoint(ref.get("url")) != endpoints["loki"]
            or answers.source_endpoint(old.get("url")) != endpoints["loki"]):
        return ["reference does not match measured Loki source"]
    url = perf_matrix.urllib.parse.urlsplit(ref["url"])
    expected = perf_matrix.urllib.parse.urlsplit(perf_matrix.request_url(target_urls["loki"], shape, actual, secs, step, end))
    rejection = isinstance(ref.get("answer_proof"), str) and ref["answer_proof"].startswith("rejection:")
    cache = report.correctness_cache_projection(ref.get("cache_raw")) if not stitched else ref.get("cache")
    observation = report.metadata_observation(shape, ref) and extra == 0
    target_parities = {t: report.correctness_parity(shape, proof["targets"][t].get("facets")) for t in ("base", "pr")}
    if (url.scheme not in ("http", "https") or not url.netloc or
            (url.path, url.query) != (expected.path, expected.query) or ref.get("reference_skip") != ""
            or rejection and ref.get("status") not in (400, 422, 500)
            or (not rejection and (ref.get("status") != 200 or not observation and (cache is None or ref.get("cache") != cache)))
            or extra and ref.get("correctness_cache_control") != "no-cache"
            or proof["targets"]["pr"].get("parity_vs_reference") != target_parities["pr"] or target_parities["pr"] == "differs"):
        return ["original-window answer differs or reference incomplete"]
    if (not isinstance(proof["targets"]["base"].get("facets"), list)
            or proof["targets"]["base"].get("parity_vs_reference") != target_parities["base"]
            or type(ref.get("response_bytes")) is not int or not 0 < ref["response_bytes"] <= perf_matrix.CORRECTNESS_BODY_CAP
            or not re.fullmatch(r"[0-9a-f]{64}", ref.get("body_sha256") or "")):
        return ["incomplete original-window reference body"]
    return []


def stitched_reference_problems(ref, shape, query, secs, step, end, loki_url):
    """A stitched Loki reference (perf_matrix.stitched_reference): the pieces are exactly stitch_plan's for the
    window, recomputed here, so no grid point is missing or asked twice; Loki computed and answered every piece;
    and Loki's whole answer of a smaller window across the middle cut equals the stitched points there."""
    problem = ["invalid stitched Loki reference"]
    plan = perf_matrix.stitch_plan(end - secs, end, step)
    pieces, actual = ref.get("partitions"), ref.get("actual_query")
    if (len(plan) < 3 or ref.get("kind") != perf_matrix.STITCHED_KIND or ref.get("partition_s") != perf_matrix.STITCH_PARTITION_S
            or not isinstance(pieces, list) or [(p.get("start"), p.get("end")) if isinstance(p, dict) else None for p in pieces] != plan):
        return problem
    def read(receipt, query_text, start, stop, computed=True):
        url = perf_matrix.urllib.parse.urlsplit(receipt.get("url") or "")
        wanted = perf_matrix.request_url(loki_url, shape, query_text, stop - start, step, stop)
        expected = perf_matrix.urllib.parse.urlsplit(wanted)
        projected = report.correctness_cache_projection(receipt.get("cache_raw"))
        return (url.scheme in ("http", "https") and url.netloc
                and answers.source_endpoint(receipt.get("url")) == answers.source_endpoint(wanted) is not None
                and (url.path, url.query) == (expected.path, expected.query)
                and receipt.get("status") == 200 and receipt.get("answer_proof") == "answer" and not receipt.get("warnings")
                and type(receipt.get("response_bytes")) is int and 0 < receipt["response_bytes"] <= perf_matrix.CORRECTNESS_BODY_CAP
                and re.fullmatch(r"[0-9a-f]{64}", receipt.get("body_sha256") or "")
                and (not computed or projected is not None and receipt.get("cache") == projected and receipt.get("reference_skip") == ""))
    if not all(read(p, actual, p["start"], p["end"]) for p in pieces) or ref.get("cache") != report.sum_cache(
            [{"cache": p["cache"]} for p in pieces]):
        return problem
    # The joined answer holds every piece's points, once: the pieces share no grid point and the join rejects a
    # repeat, so its points are their sum, and a series is in one piece at least and in each piece at most once.
    joined, parts = report.SIG_RE.fullmatch(ref.get("signature") or ""), [report.SIG_RE.fullmatch(p.get("signature") or "") for p in pieces]
    if (not joined or not all(parts) or int(joined.group(2)) != sum(int(m.group(2)) for m in parts)
            or (joined.group(1) == "ranked") != any(m.group(1) == "ranked" for m in parts)
            or joined.group(1) != "ranked" and not max(int(m.group(1)) for m in parts) <= int(joined.group(1)) <= sum(int(m.group(1)) for m in parts)):
        return ["stitched Loki answer does not hold its pieces' points"]
    check, middle = ref.get("whole_check") or {}, len(plan) // 2
    start, stop = plan[middle - 1][0], plan[middle][1]
    spelled = check.get("actual_query")
    if (check.get("start") != start or check.get("end") != stop or stop - start >= secs or not isinstance(spelled, str)
            or check.get("correctness_query_mode") != "space-tab-suffix-128" or check.get("correctness_cache_control") != "no-cache"
            or not spelled.startswith(query) or len(spelled) != len(query) + 128 or set(spelled[len(query):]) - {" ", "\t"}
            or not read(check, spelled, start, stop) or check.get("facets") != [] or check.get("matches") is not True):
        return ["stitched Loki reference has no matching whole-request check"]
    return []


def stitched_phase(row, shape, phase, count):
    """True when every run of a pass has a stitched Loki answer and the pass's original window holds a valid stitched
    reference after a Loki timeout, whatever the proxies answered."""
    wrapper = row.get("original_window_correctness", {}).get(phase) or {}
    proof = wrapper.get("proof") if isinstance(wrapper, dict) else None
    if not isinstance(proof, dict) or row.get("runtime_samples", {}).get(phase, {}).get("loki", {}).get("stitched") != count:
        return False
    ref, urls = proof.get("reference") or {}, wrapper.get("target_urls") or {}
    secs, step = selection.load_shapes()["ranges"].get(row.get("range"), (0, 0))
    return (ref.get("reference_kind") == perf_matrix.STITCHED_KIND and ref.get("complete") is True
            and perf_matrix.timed_out_row(proof.get("measured_reference")) and type(proof.get("end")) is int
            and isinstance(urls.get("loki"), str) and not stitched_reference_problems(
                ref, shape, perf_matrix.shape_query(shape, step), secs, step, proof["end"], urls["loki"]))


def cold_proof_problems(row, summary, shape, ranges, meta):
    """A cold median includes its original sample and every fresh matched request, not just a sidecar label."""
    problem = "incomplete cold-resample provenance"
    applied = [entry for entry in summary.get("cold_resampled", [])
               if isinstance(entry, dict) and (entry.get("shape"), entry.get("range")) == (row["shape"], row["range"])
               and entry.get("applied") is True]
    if "cold_samples" not in row and not row.get("cold_resample_parity") and not applied:
        return []
    proof = row.get("cold_resample_parity") or {}
    original = proof.get("original_window") or {}
    if original.get("original_window_correctness") and not original_window_problems(
            original["original_window_correctness"], shape, row["range"], row.get("runtime_samples", {}).get("matrix", {}).get("loki", {}), meta):
        original = {**original, **{key: original["original_window_correctness"]["proof"]["targets"][target]["parity_vs_reference"]
                    for key, target in (("parity_vs_reference", "pr"), ("baseline_parity_vs_reference", "base"))}}
    usable = lambda value: isinstance(value, str) and (
        value in ("same", "differs", "documented") or re.fullmatch(r"same series, sum ±[0-9]+\.[0-9]{2}%", value))
    if (proof.get("original_source") != "first-pass" or proof.get("reference_url_supplied") is not True
            or not all(usable(original.get(key)) for key in ("parity_vs_reference", "baseline_parity_vs_reference"))):
        return ["missing original-window Loki comparison for cold median"]
    if "cold_samples" not in row:
        return [problem] if applied or row.get("cold_resource_samples") else []
    if original["parity_vs_reference"] == "differs":
        return ["original candidate differs from Loki in included cold sample"]
    timings = row.get("cold_samples")
    if not isinstance(timings, dict):
        return [problem]
    samples = [timings.get(target) for target in ("base", "pr")]
    number = lambda value: type(value) in (int, float) and math.isfinite(value) and value >= 0
    if (not all(isinstance(values, list) and len(values) >= 2 and all(number(v) for v in values) for values in samples)
            or len(samples[0]) != len(samples[1])):
        return [problem]
    total, count = len(samples[0]), len(samples[0]) - 1
    if type(meta.get("cold_samples")) is not int or meta["cold_samples"] != count:
        return [problem]
    matches = applied
    if len(matches) != 1 or not isinstance(matches[0].get("requests"), dict):
        return [problem]
    requests = matches[0]["requests"]
    if any(not isinstance(requests.get(t), list) or len(requests[t]) != count for t in ("base", "pr", "loki")):
        return [problem]
    secs, step = ranges.get(row["range"], (0, 0))
    query = perf_matrix.shape_query(shape, step)
    lookback = perf_matrix.query_lookback(query)
    ends = [receipt.get("end") if isinstance(receipt, dict) else None for receipt in requests["loki"]]
    if not all(type(end) is int for end in ends) or len(set(ends)) != count:
        return [problem]
    for target in ("base", "pr", "loki"):
        for i, receipt in enumerate(requests[target]):
            if not isinstance(receipt, dict):
                return [problem]
            end, start = receipt.get("end"), receipt.get("history_start")
            request_query = query
            if target == "loki":
                actual = receipt.get("actual_query")
                query_used = query and (not shape.get("path") or shape.get("query") or shape.get("instant"))
                if receipt.get("original_query") != query or not isinstance(actual, str):
                    return [problem]
                if query_used:
                    if (receipt.get("correctness_query_mode") != "space-tab-suffix-128"
                            or not actual.startswith(query) or len(actual) != len(query) + 128
                            or set(actual[len(query):]) - {" ", "\t"}):
                        return [problem]
                elif receipt.get("correctness_query_mode") != "unchanged-no-query" or actual != query:
                    return [problem]
                request_query = actual
            expected_url = perf_matrix.urllib.parse.urlsplit(
                perf_matrix.request_url("http://reference", shape, request_query, secs, step, ends[i]))
            actual_url = perf_matrix.urllib.parse.urlsplit(receipt.get("url") or "")
            if (receipt.get("phase") != "flushed-resample" or receipt.get("status") != 200
                    or receipt.get("answer_proof") != "answer" or not number(receipt.get("seconds"))
                    or type(receipt.get("response_bytes")) is not int or receipt["response_bytes"] <= 0
                    or not isinstance(receipt.get("body_sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", receipt["body_sha256"])
                    or not receipt.get("signature") or receipt.get("signature") == "unparseable"
                    or end != ends[i] or start != end - secs - lookback
                    or not number(meta.get("end")) or not number(meta.get("seed_s"))
                    or not meta["end"] - meta["seed_s"] <= start <= end <= meta["end"]
                    or actual_url.scheme not in ("http", "https") or not actual_url.netloc
                    or (actual_url.path, actual_url.query) != (expected_url.path, expected_url.query)):
                return [problem]
            if target == "loki":
                projected_cache = report.correctness_cache_projection(receipt.get("cache_raw"))
                if (receipt.get("correctness_cache_control") != "no-cache"
                        or receipt.get("reference_skip") != ""
                        or not report.metadata_observation(shape, receipt) and (projected_cache is None or receipt.get("cache") != projected_cache
                        or report.reference_skip(receipt))):
                    return [problem]
            else:
                facets = receipt.get("parity_facets")
                parity = receipt.get("parity_vs_reference")
                if (not isinstance(facets, list) or parity != report.correctness_parity(shape, facets)
                        or target == "pr" and parity == "differs" or "vl_calls_header" not in receipt
                        or receipt["vl_calls_header"] is not None and
                        (type(receipt["vl_calls_header"]) is not int or receipt["vl_calls_header"] < 0)
                        or receipt["seconds"] != timings[target][i + 1]):
                    return [problem]
        if target == "loki":
            continue
        cost = row.get("reuse", {}).get(target, {}).get("cold") or {}
        resource_samples = row.get("cold_resource_samples", {}).get(target)
        if (not isinstance(resource_samples, list) or len(resource_samples) != total
                or not isinstance(resource_samples[0], dict) or resource_samples[0].get("phase") != "first-pass"
                or resource_samples[0].get("seconds") != timings[target][0]
                or resource_samples[1:] != requests[target]
                or any(cost.get(field) != total for field in ("metric_runs_expected", "cpu_runs_measured", "rss_runs_measured"))):
            return [problem]
        for sample in resource_samples:
            measured = sample.get("reuse") or {}
            if (measured.get("cpu_samples") != 2 or measured.get("rss_samples") != 2
                    or not number(measured.get("cpu_seconds")) or not number(measured.get("rss_max_sampled_bytes"))):
                return [problem]
        if (cost != perf_matrix.reuse.median([s["reuse"] for s in resource_samples])
                or row.get("cold", {}).get(target) != round(cold.median(timings[target]), 3)):
            return [problem]
        if proof.get("targets", {}).get(target) != [r["parity_vs_reference"] for r in requests[target]]:
            return [problem]
    return []


def runtime_api_problems(summaries, sel, meta=None, mode="auto", supported=True):
    """Validate each planned profile/cell against its measured inputs before accepting the reduced values."""
    proof = sel.get("runtime_proof", {})
    if not proof.get("required"):
        return []
    meta = meta or {}
    spec = selection.load_shapes()
    runs, _ = plan_profiles(mode, sel, spec, supported)
    cells = proof.get("cells", {})
    expected = {(profile, name, shape, rng) for profile, name, shapes in runs
                for shape in shapes for rng in cells.get(f"{name}/{shape}", [])}
    problems = selection.runtime_proof_problems(sel)
    if not expected or any(not cells.get(f"{name}/{shape}") for name, shapes in sel["sets"].items() for shape in shapes):
        problems.append("runtime API proof: required cells were not fully declared")
    seen = set()
    revision = {k: meta.get(k) for k in ("base", "head")}
    if not all(isinstance(v, str) and len(v) == 40 and all(c in "0123456789abcdef" for c in v) for v in revision.values()):
        problems.append("runtime API proof: missing revision provenance")
    end, seed = meta.get("end"), meta.get("seed_s")
    def number(value):
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    for summary in summaries:
        if revision != summary.get("revision"):
            problems.append(f"runtime API proof: {summary.get('set')} summary revision mismatch")
        if [summary.get(k) for k in ("baseline", "candidate", "reference")] != ["base", "pr", "loki"]:
            problems.append("runtime API proof: missing or unexpected measurement targets")
        for row in summary.get("rows", []):
            key = (summary.get("profile"), summary.get("set"), row.get("shape"), row.get("range"))
            if key in seen or key not in expected:
                problems.append(f"runtime API proof: unexpected or duplicate cell {key}")
                continue
            seen.add(key)
            shape = next(sh for sh in spec["sets"][summary["set"]]["shapes"] if sh["name"] == row["shape"])
            problems.extend(f"runtime API proof: {key} {issue}" for issue in
                            cold_proof_problems(row, summary, shape, spec["ranges"], meta))
            passes = {"matrix": meta.get("runs")}
            if row.get("confirmed"):
                passes["confirm"] = meta.get("confirm_runs")
            if meta.get("refresh_s"):
                passes["refresh"] = meta.get("refresh_runs")
            computed = {phase: original_window_problems(row.get("original_window_correctness", {}).get(phase),
                        shape, row["range"], row.get("runtime_samples", {}).get(phase, {}).get("loki", {}), meta, phase) for phase in passes}
            for phase in passes:
                problems.extend(f"runtime API proof: {key} {phase} {p}" for p in computed[phase])
            for target in ("base", "pr", "loki"):
                # Loki's whole request timed out on every run of every pass ("0"), and each run's answer was stitched
                # from pieces, each pass's original window with a valid stitched proof: the answer is Loki's, the
                # timing is not compared (over2x counts the row as Loki not 200).
                # Provenance only: an answer that differs from the stitched one is reported by its own proof problem.
                stitched = (target == "loki" and row.get("status", {}).get(target) == "0"
                            and all(stitched_phase(row, shape, phase, count) for phase, count in passes.items()))
                if row.get("status", {}).get(target) not in ("200", "400", "422", "500") and not stitched:
                    problems.append(f"runtime API proof: {key} {target} missing or unusable status")
                for signal in ("cold", "p50", "response_bytes"):
                    if not number(row.get(signal, {}).get(target)):
                        problems.append(f"runtime API proof: {key} {target} missing {signal}")
                for phase, count in passes.items():
                    sample = row.get("runtime_samples", {}).get(phase, {}).get(target, {})
                    if (not number(count) or count < 2 or sample.get("complete") is not True or sample.get("measured") != count
                            or target != "loki" and sample.get("resource_runs") != count):
                        problems.append(f"runtime API proof: {key} {target} incomplete {phase} request samples")
                    if not (number(end) and number(seed) and number(sample.get("start")) and number(sample.get("end"))
                            and end - seed <= sample["start"] <= sample["end"] <= end):
                        problems.append(f"runtime API proof: {key} {target} {phase} missing seeded input history")
                if target == "loki":
                    if row.get("parity_vs_reference") in (None, "n/a") and any(computed.values()):
                        problems.append(f"runtime API proof: {key} missing usable Loki comparison")
                    continue
                for phase in ("cold", "repeat"):
                    cost = row.get("reuse", {}).get(target, {}).get(phase) or {}
                    if (not number(cost.get("rss_max_sampled_bytes")) or not number(cost.get("cpu_seconds"))
                            or cost.get("rss_samples") != 2 or cost.get("cpu_samples") != 2):
                        problems.append(f"runtime API proof: {key} {target} {phase} missing CPU/RSS boundary samples")
                    if phase == "repeat":
                        count = passes.get("confirm", passes["matrix"])
                        if not number(count) or any(cost.get(k) != count - 1 for k in
                                                    ("metric_runs_expected", "rss_runs_measured", "cpu_runs_measured")):
                            problems.append(f"runtime API proof: {key} {target} incomplete repeat measurements")
    return problems + [f"runtime API proof: missing cell {key}" for key in sorted(expected - seen)]


def render_run(args, summaries, sel, meta, baseline, bounds, base_sha, problems=None):
    """The comment and verdict of a measured run, with the baseline-candidate section on a baseline run."""
    text, verdict = comment.render(summaries, sel, meta, baseline, bounds)
    if problems is None:
        problems = runtime_api_problems(summaries, sel, meta, getattr(args, "profiles", "auto"),
                                        any(meta.get("align_flag_supported", {}).values()))
    if sel.get("runtime_proof", {}).get("required"):
        verdict["runtime_proof"] = {"required": True, "complete": not problems, "problems": len(problems)}
    if problems:
        verdict.update(exit=1, state="failed", failed=True, runtime_problems=problems)
        lines = text.splitlines()
        for n, line in enumerate(lines):
            if line.startswith("### "):
                lines[n:n + 1] = ["### ❌ Performance A/B: required runtime proof failed", "",
                                  *["- " + comment.cell(p) for p in problems[:12]]]
                break
        text = "\n".join(lines) + "\n"
    if args.baseline_candidate:
        section = baseline_candidate.write(
            args.baseline_candidate, summaries, baseline or {}, baseline_candidate.committed_day(base_sha),
            date=time.strftime("%Y-%m-%d", time.gmtime()), commit=meta["head"], elapsed_s=meta["elapsed_s"])
        text = baseline_candidate.fit(text, baseline_candidate.COMMENT_LIMIT - len(section)) + section
    return baseline_candidate.fit(text), verdict


def finish_run(args, out, text, verdict, meta):
    finish(out, text, verdict, meta)
    for line in over2x.annotations(verdict.get("over_2x", {}).get("rows", []), args.over_2x_loki):
        print(line)
    return verdict["exit"]


# Below this estimated cost (shape x profile passes) a selection runs on one runner: a shard costs a stack
# and a seed (about 5 minutes on a hosted runner), which a small selection does not win back.
SHARD_MIN_COST = 40


def set_costs(sel, spec):
    """{set: estimated cost}: one pass per shape in loki-defaults, one more per range-metric shape in aligned,
    times the set's `cost_factor` in shapes.json (default 1).

    A cell is not always as long as another. The unit is about 20 seconds on a hosted runner (a shard of 51 units
    ran 24 minutes with its stack and seed). A cell of series-level-fine-step (a sliding window at a 20 s step
    over 24h) took about 200 seconds and one of mixed-parser-metrics about 180, ten and nine units: shard 10 of a
    run, holding four of the first, ran over an hour. A set is not split, so a heavy one counts as many units as it
    takes, and the others are spread around it.

    Without a required runtime proof the run measures 1h, and its shards seed about an hour; a set whose shapes run
    at their own ranges (selection.range_cells) up to 24h makes its shard seed 26 hours (about 6 minutes), so it
    also counts DAY_SEED_COST units for that seed and goes to a shard with room for it."""
    costs = {}
    cells = selection.cells_of(sel)
    own = {} if sel.get("runtime_proof", {}).get("required") else sel.get("cells", {})
    for name, shapes in sel.get("sets", {}).items():
        by_name = {sh["name"]: sh for sh in spec["sets"][name]["shapes"]}
        factor = spec["sets"][name].get("cost_factor", 1)
        costs[name] = factor * sum(max(1, len(cells.get(f"{name}/{n}", ["1h"]))) * (2 if range_metric(by_name[n]) else 1)
                                   for n in shapes)
        if any(spec.get("ranges", {}).get(r, [0])[0] >= 86400 for n in shapes for r in own.get(f"{name}/{n}", [])):
            costs[name] += DAY_SEED_COST
    return costs


# About 6 minutes of seeding (26 hours of history), in set_costs units of about 20 seconds.
DAY_SEED_COST = 18


# A shard seeds the history its longest range needs before it measures anything, and seeding is slow and grows
# faster than the history: about 6 minutes for the 26 hours that 24h ranges need, 39 minutes for the 50 hours that
# a 7d range gets (shard 4 of a run seeded 181,325 s in 2,354 s and was cancelled at the 45 minute limit after six
# minutes of measuring). Sets with a range over LONG_HISTORY_S therefore share as few shards as keep their
# measuring in reach (LONG_SHARD_COST), and the seed counts as LONG_SEED_COST units of about 20 seconds in those
# shards' load, so other sets go elsewhere instead of one long set landing in every shard.
LONG_HISTORY_S = 24 * 3600
LONG_SEED_COST = 120
# A shard of long-history sets measures up to this many units on top of its seed; more of them take a second one.
LONG_SHARD_COST = 60


def long_history_sets(sel, spec):
    """The selected sets with a cell over LONG_HISTORY_S: they decide how much history a shard has to seed."""
    cells = selection.cells_of(sel)
    out = set()
    for name, shapes in sel.get("sets", {}).items():
        ranges = {r for n in shapes for r in cells.get(f"{name}/{n}", [])} or set(spec["sets"][name].get("ranges", []))
        if any(spec.get("ranges", {}).get(r, [0])[0] > LONG_HISTORY_S for r in ranges if r != "instant"):
            out.add(name)
    return out


def split_selection(sel, n, spec=None, min_cost=SHARD_MIN_COST):
    """Up to n shard selections of `sel`, each a copy with a subset of its sets, every set in exactly one.

    The number of shards is n, at most one per set and per min_cost of the total cost. Sets with a range over
    LONG_HISTORY_S share the first shards (see long_history_sets); the others go, most costly first, to the least
    loaded shard (ties to the lower shard), and each shard keeps the selection's set order, so the split is the
    same for the same selection. A selection that does not run is one shard.
    """
    if spec is None:
        spec = selection.load_shapes()
    sets = sel.get("sets", {})
    if not sel.get("run") or not sets:
        return [sel]
    costs = set_costs(sel, spec)
    k = max(1, min(n, len(sets), -(-sum(costs.values()) // min_cost)))
    loads, members = [0] * k, [set() for _ in range(k)]
    long_sets = long_history_sets(sel, spec)
    if k > 1 and long_sets:
        # The long-history sets share as few shards as keep their measuring in reach (each pays the seed once);
        # the others are placed around them, by load, which counts that seed.
        total = sum(costs[s] for s in long_sets)
        m = min(max(1, -(-total // LONG_SHARD_COST)), len(long_sets), k if len(long_sets) == len(sets) else k - 1)
        for j in range(m):
            loads[j] = LONG_SEED_COST
        for name in sorted(long_sets, key=lambda s: (-costs[s], s)):
            i = min(range(m), key=lambda j: (loads[j], j))
            loads[i] += costs[name]
            members[i].add(name)
    placed = set().union(*members)
    for name in sorted((s for s in sets if s not in placed), key=lambda s: (-costs[s], s)):
        i = min(range(k), key=lambda j: (loads[j], j))
        loads[i] += costs[name]
        members[i].add(name)
    members = [member for member in members if member]  # all-long selections may leave a shard with nothing
    shards = []
    for member in members:
        shard = dict(sel, sets={name: shapes for name, shapes in sets.items() if name in member})
        proof = sel.get("runtime_proof", {})
        if proof.get("required"):
            cells = {key: rng for key, rng in proof["cells"].items() if key.partition("/")[0] in member}
            ranges = {r for values in cells.values() for r in values if r != "instant"}
            shard["runtime_proof"] = dict(proof, cells=cells, ranges=sorted(ranges, key=lambda r: (spec["ranges"][r][0], r)) or ["1h"])
        elif sel.get("cells"):
            cells = {key: rng for key, rng in sel["cells"].items() if key.partition("/")[0] in member}
            if cells:
                shard["cells"] = cells
            else:
                shard.pop("cells", None)
        shards.append(shard)
    return shards


def split_main(args):
    with open(args.selection) as f:
        sel = json.load(f)
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    shards = split_selection(sel, args.split)
    for i, shard in enumerate(shards, 1):
        with open(os.path.join(out, f"shard-{i}.json"), "w") as f:
            json.dump(shard, f, indent=1)
    print(json.dumps([str(i) for i in range(1, len(shards) + 1)]))
    return 0


def shard_dir(path, planned):
    """The directory a shard's outputs were downloaded to. download-artifact unpacks each artifact a pattern
    matches into its own directory, except when the pattern matches one: then the files land in the download
    root itself. So the one planned shard of a small selection may be the parent of the directory named."""
    parent = os.path.dirname(os.path.normpath(path))
    if planned == 1 and not os.path.isdir(path) and os.path.exists(os.path.join(parent, "verdict.json")):
        return parent
    return path


def read_shard(path):
    """(summaries in run order, meta, verdict) of one shard's --out; verdict is None when it wrote none."""
    def load(name):
        try:
            with open(os.path.join(path, name)) as f:
                return json.load(f)
        except (OSError, ValueError):
            return None
    verdict, meta, sel = load("verdict.json"), load("meta.json") or {}, load("selection.json") or {}
    order = {name: i for i, name in enumerate(sel.get("sets", {}))}
    summaries = []
    for name in order:
        for profile in (*EXECUTION, None):
            summary = load(f"summary-{name}{'-' + profile if profile else ''}.json")
            if summary and summary.get("set") == name and summary.get("profile") == profile:
                summaries.append(summary)
    summaries.sort(key=lambda s: (EXECUTION.index(s["profile"]) if s.get("profile") in EXECUTION else -1,
                                  order.get(s["set"], len(order))))
    return summaries, meta, verdict


# Meta a shard measures for itself; the merged meta adds them up over the shards.
SUMMED_META = ("quiet_wait_s", "cold_resample_s")


def merge_meta(metas):
    """One run's meta from the shards' metas: the settings of the first, the longest elapsed time and the
    phases of that shard, the sums of SUMMED_META, every cold re-measure, and per profile whether any shard ran it."""
    merged = dict(metas[0])
    for key in ("end", "seed_s", "windows", "phases_s", "elapsed_s", "cold_resampled", *SUMMED_META):
        merged.pop(key, None)
    slowest = max(metas, key=lambda m: m.get("elapsed_s", 0))
    for key in ("elapsed_s", "phases_s"):
        if key in slowest:
            merged[key] = slowest[key]
    for key in SUMMED_META:
        if any(key in m for m in metas):
            merged[key] = round(sum(m.get(key, 0) for m in metas), 1)
    resampled = [r for m in metas for r in m.get("cold_resampled", [])]
    if resampled:
        merged["cold_resampled"] = resampled
    merged.pop("cell_ranges", None)
    cell_ranges = sorted({r for m in metas for r in m.get("cell_ranges", [])},
                         key=lambda r: (stack_range_seconds(r), r))
    if cell_ranges:  # only some shards hold shapes at their own ranges
        merged["cell_ranges"] = cell_ranges
    profiles = {}
    for m in metas:
        for p, entry in (m.get("profiles") or {}).items():
            if p not in profiles or (entry.get("ran") and not profiles[p].get("ran")):
                profiles[p] = entry
    if profiles:
        merged["profiles"] = profiles
    return merged


def merge_main(args, out, meta, t0):
    """--merge: the comment and verdict of the shards' outputs, rendered as one run's."""
    sel = {"run": True, "sets": {}}
    try:
        if args.selection:
            with open(args.selection) as f:
                sel = json.load(f)
        paths = [shard_dir(path, len(args.merge)) for path in args.merge]
        shards = [(path, *read_shard(path)) for path in paths]
        # A shard that did not finish (cancelled at the job limit, no verdict) has no meta or selection to prove
        # against: say so before the proof reads them, instead of failing on the first missing key.
        incomplete = [os.path.basename(os.path.normpath(path)) for path, _, _, v in shards if not v or v.get("exit") == 3]
        if incomplete:
            raise RuntimeError(f"shard(s) {', '.join(incomplete)} did not complete; see their logs and artifacts")
        proof_problems = []
        if sel.get("runtime_proof", {}).get("required"):
            expected = split_selection(sel, len(shards))
            revisions = {"head": git("rev-parse", args.head or "HEAD"),
                         "base": git("rev-parse", args.base or "HEAD^1")}
            supported = any(source_supports_align(v) for v in revisions.values())
            if len(expected) != len(shards):
                proof_problems.append("runtime API proof: unexpected shard count")
            for shard, wanted in zip(shards, expected):
                path, shard_summaries, shard_meta, _ = shard
                selection_path = os.path.join(path, "selection.json")
                with open(selection_path) as f:
                    if json.load(f) != wanted:
                        proof_problems.append(f"runtime API proof: {os.path.basename(path)} ran a different shard selection")
                if any(shard_meta.get(k) != v for k, v in revisions.items()):
                    proof_problems.append(f"runtime API proof: {os.path.basename(path)} revision mismatch")
                if any(shard_meta.get(k) != getattr(args, k) for k in ("runs", "confirm_runs", "refresh_s", "refresh_runs")):
                    proof_problems.append(f"runtime API proof: {os.path.basename(path)} measurement settings differ from plan")
                proof_problems.extend(runtime_api_problems(shard_summaries, wanted, shard_meta, args.profiles, supported))
        metas = [m for _, _, m, _ in shards if m]
        if metas:
            meta.update(merge_meta(metas))
        meta["shards"] = [{"dir": os.path.basename(os.path.normpath(path)), "sets": len({s["set"] for s in summaries}),
                           "restarts": [summaries[0].get("restart_before"), summaries[-1].get("restart_after")]
                           if summaries else None,
                           **{k: m[k] for k in ("elapsed_s", "phases_s", "end", "seed_s", "windows") if k in m}}
                          for path, summaries, m, _ in shards]
        summaries = [s for _, shard_summaries, _, _ in shards for s in shard_summaries]
        # The merged outputs read like one run's: every shard's summaries and the whole selection.
        write_summaries(summaries, out)
        with open(os.path.join(out, "selection.json"), "w") as f:
            json.dump(sel, f, indent=1)
        baseline, bounds = baseline_and_bounds(args, meta["base"])
        text, verdict = render_run(args, summaries, sel, meta, baseline, bounds, meta["base"], problems=proof_problems)
    except Exception as e:  # noqa: BLE001 - any failure must still produce a comment and exit 3
        traceback.print_exc()
        meta.setdefault("elapsed_s", round(time.time() - t0))
        text, verdict = comment.errored(sel, meta, f"{type(e).__name__}: {e}")
    return finish_run(args, out, text, verdict, meta)


# The seed's wait for Loki and VictoriaLogs to agree took well under 900 s for the 86-minute window
# the smoke used to seed; a longer window gets a proportionally longer wait.
SEED_TIMEOUT_PER_SECOND = 900 / (86 * 60)


PROFILE_MODES = ("auto", "both", *stack.PROFILES)


def range_metric(shape):
    """A metric query_range shape: the only queries -align-queries-with-step (and Loki's align_queries_with_step)
    change. Log, instant and metadata (path) shapes keep the bounds the client sent."""
    return not (shape.get("instant") or shape.get("logs") or shape.get("path"))


def source_supports_align(ref=None, root=ROOT):
    """Whether cmd/proxy/main.go at a git ref (the working tree when None) defines -align-queries-with-step.

    Used to plan the profiles before the builds exist (the windows and the seed depend on it); the built
    binary's probe (stack.probe_align_support) decides what actually runs.
    """
    cmd = ["git", "grep", "-q", "-e", stack.ALIGN_FLAG.lstrip("-"), *([ref] if ref else []), "--", "cmd/proxy/main.go"]
    return subprocess.run(cmd, cwd=root, capture_output=True).returncode == 0


def plan_profiles(mode, sel, spec, supported):
    """([(profile, set, shapes)] in window order, {profile: why it does not run}).

    supported: whether any build supports the align flag (planned from source, confirmed by the probe).
    The order is both the order the profiles run in and the order of their blocks of windows (window_plan),
    loki-defaults first: its Loki is the one the seed wrote to (data in its ingesters, as in the daily history),
    so it is not slowed by a restart, and its blocks are the ones nearest to now when Loki's 10-minute
    freshness horizon applies. aligned runs second, on a Loki recreated on the same data (stack.set_profile):
    its cold runs read the store, so a cold ratio against it is the more lenient one; every range-metric
    shape it runs is also run, cold included, against the seeded Loki in loki-defaults.
    """
    sets = sel.get("sets", {})
    want = {"auto": EXECUTION, "both": EXECUTION}.get(mode, [mode])
    runs, skipped = [], {}
    for profile in want:
        if profile == "aligned" and mode == "auto" and not supported:
            skipped[profile] = ("no build supports -align-queries-with-step, so every aligned row would be "
                                "not like-for-like")
            continue
        for set_name, shapes in sets.items():
            names = shapes
            if profile == "aligned" and mode == "auto":
                by_name = {s["name"]: s for s in spec["sets"][set_name]["shapes"]}
                names = [n for n in shapes if range_metric(by_name[n])]
                if not names:
                    continue
            runs.append((profile, set_name, names))
        if profile not in skipped and not any(r[0] == profile for r in runs):
            skipped[profile] = "the selection has no shape it applies to"
    return runs, skipped


EXECUTION = ["loki-defaults", "aligned"]


def execution_order(runs):
    """The profiles of `runs` in the order they run (EXECUTION)."""
    return [p for p in EXECUTION if any(r[0] == p for r in runs)]


def window_ages(path):
    """(youngest, oldest) seconds between a request and the end of its window, over the sent requests of a raw
    file: negative when a window ends after the request was sent. None for a raw file with no timed request."""
    with open(path) as f:
        rows = [r for r in json.load(f)["rows"] if r.get("at") is not None and r.get("end") is not None]
    ages = [r["at"] - r["end"] for r in rows]
    return (min(ages), max(ages)) if ages else None


def window_plan(args, sel, end, runs=None):
    """(per-run window plans, seconds to seed ending at `end`).

    Each run, a (profile, set, shapes) of plan_profiles (default: each selected set with no profile), gets its
    own block of windows below the previous one (perf_matrix.plan_windows), so no request is sent twice across
    sets or profiles; the seed covers the longest range, every block, and the [5m] lookback plus margin. A
    plan records its profile.
    """
    spec = selection.load_shapes()
    runs = runs if runs is not None else [("", name, shapes) for name, shapes in sel["sets"].items()]
    refresh_runs = args.refresh_runs if args.refresh_s else 0
    plans = perf_matrix.plan_windows(spec, [(name, shapes) for _, name, shapes in runs], args.ranges, args.runs,
                                     args.confirm_runs, end, args.refresh_s, refresh_runs,
                                     cells=selection.cells_of(sel) or None)
    for plan, (profile, _, _) in zip(plans, runs):
        plan["profile"] = profile
    seed = max_range_secs(args) + blocks_span(plans) + 900
    proof = sel.get("runtime_proof", {})
    cells = selection.cells_of(sel)
    for plan, (_, name, names) in zip(plans, runs):
        for shape in spec["sets"][name]["shapes"]:
            if names and shape["name"] not in names:
                continue
            # A shape at its own ranges seeds them like a required proof does: its cold re-measures below every
            # block read as much history as its first pass.
            if not proof.get("required") and not shape.get("end_offset") and f"{name}/{shape['name']}" not in cells:
                continue
            for rng in cells.get(f"{name}/{shape['name']}", ["instant"] if shape.get("instant") else args.ranges):
                secs, step = spec["ranges"].get(rng, (0, 0))
                lookback = perf_matrix.query_lookback(perf_matrix.shape_query(shape, step))
                starts = [perf_matrix.run_end_for(plan["confirm_end"], shape, rng, args.confirm_runs - 1, args.confirm_runs)]
                if refresh_runs:
                    starts.append(perf_matrix.run_end_for(plan["refresh_end"], shape, rng, 0, refresh_runs, args.refresh_s))
                if getattr(args, "cold_samples", 0) and not shape.get("end_offset"):
                    starts.append(end - blocks_span(plans) - cold.SAMPLE_SHIFT * args.cold_samples)
                seed = max(seed, end - min(starts) + secs + lookback + step)
    return plans, seed


def max_range_secs(args):
    """The longest range the run measures: args.ranges and the ranges of shapes that run at their own (cells)."""
    return max(1 if r == "instant" else stack_range_seconds(r) for r in [*args.ranges, *getattr(args, "cell_ranges", [])])


def blocks_span(plans):
    """Seconds from the first set's end to the end of the last set's block."""
    return plans[-1]["offset"] + plans[-1]["span"] if plans else 0


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
    spec = selection.load_shapes()
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
        profile = summary.get("profile")
        name = f"summary-{summary['set']}{'-' + profile if profile else ''}.json"
        with open(os.path.join(out, name), "w") as f:
            json.dump(summary, f, indent=1)


def note_ages(entry, out, set_name, profile):
    """Record in the profile's meta how old the windows were when they were requested: (youngest, oldest)
    seconds between a request and its window's end, over the passes of every set so far. A negative youngest
    would be a window ending after its request."""
    for suffix in ("", "-refresh", "-confirm"):
        path = raw_path(out, set_name, profile, suffix)
        if os.path.exists(path):
            ages = window_ages(path)
            if ages:
                old = entry.get("window_age_s")
                entry["window_age_s"] = list(ages) if not old else [min(old[0], ages[0]), max(old[1], ages[1])]


def note_quiet(meta, out, set_name, profile):
    """Add to meta["quiet_wait_s"] the seconds the passes of this set spent waiting for the other build's log to go
    quiet before a cold request (perf_matrix records it per pass)."""
    for suffix in ("", "-refresh", "-confirm"):
        path = raw_path(out, set_name, profile, suffix)
        if os.path.exists(path):
            with open(path) as f:
                meta["quiet_wait_s"] = round(meta.get("quiet_wait_s", 0) + json.load(f).get("quiet_wait_s", 0), 1)


def reuse_per_profile(summaries):
    """reuse_first_cold within each profile: the profiles ran against different Loki processes, so a cold one
    computed does not stand for a query the other's cache answered."""
    for profile in dict.fromkeys(s.get("profile") for s in summaries):
        reuse_first_cold([s for s in summaries if s.get("profile") == profile])
    return summaries


def measure(args, sel, out, base_sha, meta=None, baseline=None, bounds=None):
    """Stack up, seed, build, run every selected set in every profile; returns the summaries."""
    st = stack.stack_from(args)
    proxies, trees, summaries = [], {}, []
    mode = getattr(args, "profiles", "loki-defaults")
    # The warm-up and the cold re-measure are the PR smoke's: a caller that builds its own args (the weekly run
    # has no use for them, and warming would move its run-0 conditions) gets neither.
    warm_rounds, cold_samples = getattr(args, "warm_rounds", 0), getattr(args, "cold_samples", 0)
    budget_left = getattr(args, "cold_budget_s", cold.BUDGET_S)
    phases = {}
    clock = time.time()

    def lap(name):
        nonlocal clock
        phases[name] = round(time.time() - clock)
        clock = time.time()

    try:
        spec = selection.load_shapes()
        # Planned from source: the windows and the seed depend on it, and the builds are not there yet.
        planned_support = source_supports_align(args.head or None) or (
            not args.same_build and source_supports_align(base_sha))
        runs, skipped = plan_profiles(mode, sel, spec, planned_support)
        profiles = execution_order(runs)
        if profiles:
            st.profile = profiles[0]
        now = int(time.time())
        end = (args.end or now) // 60 * 60
        plans, seed_s = window_plan(args, sel, end, runs)
        history_age = seed_s + max(0, now - end)
        if sel.get("runtime_proof", {}).get("required") or history_age + stack.HISTORY_MARGIN_S > 72 * 3600:
            st.configure_history(out, profiles, history_age)
        st.up()
        lap("stack_up")
        if meta is not None:
            meta["end"], meta["seed_s"], meta["windows"] = end, seed_s, plans
            meta["profiles"] = {p: {"ran": p in profiles, **({"why_not": skipped[p]} if p in skipped else {})}
                                for p in stack.PROFILES}
        bins = {name: os.path.join(out, f"proxy-{name}") for name in ("base", "pr")}
        builds = [("pr", args.head or None, bins["pr"])]
        if not args.same_build:
            builds.append(("base", base_sha, bins["base"]))
        try:
            trees.update(stack.build_and_seed(st, builds, (seed_s, end, max(900, int(seed_s * SEED_TIMEOUT_PER_SECOND)))))
        except BaseException as e:
            trees.update(getattr(e, "trees", {}))
            raise
        lap("seed_and_build")
        spare = cold.spare_region(end, blocks_span(plans), max_range_secs(args))
        base_name = "pr" if args.same_build else "base"
        specs = [("base", bins[base_name], trees[base_name], args.proxy_port),
                 ("pr", bins["pr"], trees["pr"], args.proxy_port + 2)]
        supported = {name: stack.probe_align_support(binary) for name, binary, _, _ in specs}
        if meta is not None:
            meta["align_flag_supported"] = supported
        for profile in profiles:
            mine = [(plan, r) for plan, r in zip(plans, runs) if r[0] == profile]
            if profile == "aligned" and mode == "auto" and not any(supported.values()):
                # The plan came from source and the probe disagrees: nothing to compare. Its windows stay unused.
                if meta is not None:
                    meta["profiles"]["aligned"].update(ran=False, why_not="the built binaries do not know "
                                                                          "-align-queries-with-step")
                continue
            restarted = profile != profiles[0]  # the first profile runs on the Loki the seed wrote to
            settings = st.set_profile(profile, (end - seed_s, end), max(900, int(seed_s * SEED_TIMEOUT_PER_SECOND)))
            lap(f"{profile}:loki_profile")
            first = len(proxies)
            # Appended to `proxies` as each starts, so teardown stops a build whose start failed too.
            stack.start_proxies(specs, out, st, args.service, proxies, profile=profile, supported=supported)
            here = proxies[first:]
            align = {p.name: p.align for p in here}
            unlike = [p.name for p in here if not stack.like_for_like(profile, p.align)]
            if meta is not None:
                meta["profiles"][profile].update(loki=settings, proxy_align=align, not_like_for_like=unlike)
            lap(f"{profile}:proxies")
            by_name = {p.name: p for p in here}
            args.quiet_logs = {p.name: p.log_path for p in here}
            if warm_rounds:
                warmed = cold.warm_up(by_name, spare["warm_end"], warm_rounds, args.noise, args.min_delta)
                stack.log(f"warm-up ({profile}): {warmed}")
                if meta is not None:
                    meta["profiles"][profile]["warm_up"] = warmed
                lap(f"{profile}:warm_up")
            targets = [("base", here[0].url), ("pr", here[1].url), ("loki", st.loki)]
            for plan, (_, set_name, shapes) in mine:
                refresh = (plan["refresh_end"], args.refresh_s, args.refresh_runs) if args.refresh_s else None
                summary = run_matrix(set_name, shapes, targets, args, plan["end"], st, out, args.runs,
                                     refresh=refresh, profile=profile, unlike=unlike)
                summary["proxy_align"] = align  # the gate compares profiles that ran the PR with the same setting
                # The cold cells the gate calls a finding on the first pass, and the first pass's colds: confirm()
                # replaces the rows, and its run 0 is a warm time for the proxies (cold.py).
                first_cold = {(r["shape"], r["range"]): dict(r["cold"]) for r in summary["rows"]}
                first_resources = {(r["shape"], r["range"]): {t: cost.get("cold") for t, cost in r.get("reuse", {}).items()}
                                   for r in summary["rows"]}
                first_semantics = {(r["shape"], r["range"]):
                                   {key: r.get(key) for key in ("parity_vs_reference", "baseline_parity_vs_reference",
                                                              "reference_skipped")} | {"original_window_correctness": r.get("original_window_correctness", {}).get("matrix")}
                                   for r in summary["rows"]}
                pairs = (cold.flagged(over2x.gate(summary, baseline, args.noise, args.min_delta, bounds, summaries))
                         if baseline is not None and cold_samples else [])
                # A cold held to the base build's (over2x.cold_vs_base) is re-measured the same way before it fails.
                if cold_samples:
                    pairs = sorted(set(pairs) | {(f["shape"], f["range"]) for f in
                                                 over2x.cold_vs_base(summary, args.noise, args.min_delta,
                                                                     bounds=bounds)})
                summary = confirm(summary, args, plan, st, out, targets, baseline, bounds, summaries)
                if pairs:
                    records = cold.recold(summary, first_cold, pairs, by_name, spec, spare["cold_end"], cold_samples,
                                          stack.log, time.time() + budget_left, first_resources=first_resources,
                                          reference_url=st.loki, first_semantics=first_semantics)
                    budget_left -= sum(r["seconds"] for r in records)
                    summary["cold_resampled"] = records
                    if meta is not None:
                        meta.setdefault("cold_resampled", []).extend(records)
                        meta["cold_resample_s"] = round(meta.get("cold_resample_s", 0) + sum(r["seconds"] for r in records), 1)
                # The set ran on its own block of windows: perf_evidence trusts its warm and refresh figures.
                # summary-<set>-<profile>.json is rewritten with the marker and the confirmed rows, as the
                # comment shows them.
                if sel.get("runtime_proof", {}).get("required"):
                    summary["revision"] = {k: meta[k] for k in ("base", "head")}
                summary["isolated_windows"] = True
                summary["proxy_align"] = align
                summary["loki_restarted"] = restarted
                write_summaries([summary], out)
                summaries.append(summary)
                if meta is not None:
                    note_ages(meta["profiles"][profile], out, set_name, profile)
                    note_quiet(meta, out, set_name, profile)
                for p in here:
                    if not p.alive():
                        raise RuntimeError(f"proxy '{p.name}' died during the run; see {p.log_path}")
            lap(f"{profile}:matrix")
            for p in here:  # the next profile starts its own processes, with their own caches
                p.stop()
        # Every set is measured and confirmed: a repeated query's Loki cold now has its reference.
        write_summaries(reuse_per_profile(summaries), out)
        if meta is not None:
            meta["phases_s"] = phases
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
