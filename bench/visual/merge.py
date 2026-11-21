#!/usr/bin/env python3
"""Join the outputs of the visual-smoke shards into the output of one run.

  merge.py --plan plan.json --of K --shards DIR --out OUT

DIR holds one directory per shard (`visual-shard-<i>`, the artifact of the shard job: what ci_run.py wrote to its
--out); with one shard, download-artifact may have put its files in DIR itself, which is read then. OUT then holds what a single ci_run.py would have written for the whole plan, and visual_comment.py renders
the comment and the gate from it exactly as before:

  plan.json      the whole plan
  compare.json   the rows of every shard, in the order compare.py writes them (by the path data/<page>/<range>);
  compare.md     the file compare.py renders from those rows (compare.render_md)
  pixeldiff.json the scores of every shard
  montage/       the montages of every shard (the names are page-range, so they never collide)
  meta.json      base, head, the re-captured / flipped / late pages of every shard, and one entry per shard (phases)
  error.json     written when the run cannot be trusted: a shard died or left no result, the shard plans are not
                 an exact split of the plan, the shards were built from different commits or seeded different data

One decision is made over the whole run and not by a shard: the cap on how many differing captures may be loaded
again (ci_run.recapture). A shard loads its own differences again when its count is within the cap and keeps the
first pass of them aside (first-pass/); the shards' counts are summed here, and when the run as a whole is over the
cap the first pass of those captures is used and nothing counts as recaptured, flipped or late: the run a single job
leaves when it finds more differences than it will reload.

Every row is a pure function of the capture of one page, and a page is never divided over shards (shards.py), so the
rows, and with them the verdict, are those of a single job over the same captures.
"""
import argparse
import glob
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from compare import render_md  # noqa: E402
from shards import split, verify  # noqa: E402
from vio import dump_json, load_json, write_text  # noqa: E402

PREFIX = "visual-shard-"
# A capture the raw data of which the run artifact keeps (see the workflow).
KEPT_DATA = os.path.join("data", "lvl-dd-logs-pill-multi-json")


def read(path, default):
    return load_json(path) if os.path.exists(path) else default


def row_order(row):
    """compare.py walks sorted(glob('data/*/*')): the order of the path strings, not of the (page, range) pairs."""
    return f"{row['page']}{os.sep}{row['range']}"


def merge(plan, k, shards_dir, out, base="", head="", build_head=""):
    """Write the merged run into `out`; returns (error or None)."""
    os.makedirs(out, exist_ok=True)
    expected = split(plan, k)
    problems = verify(plan, expected)
    dirs = [os.path.join(shards_dir, f"{PREFIX}{i}") for i in range(k)]
    # download-artifact unpacks each artifact a pattern matches into its own directory, except when the pattern
    # matches one: then the files land in DIR itself. The one shard of a small plan may be DIR.
    if k == 1 and not os.path.isdir(dirs[0]) and os.path.exists(os.path.join(shards_dir, "plan.json")):
        dirs = [shards_dir]
    metas, shard_rows, pixels, montages = [], [], {}, {}
    for i, d in enumerate(dirs):
        if not os.path.isdir(d):
            problems.append(f"shard {i} left no result")
            metas.append({})
            continue
        meta = read(os.path.join(d, "meta.json"), {})
        metas.append(meta)
        if read(os.path.join(d, "plan.json"), None) != expected[i]:
            problems.append(f"shard {i} ran a plan other than its share of the plan")
        if plan.get("runtime_proof", {}).get("required"):
            for key, wanted in (("base", base), ("head", head), ("build_head", build_head)):
                actual = meta.get(key)
                if (not isinstance(actual, str) or len(actual) != 40 or any(c not in "0123456789abcdef" for c in actual)
                        or wanted and actual != wanted):
                    problems.append(f"shard {i} missing or mismatched {key} revision")
            seed = meta.get("seed") or {}
            if (not seed.get("end") or (seed.get("hours") or 0) < expected[i]["fixture_hours"]
                    or not seed.get("vl_lines") or not seed.get("loki_lines") or (seed.get("loki_hours") or 0) < 1.5):
                problems.append(f"shard {i} lacks measured fixture history")
        err = read(os.path.join(d, "error.json"), None)
        if err:
            problems.append(f"shard {i}: {err.get('error', 'unknown')}"[:300])
        measured = read(os.path.join(d, "compare.json"), [])
        if plan.get("runtime_proof", {}).get("required"):
            wanted = sorted((p, r) for p, e in expected[i]["entries"].items() for r in e["ranges"])
            if sorted((r["page"], r["range"]) for r in measured) != wanted:
                problems.append(f"shard {i} comparison differs from its planned page/range cells")
            if any("runtime_problems" not in r for r in measured if r["range"] != "live"):
                problems.append(f"shard {i} lacks a required runtime assessment")
        shard_rows.append((i, measured))
        pixels.update(read(os.path.join(d, "pixeldiff.json"), {}))
        for png in sorted(glob.glob(os.path.join(d, "montage", "*.png"))):
            if os.path.basename(png) in montages:
                problems.append(f"montage {os.path.basename(png)} comes from two shards")
                continue
            montages[os.path.basename(png)] = png
        kept = os.path.join(d, KEPT_DATA)
        if os.path.isdir(kept):
            shutil.copytree(kept, os.path.join(out, KEPT_DATA), dirs_exist_ok=True)
        for log in sorted(glob.glob(os.path.join(d, "*.log"))):
            shutil.copyfile(log, os.path.join(out, f"shard-{i}-{os.path.basename(log)}"))
    # The cap on reloading, over the run (see the docstring): over it, the first pass of what a shard reloaded stands.
    cap = max([m.get("max_recapture", 0) for m in metas if m] or [0])
    over = sum(m.get("recapture_candidates", 0) for m in metas if m) > cap
    rows, recaptured, flipped, late, has_flipped = [], [], set(), set(), False
    for i, shard in shard_rows:
        meta, d = metas[i], dirs[i]
        if over and meta.get("recaptured"):
            first = {(r["page"], r["range"]): r for r in read(os.path.join(d, "first-pass", "compare.json"), [])}
            if not first:
                problems.append(f"shard {i} reloaded captures but kept no first pass")
            restored = []
            for observed in shard:
                original = first.get((observed["page"], observed["range"]), observed)
                failures = observed.get("runtime_problems") or []
                if original is not observed and failures:
                    original["runtime_recapture"] = {k: observed.get(k) for k in ("runtime_main", "runtime_pr", "runtime_loki", "settle_s")}
                    original.setdefault("runtime_problems", []).extend(p for p in failures if p not in original.get("runtime_problems", []))
                restored.append(original)
            shard = restored
            first_px = read(os.path.join(d, "first-pass", "pixeldiff.json"), {})
            pixels.update(first_px)
            for png in glob.glob(os.path.join(d, "first-pass", "montage", "*.png")):
                montages[os.path.basename(png)] = png
        else:
            recaptured += meta.get("recaptured") or []
            has_flipped = has_flipped or "flipped" in meta
            flipped.update(meta.get("flipped") or [])
            late.update(meta.get("late_requests") or [])
        rows += shard
    for name, png in sorted(montages.items()):
        os.makedirs(os.path.join(out, "montage"), exist_ok=True)
        shutil.copyfile(png, os.path.join(out, "montage", name))
    for key in ("base", "head", "build_head"):
        if len({m.get(key) for m in metas if m}) > 1:
            problems.append(f"the shards were built from different {key} commits")
    # loki_lines is reported, not gated: the seed already waited for Loki to count what VictoriaLogs counts.
    for key in ("end", "hours", "vl_lines"):
        values = {(m.get("seed") or {}).get(key) for m in metas if m} - {None}
        if len(values) > 1:
            problems.append(f"the shards seeded different data ({key}: {sorted(values)})")
    rows.sort(key=row_order)
    # "page range", in the order of the rows (ci_run.py lists them as the rows come)
    recaptured.sort(key=lambda x: row_order(dict(zip(("page", "range"), x.rsplit(" ", 1)))))
    live = [m for m in metas if m]
    meta = {"recapture_over_cap": over, "base": next((m["base"] for m in live if "base" in m), base), "head": next((m["head"] for m in live if "head" in m), head),
            "plan_captures": plan["captures"], "recaptured": recaptured,
            "shards": [{"index": i, "captures": expected[i]["captures"], "elapsed_s": m.get("elapsed_s"), "phases_s": m.get("phases_s"),
                        "stack_up_phases_s": m.get("stack_up_phases_s"), "seed": m.get("seed")} for i, m in enumerate(metas)]}
    if build_head or any(m.get("build_head") for m in live):
        meta["build_head"] = next((m["build_head"] for m in live if m.get("build_head")), build_head)
    if has_flipped:
        meta["flipped"], meta["late_requests"] = sorted(flipped), sorted(late)
    dump_json(os.path.join(out, "plan.json"), plan)
    dump_json(os.path.join(out, "compare.json"), rows)
    write_text(os.path.join(out, "compare.md"), "\n".join(render_md(rows)) + "\n")
    dump_json(os.path.join(out, "pixeldiff.json"), dict(sorted(pixels.items())))
    dump_json(os.path.join(out, "meta.json"), meta)
    if problems:
        dump_json(os.path.join(out, "error.json"), {"error": "; ".join(problems)[:300], "problems": problems})
        return problems
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", required=True)
    ap.add_argument("--of", type=int, required=True, help="the number of shards")
    ap.add_argument("--shards", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--base", default="", help="base commit for meta.json when no shard wrote one")
    ap.add_argument("--head", default="", help="head commit for meta.json when no shard wrote one")
    ap.add_argument("--build-head", default="", help="expected checked-out revision used to build the PR binary")
    a = ap.parse_args()
    problems = merge(load_json(a.plan), a.of, a.shards, a.out, a.base, a.head, a.build_head)
    for p in problems or []:
        print(f"merge: {p}")
    print(f"merge: {'FAILED' if problems else 'ok'} ({a.of} shard(s))")
    return 0  # the verdict (visual_comment.py reads error.json), not this status, decides


if __name__ == "__main__":
    sys.exit(main())
