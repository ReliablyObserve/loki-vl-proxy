#!/usr/bin/env python3
"""Split the captures of one visual-smoke plan over parallel jobs, and check that the split is exact.

Each shard brings up its own stack (same seed end, same images) and captures a deterministic subset of
the plan; merge.py joins the results. The split is by plan ENTRY (a page with all its ranges): the gate reads
across the ranges of a page (a query proven to answer like Loki at a short range excuses the same query at a range
Loki does not hold), so a page is never divided.

  shards.py --plan plan.json                      the shard count and the matrix of the workflow (JSON)
  shards.py --plan plan.json --shard 1 --of 3 --out plan-shard.json
  shards.py --plan plan.json --verify shard-0.json shard-1.json ...   every capture in exactly one shard

Shard count: one shard per PER_SHARD captures, at most MAX_SHARDS. A stack costs about four minutes before
its first capture (job start, images, seed, Grafana, warm-up) and a capture about ten seconds, so a shard
must hold enough captures to pay for its stack: below PER_SHARD a split saves less wall time than a second
stack costs. A plan of a dozen captures or fewer (the core set and a fix proof or two) stays one job. The
cap bounds the runner minutes of a baseline run (every page) and the runners one run asks for.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from vio import dump_json, load_json  # noqa: E402

LIVE = "live"
PER_SHARD = 12
MAX_SHARDS = 8
# The Live tail pass starts the generator, waits for it and records a minute of traffic: about five captures.
LIVE_WEIGHT = 5


def capture_count(entries):
    """The plan's own count: one per static range, one per live entry."""
    return sum(len([r for r in e["ranges"] if r != LIVE]) + (1 if e["ranges"] == [LIVE] else 0) for e in entries.values())


def shard_count(plan, per_shard=PER_SHARD, max_shards=MAX_SHARDS):
    """How many jobs the plan needs; 0 when nothing is planned."""
    if not plan.get("run") or not plan.get("entries"):
        return 0
    return max(1, min(max_shards, -(-capture_count(plan["entries"]) // per_shard), len(units(plan))))


def units(plan):
    """[(weight, [page ids])] in plan order: a static page is a unit of its range count; the live pages are ONE unit,
    because one generator serves the whole Live tail pass."""
    out, live = [], []
    for pid, e in plan["entries"].items():
        if e["ranges"] == [LIVE]:
            live.append(pid)
        else:
            out.append((len(e["ranges"]), [pid]))
    if live:
        out.append((LIVE_WEIGHT * len(live), live))
    return out


def assign(plan, k):
    """k lists of page ids: longest unit first onto the lightest shard (ties: the lower shard, then plan order)."""
    order = sorted(enumerate(units(plan)), key=lambda x: (-x[1][0], x[0]))
    load, groups = [0] * k, [[] for _ in range(k)]
    for _, (weight, pids) in order:
        i = min(range(k), key=lambda j: (load[j], j))
        load[i] += weight
        groups[i].extend(pids)
    position = {pid: n for n, pid in enumerate(plan["entries"])}
    return [sorted(g, key=position.__getitem__) for g in groups]


def split(plan, k):
    """The k shard plans: the plan with only the entries of the shard (same keys, so ci_run.py needs no special case)."""
    shards = []
    for i, pids in enumerate(assign(plan, k)):
        entries = {pid: plan["entries"][pid] for pid in pids}
        shards.append({**plan, "entries": entries, "captures": capture_count(entries), "shard": {"index": i, "of": k}})
    return shards


def planned(plan):
    return [(pid, r) for pid, e in plan["entries"].items() for r in e["ranges"]]


def verify(plan, shard_plans):
    """Problems of a split: a capture in no shard, in two, one the plan never asked for, or an entry that changed."""
    problems, seen = [], {}
    for i, sp in enumerate(shard_plans):
        for pid, e in sp["entries"].items():
            if plan["entries"].get(pid) != e:
                problems.append(f"shard {i}: entry {pid} is not the plan's entry")
            if not e["ranges"]:
                problems.append(f"shard {i}: entry {pid} has no range")
        if capture_count(sp["entries"]) != sp.get("captures"):
            problems.append(f"shard {i}: says {sp.get('captures')} captures, holds {capture_count(sp['entries'])}")
        for cap in planned(sp):
            if cap in seen:
                problems.append(f"{cap[0]} {cap[1]} is in shard {seen[cap]} and shard {i}")
            seen[cap] = i
    want = planned(plan)
    problems += [f"{p} {r} is in no shard" for p, r in want if (p, r) not in seen]
    problems += [f"{p} {r} is in a shard but not in the plan" for p, r in seen if (p, r) not in set(want)]
    if sum(sp.get("captures", 0) for sp in shard_plans) != capture_count(plan["entries"]):
        problems.append("the shard capture counts do not add up to the plan's")
    pages = {}
    for i, sp in enumerate(shard_plans):
        for pid in sp["entries"]:
            if pages.setdefault(pid, i) != i:
                problems.append(f"page {pid} is divided over shards {pages[pid]} and {i}")
    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", required=True)
    ap.add_argument("--shard", type=int)
    ap.add_argument("--of", type=int)
    ap.add_argument("--out", default="")
    ap.add_argument("--per-shard", type=int, default=PER_SHARD)
    ap.add_argument("--max-shards", type=int, default=MAX_SHARDS)
    ap.add_argument("--verify", nargs="+", metavar="SHARD_PLAN")
    a = ap.parse_args()
    plan = load_json(a.plan)
    if a.verify:
        problems = verify(plan, [load_json(p) for p in a.verify])
        for p in problems:
            print(p)
        print(f"visual shards: {'FAILED' if problems else 'ok'} ({len(problems)} problem(s))")
        return 1 if problems else 0
    if a.shard is not None:
        if a.of is None or not 0 <= a.shard < a.of:
            ap.error("--shard needs --of with 0 <= shard < of")
        sp = split(plan, a.of)[a.shard]
        if a.out:
            dump_json(a.out, sp)
        print(json.dumps({"shard": a.shard, "of": a.of, "captures": sp["captures"], "pages": list(sp["entries"])}))
        return 0
    k = shard_count(plan, a.per_shard, a.max_shards)
    sizes = [capture_count(s["entries"]) for s in split(plan, k)] if k else []
    print(json.dumps({"shards": k, "sizes": sizes, "matrix": {"shard": list(range(k))}}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
