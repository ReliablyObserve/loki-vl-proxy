#!/usr/bin/env python3
"""Pick the visual-smoke captures a change needs, from the files it touches.

Two tiers, like the A/B smoke's shape selection (bench/ab/selection.py, which
this imports: the mapping from code to registry items is not repeated here):

  core      the entries of spec.json marked "core": a small fixed pass over the
            main UI (Explore logs and a metric graph, Drilldown landing, a
            service's Logs and Fields). One range (core_range). Runs on every
            relevant change.
  detailed  entries whose `covers` (conformance registry ids) point at a source
            file the change touches, at ci_ranges. Also the entries a change to
            spec.json adds or edits. Trimmed to a capture budget when needed.

A change that touches nothing visual (docs, CI, Helm, tests, registry text)
runs nothing. Changes to this tooling, the stack harness or the workflow run
the core set, so the pipeline proves itself.

  plan.py --base origin/main --out plan.json      files from git diff base...HEAD
  plan.py --files internal/proxy/x.go             explicit file list
  plan.py --check                                 gate: every entry's covers resolve

Prints JSON: {"run", "entries": {id: {"ranges", "why"}}, "captures", "trimmed", ...}.
"""
import argparse
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "ab"))
import selection as ab  # noqa: E402

from vio import dump_json, load_json  # noqa: E402

SPEC = "bench/visual/spec.json"
# Pages whose capture is the live websocket (no window): one capture each.
LIVE = "live"
UBIQUITOUS = ("loki_api_v1_query_range", "loki_api_v1_query")
HARNESS_PREFIXES = ("bench/visual/",)
HARNESS_FILES = (".github/workflows/visual-smoke.yaml", "test/e2e-ui/tests/explore-json-filter-panels.spec.ts")
MAX_CAPTURES = 36  # static page x range captures (each is three datasources in sequence)


def load_spec(root=None):
    return load_json(os.path.join(root or ab.ROOT, SPEC))


def spec_at(ref):
    """The spec at a git ref; empty when the ref predates it or it does not parse."""
    try:
        text = subprocess.run(["git", "show", f"{ref}:{SPEC}"], cwd=ab.ROOT, capture_output=True, text=True,
                              check=True).stdout
        return json.loads(text)
    except (subprocess.CalledProcessError, ValueError):
        return {"pages": []}


def changed_entries(spec, base):
    """Ids of the entries spec.json adds or edits relative to base."""
    before = {p["id"]: p for p in spec_at(base)["pages"]}
    if not before:  # the change introduces the spec: nothing is an edit of an earlier entry
        return []
    return [p["id"] for p in spec["pages"] if before.get(p["id"]) != p]


def entry_files(entry, items):
    """Source files behind an entry: its covered items' (one hop, as the A/B selection reads them) and the handlers
    of the endpoints it covers. query_range and query are left out: every metric and log page reads them, so
    their handler would select every entry."""
    files, _ = ab.shape_files(entry, items)
    files = set(files)
    for item in entry.get("covers") or []:
        if ab.GENERIC.match(item) and item not in UBIQUITOUS:
            files |= items.get(item, (set(), set()))[0]
    return files


def ranges_of(entry, spec):
    return [LIVE] if entry["kind"] == "tail" else list(entry.get("ranges") or spec["ci_ranges"])


def plan(files, spec=None, items=None, base=None, max_captures=MAX_CAPTURES):
    spec = spec or load_spec()
    items = items if items is not None else ab.registry_items()
    kinds = {f: ab.classify(f) for f in files}
    harness = [f for f in files if f in HARNESS_FILES or f.startswith(HARNESS_PREFIXES)]
    relevant = [f for f in files if kinds[f] != "ignored"] + [f for f in harness if kinds[f] == "ignored"]
    pages = {p["id"]: p for p in spec["pages"]}
    why = {}

    def add(pid, reason):
        why.setdefault(pid, [])
        if reason not in why[pid]:
            why[pid].append(reason)

    out = {"run": bool(relevant), "changed": len(files), "relevant": relevant,
           "ignored": [f for f in files if f not in relevant], "core_range": spec["core_range"],
           "entries": {}, "captures": 0, "trimmed": [], "dropped": []}
    if not relevant:
        return out
    for p in spec["pages"]:
        if p.get("core"):
            add(p["id"], "core set")
    code = {f for f, k in kinds.items() if k == "code"}
    for p in spec["pages"]:
        specific = entry_files(p, items)
        for f in sorted(code & specific):
            add(p["id"], f)
    if base and any(f == SPEC for f in files):
        for pid in changed_entries(spec, base):
            add(pid, "entry added or edited")
    detailed = {pid for pid, r in why.items() if r != ["core set"]}
    for pid, reasons in why.items():
        entry = pages[pid]
        rs = ranges_of(entry, spec) if pid in detailed else [spec["core_range"]]
        out["entries"][pid] = {"ranges": rs, "why": reasons, "core": bool(entry.get("core")), "kind": entry["kind"]}
    order = [p["id"] for p in spec["pages"]]
    out["entries"] = {pid: out["entries"][pid] for pid in order if pid in out["entries"]}

    def count():
        return sum(len([r for r in e["ranges"] if r != LIVE]) for e in out["entries"].values())

    # Budget: first narrow the detailed entries to the core range, then drop the
    # last ones in spec order; the core set is never trimmed.
    if count() > max_captures:
        for pid, e in out["entries"].items():
            if not e["core"] and e["ranges"] != [LIVE] and len(e["ranges"]) > 1:
                e["ranges"] = [spec["core_range"]]
                out["trimmed"].append(pid)
    for pid in reversed(list(out["entries"])):
        if count() <= max_captures:
            break
        if not out["entries"][pid]["core"] and out["entries"][pid]["ranges"] != [LIVE]:
            out["dropped"].append(pid)
            del out["entries"][pid]
    out["captures"] = count() + sum(1 for e in out["entries"].values() if e["ranges"] == [LIVE])
    return out


def check(spec=None, items=None):
    spec = spec or load_spec()
    items = items if items is not None else ab.registry_items()
    problems, seen = [], set()
    if not any(p.get("core") for p in spec["pages"]):
        problems.append(f"{SPEC}: at least one entry must be marked \"core\": true")
    for rng in (*spec["ci_ranges"], spec["core_range"]):
        if rng not in spec["ranges"]:
            problems.append(f"{SPEC}: range {rng} is not in ranges")
    for p in spec["pages"]:
        pid = p["id"]
        if pid in seen:
            problems.append(f"{SPEC}: duplicate entry {pid}")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", pid):
            problems.append(f"{SPEC}: {pid} must use only letters, digits, '_' and '-' (it names capture files)")
        seen.add(pid)
        covers = p.get("covers") or []
        if not covers:
            problems.append(f"{SPEC}: {pid} has no covers (registry ids it shows)")
        for item in covers:
            if item not in items:
                problems.append(f"{SPEC}: {pid} covers unknown registry id {item}")
        specific = entry_files(p, items)
        if not p.get("core") and not specific:
            problems.append(f"{SPEC}: {pid} covers no registry item that names code, so no change can select it; "
                            "cover the behaviour, translation or case it shows")
    return problems


def changed_files(base, head="HEAD"):
    return ab.changed_files(base, head)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="", help="git ref; the change is base...HEAD")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--files", nargs="*", help="explicit changed files (instead of git diff)")
    ap.add_argument("--check", action="store_true", help="gate: every entry's covers resolve to registry items that name code")
    ap.add_argument("--max-captures", type=int, default=MAX_CAPTURES)
    ap.add_argument("--out", default="", help="also write the JSON here")
    args = ap.parse_args()
    if args.check:
        problems = check()
        for p in problems:
            print(p)
        print(f"bench/visual plan: {'FAILED' if problems else 'ok'} ({len(problems)} problem(s))")
        return 1 if problems else 0
    base = args.base or "origin/main"
    files = args.files if args.files is not None else changed_files(base, args.head)
    result = plan(files, base=base, max_captures=args.max_captures)
    if args.out:
        dump_json(args.out, result)
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
