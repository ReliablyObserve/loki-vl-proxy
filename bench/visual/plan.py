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
  plan.py --check                                 gate: every entry's covers resolve, every fixed
                                                  visual case has a fix-proof entry or an exemption

Fix proofs: a spec entry lists in `fixes` the registry case ids (conformance/registry/cases, the `gap:` block)
whose behaviour it reproduces. When a change adds a case or turns a case's gap status to `fixed`, every entry
that lists it is captured first at the ci_ranges Loki holds (LOKI_PROOF_S), `why: ["fix proof: <case>"]`, and never trimmed or dropped by
the capture budget. A fixed case whose impact a Grafana panel shows needs such an entry, or `visual: none` plus
`visual_reason:` in its gap block.

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

sys.path.insert(0, os.path.join(HERE, "..", "..", "conformance", "scripts"))
import parity_gaps as pg  # noqa: E402

from vio import dump_json, load_json  # noqa: E402

SPEC = "bench/visual/spec.json"
# Pages whose capture is the live websocket (no window): one capture each.
LIVE = "live"
UBIQUITOUS = ("loki_api_v1_query_range", "loki_api_v1_query")
HARNESS_PREFIXES = ("bench/visual/",)
HARNESS_FILES = (".github/workflows/visual-smoke.yaml", "test/e2e-ui/tests/explore-json-filter-panels.spec.ts")
CASES = "conformance/registry/cases"
VISUAL_IMPACTS = ("explore-visible", "drilldown-visible", "grafana-datasource")  # what a Grafana panel can show
FIX_CAPTURES = 18  # fix-proof captures at full ranges; further fix entries narrow to core_range, never dropped
FIX = "fix proof: "
# A fix is proven against Loki, which the CI stack holds for 1.5h (compare.py --loki-seconds): an entry selected
# only as a fix proof is captured at the ranges inside that window.
LOKI_PROOF_S = 5400
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


def case_gap(text):
    """Gap fields of a registry case file (conformance/scripts/parity_gaps.py's reader); {} without a gap block."""
    gap = pg.block(text, "gap")
    if not gap:
        return {}
    return {k: pg.scalar(gap, k) for k in ("status", "impact", "visual", "visual_reason")}


def load_cases(root=None):
    """case id (its path under cases/) -> gap fields, for every case with a gap block."""
    root = root or ab.ROOT
    found, _ = pg.cases(os.path.join(root, "conformance", "registry"))
    out = {}
    for c in found:
        with open(os.path.join(root, CASES, c["id"] + ".yaml"), encoding="utf-8") as f:
            out[c["id"]] = {**case_gap(f.read()), "status": c["status"], "impact": c["impact"]}
    return out


def git_out(root, *args):
    r = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def changed_cases(base, head="HEAD", root=None):
    """Case ids a change adds, or whose gap status it turns to fixed. Compared from the merge base of base and
    head (CI passes the merge commit's first parent, which is its own merge base); contents are read from git,
    renames are followed, an edit of another field or a deletion triggers nothing."""
    root = root or ab.ROOT
    base = (git_out(root, "merge-base", base, head) or base).strip()
    diff = git_out(root, "diff", "--name-status", "-M", base, head, "--", CASES) or ""
    out = []
    for line in diff.splitlines():
        parts = line.split("\t")
        code, new, old = parts[0][0], parts[-1], parts[1]
        if code not in "AMR" or not new.endswith(".yaml"):
            continue
        cid = new[len(CASES) + 1:-len(".yaml")]
        gap = case_gap(git_out(root, "show", f"{head}:{new}") or "")
        before = None if code == "A" else case_gap(git_out(root, "show", f"{base}:{old}") or "")
        if before is None or (gap.get("status") == "fixed" and before.get("status") != "fixed"):
            out.append(cid)
    return out


# Fields that only link an entry to the registry: editing them does not change what the entry captures.
LINK_FIELDS = ("covers", "fixes")


def capture_fields(entry):
    return {k: v for k, v in (entry or {}).items() if k not in LINK_FIELDS}


def changed_entries(spec, base):
    """Ids of the entries spec.json adds, or edits in what they capture, relative to base."""
    before = {p["id"]: p for p in spec_at(base)["pages"]}
    if not before:  # the change introduces the spec: nothing is an edit of an earlier entry
        return []
    return [p["id"] for p in spec["pages"]
            if p["id"] not in before or capture_fields(before[p["id"]]) != capture_fields(p)]


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


def plan(files, spec=None, items=None, base=None, max_captures=MAX_CAPTURES, cases=(), case_gaps=None):
    """cases: ids of the registry cases the change adds or fixes (changed_cases); case_gaps: id -> gap fields."""
    spec = spec or load_spec()
    items = items if items is not None else ab.registry_items()
    case_gaps = case_gaps if case_gaps is not None else (load_cases() if cases else {})
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
           "entries": {}, "captures": 0, "trimmed": [], "dropped": [], "fix_cases": {}, "fix_trimmed": []}
    for cid in cases:
        gap = case_gaps.get(cid, {})
        listed = [p["id"] for p in spec["pages"] if cid in (p.get("fixes") or [])]
        if not listed and (gap.get("status") != "fixed" or gap.get("impact") not in VISUAL_IMPACTS):
            continue  # open, or nothing a panel shows (api-only, edge): no fix to prove visually
        out["fix_cases"][cid] = {"entries": listed, "status": gap.get("status", ""), "impact": gap.get("impact", ""),
                                 "exempt": gap.get("visual_reason", "") if gap.get("visual") == "none" else ""}
    fix_pages = {pid for c in out["fix_cases"].values() for pid in c["entries"]}
    if not relevant and not fix_pages:
        return out
    out["run"] = True
    for p in spec["pages"]:
        if p.get("core"):
            add(p["id"], "core set")
    code = {f for f, k in kinds.items() if k == "code"}
    for p in spec["pages"]:
        specific = entry_files(p, items)
        for f in sorted(code & specific):
            add(p["id"], f)
    for cid, c in out["fix_cases"].items():
        for pid in c["entries"]:
            add(pid, FIX + cid)
    if base and any(f == SPEC for f in files):
        for pid in changed_entries(spec, base):
            add(pid, "entry added or edited")
    detailed = {pid for pid, r in why.items() if r != ["core set"]}
    for pid, reasons in why.items():
        entry = pages[pid]
        rs = ranges_of(entry, spec) if pid in detailed else [spec["core_range"]]
        if all(r.startswith(FIX) for r in reasons):
            rs = [r for r in rs if r == LIVE or spec["ranges"][r] <= LOKI_PROOF_S] or [spec["core_range"]]
        out["entries"][pid] = {"ranges": rs, "why": reasons, "core": bool(entry.get("core")), "kind": entry["kind"]}
        if pid in fix_pages:
            out["entries"][pid]["fixes"] = [c for c, v in out["fix_cases"].items() if pid in v["entries"]]
    # Fix proofs first (never trimmed), then the spec order.
    order = [p["id"] for p in spec["pages"]]
    order = [pid for pid in order if pid in fix_pages] + [pid for pid in order if pid not in fix_pages]
    out["entries"] = {pid: out["entries"][pid] for pid in order if pid in out["entries"]}
    used = 0
    for pid, e in out["entries"].items():
        if not e.get("fixes") or e["ranges"] == [LIVE]:
            continue
        if used and used + len(e["ranges"]) > FIX_CAPTURES:
            e["ranges"] = [spec["core_range"]]
            out["fix_trimmed"].append(pid)
        used += len(e["ranges"])

    def count():
        return sum(len([r for r in e["ranges"] if r != LIVE]) for e in out["entries"].values())

    # Budget: first narrow the detailed entries to the core range, then drop the
    # last ones in spec order; the core set is never trimmed.
    if count() > max_captures:
        for pid, e in out["entries"].items():
            if not e["core"] and not e.get("fixes") and e["ranges"] != [LIVE] and len(e["ranges"]) > 1:
                e["ranges"] = [spec["core_range"]]
                out["trimmed"].append(pid)
    for pid in reversed(list(out["entries"])):
        if count() <= max_captures:
            break
        if not out["entries"][pid]["core"] and not out["entries"][pid].get("fixes") and out["entries"][pid]["ranges"] != [LIVE]:
            out["dropped"].append(pid)
            del out["entries"][pid]
    out["captures"] = count() + sum(1 for e in out["entries"].values() if e["ranges"] == [LIVE])
    return out


def check(spec=None, items=None, cases=None):
    spec = spec or load_spec()
    items = items if items is not None else ab.registry_items()
    cases = cases if cases is not None else load_cases()
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
    listed = {c for p in spec["pages"] for c in p.get("fixes") or []}
    for p in spec["pages"]:
        for c in p.get("fixes") or []:
            if c not in cases:
                problems.append(f"{SPEC}: {p['id']} fixes unknown registry case {c} (a case file with a gap block)")
    for cid, gap in sorted(cases.items()):
        if gap.get("visual") == "none" and not gap.get("visual_reason"):
            problems.append(f"{CASES}: {cid} has `visual: none` without a `visual_reason:`")
        if gap.get("status") != "fixed" or gap.get("impact") not in VISUAL_IMPACTS:
            continue
        if cid not in listed and gap.get("visual") != "none":
            problems.append(f"{CASES}: {cid} is fixed with impact {gap['impact']} but no {SPEC} entry lists it in `fixes`; "
                            "add an entry that reproduces it, or set `visual: none` and `visual_reason:` in its gap block")
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
    cases = changed_cases(base, args.head) if args.files is None else []
    result = plan(files, base=base, max_captures=args.max_captures, cases=cases)
    if args.out:
        dump_json(args.out, result)
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
