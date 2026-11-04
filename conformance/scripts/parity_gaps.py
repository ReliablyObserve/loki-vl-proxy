#!/usr/bin/env python3
"""Rank every known difference between the proxy and Loki. Writes conformance/reports/parity-gaps.md.

Inputs:
  * every registry case carrying a `gap:` block (cases/**.yaml): the difference,
    its Loki reference, the proxy's behaviour, user impact, effort, code area,
    the planned test and the differential-run clusters it accounts for;
  * registry/generated/parity-discovery.json: the last differential run
    (bench/parity/run.py) recorded by bench/parity/publish.py: request counts
    per source and every gap signature with the corpus queries that hit it.

A gap's status is one of:
  open        a difference to remove
  owner-kept  a difference the owner decided to keep (with the decision in the case or its behaviour's state)
  documented  a difference documented for users (docs/KNOWN_ISSUES.md)
  upstream    not the proxy's: Loki or a client behaves this way
  fixed       removed by the change named in `fixed_by:` (a PR number or commit); its clusters stay claimed
              until the next recorded run no longer shows them, and the report lists it apart

Rank: open first, then by user impact (explore- and drilldown-visible first),
then by the number of corpus queries that hit the gap's clusters.

--prune edits the registry cases for the newest recorded run: a cluster id a case claims and the run no
longer holds is deleted from its `clusters:` list, whatever the case's status, so --check passes after a
rerun without a hand edit. Nothing is silent: every case that lost ids is listed, and a case left with no
cluster is called out with what to decide (status never changes by itself). It refuses a run that cannot
speak for the whole corpus: no cluster, a partial run (publish.py --partial), or a source with no
requests. --prune-report FILE also writes the listing as markdown.

--check writes nothing and fails when:
  * an undocumented cluster is claimed by no case, or by more than one;
  * a case claims a cluster the discovery does not hold;
  * a cluster the diff marked as a recorded deviation names a case id the
    registry does not hold, or a case whose status is not documented,
    owner-kept, upstream or fixed;
  * a case has an unknown status or impact, or `status: fixed` without `fixed_by:`.
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import load_json, read_text, write_text  # noqa: E402

ROOT = "conformance/registry"
DISCOVERY = os.path.join(ROOT, "generated", "parity-discovery.json")
REPORT = "conformance/reports/parity-gaps.md"
IMPACT = ("explore-visible", "drilldown-visible", "grafana-datasource", "api-only", "edge")
STATUS = ("open", "owner-kept", "documented", "upstream", "fixed")
RECORDED = ("documented", "owner-kept", "upstream", "fixed")
# The corpus sources of a whole run (bench/parity/corpus.py); a run missing one cannot prune.
SOURCES = ("grafana", "loki-tests", "repo", "seeded")


def block(text, name):
    """The indented lines under a top-level `name:` key."""
    found = re.search(rf"^{name}:\n((?:[ \t]+.*\n|\n)+)", text, re.M)
    return found.group(1) if found else ""


def scalar(text, key, indent="  "):
    found = re.search(rf"^{indent}{key}:\s*(.+)$", text, re.M)
    if not found:
        return ""
    value = found.group(1).strip()
    if len(value) > 1 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value.strip('"')


def items(text, key, indent="  "):
    found = re.search(rf"^{indent}{key}:\s*\[(.*)\]\s*$", text, re.M)
    if found:
        return [x.strip().strip('"') for x in found.group(1).split(",") if x.strip()]
    found = re.search(rf"^{indent}{key}:\n((?:{indent}  - .+\n)+)", text, re.M)
    return [x.strip("'\"") for x in re.findall(r"- (\S+)", found.group(1))] if found else []


def cases(root=ROOT):
    """Every case with a gap block, and the ids of all cases (with or without one)."""
    out, ids = [], set()
    base = os.path.join(root, "cases")
    for current, _, files in os.walk(base):
        for name in sorted(files):
            if not name.endswith(".yaml"):
                continue
            identifier = os.path.relpath(os.path.join(current, name), base)[:-5]
            ids.add(identifier)
            text = read_text(os.path.join(current, name))
            gap = block(text, "gap")
            if not gap:
                continue
            out.append({"id": identifier, "title": scalar(text, "title", "") or identifier,
                        "status": scalar(gap, "status") or "open", "impact": scalar(gap, "impact") or "api-only",
                        "effort": scalar(gap, "effort") or "?", "area": scalar(gap, "area"),
                        "loki": scalar(gap, "loki"), "planned_test": scalar(gap, "planned_test"),
                        "fixed_by": scalar(gap, "fixed_by"), "clusters": items(gap, "clusters"),
                        "path": os.path.join(current, name)})
    return out, ids


def analyse(root=ROOT, discovery_path=DISCOVERY):
    """(cases ranked, discovery, problems)."""
    found, ids = cases(root)
    by_id = {c["id"]: c for c in found}
    discovery = load_json(discovery_path) if os.path.exists(discovery_path) else {"clusters": [], "summary": {}}
    clusters = {c["id"]: c for c in discovery.get("clusters", [])}
    problems = []
    claimed = {}
    for case in found:
        if case["status"] not in STATUS:
            problems.append(f"{case['id']}: status {case['status']} is not one of {', '.join(STATUS)}")
        if case["impact"] not in IMPACT:
            problems.append(f"{case['id']}: impact {case['impact']} is not one of {', '.join(IMPACT)}")
        if case["status"] == "fixed" and not case["fixed_by"]:
            problems.append(f"{case['id']}: status fixed needs fixed_by (the PR number or commit that fixed it)")
        for cid in case["clusters"]:
            claimed.setdefault(cid, []).append(case["id"])
            if cid not in clusters:
                problems.append(f"{case['id']}: cluster {cid} is not in {discovery_path}")
    for cid, c in clusters.items():
        rule = c.get("documented")
        if rule:
            if rule not in ids:
                problems.append(f"cluster {cid} is marked as recorded deviation {rule}, which is no registry case")
            elif rule not in by_id or by_id[rule]["status"] not in RECORDED:
                problems.append(f"cluster {cid} is marked as recorded deviation {rule}, whose gap status is not one "
                                f"of {', '.join(RECORDED)}")
            else:
                claimed.setdefault(cid, []).append(rule)
        owners = sorted(set(claimed.get(cid, [])))
        if not owners:
            problems.append(f"cluster {cid} ({' / '.join(c['signature'])}) is accounted for by no registry case")
        elif len(owners) > 1:
            problems.append(f"cluster {cid} is claimed by {len(owners)} cases ({', '.join(owners)}); one owns it")
    for case in found:
        mine = [clusters[c] for c in clusters if case["id"] in claimed.get(c, [])]
        case["queries"] = sum(c["queries"] for c in mine)
        case["requests"] = sum(c["requests"] for c in mine)
        case["cluster_count"] = len(mine)
    found.sort(key=lambda c: (STATUS.index(c["status"]) if c["status"] in STATUS else 9,
                              IMPACT.index(c["impact"]) if c["impact"] in IMPACT else 9, -c["queries"], c["id"]))
    return found, discovery, problems


def render(found, discovery):
    summary = discovery.get("summary", {})
    lines = ["# Proxy vs Loki: ranked differences", "",
             "Generated by `conformance/scripts/parity_gaps.py` from the registry cases that carry a `gap:`",
             "block and the last differential run (`bench/parity`, recorded in",
             "`conformance/registry/generated/parity-discovery.json`). Do not edit by hand.", "",
             "Rank: open first, then user impact (explore- and drilldown-visible before the datasource path,",
             "API-only and edge cases), then the corpus queries that hit the gap's clusters. Fixed gaps are",
             "listed apart until a new run no longer shows their clusters.", ""]
    if summary:
        lines += [f"Last run: {discovery.get('recorded', '?')} on `{discovery.get('ref', '?')}`, "
                  f"window {discovery.get('window', '?')}.", "",
                  "| corpus source | requests | same | differ | both empty | blocked |", "|---|---:|---:|---:|---:|---:|"]
        for source, s in sorted(summary.items()):
            lines.append(f"| {source} | {s['requests']} | {s['same']} | {s['diff']} | {s['vacuous']} | {s['blocked']} |")
        lines.append("")
    else:
        lines += ["No differential run is recorded.", ""]
    current = [c for c in found if c["status"] != "fixed"]
    lines += ["| # | case | status | impact | queries | clusters | effort | area | Loki reference | planned test |",
              "|---:|---|---|---|---:|---:|---|---|---|---|"]
    for rank, case in enumerate(current, 1):
        lines.append(f"| {rank} | `{case['id']}` — {cell(case['title'])} | {case['status']} | {case['impact']} | "
                     f"{case['queries'] or '—'} | {case['cluster_count'] or '—'} | {case['effort']} | "
                     f"{cell(case['area'])} | {cell(case['loki'])} | {cell(case['planned_test'])} |")
    fixed = [c for c in found if c["status"] == "fixed"]
    if fixed:
        lines += ["", "## Fixed, awaiting the next run", "", "| case | fixed by | clusters still recorded | queries |",
                  "|---|---|---:|---:|"]
        for case in fixed:
            lines.append(f"| `{case['id']}` — {cell(case['title'])} | {cell(case['fixed_by'])} | "
                         f"{case['cluster_count']} | {case['queries']} |")
    lines.append("")
    return "\n".join(lines)


def cell(text, limit=140):
    text = str(text or "").replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def prune_refusal(discovery):
    """Why the run cannot be pruned against, or "" when it speaks for the whole corpus."""
    if not discovery.get("clusters"):
        return "the recorded run holds no cluster"
    if discovery.get("partial"):
        return "the recorded run is partial (a slice of the corpus): most clusters were simply not sent"
    summary = discovery.get("summary") or {}
    silent = [s for s in SOURCES if not (summary.get(s) or {}).get("requests")]
    if silent:
        return "the recorded run sent no request of the corpus source(s) " + ", ".join(silent)
    return ""


def drop_clusters(text, vanished):
    """`text` without the `vanished` ids in the gap block's `clusters:` list; (new text, ids dropped).

    Only the matching list lines go; every other line is kept as written (ids are bare or quoted, an all-digit
    id is quoted so YAML keeps it a string). An emptied list is written `clusters: []`."""
    lines = text.split("\n")
    start = next((i for i, line in enumerate(lines) if line.rstrip() == "gap:"), None)
    if start is None:
        return text, []
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or lines[end][0] in " \t"):
        end += 1
    dropped, out, i = [], lines[: start + 1], start + 1
    while i < end:
        line = lines[i]
        flow = re.match(r"^  clusters:\s*\[(.*)\]\s*$", line)
        if flow:
            kept = [x.strip() for x in flow.group(1).split(",") if x.strip()]
            dropped += [x for x in kept if x.strip("'\"") in vanished]
            kept = [x for x in kept if x.strip("'\"") not in vanished]
            out.append("  clusters: [" + ", ".join(kept) + "]")
            i += 1
        elif re.match(r"^  clusters:\s*$", line):
            j, kept = i + 1, []
            while j < end and re.match(r"^    - ", lines[j]):
                ident = lines[j].strip()[2:].strip().strip("'\"")
                if ident in vanished:
                    dropped.append(ident)
                else:
                    kept.append(lines[j])
                j += 1
            out += [line] + kept if kept else ["  clusters: []"]
            i = j
        else:
            out.append(line)
            i += 1
    out += lines[end:]
    return "\n".join(out), [x.strip("'\"") for x in dropped]


def prune(root=ROOT, discovery_path=DISCOVERY):
    """Delete the cluster ids the newest run no longer holds from every case's claim list.

    Returns (changes, refusal): changes is one dict per case that lost ids (id, status, dropped, left);
    refusal is why nothing was touched ("" when the run was pruned against)."""
    discovery = load_json(discovery_path) if os.path.exists(discovery_path) else {"clusters": []}
    refusal = prune_refusal(discovery)
    if refusal:
        return [], refusal
    held = {c["id"] for c in discovery["clusters"]}
    changes = []
    found, _ = cases(root)
    for case in found:
        vanished = {c for c in case["clusters"] if c not in held}
        if not vanished:
            continue
        text, dropped = drop_clusters(read_text(case["path"]), vanished)
        write_text(case["path"], text)
        changes.append({"id": case["id"], "status": case["status"], "dropped": dropped,
                        "left": len(case["clusters"]) - len(dropped)})
    return sorted(changes, key=lambda c: c["id"]), ""


def prune_report(changes):
    """Markdown listing of a prune; the cases left with no cluster come first."""
    if not changes:
        return "Prune: every cluster a case claims is still in the run; nothing dropped.\n"
    emptied = [c for c in changes if not c["left"]]
    lines = [f"Prune: {sum(len(c['dropped']) for c in changes)} vanished cluster id(s) dropped from "
             f"{len(changes)} case(s).", ""]
    gone = [c for c in emptied if c["status"] == "fixed"]
    if gone:
        lines += ["Fixed, and no run shows the difference any more (the fix is confirmed):", ""]
        lines += [f"- `{c['id']}`" for c in gone]
        lines.append("")
    undecided = [c for c in emptied if c["status"] != "fixed"]
    if undecided:
        lines += ["Not fixed in the registry, yet left with no cluster: the run no longer shows the difference. "
                  "Decide each: set `status: fixed` with `fixed_by:`, or find out whether its signature moved "
                  "to a new cluster that is still unclaimed:", ""]
        lines += [f"- `{c['id']}` (status {c['status']}, {len(c['dropped'])} dropped)" for c in undecided]
        lines.append("")
    rest = [c for c in changes if c["left"]]
    if rest:
        lines += ["Lost some clusters and keep others:", ""]
        lines += [f"- `{c['id']}` ({c['status']}): dropped {', '.join(c['dropped'])}; {c['left']} left" for c in rest]
        lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--prune", action="store_true",
                    help="drop the cluster ids the recorded run no longer holds from the registry cases, then write the report")
    ap.add_argument("--prune-report", default="", help="with --prune: also write the listing as markdown to this file")
    a = ap.parse_args()
    if a.prune:
        changes, refusal = prune()
        if refusal:
            print(f"prune refused: {refusal}")
            return 2
        listing = prune_report(changes)
        print(listing)
        if a.prune_report:
            write_text(a.prune_report, listing)
    found, discovery, problems = analyse()
    if not a.check:  # --check only validates; the report generator run writes (and the gate diffs) the report
        write_text(REPORT, render(found, discovery))
    print(f"{REPORT}: {len(found)} registered differences, {len(discovery.get('clusters', []))} discovered clusters, "
          f"{len(problems)} problem(s)")
    for problem in problems:
        print("  " + problem)
    return 1 if a.check and problems else 0


if __name__ == "__main__":
    sys.exit(main())
