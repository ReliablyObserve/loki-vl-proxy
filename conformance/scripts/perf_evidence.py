#!/usr/bin/env python3
"""Fold the saved A/B performance runs into the registry.

Every query shape in bench/ab/shapes.json names the registry items it measures
(`covers`). This joins those shapes with the latest saved summary for each
(set, shape, range) in bench/ab/results/ and writes, per registry item:

  conformance/registry/generated/perf-evidence.json  the latest measurements
  conformance/reports/performance.md                  the report, with every
                                                      item the proxy answers
                                                      slower than Loki cold

Loki is compared on cold (first-run) timings: warm timings on both sides come
largely from results caches and would hide the difference a user sees when a
panel first loads. A row where Loki answered identical results for windows a
minute apart is marked cache-served and is not counted as a gap.

--check fails when a shape covers an id the registry does not have, a shape
covers nothing, or a saved result names a set or shape shapes.json does not
define — the performance evidence and the registry cannot drift apart.

Usage: python3 conformance/scripts/perf_evidence.py [--check]
"""
import argparse
import glob
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import dump_json, load_json, read_text, write_text  # noqa: E402

ROOT = "conformance/registry"
SHAPES = "bench/ab/shapes.json"
RESULTS = "bench/ab/results"
EVIDENCE = f"{ROOT}/generated/perf-evidence.json"
REPORT = "conformance/reports/performance.md"
# A shape is slower than Loki when its cold time exceeds Loki's by both.
NOISE, MIN_DELTA = 0.25, 0.05
# Endpoint items sit under every shape; the gap table names the specific ones.
GENERIC = re.compile(r"^(loki_api_v1_|api_prom_)")


def registry_ids():
    ids = set()
    for path in glob.glob(f"{ROOT}/**/*.yaml", recursive=True):
        if "/generated/" in path or "/state/" in path or "/schema/" in path:
            continue
        match = re.search(r"^id:\s*(\S+)", read_text(path), re.M)
        if match:
            ids.add(match.group(1).strip("'\""))
    return ids


def latest_runs(shape_sets, problems):
    """(set, shape, range) -> the row of the most recent saved summary."""
    latest = {}
    for path in sorted(glob.glob(f"{RESULTS}/*.json")):
        summary = load_json(path)
        set_name = summary.get("set")
        if set_name not in shape_sets:
            problems.append(f"{path}: set '{set_name}' is not defined in {SHAPES}")
            continue
        names = {s["name"] for s in shape_sets[set_name]["shapes"]}
        for row in summary["rows"]:
            if row["shape"] not in names:
                problems.append(f"{path}: shape '{row['shape']}' is not in set '{set_name}'")
                continue
            key = (set_name, row["shape"], row["range"])
            stamp = (summary.get("date", ""), os.path.basename(path))
            if key not in latest or stamp >= latest[key][0]:
                latest[key] = (stamp, summary, row)
    return latest


def measurement(summary, row):
    cand, ref, base = summary["candidate"], summary["reference"], summary["baseline"]
    stale = row.get("stale", [])
    cand_cold, ref_cold = row["cold"].get(cand), row["cold"].get(ref)
    cand_ok = row["status"].get(cand) == "200"
    ref_ok = row["status"].get(ref) == "200"
    slower = (cand_ok and ref_ok and ref not in stale and cand_cold is not None and ref_cold is not None
              and cand_cold > ref_cold * (1 + NOISE) and cand_cold - ref_cold > MIN_DELTA)
    return {
        "label": summary["label"], "date": summary["date"], "valid": summary.get("valid", True),
        "verdict_vs_baseline": row["verdict"], "baseline": base,
        "status": row["status"].get(cand), "baseline_status": row["status"].get(base),
        "cold": cand_cold, "warm_p50": row["p50"].get(cand),
        "loki_cold": ref_cold, "loki_warm_p50": row["p50"].get(ref), "loki_cache_served": ref in stale,
        "parity_vs_loki": row.get("parity_vs_reference", "n/a"),
        "slower_than_loki_cold": bool(slower),
    }


def fmt(value, status=None):
    if status and status != "200":
        return f"**{status}**"
    return "—" if value is None else f"{value:.2f}s"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    spec = load_json(SHAPES)
    shape_sets = spec["sets"]
    known = registry_ids()
    problems = []
    covers = {}
    for set_name, shape_set in shape_sets.items():
        for shape in shape_set["shapes"]:
            ids = shape.get("covers") or []
            if not ids:
                problems.append(f"{SHAPES}: {set_name}/{shape['name']} covers no registry item")
            for item in ids:
                if item not in known:
                    problems.append(f"{SHAPES}: {set_name}/{shape['name']} covers unknown registry id '{item}'")
                covers.setdefault(item, []).append((set_name, shape["name"]))

    latest = latest_runs(shape_sets, problems)
    evidence = {}
    for item, shapes in sorted(covers.items()):
        rows = []
        for set_name, shape_name in shapes:
            for (s, n, rng), (_, summary, row) in sorted(latest.items()):
                if (s, n) == (set_name, shape_name):
                    rows.append({"set": s, "shape": n, "range": rng, **measurement(summary, row)})
        evidence[item] = rows

    if args.check:
        for problem in problems:
            print("  ERROR " + problem)
        print(f"performance evidence: {sum(len(s['shapes']) for s in shape_sets.values())} shapes, "
              f"{len(covers)} registry items covered, {len(latest)} measured shape×range")
        return 1 if problems else 0

    dump_json(EVIDENCE, evidence)
    write_text(REPORT, render(evidence, latest, problems))
    return 0


def render(evidence, latest, problems):
    gaps = {}
    for item, rows in evidence.items():
        for r in rows:
            if r["slower_than_loki_cold"]:
                gaps.setdefault((r["set"], r["shape"], r["range"]), (r, []))[1].append(item)
    labels = sorted({(s["date"], s["label"]) for _, s, _ in latest.values()})
    out = ["# Performance evidence", "",
           "Generated by `conformance/scripts/perf_evidence.py` from the saved A/B runs in",
           "`bench/ab/results/` and the registry items each shape in `bench/ab/shapes.json` covers.",
           "Do not edit by hand. Timings are the candidate build's; Loki is compared on cold",
           "(first-run) timings, since warm timings on both sides come largely from results caches.",
           "Loki's first run can still hit split-level caches warmed by earlier requests, so a Loki cold",
           "time near zero understates what a truly cold Loki takes.", "",
           f"Runs: {', '.join(f'{label} ({date})' for date, label in labels) or 'none yet'}.", ""]
    out += ["## Slower than Loki on first load", ""]
    if gaps:
        out += ["| shape | range | proxy cold | Loki cold | proxy warm | Loki warm | registry items |",
                "|---|---|---|---|---|---|---|"]
        for (s, n, rng), (r, items) in sorted(gaps.items()):
            out.append(f"| {n.replace('|', chr(92) + '|')} | {rng} | {fmt(r['cold'])} | {fmt(r['loki_cold'])} | "
                       f"{fmt(r['warm_p50'])} | {fmt(r['loki_warm_p50'])} | "
                       f"{', '.join(f'`{i}`' for i in sorted(items) if not GENERIC.match(i)) or ', '.join(f'`{i}`' for i in items)} |")
        out += ["", f"{len(gaps)} shape×range slower than Loki beyond noise "
                f"({int(NOISE * 100)}% and {int(MIN_DELTA * 1000)} ms)."]
    else:
        out.append("None measured.")
    out += ["", "## By registry item", "",
            "| registry item | shapes | vs previous build | proxy cold / warm | Loki cold / warm | results vs Loki |",
            "|---|---|---|---|---|---|"]
    for item, rows in sorted(evidence.items()):
        if not rows:
            out.append(f"| `{item}` | — | not measured | | | |")
            continue
        verdicts = {}
        for r in rows:
            verdicts[r["verdict_vs_baseline"]] = verdicts.get(r["verdict_vs_baseline"], 0) + 1
        slow = sum(1 for r in rows if r["slower_than_loki_cold"])
        worst = max(rows, key=lambda r: (r["cold"] or 0))
        parity = "differs" if any(r["parity_vs_loki"] == "differs" for r in rows) else "same"
        out.append(f"| `{item}` | {len({(r['set'], r['shape']) for r in rows})} × {len({r['range'] for r in rows})} ranges | "
                   f"{', '.join(f'{n} {k}' for k, n in sorted(verdicts.items()))} | "
                   f"worst {fmt(worst['cold'], worst['status'])} / {fmt(worst['warm_p50'], worst['status'])} ({worst['range']}) | "
                   f"{fmt(worst['loki_cold'])} / {fmt(worst['loki_warm_p50'])} | "
                   f"{parity}{f'; {slow} slower than Loki cold' if slow else ''} |")
    if problems:
        out += ["", "## Problems", ""] + [f"- {p}" for p in problems]
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    sys.exit(main())
