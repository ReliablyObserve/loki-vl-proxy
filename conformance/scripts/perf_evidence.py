#!/usr/bin/env python3
"""Fold the saved A/B performance runs into the registry.

Every query shape in bench/ab/shapes.json names the registry items it measures
(`covers`). This joins those shapes with the latest saved summary for each
(set, shape, range) in bench/ab/results/ and writes, per registry item:

  conformance/registry/generated/perf-evidence.json  the latest measurements
  conformance/reports/performance.md                  the report, with every
                                                      item the proxy answers
                                                      slower than Loki cold,
                                                      and the rows above 2× Loki
                                                      (bench/ab/history/
                                                      over-2x-loki.json)

Loki is compared on cold (first-run) timings: warm timings on both sides come
largely from results caches and would hide the difference a user sees when a
panel first loads. Warm (p50 of the runs after the first) and, when a run
measured it, refresh (windows moving forward like a dashboard refresh) are
compared with the same noise rules and recorded next to cold. A row where Loki
answered identical results for windows a minute apart (the heuristic), or whose
own cache counters (data.stats.cache; statsResult, the query-planning index-stats
cache, is ignored) say its results cache answered the cold run, is marked
cache-served and is not counted as a gap on any of the three.

Fields a run did not record (Loki's cache counters, the refresh timings) are
absent: unknown, never false. Older results have none of them: their warm and
refresh flags are absent too (unknown), though the warm p50s they recorded are
still shown. Only a run that gives each set its own windows (isolated_windows) is
compared warm and on refresh. A query two sets share can be answered by Loki's
cache from the earlier set's extents; its Loki cold is then the first set's computed
run (loki_cold_reused_from), which keeps the row comparable.

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
# Rows above 2× Loki, kept by the daily run (bench/ab/over2x.py has the definition).
OVER_2X = "bench/ab/history/over-2x-loki.json"
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


def beyond_noise(cand, ref):
    """cand is slower than ref by both the relative and the absolute margin."""
    return cand is not None and ref is not None and cand > ref * (1 + NOISE) and cand - ref > MIN_DELTA


def cache_served(summary, row):
    """A cold run Loki's cache answered, by the heuristic or by Loki's own counters."""
    ref = summary["reference"]
    return ref in row.get("stale", []) or ref in row.get("cold_cache_served", [])


def is_comparable(summary, row):
    """Both sides answered 200 and Loki's results cache did not answer the cold run."""
    cand, ref = summary["candidate"], summary["reference"]
    return (row["status"].get(cand) == "200" and row["status"].get(ref) == "200"
            and not cache_served(summary, row))


def measurement(summary, row):
    cand, ref, base = summary["candidate"], summary["reference"], summary["baseline"]
    cand_cold, ref_cold = row["cold"].get(cand), row["cold"].get(ref)
    served = cache_served(summary, row)
    comparable = is_comparable(summary, row)
    # p50 of a --cold summary holds first runs: no warm timing was recorded there.
    cold_only = summary.get("timing") == "cold"
    cand_warm, ref_warm = (None, None) if cold_only else (row["p50"].get(cand), row["p50"].get(ref))
    refresh, refresh_status = row.get("refresh"), row.get("refresh_status", {})
    cand_refresh, ref_refresh = ((refresh.get(cand), refresh.get(ref)) if refresh else (None, None))
    refresh_ok = bool(refresh) and refresh_status.get(cand) == "200" and refresh_status.get(ref) == "200"
    cold_stats = (row.get("cache") or {}).get(ref)
    # Only a run of the harness that gives each set its own windows (pr_smoke marks its summaries) has warm
    # and refresh figures that are comparable; daily runs share windows across sets.
    isolated = summary.get("isolated_windows") is True
    measured = {
        "label": summary["label"], "date": summary["date"], "valid": summary.get("valid", True),
        "verdict_vs_baseline": row["verdict"], "baseline": base,
        "status": row["status"].get(cand), "baseline_status": row["status"].get(base),
        "cold": cand_cold, "warm_p50": row["p50"].get(cand),
        "loki_cold": ref_cold, "loki_warm_p50": row["p50"].get(ref), "loki_cache_served": served,
        "parity_vs_loki": row.get("parity_vs_reference", "n/a"),
        "slower_than_loki_cold": bool(comparable and beyond_noise(cand_cold, ref_cold)),
    }
    # Recorded only when the run measured them (older results did not): absent means unknown, never false.
    known = {
        "slower_than_loki_warm": bool(comparable and beyond_noise(cand_warm, ref_warm)) if isolated else None,
        "loki_cold_cache": cold_stats["cold"] if cold_stats else None,
        "loki_cold_reused_from": row.get("loki_cold_reused_from"),
        "refresh_p50": cand_refresh, "loki_refresh_p50": ref_refresh,
        "slower_than_loki_refresh": (bool(comparable and refresh_ok and beyond_noise(cand_refresh, ref_refresh))
                                     if refresh and isolated else None),
    }
    return {**measured, **{k: v for k, v in known.items() if v is not None}}


# Workloads of the summary table. The shape sets carry no category, so the grouping is explicit:
# an endpoint call is metadata, the sets and names below are Drilldown and Explore pages, a log
# query is a log query, and every other shape is a metric query.
DRILLDOWN_SETS = {"drilldown-breakdown-exact", "metric-latency-gap", "json-filter-pushdown", "json-detected-level"}
WORKLOADS = ("Metadata and label browser", "Drilldown and Explore pages", "Log queries", "Metric queries")


def workload(set_name, shape):
    if shape.get("path"):
        return WORKLOADS[0]
    name = shape["name"].lower()
    if set_name in DRILLDOWN_SETS or "drilldown" in name or "explore" in name:
        return WORKLOADS[1]
    return WORKLOADS[2] if shape.get("logs") else WORKLOADS[3]


def compare(cand, ref):
    """faster, par or slower by the same noise rule that flags a gap; None when a time is missing."""
    if cand is None or ref is None:
        return None
    if beyond_noise(cand, ref):
        return "slower"
    return "faster" if beyond_noise(ref, cand) else "par"


def median(values):
    values = sorted(values)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


def metadata_kind(path):
    if path.endswith("/labels"):
        return "Labels (label browser)"
    if "/label/" in path:
        return "Label values"
    if path.endswith("/detected_labels") or path.endswith("/detected_fields"):
        return "Drilldown detected labels and fields"
    return "Volume (Drilldown landing)"


def metadata_summary(latest, shape_sets):
    """Per endpoint kind, every pair both sides answered with 200, whether or not Loki's cache answered it."""
    by_name = {(s, sh["name"]): sh for s, v in shape_sets.items() for sh in v["shapes"]}
    kinds = {}
    for (set_name, shape_name, _), (_, summary, row) in latest.items():
        path = by_name[(set_name, shape_name)].get("path")
        cand, ref = summary["candidate"], summary["reference"]
        if not path or row["status"].get(cand) != "200" or row["status"].get(ref) != "200":
            continue
        k = kinds.setdefault(metadata_kind(path), {"pairs": 0, "served": 0, "cand_cold": [], "ref_cold": [],
                                                   "cand_warm": [], "ref_warm": []})
        k["pairs"] += 1
        k["served"] += cache_served(summary, row)
        k["cand_cold"].append(row["cold"].get(cand))
        k["ref_cold"].append(row["cold"].get(ref))
        k["cand_warm"].append(row["p50"].get(cand))
        k["ref_warm"].append(row["p50"].get(ref))
    return kinds


def render_metadata(kinds):
    out = ["### Metadata calls", "",
           "Label, label value, Drilldown detected-field and volume calls, over 1h to 7d. Unlike the table above,",
           "every pair both sides answered with 200 is included, because most of these calls are marked as answered",
           "by Loki's cache (see above): the `Loki cache-answered` column counts them, and a Loki time near zero",
           "there is a cache hit.",
           "Medians of the recorded cold time and warm p50.", "",
           "| Call | Pairs | Loki cache-answered | Proxy cold | Loki cold | Proxy warm | Loki warm |",
           "|---|---|---|---|---|---|---|"]
    for name, k in sorted(kinds.items()):
        vals = {key: median([v for v in k[key] if v is not None]) for key in ("cand_cold", "ref_cold", "cand_warm", "ref_warm")}
        out.append(f"| {name} | {k['pairs']} | {k['served']} | {fmt(vals['cand_cold'])} | {fmt(vals['ref_cold'])} | "
                   f"{fmt(vals['cand_warm'])} | {fmt(vals['ref_warm'])} |")
    return out


def summarize(latest, shape_sets):
    """Counts and medians over the latest run of every (set, shape, range), by the rules of measurement()."""
    by_name = {(s, sh["name"]): sh for s, v in shape_sets.items() for sh in v["shapes"]}
    out = {"pairs": 0, "comparable": 0, "isolated": 0, "cold": {}, "warm": {}, "refresh": {}, "results": {},
           "workloads": {w: {"pairs": 0, "comparable": 0, "cold": {}, "cand_cold": [], "ref_cold": [],
                             "cand_warm": [], "ref_warm": []} for w in WORKLOADS}}

    def bump(counts, key):
        counts[key] = counts.get(key, 0) + 1

    for (set_name, shape_name, _), (_, summary, row) in latest.items():
        cand, ref = summary["candidate"], summary["reference"]
        group = out["workloads"][workload(set_name, by_name[(set_name, shape_name)])]
        out["pairs"] += 1
        group["pairs"] += 1
        parity = row.get("parity_vs_reference", "n/a")
        bump(out["results"], "differs" if parity == "differs"
             else "same" if parity == "same" or parity.startswith("same series") else "not compared")
        if not is_comparable(summary, row):
            continue
        out["comparable"] += 1
        group["comparable"] += 1
        cold = compare(row["cold"].get(cand), row["cold"].get(ref))
        if cold:
            bump(out["cold"], cold)
            bump(group["cold"], cold)
            group["cand_cold"].append(row["cold"][cand])
            group["ref_cold"].append(row["cold"][ref])
        # p50 of a --cold summary holds first runs; warm and refresh need a run with its own windows.
        if summary.get("timing") != "cold" and row["p50"].get(cand) is not None and row["p50"].get(ref) is not None:
            group["cand_warm"].append(row["p50"][cand])
            group["ref_warm"].append(row["p50"][ref])
        if summary.get("isolated_windows") is not True:
            continue
        out["isolated"] += 1
        warm = None if summary.get("timing") == "cold" else compare(row["p50"].get(cand), row["p50"].get(ref))
        if warm:
            bump(out["warm"], warm)
        refresh, status = row.get("refresh"), row.get("refresh_status", {})
        if refresh and status.get(cand) == "200" and status.get(ref) == "200":
            moved = compare(refresh.get(cand), refresh.get(ref))
            if moved:
                bump(out["refresh"], moved)
    out["metadata"] = metadata_summary(latest, shape_sets)
    return out


def render_summary(summary):
    def line(label, counts):
        total = sum(counts.values())
        cells = " | ".join(f"{counts.get(k, 0)}" for k in ("faster", "par", "slower"))
        return f"| {label} | {cells} | {total} |"

    out = ["## Summary versus Loki", "",
           "Computed by this script over the latest saved run of each shape×range, with the rules above. A pair",
           "is comparable when the proxy and Loki both answered 200 and Loki's results cache did not answer the",
           "cold run. Faster, on par and slower use the noise rule of the tables below: slower or faster means",
           f"beyond {int(NOISE * 100)}% and {int(MIN_DELTA * 1000)} ms, and everything else is on par.", "",
           f"{summary['pairs']} shape×range pairs are measured and {summary['comparable']} are comparable.", "",
           "| Timing | Faster | On par | Slower | Pairs compared |", "|---|---|---|---|---|",
           line("Cold first request", summary["cold"]),
           line("Warm repeat", summary["warm"]),
           line("Refresh (window moved forward)", summary["refresh"]), ""]
    out.append(f"Warm and refresh are compared only for runs that give each set its own windows: "
               f"{summary['isolated']} of {summary['comparable']} comparable pairs have that data."
               + (" The warm columns under By workload are the recorded p50s, whose Loki side may come from a cache "
                  "an earlier set primed." if not summary["isolated"] else ""))
    results = summary["results"]
    out += ["", "| Results versus Loki | Pairs |", "|---|---|"]
    out += [f"| {k} | {results.get(k, 0)} |" for k in ("same", "differs", "not compared")]
    out += ["", "### By workload", "",
            "Medians over the comparable pairs of each workload: the proxy and Loki, first request (cold) and the",
            "recorded warm p50.", "",
            "| Workload | Pairs | Comparable | Cold faster / on par / slower | Proxy cold | Loki cold | Proxy warm | Loki warm |",
            "|---|---|---|---|---|---|---|---|"]
    for name, g in summary["workloads"].items():
        c = g["cold"]
        out.append(f"| {name} | {g['pairs']} | {g['comparable']} | "
                   f"{c.get('faster', 0)} / {c.get('par', 0)} / {c.get('slower', 0)} | "
                   f"{fmt(median(g['cand_cold']))} | {fmt(median(g['ref_cold']))} | "
                   f"{fmt(median(g['cand_warm']))} | {fmt(median(g['ref_warm']))} |")
    return out + [""] + render_metadata(summary["metadata"])


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
    over = load_json(OVER_2X) if os.path.exists(OVER_2X) else None
    write_text(REPORT, render(evidence, latest, problems, shape_sets, over))
    return 0


def over_2x(over):
    """The "Above 2× Loki (must fix)" section from the list the daily run keeps; nothing without the file."""
    if over is None:
        return []
    rows = over.get("rows", [])
    modes = {m: sum(1 for r in rows if r["mode"] == m) for m in ("cold", "warm", "refresh")}
    out = ["## Above 2× Loki (must fix)", "",
           f"Rows the daily run measured above 2× Loki ({over.get('rule', '')}), from `{OVER_2X}`. A shape × range",
           "slower than 2× Loki on any mode is a defect. The list only shrinks: the daily run removes a row after two",
           "consecutive runs within 2×, and a pull request that adds or worsens one is flagged by `perf-smoke`.", "",
           f"{len(rows)} rows: " + ", ".join(f"{m} {n}" for m, n in modes.items()) + ".", ""]
    if rows:
        out += ["| set | shape | range | mode | proxy | Loki | proxy ÷ Loki | since |", "|---|---|---|---|---|---|---|---|"]
        for r in sorted(rows, key=lambda r: (r["set"], r["shape"], r["range"], r["mode"])):
            out.append(f"| {r['set']} | {r['shape'].replace('|', chr(92) + '|')} | {r['range']} | {r['mode']} | "
                       f"{fmt(r.get('proxy_s'))} | {fmt(r.get('loki_s'))} | {r['ratio']:.2f}× | {r.get('since', '')} |")
    else:
        out.append("None.")
    return out + [""]


def render(evidence, latest, problems, shape_sets=None, over=None):
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
           "time near zero understates what a truly cold Loki takes. Loki 3.7.7 ignores",
           "`Cache-Control: no-cache` on query_range and volume_range, so no header is sent. A cold run",
           "Loki's results cache answered is detected from Loki's own cache counters (`data.stats.cache`;",
           "statsResult, the query-planning cache, is ignored) and by the identical-answer heuristic; rows",
           "without counters are older and their Loki cold time is unverified. A query two sets share takes",
           "the first set's computed Loki cold (`loki_cold_reused_from`). Warm and refresh timings are",
           "compared with the same noise rules; a row Loki's cache answered is excluded from all three.", "",
           f"Runs: {', '.join(f'{label} ({date})' for date, label in labels) or 'none yet'}.", ""]
    if shape_sets:
        out += render_summary(summarize(latest, shape_sets)) + [""]
    out += over_2x(over)
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
    repeat = {}
    for item, rows in evidence.items():
        for r in rows:
            if (r.get("slower_than_loki_warm") or r.get("slower_than_loki_refresh")) and not r["loki_cache_served"]:
                repeat.setdefault((r["set"], r["shape"], r["range"]), (r, []))[1].append(item)
    out += ["", "## Slower than Loki on repeat views", "",
            "Warm p50 (runs after the first) and refresh p50 (windows moving forward), where the proxy is",
            "slower beyond the same noise. Only runs of the harness that gives each set its own windows are",
            "compared; older",
            "results shared windows between sets, so their Loki warm figures may come from a cache an",
            "earlier set primed, and they are unknown here.", ""]
    if repeat:
        out += ["| shape | range | proxy warm | Loki warm | proxy refresh | Loki refresh | registry items |",
                "|---|---|---|---|---|---|---|"]
        for (s_, n, rng), (r, items) in sorted(repeat.items()):
            out.append(f"| {n.replace('|', chr(92) + '|')} | {rng} | {fmt(r['warm_p50'])} | {fmt(r['loki_warm_p50'])} | "
                       f"{fmt(r.get('refresh_p50'))} | {fmt(r.get('loki_refresh_p50'))} | "
                       f"{', '.join(f'`{i}`' for i in sorted(items) if not GENERIC.match(i)) or ', '.join(f'`{i}`' for i in items)} |")
        out += ["", f"{len(repeat)} shape×range slower than Loki beyond noise on a repeat view."]
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
