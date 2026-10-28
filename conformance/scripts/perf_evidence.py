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

The acceptance section applies the owner's tiers (2026-10-28: at most 2× Loki in the worst case, close to
Loki in general, below Loki for most queries) with bench/ab/over2x.py's definitions: the distribution of
proxy ÷ Loki per mode (below Loki, close, 1–2×, above 2×), the registry's perf: blocks (locked items and
rows) and every row between 1× and 2× with or without an effort exception.

--check fails when a shape covers an id the registry does not have, a shape
covers nothing, or a saved result names a set or shape shapes.json does not
define — the performance evidence and the registry cannot drift apart.

Quality summary: `--quality-summary PATH` rewrites only the text between the
`<!-- quality-summary:start -->` and `<!-- quality-summary:end -->` markers of PATH (README.md and
docs/real-window-compatibility-gaps.md) with the "Quality and performance vs Loki" section. Every figure comes from
files: the latest valid daily run (bench/ab/results/daily-*.json; the date and commit from
bench/ab/history/acceptance.json), the above-2× list (bench/ab/history/over-2x-loki.json), the registry (summary.json,
the cases' gap blocks, the behaviour states, the perf: blocks) and the Loki version pinned in
test/e2e-compat/docker-compose.yml. The gate (scripts/ci/check_conformance.py) fails when the text is stale.

Usage: python3 conformance/scripts/perf_evidence.py [--check | --quality-summary PATH]
"""
import argparse
import glob
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import SCOPE_SENTENCE, dump_json, load_json, read_text, write_text  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "bench", "ab"))
import over2x  # noqa: E402
import parity_gaps  # noqa: E402
import roadmap  # noqa: E402

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


# A saved summary belongs to a Loki profile (bench/ab/stack.py PROFILES); one saved before the profiles existed ran
# against the stack's Loki with proxy builds that did not align queries: "aligned". Of two summaries of the same
# date the loki-defaults one is the evidence: every build is like-for-like with Loki's own defaults.
LEGACY_PROFILE = "aligned"
PROFILE_RANK = {"aligned": 0, "loki-defaults": 1}


def profile_of(summary):
    return summary.get("profile") or LEGACY_PROFILE


def latest_runs(shape_sets, problems, pattern="*.json", keep=None):
    """(set, shape, range) -> the row of the most recent saved summary; `keep` (summary -> bool) narrows the summaries."""
    latest = {}
    for path in sorted(glob.glob(f"{RESULTS}/{pattern}")):
        summary = load_json(path)
        if keep is not None and not keep(summary):
            continue
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
            stamp = (summary.get("date", ""), PROFILE_RANK.get(profile_of(summary), 0), os.path.basename(path))
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
            and cand not in summary.get("not_like_for_like", []) and not cache_served(summary, row))


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
        "profile": profile_of(summary),
        "verdict_vs_baseline": row["verdict"], "baseline": base,
        "status": row["status"].get(cand), "baseline_status": row["status"].get(base),
        "cold": cand_cold, "warm_p50": row["p50"].get(cand),
        "loki_cold": ref_cold, "loki_warm_p50": row["p50"].get(ref), "loki_cache_served": served,
        "parity_vs_loki": row.get("parity_vs_reference", "n/a"),
        "slower_than_loki_cold": bool(comparable and beyond_noise(cand_cold, ref_cold)),
    }
    # Recorded only when the run measured them (older results did not): absent means unknown, never false.
    known = {
        "not_like_for_like": True if cand in summary.get("not_like_for_like", []) else None,
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
    parser.add_argument("--quality-summary", metavar="PATH", help="rewrite the generated section of PATH and nothing else")
    args = parser.parse_args()

    spec = load_json(SHAPES)
    shape_sets = spec["sets"]
    if args.quality_summary:
        return write_quality_summary(args.quality_summary, shape_sets)
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
    write_text(REPORT, render(evidence, latest, problems, shape_sets, over, over2x.read_bounds(spec=spec)))
    return 0


def acceptance(latest, bounds):
    """The distribution of proxy ÷ Loki per mode, the locked rows and the rows between 1× and 2×, over the
    latest run of every shape×range, by bench/ab/over2x.py's comparability and bands."""
    pairs, between, locked = {p: [] for p in PROFILE_RANK}, [], {m: 0 for m in over2x.MODES}
    for (set_name, shape, rng), (_, summary, row) in sorted(latest.items()):
        for mode, pair in over2x.summary_pairs(summary, row, summary["candidate"]).items():
            if over2x.judge(*pair) not in ("above", "within"):
                continue
            band = over2x.band(*pair[:2])
            pairs[profile_of(summary)].append((mode, band))
            bound = over2x.bound_for(bounds, set_name, shape, mode)
            locked[mode] += bound["locked"]
            if band == "between":
                between.append((shape, rng, mode, pair[0], pair[1], bound))
    return {p: over2x.distribution(ps) for p, ps in pairs.items() if ps}, locked, between


def render_acceptance(latest, bounds):
    dist, locked, between = acceptance(latest, bounds)
    blocks = (bounds or {}).get("blocks", {})
    n_locked = sum(1 for b in blocks.values() if b["locked"] is True)
    n_exc = sum(1 for b in blocks.values() if b.get("effort_exception"))
    out = ["## Acceptance versus Loki", "",
           "The owner's rule (2026-10-28): at most 2× Loki in the worst case; close to Loki in general; below Loki",
           "for most queries. Each registry item an A/B shape covers carries a `perf:` block (`max_ratio` 2.0, the",
           "hard bound; `target_ratio` 1.0); a locked item's rows above their bound are a regression on the daily",
           "run and in perf-smoke, and a row between 1× and 2× needs an `effort_exception` on an item that covers it.",
           f"{n_locked} of {len(blocks)} items are locked; {n_exc} carry an effort exception.", "",
           "Rows compared: both sides answered 200 and, on cold, Loki's results cache did not answer (the",
           "definitions of `bench/ab/over2x.py`). Unlike the summary above, warm is counted for every run as",
           "measured (Loki's warm is its results cache by design), so these warm figures are not the summary's",
           f"Warm repeat row. Below or above Loki means beyond {over2x.DIST_NOISE * 100:.0f}% and "
           f"{over2x.MIN_GAP * 1000:.0f} ms; close is within that.", "",
           "Each row is counted in the Loki profile of the run it comes from (`aligned`: the stack's Loki; `loki-defaults`:",
           "Loki with its own defaults, preferred when both ran); a locked row's bound holds in each profile.", "",
           "| Profile | Mode | Rows compared | Below Loki | Close to Loki | 1–2× | Above 2× | Locked rows | Most below Loki | Worst case ≤ 2× |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    for profile, by_mode in dist.items():
        for mode in over2x.MODES:
            d = by_mode.get(mode)
            if not d:
                continue
            out.append(f"| {profile} | {mode} | {d['rows']} | {d['below']} ({d['share_below']:.1f}%) | {d['close']} | "
                       f"{d['between']} | {d['above']} | {locked[mode]} | {'yes' if d['share_below'] > 50 else '**no**'} | "
                       f"{'yes' if not d['above'] else '**no**'} |")
    flagged = sum(1 for b in between if not b[5]["exception"])
    out += ["", "### Between 1× and 2× Loki", "",
            f"{len(between)} rows: {flagged} without an effort exception (flagged for review: below Loki is the",
            f"target) and {len(between) - flagged} with one (accepted up to the bound).", ""]
    if between:
        out += ["| shape | range | mode | proxy | Loki | proxy ÷ Loki | locked by | effort exception |",
                "|---|---|---|---|---|---|---|---|"]
        for shape, rng, mode, proxy, loki, b in between:
            out.append(f"| {shape.replace('|', chr(92) + '|')} | {rng} | {mode} | {fmt(proxy)} | {fmt(loki)} | "
                       f"{over2x.times(over2x.ratio(proxy, loki))} | "
                       f"{', '.join(f'`{i}`' for i in b['locked_by']) or '—'} | "
                       f"{', '.join(f'`{i}`' for i in b['exception']) or '**flagged: none**'} |")
    return out + [""]


def over_2x(over):
    """The "Above 2× Loki (must fix)" section from the list the daily run keeps; nothing without the file."""
    if over is None:
        return []
    rows = over.get("rows", [])
    modes = {m: sum(1 for r in rows if r["mode"] == m) for m in ("cold", "warm", "refresh")}
    profiles = {p: sum(1 for r in rows if (r.get("profile") or LEGACY_PROFILE) == p) for p in PROFILE_RANK}
    out = ["## Above 2× Loki (must fix)", "",
           f"Rows the daily run measured above 2× Loki ({over.get('rule', '')}), from `{OVER_2X}`. A shape × range",
           "slower than 2× Loki on any mode is a defect. The list only shrinks: the daily run removes a row after two",
           "consecutive runs within 2×, and a pull request that adds or worsens one is flagged by `perf-smoke`. A row is",
           "above 2× when it is in either Loki profile: `aligned` is the stack's Loki (align_queries_with_step on, freshness 0s;",
           "rows measured before the profiles existed had an unaligned proxy side), `loki-defaults` is Loki with its own defaults.", "",
           f"{len(rows)} rows: " + ", ".join(f"{m} {n}" for m, n in modes.items()) + "; "
           + ", ".join(f"{p} {n}" for p, n in profiles.items()) + ".", ""]
    if rows:
        out += ["| profile | set | shape | range | mode | proxy | Loki | proxy ÷ Loki | since |", "|---|---|---|---|---|---|---|---|---|"]
        for r in sorted(rows, key=lambda r: (PROFILE_RANK.get(r.get("profile") or LEGACY_PROFILE, 0), r["set"], r["shape"],
                                             r["range"], r["mode"])):
            profile = (r.get("profile") or LEGACY_PROFILE) + (" (proxy unaligned)" if r.get("proxy_side") == "unaligned" else "")
            out.append(f"| {profile} | {r['set']} | {r['shape'].replace('|', chr(92) + '|')} | {r['range']} | {r['mode']} | "
                       f"{fmt(r.get('proxy_s'))} | {fmt(r.get('loki_s'))} | {r['ratio']:.2f}× | {r.get('since', '')} |")
    else:
        out.append("None.")
    return out + [""]


def render(evidence, latest, problems, shape_sets=None, over=None, bounds=None):
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
    if bounds is not None:
        out += render_acceptance(latest, bounds)
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
    blocks = (bounds or {}).get("blocks", {})

    def lock(item):
        b = blocks.get(item)
        if not b:
            return "—"
        text = f"locked {b['locked_since']}" if b["locked"] is True else "not locked"
        return text + ("; exception" if b.get("effort_exception") else "")

    out += ["", "## By registry item", "",
            "| registry item | shapes | vs previous build | proxy cold / warm | Loki cold / warm | results vs Loki | acceptance |",
            "|---|---|---|---|---|---|---|"]
    for item, rows in sorted(evidence.items()):
        if not rows:
            out.append(f"| `{item}` | — | not measured | | | | {lock(item)} |")
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
                   f"{fmt(worst['loki_cold'])}{' †' if worst['loki_cache_served'] else ''} / {fmt(worst['loki_warm_p50'])} | "
                   f"{parity}{f'; {slow} slower than Loki cold' if slow else ''} | {lock(item)} |")
    out += ["", "† Loki's results cache answered that cold run: not a computed time, and in no comparison."]
    if problems:
        out += ["", "## Problems", ""] + [f"- {p}" for p in problems]
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------------------------------------------
# The "Quality and performance vs Loki (current)" section of README.md and docs/real-window-compatibility-gaps.md.
# It states the position now: figures from the latest valid daily run and the registry as they are, no history.
# ---------------------------------------------------------------------------------------------------------------
SUMMARY_START, SUMMARY_END = "<!-- quality-summary:start -->", "<!-- quality-summary:end -->"
COMPOSE = "test/e2e-compat/docker-compose.yml"
ACCEPTANCE = "bench/ab/history/acceptance.json"
SUMMARY_JSON = f"{ROOT}/generated/summary.json"
REPO_URL = "https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/"
# Behaviours stated as facts only while the registry holds them: every case in `cases` has gap.status fixed, every
# test in `tests` ((name, directory)) still exists and every item in `behaviours` has a state other than gap/unknown.
CLAIMS = (
    {"cases": ["semantics/unwrap-over-time-range-windows", "semantics/unwrap-over-time-short-range-windows"],
     "text": "`sum_over_time`, `avg_over_time`, `max_over_time` and `min_over_time` over an unwrapped field answer "
             "Loki's window on `query_range` for a range longer or shorter than the step",
     "open": {"semantics/unwrap-over-time-range-windows-residual-forms":
              "some outer aggregations of them are still listed as open differences"}},
    {"cases": ["quality/unwrap-query-range-extent-cache"],
     "text": "an unwrap metric moved forward by a refresh or a re-run reads from VictoriaLogs only the buckets it has "
             "not read before"},
    {"cases": ["semantics/binary-division-by-zero-nan"], "text": "a zero divisor of `/` and `%` is NaN, like Loki"},
    {"tests": [("TestBinaryMatchingErrorTextAndStepOrder", "internal/proxy")],
     "text": "binary vector matching raises Loki's duplicate-series error text at the first step where Loki's join fails"},
    {"behaviours": ["peer-cache-replication"], "tests": [("TestFleetPeerCacheProof", "test/e2e-fleet")],
     "text": "every peer-cache key has a backup replica (`-peer-replication-factor`, default 2); a three-proxy fleet "
             "test covers a replica that lost its disk refilling from its peers, the owner being down and every "
             "cache being lost"},
)
RANKED_TEST = ("RankedSignatureTest", "bench/ab/tests")


def pinned_loki(compose_text):
    """The Loki release the compatibility stack, CI and the A/B stack run: the default of LOKI_IMAGE."""
    found = re.search(r"\$\{LOKI_IMAGE:-grafana/loki:(\d+\.\d+\.\d+)\}", compose_text)
    if not found:
        raise ValueError(f"{COMPOSE}: no ${{LOKI_IMAGE:-grafana/loki:X.Y.Z}} default to read the Loki version from")
    return found.group(1)


def daily_runs(shape_sets, problems):
    """(latest rows, date, profile): the newest valid daily summaries, all of one date and one Loki profile
    (loki-defaults when that profile ran that day, otherwise aligned)."""
    summaries = [load_json(p) for p in sorted(glob.glob(f"{RESULTS}/daily-*.json"))]
    valid = [x for x in summaries if x.get("valid", True)]
    if not valid:
        raise ValueError(f"{RESULTS}: no valid daily-*.json summary")
    date = max(x["date"] for x in valid)
    profile = max((profile_of(x) for x in valid if x["date"] == date), key=lambda p: PROFILE_RANK.get(p, 0))
    latest = latest_runs(shape_sets, problems, "daily-*.json",
                         lambda x: x.get("valid", True) and x["date"] == date and profile_of(x) == profile)
    return latest, date, profile


def range_seconds(text):
    return int(text[:-1]) * {"m": 60, "h": 3600, "d": 86400}[text[-1]]


def test_names(directory):
    """Names of the Go tests (TestXxx), Python test classes and test_ functions defined under directory."""
    names = set()
    for current, _, files in os.walk(directory):
        for name in files:
            if name.endswith("_test.go") or name.endswith(".py"):
                text = read_text(os.path.join(current, name))
                names.update(re.findall(r"^func (Test\w+)\(", text, re.M))
                names.update(re.findall(r"^class (\w+)\b", text, re.M))
    return names


def behaviour_claims(found):
    """The sentences of CLAIMS the registry and the tests hold now."""
    status = {c["id"]: c["status"] for c in found}
    state_of = {}
    for track, directory in (("behaviours", "behaviours"), ("translations", "translations")):
        for item in roadmap.collect(track, directory):
            state_of[item["id"]] = item["state"]
    names = {}
    out = []
    for claim in CLAIMS:
        if any(status.get(c) != "fixed" for c in claim.get("cases", [])):
            continue
        if any(state_of.get(b, "unknown") in ("gap", "unknown") for b in claim.get("behaviours", [])):
            continue
        if any(t not in names.setdefault(d, test_names(d)) for t, d in claim.get("tests", [])):
            continue
        notes = [text for case, text in claim.get("open", {}).items() if status.get(case) == "open"]
        out.append(claim["text"] + (f" ({'; '.join(notes)})" if notes else ""))
    return out


def quality_inputs():
    """Everything the section states, read from the committed files."""
    spec = load_json(SHAPES)
    problems = []
    latest, date, profile = daily_runs(spec["sets"], problems)
    if problems:
        raise ValueError("; ".join(problems))
    acceptance_file = load_json(ACCEPTANCE)
    day = next((d for d in acceptance_file["days"] if d["date"] == date), None)
    found, _, parity_problems = parity_gaps.analyse()
    behaviours = roadmap.collect("behaviours", "behaviours") + roadmap.collect("translations", "translations")
    states = {}
    for item in behaviours:
        states[item["state"]] = states.get(item["state"], 0) + 1
    return {
        "loki": pinned_loki(read_text(COMPOSE)), "date": date, "profile": profile,
        "commits": (day or {}).get("commits", []), "sets": len({k[0] for k in latest}),
        "ranges": sorted({k[2] for k in latest if re.fullmatch(r"\d+[mhd]", k[2])}, key=range_seconds),
        "summary": summarize(latest, spec["sets"]),
        "distribution": ((day or {}).get("profiles", {}).get(profile)),
        "targets": acceptance_file.get("targets", {}),
        "over": load_json(OVER_2X) if os.path.exists(OVER_2X) else {"rows": []},
        "blocks": over2x.read_bounds(spec=spec).get("blocks", {}),
        "stats": load_json(SUMMARY_JSON), "states": states, "differences": found,
        "claims": behaviour_claims(found),
        "ranked_signature": RANKED_TEST[0] in test_names(RANKED_TEST[1]),
    }


def pct(part, whole):
    return f"{round(100 * part / whole)}%" if whole else "0%"


def sec(value):
    return "—" if value is None else f"{value:.2f} s"


def span(values):
    return f"{sec(min(values))} to {sec(max(values))}" if min(values) != max(values) else sec(values[0])


def code(text):
    return f"`` {text} ``" if "`" in text else f"`{text}`"


def plain_title(title):
    """A registry case title as a phrase: without the trailing state, starting in lower case."""
    title = re.sub(r"\s*\((open|documented|owner-kept|upstream)\)$", "", title)
    return title[0].lower() + title[1:] if title[:2].isalpha() and title[1].islower() else title


def links(target_is_docs):
    """The closing links, relative to where the section is published: docs pages by file, the rest on GitHub."""
    if target_is_docs:
        return [("performance summary", REPO_URL + "conformance/reports/performance.md#summary-versus-loki"),
                ("performance guide", "performance.md#current-position-versus-loki"),
                ("ranked differences", REPO_URL + "conformance/reports/parity-gaps.md"),
                ("coverage map", "compatibility-coverage.md"), ("Known Issues", "KNOWN_ISSUES.md")]
    return [("performance summary", "conformance/reports/performance.md#summary-versus-loki"),
            ("performance guide", "docs/performance.md#current-position-versus-loki"),
            ("ranked differences", "conformance/reports/parity-gaps.md"),
            ("coverage map", "conformance/reports/coverage-map.md"), ("Known Issues", "docs/KNOWN_ISSUES.md")]


def render_quality_summary(q, target_is_docs=False):
    """The section body (without the markers) from inputs read by quality_inputs()."""
    s, workloads = q["summary"], q["summary"]["workloads"]
    cold, comparable = s["cold"], s["comparable"]
    drill, logs, metric = (workloads[w] for w in WORKLOADS[1:])
    dist = q["distribution"] or {}
    first, repeat = dist.get("cold"), dist.get("warm")
    over = q["over"].get("rows", [])
    over_modes = {m: sum(1 for r in over if r["mode"] == m) for m in over2x.MODES}
    commits = ", ".join(f"`{c[:10]}`" for c in q["commits"])
    notes = {p: re.split(r"\.\s", n)[0].rstrip(".") for p, n in over2x.PROFILE_NOTES.items()}
    other = [p for p in PROFILE_RANK if p != q["profile"]]
    out = [f"Tested against Loki v{q['loki']}, the release the compatibility stack and CI pin. Timings were measured "
           f"on {q['date']}{f' at commit {commits}' if commits else ''} on the project's A/B stack: the same data, "
           f"the same hardware, the latest valid daily run of each of {q['sets']} query-shape sets over "
           f"{q['ranges'][0]} to {q['ranges'][-1]}. A pair is one query shape over one time range; \"comparable\" "
           f"means both sides answered 200 and Loki's results cache did not answer the first request. Faster and "
           f"slower mean beyond {int(NOISE * 100)}% and {int(MIN_DELTA * 1000)} ms; everything else is on par."
           + (" A `topk` or `bottomk` answer is compared by its ranked points and their sum, not by its series "
              "count." if q["ranked_signature"] else ""),
           "",
           f"Loki profile shown: `{q['profile']}`, {notes[q['profile']]}. "
           + (f"The stack's Loki settings differ from Loki's own defaults ({notes['loki-defaults'].split('(')[1].split(')')[0]}); "
              f"the `loki-defaults` profile has no valid daily run yet." if q["profile"] == "aligned" else
              f"The `aligned` profile ({notes['aligned']}) is measured too and is not shown here.") + " "
           + SCOPE_SENTENCE, "",
           "**Where the proxy is better than Loki**", ""]
    out += [f"- First request: **{cold.get('faster', 0)} of {comparable}** comparable pairs are faster "
            f"({pct(cold.get('faster', 0), comparable)}).",
            f"- Drilldown and Explore pages: **{drill['cold'].get('faster', 0)} of {drill['comparable']}** faster "
            f"({pct(drill['cold'].get('faster', 0), drill['comparable'])}); median first request "
            f"**{sec(median(drill['cand_cold']))}** against Loki's **{sec(median(drill['ref_cold']))}**.",
            f"- Metric queries: **{metric['cold'].get('faster', 0)} of {metric['comparable']}** faster "
            f"({pct(metric['cold'].get('faster', 0), metric['comparable'])}); median first request "
            f"**{sec(median(metric['cand_cold']))}** against **{sec(median(metric['ref_cold']))}**."]
    labels = [k for n, k in sorted(s["metadata"].items()) if n in ("Labels (label browser)", "Label values")]
    if labels:
        mine = [v for k in labels for v in k["cand_warm"] if v is not None]
        theirs = [v for k in labels for v in k["ref_warm"] if v is not None]
        out.append(f"- Label and label-value calls: median repeat **{sec(median(mine))}** against Loki's "
                   f"**{sec(median(theirs))}**, over {sum(k['pairs'] for k in labels)} pairs "
                   f"({sum(k['served'] for k in labels)} of them answered by Loki's results cache).")
    if first:
        out.append(f"- Share of rows below Loki, first request: **{first['share_below']:.1f}%** "
                   f"({first['below']} of {first['rows']} rows)"
                   + (f"; repeat: **{repeat['share_below']:.1f}%** ({repeat['below']} of {repeat['rows']})."
                      if repeat else ".") + f" The target is {q['targets'].get('share_below', 'most queries below Loki')}.")
    out += ["", "**Where it is on par**", ""]
    out += [f"- First request: **{cold.get('par', 0)} of {comparable}** pairs are within noise "
            f"({pct(cold.get('par', 0), comparable)}).",
            f"- Log queries: **{logs['cold'].get('par', 0)} of {logs['comparable']}** on par; metric queries: "
            f"**{metric['cold'].get('par', 0)} of {metric['comparable']}**; Drilldown and Explore pages: "
            f"**{drill['cold'].get('par', 0)} of {drill['comparable']}**."]
    if first:
        out.append(f"- Rows close to Loki (within the same noise): **{first['close']}** on the first request"
                   + (f", **{repeat['close']}** on a repeat." if repeat else "."))
    out.append("- Repeat of the same query, median p50: " + "; ".join(
        f"{name if name == WORKLOADS[1] else name[0].lower() + name[1:]} **{sec(median(g['cand_warm']))}** "
        f"against **{sec(median(g['ref_warm']))}**" for name, g in ((WORKLOADS[1], drill), (WORKLOADS[3], metric), (WORKLOADS[2], logs))) + ".")
    results = s["results"]
    out.append(f"- Answers: **{results.get('same', 0)} of {s['pairs']}** pairs return the same result as Loki "
               f"({pct(results.get('same', 0), s['pairs'])}).")
    for claim in q["claims"]:
        out.append(f"- {claim[0].upper()}{claim[1:]}.")
    out += ["", "**Where it is worse**", ""]
    out += [f"- First request: **{cold.get('slower', 0)} of {comparable}** pairs are slower "
            f"({pct(cold.get('slower', 0), comparable)}): log queries **{logs['cold'].get('slower', 0)} of "
            f"{logs['comparable']}**, Drilldown and Explore pages **{drill['cold'].get('slower', 0)} of "
            f"{drill['comparable']}**, metric queries **{metric['cold'].get('slower', 0)} of {metric['comparable']}**."]
    if first:
        out.append(f"- Between 1× and 2× Loki: **{first['between']}** rows on the first request"
                   + (f", **{repeat['between']}** on a repeat" if repeat else "")
                   + f"; above 2× Loki: **{first['above']}** on the first request"
                   + (f" and **{repeat['above']}** on a repeat" if repeat else "") + f" (the {q['date']} distribution).")
    locked = sum(1 for b in q["blocks"].values() if b["locked"] is True)
    if over:
        out.append(f"- **{len(over)} shape, range and mode rows are on the above-2× list** (more than 2× Loki and more "
                   f"than 50 ms slower): **{over_modes['cold']}** on the first request, **{over_modes['warm']}** on a "
                   f"repeat, **{over_modes['refresh']}** on a refresh. Each is a defect being fixed: a row leaves the "
                   f"list after two consecutive daily runs within 2×, so it can hold more rows than the day's "
                   f"distribution above; a change that adds or worsens a row is flagged. {locked} of "
                   f"{len(q['blocks'])} covered registry items are locked at the 2× bound.")
    else:
        out.append(f"- No row is on the above-2× list. {locked} of {len(q['blocks'])} covered registry items are "
                   f"locked at the 2× bound.")
    gaps = {m: sorted((r for r in over if r["mode"] == m), key=lambda r: r["loki_s"] - r["proxy_s"])[:limit]
            for m, limit in (("cold", 3), ("warm", 2))}
    for mode, label in (("cold", "first request"), ("warm", "repeat")):
        if gaps[mode]:
            out.append(f"- Largest gaps on that list, {label}: " + "; ".join(
                f"{code(r['shape'])} over {r['range']}, **{sec(r['proxy_s'])}** against **{sec(r['loki_s'])}**"
                for r in gaps[mode]) + ".")
    out.append(f"- Answers: **{results.get('differs', 0)} of {s['pairs']}** pairs return a different result from Loki, "
               f"and **{results.get('not compared', 0)}** could not be compared.")
    stats = q["stats"]
    found = q["differences"]
    count = {k: sum(1 for c in found if c["status"] == k) for k in ("open", "documented", "owner-kept", "upstream")}
    states = q["states"]
    native = stats["execution"].get("native_vl", 0)
    out += ["", "**Quality of the Loki read API**", "", "| | Now |", "|---|---|",
            f"| Loki endpoints the proxy routes | **{stats['implemented']} of {stats['endpoints']}** "
            f"({pct(stats['implemented'], stats['endpoints'])}) |",
            f"| Endpoints with tests that compare the proxy against Loki | **{stats['with_differential_tests']} of "
            f"{stats['endpoints']}** ({pct(stats['with_differential_tests'], stats['endpoints'])}) |",
            f"| Endpoints whose registry state is `proven` | **{stats['proven']} of {stats['endpoints']}** "
            f"({pct(stats['proven'], stats['endpoints'])}) |",
            f"| Routed endpoints answered natively by VictoriaLogs | {native} of {stats['implemented']} |",
            f"| Behaviour and feature items | {states.get('proven', 0)} proven, {states.get('partial', 0)} partial, "
            f"{states.get('gap', 0)} gaps |",
            f"| Known differences from Loki (ranked) | **{sum(count.values())}**: {count['open']} open, "
            f"{count['documented']} documented, {count['owner-kept']} owner-kept, {count['upstream']} upstream |", ""]
    top = [c for c in found if c["status"] == "open"][:5]
    if top:
        impacts = {c["impact"] for c in top}
        out.append(f"The {len(top)} open differences with the most impact"
                   + (f" (all {next(iter(impacts))})" if len(impacts) == 1 else "") + ": "
                   + ", ".join(f"`{c['id']}` ({plain_title(c['title'])})" for c in top)
                   + ". Differences kept on purpose are listed as documented or owner-kept.")
    out += ["", "Details, the per-workload tables and the method: "
            + " · ".join(f"[{name}]({url})" for name, url in links(target_is_docs)) + "."]
    return "\n".join(out)


def replace_section(text, body):
    """text with what lies between the markers replaced by body; the rest is untouched."""
    if text.count(SUMMARY_START) != 1 or text.count(SUMMARY_END) != 1:
        raise ValueError(f"expected exactly one {SUMMARY_START} and one {SUMMARY_END}")
    if text.index(SUMMARY_START) > text.index(SUMMARY_END):
        raise ValueError("the end marker comes before the start marker")
    head = text.split(SUMMARY_START)[0]
    tail = text.split(SUMMARY_END)[1]
    return f"{head}{SUMMARY_START}\n\n{body}\n\n{SUMMARY_END}{tail}"


def write_quality_summary(path, shape_sets=None):
    try:
        body = render_quality_summary(quality_inputs(), os.path.normpath(path).startswith("docs" + os.sep))
        write_text(path, replace_section(read_text(path), body))
    except (ValueError, KeyError, OSError) as error:
        print(f"quality summary {path}: {error}", file=sys.stderr)
        return 1
    print(f"{path}: quality summary written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
