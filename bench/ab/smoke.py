#!/usr/bin/env python3
"""The daily perf smoke: the main shapes of every set at 1h and 6h, cold and warm, both Loki profiles.

daily.py --smoke runs it on the daily report's stack. The main shapes of a set are the first SHAPES_PER_SET of
its list in shapes.json (each set lists its headline shapes first); a set is measured at the ranges of RANGES it
defines, and a set that defines neither is left to the weekly run. Every target is timed cold (first run) and
warm (the p50 of the repeats), under each Loki profile the builds support (pr_smoke.plan_profiles, as the
weekly run).

The smoke's history is kept apart from the weekly full history: one line per set and profile per day in
bench/ab/history/daily-smoke/<set>[.<profile>].jsonl (history.entry format). Nothing it writes is read by the
weekly run, its trend, ratchet or above-2x list, perf_evidence.py or the README: the weekly full run alone
states the published performance figures.

judge() is the smoke's verdict, red when
  * a row is above 2x Loki (over2x.judge: both answer 200, beyond the noise floor, on cold Loki's cache did
    not answer), cold or warm, under either profile; or
  * a row the weekly ratchet holds (bench/ab/history/ratchet.json, read only) lost it: a result that differs
    from Loki or a new error at once, a cold run slower than Loki's computed cold on this and the previous
    smoke line (history.ratchet_loss).
  * the cache reuse dropped against the previous smoke day: a row whose repeat (the later runs, perf_matrix.py
    --proxy-metrics) calls VictoriaLogs more often, or whose hit ratio of a cache layer and tier fell by more
    than REUSE_RATIO_NOISE.
The distribution (below, close, between, above 2x Loki) is reported per mode beside it, and the reuse per row
(VictoriaLogs calls and bytes, cold, repeat and refresh, and the repeat's hit ratio per layer and tier) is kept
in bench/ab/history/daily-smoke/reuse.jsonl, one line per day.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import history  # noqa: E402
import over2x  # noqa: E402
import reuse  # noqa: E402

SHAPES_PER_SET = 2
RANGES = ("1h", "6h")
MAX_RANGE = "6h"
RUNS = 3
DIRECTORY = os.path.join(history.HISTORY, "daily-smoke")
REUSE_HISTORY = "reuse.jsonl"
REUSE_RATIO_NOISE = 0.1


def reuse_rows(summaries):
    """(set, shape, range, profile) -> the candidate's reuse: VictoriaLogs calls and bytes cold, on the repeats
    and on the refresh pass, and the repeats' hit ratio per cache layer and tier."""
    out = {}
    for summary in summaries:
        cand = summary["candidate"]
        profile = history.profile_of(summary)
        for row in summary["rows"]:
            got = (row.get("reuse") or {}).get(cand)
            if not got:
                continue
            cold, repeat, refresh = got.get("cold") or {}, got.get("repeat") or {}, got.get("refresh") or {}
            out["|".join((summary["set"], row["shape"], row["range"], profile))] = {
                "cold_vl_calls": cold.get("vl_calls", 0.0), "cold_vl_bytes": cold.get("vl_bytes", 0.0),
                "repeat_vl_calls": repeat.get("vl_calls", 0.0), "repeat_vl_bytes": repeat.get("vl_bytes", 0.0),
                "refresh_vl_calls": refresh.get("vl_calls", 0.0), "repeat_hits": reuse.hit_ratio(repeat)}
    return out


def previous_reuse(directory, date):
    """The newest reuse line before date, or {}."""
    path = os.path.join(directory, REUSE_HISTORY)
    if not os.path.exists(path):
        return {}
    lines = []
    with open(path) as f:
        for raw in f:
            if raw.strip():
                line = json.loads(raw)
                if line["date"] < date:
                    lines.append(line)
    return max(lines, key=lambda line: line["date"])["rows"] if lines else {}


def reuse_lost(rows, previous):
    """Rows whose reuse dropped against the previous day."""
    out = []
    for key, now in sorted(rows.items()):
        old = previous.get(key)
        if not old:
            continue
        why = []
        if now["repeat_vl_calls"] > old["repeat_vl_calls"]:
            why.append(f"repeat VictoriaLogs calls {old['repeat_vl_calls']:g} -> {now['repeat_vl_calls']:g}")
        for tier, ratio in sorted(old.get("repeat_hits", {}).items()):
            got = now.get("repeat_hits", {}).get(tier, 0.0)
            if ratio - got > REUSE_RATIO_NOISE:
                why.append(f"{tier} hit ratio {ratio:.2f} -> {got:.2f}")
        if why:
            s, shape, rng, profile = key.split("|")
            out.append({"set": s, "shape": shape, "range": rng, "profile": profile, "detail": "; ".join(why)})
    return out


def plan(spec):
    """set -> (shape names, ranges) of the smoke, in shapes.json order."""
    out = {}
    for name, shape_set in spec["sets"].items():
        ranges = [r for r in shape_set["ranges"] if r in RANGES]
        if ranges:
            out[name] = ([s["name"] for s in shape_set["shapes"][:SHAPES_PER_SET]], ranges)
    return out


def rows_of(line):
    """A history line's rows as dicts (history.COLS)."""
    return [dict(zip(line["cols"], r)) for r in line["rows"]]


def previous_rows(directory, name, profile, date):
    """(shape, range) -> the row of the newest line before `date` in the smoke history of the set and profile."""
    lines = [e for e in history.read(os.path.join(directory, history.history_name(name, profile))) if e["date"] < date]
    if not lines:
        return {}
    return {(r["shape"], r["range"]): r for r in rows_of(max(reversed(lines), key=history.stamp))}


def judge(summaries, ratchet, directory=DIRECTORY):
    """The verdict of one smoke: rows above 2x Loki, ratchet rows lost, and the distribution per mode.

    summaries are report.py summaries of this run (history.entry turns each into a line); ratchet the weekly
    ratchet's entries; directory the smoke history holding the previous lines."""
    held = {(r["set"], r["shape"], r["range"]) for r in ratchet}
    out = {"rows": 0, "comparable": 0, "over_2x": [], "ratchet_lost": [], "loki_failed": 0, "pairs": []}
    for summary in summaries:
        line = history.entry(summary)
        profile = line["profile"]
        old = previous_rows(directory, summary["set"], profile, line["date"])
        by_row = {(r["shape"], r["range"]): r for r in summary["rows"]}
        for row in rows_of(line):
            out["rows"] += 1
            key = (row["shape"], row["range"])
            for mode, (proxy, loki, status, loki_status, served) in over2x.summary_pairs(
                    summary, by_row[key], summary["candidate"]).items():
                verdict = over2x.judge(proxy, loki, status, loki_status, served)
                if verdict == "loki_failed":
                    out["loki_failed"] += 1
                    continue
                if verdict == "not_comparable":
                    continue
                out["comparable"] += 1
                out["pairs"].append((mode, over2x.band(proxy, loki)))
                if verdict == "above":
                    out["over_2x"].append({"set": summary["set"], "shape": row["shape"], "range": row["range"],
                                           "profile": profile, "mode": mode, "proxy_s": proxy, "loki_s": loki})
            if (summary["set"], *key) in held:
                why = history.ratchet_loss(row, old.get(key))
                if why:
                    out["ratchet_lost"].append({"set": summary["set"], "shape": row["shape"], "range": row["range"],
                                                "profile": profile, "detail": why})
    out["distribution"] = over2x.distribution(out.pop("pairs"))
    rows = reuse_rows(summaries)
    out["reuse"] = {"rows": len(rows), "cold_vl_calls": round(sum(r["cold_vl_calls"] for r in rows.values()), 6),
                    "repeat_vl_calls": round(sum(r["repeat_vl_calls"] for r in rows.values()), 6),
                    "repeat_without_vl_calls": sum(1 for r in rows.values() if r["repeat_vl_calls"] == 0)}
    date = summaries[0]["date"] if summaries else ""
    out["reuse_lost"] = reuse_lost(rows, previous_reuse(directory, date)) if date else []
    out["reuse_rows"] = rows
    out["red"] = bool(out["over_2x"] or out["ratchet_lost"] or out["reuse_lost"])
    return out


def render(verdict, date, commit):
    """The smoke's section of the daily report."""
    lines = [f"### Daily perf smoke ({date}, main `{commit[:10]}`)", "",
             f"{verdict['rows']} shape x range rows, {verdict['comparable']} comparable timings "
             f"(cold and warm, both Loki profiles), {verdict['loki_failed']} where Loki did not answer 200.", "",
             f"Cache reuse: {verdict['reuse']['rows']} rows measured, VictoriaLogs calls {verdict['reuse']['cold_vl_calls']:g} cold "
             f"and {verdict['reuse']['repeat_vl_calls']:g} on the repeats; {verdict['reuse']['repeat_without_vl_calls']} rows repeat "
             "without calling VictoriaLogs.", "",
             "| Mode | Rows | Below Loki | Close | 1-2x | Above 2x |", "|---|---|---|---|---|---|"]
    for mode, d in verdict["distribution"].items():
        lines.append(f"| {mode} | {d['rows']} | {d['below']} | {d['close']} | {d['between']} | {d['above']} |")
    for title, key, fmt in (
            ("Rows above 2x Loki", "over_2x",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']} / {r['mode']}: "
                       f"{r['proxy_s']:.3f}s against Loki {r['loki_s']:.3f}s"),
            ("Rows whose cache reuse dropped since the previous day", "reuse_lost",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']}: {r['detail']}"),
            ("Weekly ratchet rows lost", "ratchet_lost",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']}: {r['detail']}")):
        lines += ["", f"#### {title}: {len(verdict[key])}", ""]
        lines += [f"- {fmt(r)}" for r in verdict[key][:50]]
    return "\n".join(lines) + "\n"


def write(summaries, out_dir, date, commit, directory=DIRECTORY, ratchet_dir=history.HISTORY):
    """Judge the run, append its lines to the smoke history, and write smoke.json and smoke.md to out_dir."""
    ratchet, problem = history.read_ratchet(ratchet_dir)
    verdict = judge(summaries, ratchet, directory)
    verdict["ratchet_problem"] = problem
    verdict["date"], verdict["commit"] = date, commit
    for summary in summaries:
        history.append(summary, directory=directory, commit=commit)
    if verdict["reuse_rows"]:
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, REUSE_HISTORY), "a") as f:
            f.write(json.dumps({"date": date, "commit": commit, "rows": verdict["reuse_rows"]}, sort_keys=True,
                               separators=(",", ":")) + "\n")
    with open(os.path.join(out_dir, "smoke.json"), "w") as f:
        json.dump(verdict, f, indent=1, sort_keys=True)
        f.write("\n")
    with open(os.path.join(out_dir, "smoke.md"), "w") as f:
        f.write(render(verdict, date, commit))
    return verdict
