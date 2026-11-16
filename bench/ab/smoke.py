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
  * a first ask (over2x.first_ask_bound: the cold run of a shape whose state the proxy has not built, which Loki
    answers from a structure built at ingest) took longer than its absolute bound; it is not compared with Loki;
  * the cache reuse dropped against the previous smoke day, judged on two repeats apart:
      - the exact repeat (perf_matrix.py --exact-repeat: the same request asked twice in a row at a settled window,
        the second one measured; an exact repeat is the cache's to answer, 0 VictoriaLogs calls expected): a row
        whose exact repeat calls VictoriaLogs more often than the day before;
      - the repeat that moves the window by a minute (the later runs of the matrix, which legitimately reads the
        new buckets): a row that reads more bytes from VictoriaLogs than the day before (beyond REUSE_BYTES_NOISE)
        or whose hit ratio of a cache layer and tier fell by more than REUSE_RATIO_NOISE. Its calls are reported,
        not judged: a moved window asks for the buckets it lacks.
    The calls are the request's own count (X-Loki-VL-Backend-Calls of a proxy run with -debug-vl-calls-header),
    which leaves the proxy's background calls out; a proxy build without the header is counted by the process-wide
    metrics (reuse.py).
The distribution (below, close, between, above 2x Loki) is reported per mode beside it, and the reuse per row
(VictoriaLogs calls and bytes, cold, repeat, exact repeat and refresh, and the repeat's hit ratio per layer and
tier) is kept in bench/ab/history/daily-smoke/reuse.jsonl, one line per day.

  python3 bench/ab/smoke.py judge --out verdict.json SUMMARY.json...
judges report.py summaries of any run the same way, with no previous line (the weekly run uses it for its
full A/B on the oldest supported VictoriaLogs, which writes no history).
"""
import argparse
import json
import os
import sys
import tempfile

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
# A moved-window repeat reads more bytes than the day before only when it grew by this factor and by more than the
# floor: its data volume moves with the day's lines, a bucket more or less is not a cache that stopped working.
REUSE_BYTES_NOISE = 1.5
REUSE_BYTES_FLOOR = 4096


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
            entry = {
                "cold_vl_calls": cold.get("vl_calls", 0.0), "cold_vl_bytes": cold.get("vl_bytes", 0.0),
                "repeat_vl_calls": repeat.get("vl_calls", 0.0), "repeat_vl_bytes": repeat.get("vl_bytes", 0.0),
                "refresh_vl_calls": refresh.get("vl_calls", 0.0), "repeat_hits": reuse.hit_ratio(repeat)}
            # The exact repeat, when this run measured one: absent keys mean "not measured", never 0 calls.
            exact = got.get("exact_repeat")
            if exact is not None:
                entry["exact_repeat_vl_calls"] = exact.get("vl_calls", 0.0)
                entry["exact_repeat_vl_bytes"] = exact.get("vl_bytes", 0.0)
                entry["exact_fill_vl_calls"] = (got.get("exact_fill") or {}).get("vl_calls", 0.0)
            elif got.get("exact_repeat_problem"):
                entry["exact_repeat_problem"] = got["exact_repeat_problem"]
            out["|".join((summary["set"], row["shape"], row["range"], profile))] = entry
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
        # The exact repeat is judged by its calls (expected 0), the moved-window repeat by the bytes it reads.
        if "exact_repeat_vl_calls" in now and "exact_repeat_vl_calls" in old and now["exact_repeat_vl_calls"] > old["exact_repeat_vl_calls"]:
            why.append(f"exact repeat VictoriaLogs calls {old['exact_repeat_vl_calls']:g} -> {now['exact_repeat_vl_calls']:g}")
        if now["repeat_vl_bytes"] > old["repeat_vl_bytes"] * REUSE_BYTES_NOISE and now["repeat_vl_bytes"] - old["repeat_vl_bytes"] > REUSE_BYTES_FLOOR:
            why.append(f"moved-window repeat VictoriaLogs bytes {old['repeat_vl_bytes']:g} -> {now['repeat_vl_bytes']:g}")
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
    out = {"rows": 0, "comparable": 0, "over_2x": [], "ratchet_lost": [], "loki_failed": 0, "pairs": [],
           "first_ask": [], "first_ask_over": []}
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
            bound = over2x.first_ask_bound(*key)
            if bound is not None:
                src = by_row[key]
                cand, ref = summary["candidate"], summary["reference"]
                proxy = src["cold"].get(cand) if over2x.like_for_like(summary, cand) else None
                status = src["status"].get(cand) if over2x.like_for_like(summary, cand) else None
                state = over2x.first_ask_judge(proxy, status, bound)
                if state != "not_comparable":
                    item = {"set": summary["set"], "shape": row["shape"], "range": row["range"], "profile": profile,
                            "proxy_s": proxy, "loki_s": src["cold"].get(ref), "max_s": bound, "state": state}
                    out["first_ask"].append(item)
                    if state == "above_bound":
                        out["first_ask_over"].append(item)
            if (summary["set"], *key) in held:
                why = history.ratchet_loss(row, old.get(key))
                if why:
                    out["ratchet_lost"].append({"set": summary["set"], "shape": row["shape"], "range": row["range"],
                                                "profile": profile, "detail": why})
    out["distribution"] = over2x.distribution(out.pop("pairs"))
    rows = reuse_rows(summaries)
    exact = [r for r in rows.values() if "exact_repeat_vl_calls" in r]
    out["reuse"] = {"rows": len(rows), "cold_vl_calls": round(sum(r["cold_vl_calls"] for r in rows.values()), 6),
                    "repeat_vl_calls": round(sum(r["repeat_vl_calls"] for r in rows.values()), 6),
                    "repeat_vl_bytes": round(sum(r["repeat_vl_bytes"] for r in rows.values()), 6),
                    "cold_vl_bytes": round(sum(r["cold_vl_bytes"] for r in rows.values()), 6),
                    "repeat_without_vl_calls": sum(1 for r in rows.values() if r["repeat_vl_calls"] == 0),
                    # The exact repeats (--exact-repeat): rows measured, their calls, and the rows that asked VictoriaLogs.
                    "exact_rows": len(exact),
                    "exact_repeat_vl_calls": round(sum(r["exact_repeat_vl_calls"] for r in exact), 6),
                    "exact_without_vl_calls": sum(1 for r in exact if r["exact_repeat_vl_calls"] == 0),
                    "exact_problems": sum(1 for r in rows.values() if r.get("exact_repeat_problem"))}
    out["exact_repeat_called"] = [
        {"set": k.split("|")[0], "shape": k.split("|")[1], "range": k.split("|")[2], "profile": k.split("|")[3],
         "calls": r["exact_repeat_vl_calls"], "bytes": r["exact_repeat_vl_bytes"]}
        for k, r in sorted(rows.items()) if r.get("exact_repeat_vl_calls", 0) > 0]
    date = summaries[0]["date"] if summaries else ""
    out["reuse_lost"] = reuse_lost(rows, previous_reuse(directory, date)) if date else []
    out["reuse_rows"] = rows
    out["red"] = bool(out["over_2x"] or out["ratchet_lost"] or out["reuse_lost"] or out["first_ask_over"])
    return out


def render(verdict, date, commit):
    """The smoke's section of the daily report."""
    lines = [f"### Daily perf smoke ({date}, main `{commit[:10]}`)", "",
             f"{verdict['rows']} shape x range rows, {verdict['comparable']} comparable timings "
             f"(cold and warm, both Loki profiles), {verdict['loki_failed']} where Loki did not answer 200.", "",
             f"Cache reuse: {verdict['reuse']['rows']} rows measured, VictoriaLogs calls {verdict['reuse']['cold_vl_calls']:g} cold "
             f"and {verdict['reuse']['repeat_vl_calls']:g} on the repeats; {verdict['reuse']['repeat_without_vl_calls']} rows repeat "
             "without calling VictoriaLogs.", "",
             *render_exact(verdict["reuse"]),
             "| Mode | Rows | Below Loki | Close | 1-2x | Above 2x |", "|---|---|---|---|---|---|"]
    for mode, d in verdict["distribution"].items():
        lines.append(f"| {mode} | {d['rows']} | {d['below']} | {d['close']} | {d['between']} | {d['above']} |")
    lines += ["", f"First asks (the pattern state not built yet; held to an absolute bound, not to Loki's time): "
              f"{len(verdict.get('first_ask', []))} rows, {len(verdict.get('first_ask_over', []))} above their bound."]
    lines += [f"- {r['set']} / {r['shape']} / {r['range']} / {r['profile']}: {r['proxy_s']:.3f}s, bound {r['max_s']:g}s"
              + (f" (Loki {r['loki_s']:.3f}s from its ingester's state)" if r.get("loki_s") is not None else "")
              + (" **above the bound**" if r["state"] == "above_bound" else "")
              for r in verdict.get("first_ask", [])[:20]]
    for title, key, fmt in (
            ("Rows above 2x Loki", "over_2x",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']} / {r['mode']}: "
                       f"{r['proxy_s']:.3f}s against Loki {r['loki_s']:.3f}s"),
            ("Exact repeats that called VictoriaLogs", "exact_repeat_called",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']}: {r['calls']:g} call(s), {r['bytes']:g} bytes"),
            ("Rows whose cache reuse dropped since the previous day", "reuse_lost",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']}: {r['detail']}"),
            ("Weekly ratchet rows lost", "ratchet_lost",
             lambda r: f"{r['set']} / {r['shape']} / {r['range']} / {r['profile']}: {r['detail']}")):
        lines += ["", f"#### {title}: {len(verdict[key])}", ""]
        lines += [f"- {fmt(r)}" for r in verdict[key][:50]]
    return "\n".join(lines) + "\n"


def render_exact(reuse_totals):
    """The reuse paragraph of both repeats: the exact repeat (the cache's to answer) and the one that moves the window."""
    r = reuse_totals
    out = ["The two repeats are measured apart. A repeat that moves the window by a minute reads the buckets it lacks: "
           f"{r['repeat_vl_calls']:g} VictoriaLogs calls, {r.get('repeat_vl_bytes', 0):g} bytes against {r.get('cold_vl_bytes', 0):g} "
           "bytes cold, judged by bytes. The calls are each request's own count where the proxy reports it, without its "
           "background calls.", ""]
    if r.get("exact_rows"):
        out += [f"An exact repeat (the same request asked again at a settled window) is the cache's to answer: {r['exact_rows']} rows "
                f"measured, {r['exact_without_vl_calls']} without a VictoriaLogs call, {r['exact_repeat_vl_calls']:g} calls in all"
                + (f", {r['exact_problems']} not measured" if r.get("exact_problems") else "") + ".", ""]
    else:
        out += ["No exact repeat was measured in this run.", ""]
    return out


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


def main(argv=None):
    ap = argparse.ArgumentParser(description="judge report.py summaries like the daily perf smoke")
    sub = ap.add_subparsers(dest="cmd", required=True)
    j = sub.add_parser("judge")
    j.add_argument("--out", required=True)
    j.add_argument("--ratchet-dir", default=history.HISTORY)
    j.add_argument("summaries", nargs="+")
    a = ap.parse_args(argv)
    summaries = []
    for path in a.summaries:
        with open(path) as f:
            summaries.append(json.load(f))
    ratchet, problem = history.read_ratchet(a.ratchet_dir)
    with tempfile.TemporaryDirectory() as empty:
        verdict = judge(summaries, ratchet, empty)
    verdict["ratchet_problem"] = problem
    with open(a.out, "w") as f:
        json.dump(verdict, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"{verdict['rows']} rows, {len(verdict['over_2x'])} above 2x Loki, {len(verdict['ratchet_lost'])} ratchet rows lost")
    return 0


if __name__ == "__main__":
    sys.exit(main())
