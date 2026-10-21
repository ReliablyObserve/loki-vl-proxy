#!/usr/bin/env python3
"""Append-only A/B history and the trend report built from it.

The daily run from main (daily.py) appends one line per shape set per day to
bench/ab/history/<set>.jsonl and regenerates bench/ab/history/trend.md. Docs
and plots read the JSONL; nothing edits past lines.

  history.py append SUMMARY.json [--commit SHA] [--dir bench/ab/history]
  history.py trend [--dir bench/ab/history] [--out bench/ab/history/trend.md]
  history.py last-commit [--dir bench/ab/history]
  history.py alerts [--dir bench/ab/history] [--sets a,b] [--json OUT] [--markdown OUT]

A line holds the run's metadata and one row per shape x range in the column
order named by "cols" (so a line stays small and self-describing):

  {"date": "2026-09-24", "commit": "...", "baseline": "v1.93.0", "candidate": "main",
   "runs": 7, "valid": true, "cols": ["shape", "range", ...], "rows": [[...], ...]}

Columns: shape, range, status (candidate HTTP status), p50 and cold (candidate
warm p50 and first-run seconds), loki_p50, loki_cold, loki_status, parity
(candidate result vs Loki), base_p50 and verdict (vs the baseline build) and
loki_cold_computed (true: Loki computed its cold run; false: its results cache
answered it; null: not known, as on every line written before the column).

alerts compares each set's newest line with the previous valid one and reports
transitions, never the state many rows are already in: a regression (result
same -> differs, 200 -> error, or a slowdown that held for two runs or that the
same run's verdict against the release also shows), an improvement, and the
ratchet. history/ratchet.json lists the rows that were as good as Loki (same
result, 200, cold no slower than Loki's computed cold); the generator only adds
to it, an owner removes a row by hand, and a row that stops being as good is a
ratchet loss. Cold, never warm p50: Loki's warm is its results cache.
"""
import argparse
import datetime
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
HISTORY = os.path.join(HERE, "history")
COLS = ["shape", "range", "status", "p50", "cold", "loki_p50", "loki_cold", "loki_status", "parity", "base_p50",
        "verdict", "loki_cold_computed"]
SPARK = "▁▂▃▄▅▆▇█"
SHAPES = os.path.join(HERE, "shapes.json")
# What one full daily measurement is: this many runs per shape x range and
# every range of the set up to this one (daily.py defaults to both).
FULL_RUNS, FULL_MAX_RANGE = 7, "24h"
# A day-over-day or week-over-week move is reported only beyond both.
NOISE, MIN_DELTA = 0.25, 0.05
RATCHET = "ratchet.json"


def cold_computed(row, ref):
    """Whether Loki computed the reference's cold run: False when its results cache answered it
    (its own counters, or identical answers for moved windows), None without counters."""
    if ref in row.get("cold_cache_served", []) or ref in row.get("stale", []):
        return False
    return True if (row.get("cache", {}).get(ref) or {}).get("cold") else None


def entry(summary, commit=""):
    c, r, b = summary["candidate"], summary["reference"], summary["baseline"]
    rows = []
    for row in summary["rows"]:
        rows.append([row["shape"], row["range"], row["status"].get(c), row["p50"].get(c), row["cold"].get(c),
                     row["p50"].get(r), row["cold"].get(r), row["status"].get(r),
                     row.get("parity_vs_reference", "n/a"), row["p50"].get(b), row["verdict"], cold_computed(row, r)])
    return {"date": summary["date"], "commit": commit, "set": summary["set"], "label": summary.get("label", ""),
            "baseline": b, "candidate": c, "reference": r, "runs": summary.get("runs"),
            "valid": summary.get("valid", True), "restarts": [summary.get("restart_before"), summary.get("restart_after")],
            "cols": COLS, "rows": rows}


def append(summary, directory=HISTORY, commit=""):
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{summary['set']}.jsonl")
    line = entry(summary, commit)
    existing = read(path)
    if any(e["date"] == line["date"] and e.get("label") == line["label"] for e in existing):
        # One line per set per day: a re-run replaces nothing, it is skipped.
        return path, False
    with open(path, "a") as f:
        f.write(json.dumps(line, separators=(",", ":"), ensure_ascii=False) + "\n")
    return path, True


def read(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def last_commit(directory=HISTORY, spec=None):
    """The commit every current set's newest full measurement was taken at, or "" when they disagree.

    Only sets in shapes.json count (a renamed or removed set's file is stale),
    and only full measurements: FULL_RUNS runs and every range of the set up
    to FULL_MAX_RANGE, so a cut-down manual run does not stand for the day.
    A current set without one, or lines without a recorded commit, give "".
    """
    if spec is None:
        with open(SHAPES) as f:
            spec = json.load(f)
    defs = spec["ranges"]
    commits = set()
    for name, shape_set in spec["sets"].items():
        want = {r for r in shape_set["ranges"] if defs[r][0] <= defs[FULL_MAX_RANGE][0]}
        if not want:
            continue  # the daily run skips a set with no range that short
        full = [e for e in read(os.path.join(directory, f"{name}.jsonl"))
                if e.get("runs") == FULL_RUNS and want <= {r[e["cols"].index("range")] for r in e["rows"]}]
        if not full:
            return ""
        commits.add(full[-1].get("commit") or "")
    return commits.pop() if len(commits) == 1 else ""


def rows_of(e):
    cols = e["cols"]
    return {(r[cols.index("shape")], r[cols.index("range")]): dict(zip(cols, r)) for r in e["rows"]}


def spark(values):
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    span = hi - lo or 1
    return "".join(" " if v is None else SPARK[min(len(SPARK) - 1, int((v - lo) / span * (len(SPARK) - 1)))]
                   for v in values)


def moved(old, new):
    if old is None or new is None:
        return ""
    pct = (new - old) / old * 100 if old else 0
    if new > old * (1 + NOISE) and new - old > MIN_DELTA:
        icon = "🔴"
    elif new < old / (1 + NOISE) and old - new > MIN_DELTA:
        icon = "🟢"
    else:
        icon = "⚪"
    return f"{icon} {pct:+.0f}%"


def secs(v, status=None):
    if status not in (None, "200"):
        return f"**{status}**"
    if v is None:
        return "—"
    return f"{v * 1000:.0f} ms" if v < 1 else f"{v:.2f} s"


def previous(entries, days):
    """The newest valid entry at least `days` before the latest one."""
    latest = datetime.date.fromisoformat(entries[-1]["date"])
    for e in reversed(entries[:-1]):
        if e.get("valid", True) and (latest - datetime.date.fromisoformat(e["date"])).days >= days:
            return e
    return None


def trend(directory=HISTORY, window=14):
    lines = ["# Performance trend", "",
             "Generated by `bench/ab/history.py trend` from the daily A/B run on `main` "
             "(`bench/ab/daily.py`); do not edit. Each row is one query shape over one range on the same "
             "fresh stack and seeded data every day: the proxy built from `main` against Loki direct, warm p50 "
             f"(the first run is cold). Day and week columns compare with the previous run and the run seven "
             f"or more days earlier; a move counts only beyond {NOISE * 100:.0f}% and {MIN_DELTA * 1000:.0f} ms. "
             f"The sparkline is the last {window} runs of the proxy's warm p50.", ""]
    files = sorted(glob.glob(os.path.join(directory, "*.jsonl")))
    if not files:
        lines.append("No history yet.")
    for path in files:
        entries = [e for e in read(path)]
        if not entries:
            continue
        last = entries[-1]
        day, week = previous(entries, 1), previous(entries, 7)
        cur = rows_of(last)
        d_rows = rows_of(day) if day else {}
        w_rows = rows_of(week) if week else {}
        recent = entries[-window:]
        lines += [f"## {last['set']}", "",
                  f"Latest {last['date']} (`{(last.get('commit') or '?')[:10]}`, {last.get('runs')} runs"
                  f"{'' if last.get('valid', True) else ', **invalid: VictoriaLogs restarted**'}); "
                  f"previous {day['date'] if day else '—'}; week {week['date'] if week else '—'}.", "",
                  "| shape | range | proxy p50 | Δ day | Δ week | Loki p50 | proxy ÷ Loki | cold proxy / Loki | result vs Loki | trend |",
                  "|---|---|--:|--:|--:|--:|--:|--:|---|---|"]
        for key, row in cur.items():
            ratio = "—"
            if row["status"] == "200" and row["loki_status"] == "200" and row["p50"] and row["loki_p50"]:
                ratio = f"{row['p50'] / row['loki_p50']:.2f}×"
            d = d_rows.get(key, {}).get("p50") if d_rows.get(key, {}).get("status") == "200" else None
            w = w_rows.get(key, {}).get("p50") if w_rows.get(key, {}).get("status") == "200" else None
            series = [rows_of(e).get(key, {}).get("p50") for e in recent]
            parity = row["parity"] if row["parity"] != "n/a" else "—"
            lines.append(
                f"| {row['shape'].replace('|', chr(92) + '|')} | {row['range']} | {secs(row['p50'], row['status'])} | "
                f"{moved(d, row['p50']) if row['status'] == '200' else ''} | "
                f"{moved(w, row['p50']) if row['status'] == '200' else ''} | "
                f"{secs(row['loki_p50'], row['loki_status'])} | {ratio} | "
                f"{secs(row['cold'], row['status'])} / {secs(row['loki_cold'], row['loki_status'])} | {parity} | "
                f"{spark(series)} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def slower(old, new):
    return old is not None and new is not None and new > old * (1 + NOISE) and new - old > MIN_DELTA


def faster(old, new):
    return old is not None and new is not None and new < old / (1 + NOISE) and old - new > MIN_DELTA


def short(sha):
    return (sha or "?")[:10]


def same(parity):
    # "same" and the tolerance pass "same series, sum ±0.24%"; "differs" and "n/a" are not.
    return isinstance(parity, str) and parity.startswith("same")


def fails_like_loki(row):
    """An error Loki gives too, with the same result: a fix towards Loki, not a new error."""
    return row.get("status") == row.get("loki_status") and same(row.get("parity"))


def read_ratchet(directory=HISTORY):
    path = os.path.join(directory, RATCHET)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def write_ratchet(rows, directory=HISTORY):
    rows = sorted(rows, key=lambda r: (r["set"], r["shape"], r["range"]))
    with open(os.path.join(directory, RATCHET), "w") as f:
        f.write(json.dumps(rows, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def cold_slower(row):
    """The proxy's cold run is slower than a Loki cold run Loki computed, beyond the noise rule."""
    return (row["status"] == "200" and row.get("loki_status") == "200" and row.get("loki_cold_computed") is True
            and slower(row["loki_cold"], row["cold"]))


def ratchet_loss(row, old):
    """Why a ratcheted row is no longer as good as Loki, or "". A different result or an error counts at
    once; a cold run slower than Loki's only when the previous valid line was too (one cold run is one sample)."""
    if row["parity"] == "differs":
        return "result differs from Loki"
    if row["status"] not in (None, "200") and not fails_like_loki(row):
        return f"status {row['status']}"
    if cold_slower(row) and old and cold_slower(old):
        return (f"cold {secs(old['cold'])} → {secs(row['cold'])} against Loki's computed "
                f"{secs(old['loki_cold'])} → {secs(row['loki_cold'])}, held for two runs")
    return ""


def ratchets(row):
    """A row enters the ratchet when its result matches Loki's, it answers 200 and its cold run is
    no slower than the cold run Loki computed (never the warm p50: that is Loki's results cache)."""
    return (same(row["parity"]) and row["status"] == "200" and row.get("loki_status") == "200"
            and row.get("loki_cold_computed") is True and row["cold"] is not None and row["loki_cold"] is not None
            and row["cold"] <= row["loki_cold"])


def alerts(directory=HISTORY, sets=None):
    """Transitions between each set's newest line and the previous valid one, plus the ratchet.

    sets limits it to the sets a run just wrote. Pure: ratchet.json is read, never written.
    """
    out = {"regressions": [], "improvements": [], "ratchet_lost": [], "ratchet_added": [], "range": {}}
    held = {(r["set"], r["shape"], r["range"]): r for r in read_ratchet(directory)}
    added = []
    for path in sorted(glob.glob(os.path.join(directory, "*.jsonl"))):
        entries = read(path)
        if not entries or not entries[-1].get("valid", True) or (sets is not None and entries[-1]["set"] not in sets):
            continue
        new, name = entries[-1], entries[-1]["set"]
        valid = [e for e in entries[:-1] if e.get("valid", True)]
        prev, prev2 = (valid[-1] if valid else None), (valid[-2] if len(valid) > 1 else None)
        cur = rows_of(new)
        old_rows = rows_of(prev) if prev else {}
        old2_rows = rows_of(prev2) if prev2 else {}
        if prev:
            out["range"][name] = f"{short(prev.get('commit'))}..{short(new.get('commit'))}"

        def alert(kind, key, detail, first=prev):
            return {"set": name, "shape": key[0], "range": key[1], "kind": kind, "detail": detail,
                    "commits": f"{short(first.get('commit'))}..{short(new.get('commit'))}"}

        for key, row in cur.items():
            old = old_rows.get(key)
            if old:
                if same(old["parity"]) and row["parity"] == "differs":
                    out["regressions"].append(alert("result", key, "result vs Loki: same → differs"))
                elif old["parity"] == "differs" and same(row["parity"]):
                    out["improvements"].append(alert("result", key, "result vs Loki: differs → same"))
                if old["status"] == "200" and row["status"] not in (None, "200") and not fails_like_loki(row):
                    out["regressions"].append(alert("status", key, f"status 200 → {row['status']}"))
                elif old["status"] not in (None, "200") and row["status"] == "200":
                    out["improvements"].append(alert("status", key, f"status {old['status']} → 200"))
                if old["status"] == "200" and row["status"] == "200":
                    span = f"warm p50 {secs(old['p50'])} → {secs(row['p50'])}"
                    old2 = old2_rows.get(key) or {}
                    if faster(old["p50"], row["p50"]):
                        out["improvements"].append(alert("faster", key, span))
                    elif slower(old["p50"], row["p50"]) and row["verdict"] == "slower":
                        out["regressions"].append(alert("slower", key, f"{span}, and slower than the release in the same run"))
                    # Worse than the run before both of them, in both: one run of noise is not a regression.
                    # The change happened after prev2, so that is where the range starts (and it was
                    # already reported when the first slow run itself was slower than the release).
                    elif (old2.get("status") == "200" and slower(old2["p50"], old["p50"]) and slower(old2["p50"], row["p50"])
                          and old["verdict"] != "slower"):
                        out["regressions"].append(alert(
                            "slower", key, f"warm p50 {secs(old2['p50'])} → {secs(old['p50'])} → {secs(row['p50'])}, "
                            "held for two runs", prev2))
            where = (name, key[0], key[1])
            if where in held:
                reason = ratchet_loss(row, old)
                if reason:
                    # A cold loss that took two runs to show began after prev2.
                    out["ratchet_lost"].append(alert("ratchet", key, reason, (prev2 if "held" in reason else prev) or {}))
            elif ratchets(row):
                added.append({"set": name, "shape": key[0], "range": key[1], "added": new["date"],
                              "commit": new.get("commit") or ""})
    for k in ("regressions", "improvements", "ratchet_lost"):
        out[k].sort(key=lambda a: (a["set"], a["shape"], a["range"], a["kind"]))
    out["ratchet_added"] = added
    out["ratchet"] = sorted(list(held.values()) + added, key=lambda r: (r["set"], r["shape"], r["range"]))
    return out


def render_alerts(a):
    def items(rows, none="none"):
        return [f"- `{r['set']}` {r['shape']} · {r['range']}: {r['detail']} (`{r['commits']}`)" for r in rows] or [none]

    return "\n".join(
        ["## Regressions", "", *items(a["regressions"]), "",
         "## Improvements", "", *items(a["improvements"]), "",
         "## Ratchet", "", f"Lost ({len(a['ratchet_lost'])}):", "", *items(a["ratchet_lost"]), "",
         f"Held: {len(a['ratchet'])} rows ({len(a['ratchet_added'])} added by this run); "
         f"the list is `bench/ab/history/{RATCHET}`.", ""])


def write_alerts(directory=HISTORY, sets=None, json_out="", markdown_out=""):
    """alerts(), with ratchet.json updated (additions only) and the outputs written."""
    a = alerts(directory, sets)
    os.makedirs(directory, exist_ok=True)
    write_ratchet(a["ratchet"], directory)
    if json_out:
        with open(json_out, "w") as f:
            json.dump(a, f, indent=1, sort_keys=True, ensure_ascii=False)
            f.write("\n")
    if markdown_out:
        with open(markdown_out, "w") as f:
            f.write(render_alerts(a))
    return a


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("append")
    a.add_argument("summaries", nargs="+")
    a.add_argument("--commit", default="")
    a.add_argument("--dir", default=HISTORY)
    t = sub.add_parser("trend")
    t.add_argument("--dir", default=HISTORY)
    t.add_argument("--out", default=os.path.join(HISTORY, "trend.md"))
    lc = sub.add_parser("last-commit")
    lc.add_argument("--dir", default=HISTORY)
    al = sub.add_parser("alerts")
    al.add_argument("--dir", default=HISTORY)
    al.add_argument("--sets", default="", help="comma-separated sets (default: all)")
    al.add_argument("--json", default="")
    al.add_argument("--markdown", default="")
    args = ap.parse_args()
    if args.cmd == "append":
        for path in args.summaries:
            with open(path) as f:
                target, added = append(json.load(f), args.dir, args.commit)
            print(f"{'appended to' if added else 'already recorded in'} {target}")
    elif args.cmd == "last-commit":
        print(last_commit(args.dir))
    elif args.cmd == "alerts":
        a = write_alerts(args.dir, [x for x in args.sets.split(",") if x] or None, args.json, args.markdown)
        if not args.markdown:
            print(render_alerts(a), end="")
    else:
        text = trend(args.dir)
        with open(args.out, "w") as f:
            f.write(text)
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
