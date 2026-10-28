#!/usr/bin/env python3
"""Append-only A/B history and the trend report built from it.

The daily run from main (daily.py) appends one line per shape set per day to
bench/ab/history/<set>.jsonl and regenerates bench/ab/history/trend.md. Docs
and plots read the JSONL; nothing edits past lines.

  history.py append SUMMARY.json [--commit SHA] [--dir bench/ab/history]
  history.py trend [--dir bench/ab/history] [--out bench/ab/history/trend.md]
  history.py last-commit [--dir bench/ab/history]
  history.py alerts [--dir bench/ab/history] [--sets a,b] [--json OUT] [--markdown OUT]
  history.py acceptance [--dir bench/ab/history]     (regenerates history/acceptance.json; daily.py does too)

A line holds the run's metadata and one row per shape x range in the column
order named by "cols" (so a line stays small and self-describing):

  {"date": "2026-09-24", "commit": "...", "baseline": "v1.93.0", "candidate": "main",
   "runs": 7, "valid": true, "cols": ["shape", "range", ...], "rows": [[...], ...]}

Columns: shape, range, status (candidate HTTP status), p50 and cold (candidate
warm p50 and first-run seconds), loki_p50, loki_cold, loki_status, parity
(candidate result vs Loki), base_p50 and verdict (vs the baseline build) and
loki_cold_computed (true: Loki computed its cold run; false: its results cache
answered it; null: not known, as on every line written before the column).

alerts compares each set's newest full line with the earlier full ones (a
cut-down manual run is neither compared nor a baseline) and reports
transitions, never the state many rows are already in: a regression (result
same -> differs while Loki answers 200, 200 -> error, or a slowdown that held
for two runs or that the same run's verdict against the release also shows),
an improvement, and the ratchet. history/ratchet.json lists the rows that were
as good as Loki (same result, 200, cold no slower than Loki's computed cold);
the generator only adds to it, an owner removes a row by hand, and a row that
stops being as good is a ratchet loss. Cold, never warm p50: Loki's warm is its results cache.
alerts also keeps history/over-2x-loki.json, the rows above 2× Loki (over2x.py has
the definition): a listed row is removed once two consecutive full lines measure
it comparable and within 2×, its ratio is tightened when a line measures it
lower, and a row above 2× that is not listed is reported (never added).

alerts also applies the acceptance tiers (over2x.py: the registry's perf: blocks). A locked row above its
bound is a regression: warm after one run (a p50 of seven), cold when the previous full line was above it
too (one cold run is one sample; the first time it is listed as pending). A row that was below Loki on the
previous full line and is above Loki now is flagged as lost below-Loki; rows between 1× and 2× are listed
with and without an effort exception (the latter flagged for review); unlocked items whose rows were within
their bound on this line and the previous one are listed as ready to lock (people lock them: the daily
pull request never edits the registry). The distribution of the rows compared (below Loki, close, 1–2×,
above 2×; cold and warm) is ratcheted against the previous full lines and the lines a week earlier, on the
rows comparable on both: the share below Loki may not fall and the count above 2× may not rise beyond
over2x.SHARE_NOISE_* and OVER_NOISE_*, or it is a regression; a row moves to a worse band there (and is
flagged as lost below-Loki) only when the proxy's own time got slower beyond noise, so Loki's cache
timings and rows crossing a band edge by a few milliseconds do not count. history/acceptance.json records the
distribution of every day of the history (regenerated from the JSONL lines each run).
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
ACCEPTANCE = "acceptance.json"
sys.path.insert(0, HERE)
import over2x  # noqa: E402


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


def load_spec():
    with open(SHAPES) as f:
        return json.load(f)


def full_ranges(spec, name):
    """The ranges one full measurement of a set covers (None for a set shapes.json does not define)."""
    shape_set = spec["sets"].get(name)
    if shape_set is None:
        return None
    defs = spec["ranges"]
    return {r for r in shape_set["ranges"] if defs[r][0] <= defs[FULL_MAX_RANGE][0]}


def is_full(e, want):
    """A full measurement: FULL_RUNS runs and every range of the set up to FULL_MAX_RANGE."""
    return bool(want) and e.get("runs") == FULL_RUNS and want <= {r[e["cols"].index("range")] for r in e["rows"]}


def append(summary, directory=HISTORY, commit="", spec=None):
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{summary['set']}.jsonl")
    line = entry(summary, commit)
    existing = read(path)
    same_day = [i for i, e in enumerate(existing) if e["date"] == line["date"] and e.get("label") == line["label"]]
    if same_day:
        # One line per set per day: a re-run replaces nothing, it is skipped. Except that the full measurement
        # replaces a cut-down manual run of the same day, which would otherwise keep the scheduled one out.
        want = full_ranges(spec or load_spec(), summary["set"])
        if not (is_full(line, want) and not is_full(existing[same_day[0]], want)):
            return path, False
        existing[same_day[0]] = line
        with open(path, "w") as f:
            for e in existing:
                f.write(json.dumps(e, separators=(",", ":"), ensure_ascii=False) + "\n")
        return path, True
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
    spec = spec or load_spec()
    commits = set()
    for name in spec["sets"]:
        want = full_ranges(spec, name)
        if not want:
            continue  # the daily run skips a set with no range that short
        full = [e for e in read(os.path.join(directory, f"{name}.jsonl")) if is_full(e, want)]
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
    """(entries, problem): a file that cannot be read gives no entries and a problem; an entry without
    its set, shape and range is skipped with a warning."""
    path = os.path.join(directory, RATCHET)
    if not os.path.exists(path):
        return [], ""
    try:
        with open(path) as f:
            rows = json.load(f)
        if not isinstance(rows, list):
            raise ValueError("not a list")
    except ValueError as e:  # includes json.JSONDecodeError
        return [], f"{RATCHET} cannot be read ({e}); the ratchet is left alone this run"
    good = [r for r in rows if isinstance(r, dict) and all(isinstance(r.get(k), str) for k in ("set", "shape", "range"))]
    if len(good) != len(rows):
        print(f"warning: {len(rows) - len(good)} entries of {RATCHET} lack set, shape or range; skipped "
              "(and dropped when the file is next written)", file=sys.stderr)
    return good, ""


def write_ratchet(rows, directory=HISTORY):
    rows = sorted(rows, key=lambda r: (r["set"], r["shape"], r["range"]))
    with open(os.path.join(directory, RATCHET), "w") as f:
        f.write(json.dumps(rows, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def cold_slower(row):
    """The proxy's cold run is slower than a Loki cold run Loki computed, beyond the noise rule."""
    return (row["status"] == "200" and row.get("loki_status") == "200" and row.get("loki_cold_computed") is True
            and slower(row["loki_cold"], row["cold"]))


def instant_loss(row):
    """Why a ratcheted row is no longer as good as Loki on this line alone, or "". The results are compared
    only when Loki answered 200: a Loki failure says nothing about the proxy."""
    if row["parity"] == "differs" and row.get("loki_status") == "200":
        return "result differs from Loki"
    if row["status"] != "200" and not fails_like_loki(row):
        return f"status {row['status'] or 'none'}"
    return ""


def ratchet_loss(row, old):
    """Why a ratcheted row is lost, or "". A different result or an error counts at once; a cold run slower
    than Loki's only when the previous line was too (one cold run is one sample)."""
    if instant_loss(row):
        return instant_loss(row)
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


def line_bands(name, e, times=False):
    """(set, shape, range, mode) -> over2x.band of every pair of a line both sides answered comparably
    (with times: (band, the proxy's seconds))."""
    out = {}
    for key, row in rows_of(e).items():
        for mode, pair in over2x.history_pairs(row).items():
            if over2x.judge(*pair) in ("above", "within"):
                b = over2x.band(*pair[:2])
                out[(name, *key, mode)] = (b, pair[0]) if times else b
    return out


def settled(old, new):
    """The band of each row of new for the ratchet: a move to a worse band counts only when the proxy's own time
    got slower beyond noise (25% and 50 ms) between the two lines. A row whose proxy time held while Loki's moved
    (Loki's warm is its results cache), or that crossed a band edge by a few milliseconds, keeps its old band."""
    out = {}
    for k, (b, proxy) in new.items():
        was, before = old[k]
        worse = over2x.BANDS.index(b) > over2x.BANDS.index(was)
        out[k] = b if not worse or slower(before, proxy) else was
    return out


def week_before(new, older):
    """The newest earlier full line at least seven days older than new (older is newest first), or None."""
    day = datetime.date.fromisoformat(new["date"])
    return next((e for e, _ in older if (day - datetime.date.fromisoformat(e["date"])).days >= 7), None)


def alerts(directory=HISTORY, sets=None, spec=None, bounds=None):
    """Transitions between each set's newest line and the earlier full measurements, plus the ratchet.

    Only full measurements count (is_full), on either side: a cut-down manual run is neither compared nor a
    baseline, and each row is compared with the newest earlier full line that has it. Only sets in shapes.json
    are looked at; sets limits it to the sets a run just wrote. Pure: ratchet.json is read, never written.
    bounds: the registry's perf: blocks (over2x.read_bounds, the default).
    """
    spec = spec or load_spec()
    out = {"regressions": [], "improvements": [], "ratchet_lost": [], "ratchet_added": [], "loki_failed": [],
           "ratchet_unmeasured": 0, "ratchet_problem": "", "range": {},
           "over_2x_new": [], "over_2x_removed": [], "over_2x_pending": [],
           "locked_pending": [], "lost_below": [], "between": [], "lockable": [], "distribution": {},
           "distribution_ratchet": [], "acceptance_problem": ""}
    if bounds is None:
        try:
            bounds = over2x.read_bounds(spec=spec)
        except (OSError, ValueError, KeyError) as e:
            bounds, out["acceptance_problem"] = None, f"the registry's perf: blocks cannot be read ({e!r}); no row is locked this run"
    # Active once the registry carries perf: blocks for the shapes measured (as the above-2× list once seeded).
    out["acceptance_active"] = bool(bounds and any(i in bounds["blocks"] for ids in bounds["covers"].values()
                                                   for i in ids))
    cur_bands, prev_bands, week_bands, items, commit = {}, {}, {}, {}, ""
    entries_held, out["ratchet_problem"] = read_ratchet(directory)
    held = {(r["set"], r["shape"], r["range"]): r for r in entries_held}
    # Without the list (it is seeded by over2x.py seed) there is nothing to keep and nothing to compare with.
    baseline_path = os.path.join(directory, over2x.BASELINE_NAME)
    out["over_2x_active"] = os.path.exists(baseline_path)
    listed, out["over_2x_problem"] = over2x.read_baseline(baseline_path)
    listed = {k: dict(v) for k, v in listed.items()}
    added, measured, looked = [], set(), set()
    for path in sorted(glob.glob(os.path.join(directory, "*.jsonl"))):
        entries = read(path)
        name = entries[-1]["set"] if entries else ""
        want = full_ranges(spec, name)
        if want is None or (sets is not None and name not in sets):
            continue
        new = entries[-1]
        if not new.get("valid", True) or not is_full(new, want):
            continue
        looked.add(name)
        # Earlier full lines, newest first, each with its rows.
        older = [(e, rows_of(e)) for e in reversed(entries[:-1]) if e.get("valid", True) and is_full(e, want)]
        cur = rows_of(new)
        commit = commit or short(new.get("commit"))
        if older:
            out["range"][name] = f"{short(older[0][0].get('commit'))}..{short(new.get('commit'))}"
        cur_bands.update(line_bands(name, new, True))
        if older:
            prev_bands.update(line_bands(name, older[0][0], True))
        week = week_before(new, older)
        if week:
            week_bands.update(line_bands(name, week, True))

        def alert(kind, key, detail, first):
            return {"set": name, "shape": key[0], "range": key[1], "kind": kind, "detail": detail,
                    "commits": f"{short(first.get('commit'))}..{short(new.get('commit'))}"}

        for key, row in cur.items():
            measured.add((name, *key))
            hist = [(e, rs[key]) for e, rs in older if key in rs]
            (prev, old), (prev2, old2) = (hist[0] if hist else ({}, None)), (hist[1] if len(hist) > 1 else ({}, {}))
            if row.get("loki_status") not in (None, "200") and not fails_like_loki(row):
                out["loki_failed"].append({"set": name, "shape": key[0], "range": key[1], "kind": "loki",
                                           "detail": f"Loki status {row['loki_status']}, proxy status "
                                                     f"{row['status'] or 'none'}; the result is not compared"})
            if old:
                # A result is compared only when Loki answered 200 on both lines.
                if old.get("loki_status") == "200" and row.get("loki_status") == "200":
                    if same(old["parity"]) and row["parity"] == "differs":
                        out["regressions"].append(alert("result", key, "result vs Loki: same → differs", prev))
                    elif old["parity"] == "differs" and same(row["parity"]):
                        out["improvements"].append(alert("result", key, "result vs Loki: differs → same", prev))
                if old["status"] == "200" and row["status"] != "200" and not fails_like_loki(row):
                    out["regressions"].append(alert("status", key, f"status 200 → {row['status'] or 'none'}", prev))
                elif (old["status"] not in (None, "200") and row["status"] == "200"
                      and row.get("loki_status") != old["status"]):
                    # Not while Loki still gives that error: the proxy now answers where Loki does not.
                    out["improvements"].append(alert("status", key, f"status {old['status']} → 200", prev))
                if old["status"] == "200" and row["status"] == "200":
                    span = f"warm p50 {secs(old['p50'])} → {secs(row['p50'])}"
                    if faster(old["p50"], row["p50"]):
                        out["improvements"].append(alert("faster", key, span, prev))
                    elif slower(old["p50"], row["p50"]) and row["verdict"] == "slower" and old["verdict"] != "slower":
                        out["regressions"].append(alert(
                            "slower", key, f"{span}, and slower than the release in the same run", prev))
                    # Worse than the run before both of them, in both: one run of noise is not a regression.
                    # The change happened after prev2, so that is where the range starts (and it was
                    # already reported when the first slow run itself was slower than the release).
                    elif (old2.get("status") == "200" and slower(old2["p50"], old["p50"]) and slower(old2["p50"], row["p50"])
                          and old["verdict"] != "slower"):
                        out["regressions"].append(alert(
                            "slower", key, f"warm p50 {secs(old2['p50'])} → {secs(old['p50'])} → {secs(row['p50'])}, "
                            "held for two runs", prev2))
            if out["over_2x_active"] and not out["over_2x_problem"]:
                over_2x(out, listed, name, key, row, old, new)
            acceptance(out, bounds, items, name, key, row, old, (prev, prev2), alert)
            where = (name, *key)
            if where in held and not out["ratchet_problem"]:
                reason = ratchet_loss(row, old)
                if reason:
                    # The range starts at the newest line the row was still as good as Loki on (or when the
                    # ratchet took it in), however many runs it has been lost.
                    start = next((e for e, r in hist if not instant_loss(r) and not cold_slower(r)),
                                 {"commit": held[where].get("commit")})
                    out["ratchet_lost"].append(alert("ratchet", key, reason, start))
            elif where not in held and ratchets(row) and not out["ratchet_problem"]:
                added.append({"set": name, "shape": key[0], "range": key[1], "added": new["date"],
                              "commit": new.get("commit") or ""})
    for k in ("regressions", "improvements", "ratchet_lost", "loki_failed"):
        out[k].sort(key=lambda a: (a["set"], a["shape"], a["range"], a["kind"]))
    for k in ("over_2x_new", "over_2x_removed", "over_2x_pending"):
        out[k].sort(key=lambda a: (a["set"], a["shape"], a["range"], over2x.MODES.index(a["mode"])))
    # Ratchet rows whose set was measured (or no longer exists) but whose shape/range this run did not.
    if out["acceptance_active"]:
        ready(out, bounds, items, looked)
    out["distribution"] = over2x.distribution((k[3], v[0]) for k, v in cur_bands.items())
    for against, other in ((("the previous full run", prev_bands), ("the full run a week earlier", week_bands))
                           if out["acceptance_active"] else ()):
        common = cur_bands.keys() & other.keys()
        if not common:
            continue
        old = over2x.distribution((k[3], other[k][0]) for k in common)
        moved = settled(other, {k: cur_bands[k] for k in common})
        now = over2x.distribution((k[3], moved[k]) for k in common)
        out["distribution_ratchet"].append({"against": against, "old": old, "new": now})
        for mode, kind, detail in over2x.ratchet_moves(old, now):
            what = "share below Loki" if kind == "share_below" else "rows above 2× Loki"
            out["regressions"].append({"set": "all sets", "shape": f"{mode}: {what}", "range": "—",
                                       "kind": "distribution", "detail": f"{detail}, against {against}",
                                       "commits": commit})
    out["regressions"].sort(key=lambda a: (a["set"], a["shape"], a["range"], a["kind"]))
    for k in ("locked_pending", "lost_below", "between"):
        out[k].sort(key=lambda a: (a["set"], a["shape"], a["range"], over2x.MODES.index(a["mode"])))
    out["ratchet_unmeasured"] = sum(1 for k in held if k not in measured and (
        k[0] in looked or (sets is None and full_ranges(spec, k[0]) is None)))
    out["ratchet_added"] = added
    out["ratchet"] = sorted(list(held.values()) + added, key=lambda r: (r["set"], r["shape"], r["range"]))
    gone = {over2x.key(r) for r in out["over_2x_removed"]}
    out["over_2x"] = [r for k, r in sorted(listed.items()) if k not in gone]
    return out


def over_2x(out, listed, name, key, row, old, new):
    """One history row against the above-2× Loki list, cold and warm: a listed row within 2× on this line and
    on the previous full line is removed, a lower ratio tightens it, an unlisted row above 2× is reported."""
    for mode, pair in over2x.history_pairs(row).items():
        k, state = (name, key[0], key[1], mode), over2x.judge(*pair)
        measured = over2x.ratio(*pair[:2])
        detail = f"{secs(pair[0])} against Loki's {secs(pair[1])}, {over2x.times(measured)}" + (
            f" (inside the {over2x.MIN_GAP * 1000:.0f} ms floor)" if state == "within" and (measured or 0) > over2x.RATIO
            else "")
        if k not in listed:
            if state == "above":
                out["over_2x_new"].append({"set": name, "shape": key[0], "range": key[1], "mode": mode,
                                           "kind": "over_2x", "detail": detail, "commits": short(new.get("commit"))})
            continue
        if state == "above" and measured is not None and measured < listed[k]["ratio"]:
            listed[k].update(ratio=measured, proxy_s=pair[0], loki_s=pair[1])
        if state != "within":
            continue
        before = over2x.judge(*over2x.history_pairs(old)[mode]) if old else ""
        item = {**listed[k], "detail": detail + (", and within 2× on the previous run too" if before == "within"
                                                  else "; removed if the next run agrees")}
        out["over_2x_removed" if before == "within" else "over_2x_pending"].append(item)


def acceptance(out, bounds, items, name, key, row, old, prevs, alert):
    """One history row against the registry's perf: blocks, cold and warm: a locked row above its bound (a
    regression, or pending for a first cold run), a row that lost its place below Loki, a row between 1× and
    2×, and the per-item counts ready() reads."""
    prev, prev2 = prevs
    for mode, pair in over2x.history_pairs(row).items():
        b = over2x.bound_for(bounds, name, key[0], mode)
        limit = b["max_ratio"] if b["locked"] else over2x.RATIO
        for item in (bounds or {}).get("covers", {}).get((name, key[0]), []):
            block = bounds["blocks"].get(item)
            if block and block["locked"] is not True and mode in block["modes"]:
                st = over2x.judge(*pair, bound=block["max_ratio"])
                before = over2x.judge(*over2x.history_pairs(old)[mode], bound=block["max_ratio"]) if old else ""
                c = items.setdefault(item, {"within": 0, "above": 0})
                c["within"] += st == "within" and before == "within"
                c["above"] += "above" in (st, before)
        state = over2x.judge(*pair, bound=limit)
        if state not in ("above", "within"):
            continue
        measured = over2x.ratio(*pair[:2])
        where = {"set": name, "shape": key[0], "range": key[1], "mode": mode, "ratio": measured,
                 "detail": f"{mode} {secs(pair[0])} against Loki's {secs(pair[1])}, {over2x.times(measured)}"}
        if b["locked"] and state == "above":
            since = ", ".join(f"{i} since {bounds['blocks'][i]['locked_since']}" for i in b["locked_by"])
            before = over2x.judge(*over2x.history_pairs(old)[mode], bound=limit) if old else ""
            text = f"{where['detail']}, above its {over2x.times(limit)} bound (locked: {since})"
            if mode == "warm" or before == "above":
                out["regressions"].append(alert("locked", key, text + ("" if mode == "warm" else ", held for two runs"),
                                                prev2 if mode == "cold" and prev2 else prev or {}))
            else:
                out["locked_pending"].append({**where, "detail": text + "; a regression if the next run agrees"})
        band = over2x.band(*pair[:2])
        if band == "between":
            out["between"].append({**where, "exception": b["exception"], "locked": b["locked"]})
        if old and band in ("between", "above"):
            before = over2x.history_pairs(old)[mode]
            if (over2x.judge(*before) in ("above", "within") and over2x.band(*before[:2]) == "below"
                    and slower(before[0], pair[0])):
                out["lost_below"].append({**where, "detail": f"{where['detail']}; below Loki on the previous run "
                                          f"({secs(before[0])} against {secs(before[1])})"})


def ready(out, bounds, items, looked):
    """Unlocked items every set of whose shapes this run measured, with at least one row within their bound on
    this line and the previous one and none above it on either: ready to lock (by people, `over2x.py locks
    lock`)."""
    sets_of = {}
    for (set_name, _), ids in bounds["covers"].items():
        for i in ids:
            sets_of.setdefault(i, set()).add(set_name)
    for item, c in sorted(items.items()):
        if c["within"] and not c["above"] and sets_of.get(item, set()) <= looked:
            out["lockable"].append(item)


def acceptance_history(directory=HISTORY, spec=None):
    """The distribution of every day of the history: per date, the full valid lines of the current sets."""
    spec = spec or load_spec()
    days = {}
    for name in spec["sets"]:
        want = full_ranges(spec, name)
        if not want:
            continue
        for e in read(os.path.join(directory, f"{name}.jsonl")):
            if e.get("valid", True) and is_full(e, want):
                d = days.setdefault(e["date"], {"sets": 0, "commits": set(), "bands": {}})
                d["sets"] += 1
                d["commits"].add(short(e.get("commit")))
                d["bands"].update(line_bands(name, e))
    out = []
    for date, d in sorted(days.items()):
        dist = over2x.distribution((k[3], v) for k, v in d["bands"].items())
        out.append({"date": date, "sets": d["sets"], "commits": sorted(d["commits"]),
                    **{m: {k: dist[m][k] for k in ("rows", "share_below", "below", "close", "between", "above")}
                       for m in dist}})
    return {"about": "The distribution of proxy ÷ Loki over the rows each daily run compared (both answered 200; "
                     "on cold, Loki's results cache did not answer): below Loki, close to Loki (within "
                     f"{over2x.DIST_NOISE * 100:.0f}% or {over2x.MIN_GAP * 1000:.0f} ms), between 1× and 2× and above "
                     "2×. Generated by bench/ab/history.py from the JSONL lines; see bench/ab/README.md.",
            "targets": {"share_below": "most queries below Loki (above 50%)", "above": "worst case at most 2× (0)"},
            "days": out}


PR_LIMIT = 50


def render_alerts(a, limit=None):
    """The alerts as markdown. limit caps the entries listed in all sections together (regressions first,
    then ratchet losses, improvements, Loki failures); the rest is counted."""
    shown, left = {}, limit
    for k in ("regressions", "ratchet_lost", "improvements", "loki_failed"):
        shown[k] = len(a[k]) if left is None else min(len(a[k]), left)
        left = left if left is None else left - shown[k]

    def items(key):
        rows = a[key][:shown[key]]
        text = [f"- `{r['set']}` {r['shape']} · {r['range']}: {r['detail']}" + (f" (`{r['commits']}`)" if r.get("commits") else "")
                for r in rows]
        if limit is not None:
            text = [t if len(t) <= 300 else t[:299] + "…" for t in text]
        if len(a[key]) > len(rows):
            text.append(f"… and {len(a[key]) - len(rows)} more, see the job summary")
        return text or ["none"]

    def over(key, cap):
        rows = a.get(key, [])
        text = [f"- `{r['set']}` {r['shape']} · {r['range']} · {r['mode']}: {r['detail']}" for r in rows[:cap]]
        if len(rows) > len(text):
            text.append(f"… and {len(rows) - len(text)} more, see the job summary")
        return text or ["none"]

    cap = None if limit is None else 20
    over_2x = ["## ⚠️ Above 2× Loki", "",
               f"**{over2x.count_line(a.get('over_2x', []))}.** Any shape × range slower than 2× Loki (cold, warm or "
               f"refresh) is a defect; the list is `bench/ab/history/{over2x.BASELINE_NAME}` and only shrinks.", "",
               f"New this run, not listed (warning; never added automatically) ({len(a.get('over_2x_new', []))}):", "",
               *over("over_2x_new", cap), "",
               f"Removed: within 2× on two consecutive runs ({len(a.get('over_2x_removed', []))}):", "",
               *over("over_2x_removed", cap), "",
               f"Within 2× once, removed if the next run agrees ({len(a.get('over_2x_pending', []))}):", "",
               *over("over_2x_pending", cap), "",
               *([f"**{a['over_2x_problem']}**", ""] if a.get("over_2x_problem") else [])] if a.get("over_2x_active") else []
    accept = render_acceptance(a, cap)
    held = f"Held: {len(a['ratchet'])} rows ({len(a['ratchet_added'])} added by this run)"
    if a.get("ratchet_unmeasured"):
        held += f"; {a['ratchet_unmeasured']} not measured by this run (renamed or removed shapes or sets?)"
    return "\n".join(
        [*over_2x, "## Regressions", "", *items("regressions"), "",
         "## Improvements", "", *items("improvements"), "",
         "## Ratchet", "", f"Lost ({len(a['ratchet_lost'])}):", "", *items("ratchet_lost"), "",
         f"{held}; the list is `bench/ab/history/{RATCHET}`.",
         *(["", f"**{a['ratchet_problem']}**"] if a.get("ratchet_problem") else []), "",
         *accept, "## Loki failed (informational)", "",
         "Rows where Loki did not answer 200: their results are not compared and they do not fail the run.", "",
         *items("loki_failed"), ""])


def render_acceptance(a, cap):
    """The acceptance section of the alerts: the distribution with the owner's targets, its ratchet and the
    flagged lists."""
    if not a.get("acceptance_active"):
        return []

    def listing(key, rows=None):
        rows = a.get(key, []) if rows is None else rows
        text = [f"- `{r['set']}` {r['shape']} · {r['range']} · {r['mode']}: {r['detail']}"
                + (f" (exception: {', '.join(r['exception'])})" if r.get("exception") else "")
                + (" · locked" if r.get("locked") else "") for r in rows[:cap]]
        if cap is not None and len(rows) > cap:
            text.append(f"… and {len(rows) - cap} more, see the job summary")
        return text or ["none"]

    dist = a["distribution"]
    out = ["## Acceptance versus Loki", "",
           "The owner's rule (2026-10-28): at most 2× Loki in the worst case; close to Loki in general; below Loki "
           "for most queries. Rows both sides answered comparably this run, by "
           f"`bench/ab/over2x.py` (below or above Loki beyond {over2x.DIST_NOISE * 100:.0f}% and "
           f"{over2x.MIN_GAP * 1000:.0f} ms):", "",
           "| mode | rows | below Loki | close | 1–2× | above 2× | most below Loki | worst case ≤ 2× |",
           "|---|--:|--:|--:|--:|--:|---|---|"]
    for mode, d in dist.items():
        out.append(f"| {mode} | {d['rows']} | {d['below']} ({d['share_below']:.1f}%) | {d['close']} | {d['between']} | "
                   f"{d['above']} | {'✅' if d['share_below'] > 50 else '❌'} | {'✅' if not d['above'] else '❌'} |")
    out += ["", "Ratchet (the rows comparable on both runs; a fall of the share below Loki beyond "
            f"{over2x.SHARE_NOISE_PP:g} points and {over2x.SHARE_NOISE_ROWS} rows, or a rise of the rows above 2× beyond "
            f"{over2x.OVER_NOISE_PP:g} points and {over2x.OVER_NOISE_ROWS} rows, is a regression above):", ""]
    for r in a.get("distribution_ratchet", []):
        out.append(f"- against {r['against']}: " + "; ".join(
            f"{m} {r['old'][m]['share_below']:.1f}% → {r['new'][m]['share_below']:.1f}% below, "
            f"{r['old'][m]['above']} → {r['new'][m]['above']} above 2× ({r['new'][m]['rows']} rows)"
            for m in r["new"] if m in r["old"]))
    if not a.get("distribution_ratchet"):
        out.append("- no earlier full run to compare with")
    without = [r for r in a.get("between", []) if not r.get("exception")]
    out += ["", f"Locked rows above their bound once (cold: a regression if the next run agrees) "
            f"({len(a.get('locked_pending', []))}):", "", *listing("locked_pending"), "",
            f"Lost below-Loki: below Loki on the previous run, above it now (flagged, not failing) "
            f"({len(a.get('lost_below', []))}):", "", *listing("lost_below"), "",
            f"1–2× Loki without an effort exception (flagged for review: below Loki is the target) ({len(without)}):", "",
            *listing("between", without), "",
            f"1–2× Loki with an effort exception (accepted up to the bound) "
            f"({len(a.get('between', [])) - len(without)}):", "",
            *listing("between", [r for r in a.get("between", []) if r.get("exception")]), "",
            f"Ready to lock: every row within the bound on this run and the previous one "
            f"({len(a.get('lockable', []))}; lock with `python3 bench/ab/over2x.py locks lock <id>` in a pull request):",
            "", *([f"- `{i}`" for i in a.get("lockable", [])[:cap]] or ["none"]), ""]
    if a.get("acceptance_problem"):
        out += [f"**{a['acceptance_problem']}**", ""]
    return out


def write_alerts(directory=HISTORY, sets=None, json_out="", markdown_out="", body_out="", spec=None):
    """alerts(), with ratchet.json updated (additions only) and the outputs written; body_out gets the
    markdown capped to PR_LIMIT entries for a pull request body."""
    a = alerts(directory, sets, spec)
    os.makedirs(directory, exist_ok=True)
    if not a["ratchet_problem"]:
        write_ratchet(a["ratchet"], directory)
    # The list is seeded by over2x.py seed; alerts only ever shrinks or tightens an existing one.
    if a["over_2x_active"] and not a["over_2x_problem"]:
        over2x.write_baseline(a["over_2x"], os.path.join(directory, over2x.BASELINE_NAME))
    if json_out:
        with open(json_out, "w") as f:
            json.dump(a, f, indent=1, sort_keys=True, ensure_ascii=False)
            f.write("\n")
    for path, limit in ((markdown_out, None), (body_out, PR_LIMIT)):
        if path:
            with open(path, "w") as f:
                f.write(render_alerts(a, limit))
    return a


def write_acceptance(directory=HISTORY, spec=None):
    with open(os.path.join(directory, ACCEPTANCE), "w") as f:
        f.write(json.dumps(acceptance_history(directory, spec), indent=1, sort_keys=True, ensure_ascii=False) + "\n")


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
    al.add_argument("--body", default="", help="markdown capped for a pull request body")
    ac = sub.add_parser("acceptance")
    ac.add_argument("--dir", default=HISTORY)
    args = ap.parse_args()
    if args.cmd == "append":
        for path in args.summaries:
            with open(path) as f:
                target, added = append(json.load(f), args.dir, args.commit)
            print(f"{'appended to' if added else 'already recorded in'} {target}")
    elif args.cmd == "last-commit":
        print(last_commit(args.dir))
    elif args.cmd == "acceptance":
        write_acceptance(args.dir)
        print(f"wrote {os.path.join(args.dir, ACCEPTANCE)}")
    elif args.cmd == "alerts":
        a = write_alerts(args.dir, [x for x in args.sets.split(",") if x] or None, args.json, args.markdown,
                         args.body)
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
