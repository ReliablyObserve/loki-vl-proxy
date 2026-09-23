#!/usr/bin/env python3
"""Reduce perf_matrix.py output to a compact, comparable summary.

  report.py summarize RAW.json --baseline main --candidate branch [--reference loki]
            --label pr611-json-filter-pushdown [--save bench/ab/results/]
      Prints one markdown row per shape x range (baseline / candidate / Loki warm
      p50, speedup, result parity) and a one-line verdict. --save writes the
      summary JSON (small; commit it) as <results>/<date>-<label>.json.

  report.py compare OLD.json NEW.json [--target branch]
      Compares two saved summaries shape by shape (for example the previous
      release's run with this one) and lists only what changed beyond noise.

Timing verdicts use warm p50 (runs after the first); the first run is reported
as cold. A shape is slower or faster only when the p50 moves by more than
--noise (relative, default 25%) and --min-delta seconds (default 0.05).
"""
import argparse
import datetime
import json
import os
import re
import sys

SIG_RE = re.compile(r"series=(\d+) points=(\d+) sum=(\S+)")


def load(path):
    with open(path) as f:
        return json.load(f)


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def parity(a, b, tolerance):
    """Compare two result signatures of the same window."""
    if a == b:
        return "same"
    ma, mb = SIG_RE.match(a or ""), SIG_RE.match(b or "")
    if ma and mb and ma.group(1) == mb.group(1):
        sa, sb = float(ma.group(3)), float(mb.group(3))
        rel = abs(sa - sb) / max(abs(sa), 1e-9)
        if rel <= tolerance:
            return f"same series, sum ±{rel * 100:.2f}%"
    return "differs"


def timing_verdict(base, cand, noise, min_delta):
    if base is None or cand is None:
        return "n/a"
    if cand > base * (1 + noise) and cand - base > min_delta:
        return "slower"
    if cand < base / (1 + noise) and base - cand > min_delta:
        return "faster"
    return "same"


def summarize(args):
    raw = load(args.raw)
    rows = raw["rows"]
    keys = []
    for r in rows:
        k = (r["shape"], r["range"])
        if k not in keys:
            keys.append(k)
    out = []
    for shape, rng in keys:
        entry = {"shape": shape, "range": rng, "p50": {}, "p95": {}, "cold": {}, "status": {}, "vl_cpu_max": {}}
        by_target = {}
        for r in rows:
            if (r["shape"], r["range"]) == (shape, rng):
                by_target.setdefault(r["target"], []).append(r)
        for t, rs in by_target.items():
            rs.sort(key=lambda r: r["run"])
            warm = [r["seconds"] for r in rs[1:]] or [rs[0]["seconds"]]
            entry["cold"][t] = rs[0]["seconds"]
            entry["p50"][t] = round(pct(warm, 0.5), 3)
            entry["p95"][t] = round(pct(warm, 0.95), 3)
            if args.cold:
                entry["p50"][t] = rs[0]["seconds"]
            entry["status"][t] = "/".join(sorted({str(r["status"]) for r in rs}))
            entry["vl_cpu_max"][t] = max((r["vl_cpu_max"] or 0) for r in rs)
            # Each run moves the window by a minute while logs keep arriving, so a
            # correct answer changes between runs. Identical answers for different
            # windows mean a cache or a stale view answered: not a latency reference.
            ok = [r for r in rs if r["status"] == 200 and r.get("end") is not None]
            if len({r["end"] for r in ok}) > 1 and len({r["signature"] for r in ok}) == 1:
                entry.setdefault("stale", []).append(t)

        def sig_by_run(t):
            return {r["run"]: r["signature"] for r in by_target.get(t, [])}

        cand, base, ref = sig_by_run(args.candidate), sig_by_run(args.baseline), sig_by_run(args.reference)
        vs_ref = [parity(ref[run], cand[run], args.tolerance) for run in cand if run in ref]
        entry["parity_vs_reference"] = "differs" if "differs" in vs_ref else (max(vs_ref, key=len) if vs_ref else "n/a")
        base_vs_ref = [parity(ref[run], base[run], args.tolerance) for run in base if run in ref]
        entry["baseline_parity_vs_reference"] = "differs" if "differs" in base_vs_ref else ("same" if base_vs_ref else "n/a")
        vs_base = [parity(base[run], cand[run], args.tolerance) for run in cand if run in base
                   and not base[run].startswith("ERR")]
        entry["parity_vs_baseline"] = "differs" if "differs" in vs_base else (max(vs_base, key=len) if vs_base else "n/a")

        b_ok = entry["status"].get(args.baseline) == "200"
        c_ok = entry["status"].get(args.candidate) == "200"
        if not b_ok and c_ok:
            verdict = "fixed"
        elif b_ok and not c_ok:
            verdict = "broken"
        else:
            verdict = timing_verdict(entry["p50"].get(args.baseline), entry["p50"].get(args.candidate),
                                     args.noise, args.min_delta)
        entry["verdict"] = verdict
        b, c = entry["p50"].get(args.baseline), entry["p50"].get(args.candidate)
        entry["speedup"] = round(b / c, 2) if b and c and b_ok and c_ok else None
        out.append(entry)

    counts = {}
    for e in out:
        counts[e["verdict"]] = counts.get(e["verdict"], 0) + 1
    differs = sum(1 for e in out if e["parity_vs_reference"] == "differs")
    # A difference main already had is a known gap, not something this change did.
    preexisting = sum(1 for e in out if e["parity_vs_reference"] == "differs"
                      and e["baseline_parity_vs_reference"] == "differs")
    summary = {"label": args.label, "date": datetime.date.today().isoformat(), "set": raw.get("set") or (rows[0].get("set") if rows else None),
               "description": raw.get("description", ""), "baseline": args.baseline, "candidate": args.candidate,
               "reference": args.reference, "runs": raw.get("runs"), "valid": raw.get("valid", True),
               "restart_before": raw.get("restart_before"), "restart_after": raw.get("restart_after"),
               "noise": args.noise, "timing": "cold" if args.cold else "warm", "verdicts": counts, "result_differs_from_reference": differs,
               "result_differs_preexisting": preexisting,
               "stale_answers": {t: sum(1 for e in out if t in e.get("stale", [])) for t in (args.baseline, args.candidate, args.reference)},
               "rows": out}
    print(markdown(summary))
    if args.save:
        path = args.save
        if os.path.isdir(path):
            path = os.path.join(path, f"{summary['date']}-{args.label}.json")
        with open(path, "w") as f:
            json.dump(summary, f, indent=1)
        print(f"\nsaved {path}", file=sys.stderr)


def cell(text):
    return str(text).replace("|", "\\|")


def fmt(v, status, stale=False):
    if status and status != "200":
        return f"**{status}**"
    return "—" if v is None else f"{v:.2f}s" + ("†" if stale else "")


def markdown(s):
    b, c, r = s["baseline"], s["candidate"], s["reference"]
    lines = [f"**{s['set']}** — {s['label']} ({s['date']}, {s['runs']} runs, "
             f"{'cold (first run)' if s.get('timing') == 'cold' else 'warm p50'}; "
             f"VictoriaLogs restarts {s['restart_before']}→{s['restart_after']}{'' if s['valid'] else ', INVALID'})", "",
             f"| shape | range | {b} | {c} | {r} | change | result vs {r} |", "|---|---|---|---|---|---|---|"]
    for e in s["rows"]:
        change = {"fixed": "fixed", "broken": "**broken**", "slower": "**slower**", "n/a": "n/a"}.get(e["verdict"])
        if change is None:
            change = f"{e['speedup']}×" if e["speedup"] and e["verdict"] == "faster" else "same"
        st = e.get("stale", [])
        lines.append(f"| {cell(e['shape'])} | {e['range']} | {fmt(e['p50'].get(b), e['status'].get(b), b in st)} | "
                     f"{fmt(e['p50'].get(c), e['status'].get(c), c in st)} | "
                     f"{fmt(e['p50'].get(r), e['status'].get(r), r in st)} | {change} | {e['parity_vs_reference']} |")
    stale = {t: sum(1 for e in s["rows"] if t in e.get("stale", [])) for t in (b, c, r)}
    v = s["verdicts"]
    if any(stale.values()):
        lines += ["", "† identical answers for windows a minute apart: served from a cache or a stale view, so the "
                  "timing is not a like-for-like reference (" +
                  ", ".join(f"{t}: {n} of {len(s['rows'])}" for t, n in stale.items() if n) + ")."]
    lines += ["", f"Verdict: {len(s['rows'])} shape×range — " +
              ", ".join(f"{v[k]} {k}" for k in ("fixed", "faster", "same", "slower", "broken") if v.get(k)) +
              f"; results: {s['result_differs_from_reference'] - s.get('result_differs_preexisting', 0)} new "
              f"difference(s) from {r}, {s.get('result_differs_preexisting', 0)} pre-existing (also on {b})."]
    return "\n".join(lines)


def compare(args):
    old, new = load(args.old), load(args.new)
    t = args.target
    index = {(e["shape"], e["range"]): e for e in old["rows"]}
    lines = [f"Comparing {old['label']} ({old['date']}) → {new['label']} ({new['date']}), target '{t}', warm p50", "",
             "| shape | range | before | after | change |", "|---|---|---|---|---|"]
    changed = 0
    for e in new["rows"]:
        o = index.get((e["shape"], e["range"]))
        if not o:
            continue
        ob, nb = o["p50"].get(t), e["p50"].get(t)
        os_, ns = o["status"].get(t), e["status"].get(t)
        if os_ != ns:
            verdict = f"status {os_}→{ns}"
        else:
            verdict = timing_verdict(ob, nb, args.noise, args.min_delta)
        if verdict != "same":
            changed += 1
            lines.append(f"| {cell(e['shape'])} | {e['range']} | {fmt(ob, os_)} | {fmt(nb, ns)} | {verdict} |")
    common = sum(1 for e in new["rows"] if (e["shape"], e["range"]) in index)
    lines.append("")
    lines.append(f"{common} shape×range in common; {changed} changed beyond noise ({args.noise * 100:.0f}%, "
                 f"{args.min_delta}s), {common - changed} unchanged.")
    print("\n".join(lines if changed else lines[:2] + lines[-1:]))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("summarize")
    s.add_argument("raw")
    s.add_argument("--baseline", required=True)
    s.add_argument("--candidate", required=True)
    s.add_argument("--reference", default="loki")
    s.add_argument("--label", required=True)
    s.add_argument("--save", default="")
    s.add_argument("--tolerance", type=float, default=0.01, help="relative sum difference still counted as same result")
    s.add_argument("--cold", action="store_true",
                   help="compare first-run (cold) timings instead of warm p50; caches on either side hide less")
    c = sub.add_parser("compare")
    c.add_argument("old")
    c.add_argument("new")
    c.add_argument("--target", default="branch")
    for p in (s, c):
        p.add_argument("--noise", type=float, default=0.25)
        p.add_argument("--min-delta", type=float, default=0.05)
    args = ap.parse_args()
    summarize(args) if args.cmd == "summarize" else compare(args)


if __name__ == "__main__":
    main()
