#!/usr/bin/env python3
"""Above 2× Loki: the one definition the PR smoke, the daily run and the performance report share.

A shape × range × mode (cold, warm or refresh) is above 2× Loki when the
proxy's time is more than twice Loki's AND exceeds it by more than MIN_GAP
seconds (the absolute noise floor of the daily report and perf_evidence.py, so
10 ms against 3 ms does not count). The pair is compared only when it is
comparable:

  - Loki answered 200. A Loki error or timeout is not a proxy defect: it is
    recorded separately (state "loki_failed"), never counted;
  - the proxy answered 200 (a proxy error is the broken/status checks' job);
  - for cold only: Loki's results cache did not answer the cold run (its own
    counters, or identical answers for moved windows). Loki's warm and refresh
    timings come from its results cache by design: that is what a repeat view of
    Loki costs, so they are compared as measured.

bench/ab/history/over-2x-loki.json is the baseline: every row the daily run
measured above 2× Loki when the list was seeded. It only shrinks. history.py
alerts (the daily run) removes a row once two consecutive full daily runs
measured it comparable and not above 2×, tightens a row's recorded ratio when a
run measures it lower, and never adds one: a new above-2× row is reported as a
warning in the daily pull request. pr_smoke.py / comment.py warn (or, with
--over-2x-loki fail, fail) when a pull request adds a row above 2× Loki or makes
a listed one worse; see gate().

  over2x.py seed [--results bench/ab/results] [--out bench/ab/history/over-2x-loki.json]
      Writes the baseline from the latest daily-*.json results.
  over2x.py replay SUMMARY.json [SUMMARY.json ...] [--baseline PATH] [--noise 0.30] [--min-delta 0.10]
      What the PR gate says about saved pr_smoke summaries.
"""
import argparse
import glob
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
RATIO = 2.0
# The absolute noise floor of history.py and conformance/scripts/perf_evidence.py (MIN_DELTA).
MIN_GAP = 0.05
MODES = ("cold", "warm", "refresh")
BASELINE_NAME = "over-2x-loki.json"
BASELINE = os.path.join(HERE, "history", BASELINE_NAME)
BASELINE_REL = f"bench/ab/history/{BASELINE_NAME}"
# The PR gate: "warn" reports new and worsened rows without changing the verdict; "fail" fails perf-smoke on them.
GATE_MODES = ("warn", "fail")
DEFAULT_GATE = "warn"
RULE = (f"proxy time > {RATIO:g} x Loki time and more than {MIN_GAP * 1000:.0f} ms slower; both answer 200; "
        "on cold, Loki's results cache did not answer")


def above(proxy, loki):
    """The proxy's time is above 2× Loki's beyond the absolute noise floor."""
    return proxy is not None and loki is not None and proxy > RATIO * loki and proxy - loki > MIN_GAP


def judge(proxy, loki, proxy_status, loki_status, cache_served):
    """'above', 'within', 'loki_failed' (Loki did not answer 200: not a proxy defect) or 'not_comparable'."""
    if loki_status != "200":
        return "loki_failed"
    if proxy_status != "200" or cache_served or proxy is None or loki is None:
        return "not_comparable"
    return "above" if above(proxy, loki) else "within"


def ratio(proxy, loki):
    return round(proxy / loki, 2) if proxy is not None and loki else None


def summary_pairs(summary, row, target):
    """mode -> (target seconds, Loki seconds, target status, Loki status, Loki's cache answered the cold run)
    for a report.py row; the cache flag is set for the cold mode only."""
    ref = summary["reference"]
    served = ref in row.get("cold_cache_served", []) or ref in row.get("stale", [])
    status = row["status"]
    out = {"cold": (row["cold"].get(target), row["cold"].get(ref), status.get(target), status.get(ref), served)}
    if summary.get("timing") != "cold":  # a --cold summary's p50 holds first runs
        out["warm"] = (row["p50"].get(target), row["p50"].get(ref), status.get(target), status.get(ref), False)
    if row.get("refresh"):
        rs = row.get("refresh_status", {})
        out["refresh"] = (row["refresh"].get(target), row["refresh"].get(ref), rs.get(target), rs.get(ref), False)
    return out


def history_pairs(row):
    """The same for a history.py line's row (cold and warm; the daily run has no refresh pass)."""
    served = row.get("loki_cold_computed") is False
    return {"cold": (row["cold"], row["loki_cold"], row["status"], row["loki_status"], served),
            "warm": (row["p50"], row["loki_p50"], row["status"], row["loki_status"], False)}


def key(entry):
    return (entry["set"], entry["shape"], entry["range"], entry["mode"])


def entry(set_name, shape, rng, mode, proxy, loki, **extra):
    return {"set": set_name, "shape": shape, "range": rng, "mode": mode, "proxy_s": proxy, "loki_s": loki,
            "ratio": ratio(proxy, loki), **extra}


# --- the baseline file ---------------------------------------------------------------------------------------

def parse_baseline(text):
    """key -> entry from the file's text; raises ValueError on a file that is not the expected shape."""
    data = json.loads(text)
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ValueError("no rows list")
    out = {}
    for r in rows:
        if not (isinstance(r, dict) and all(isinstance(r.get(k), str) for k in ("set", "shape", "range", "mode"))
                and isinstance(r.get("ratio"), (int, float))):
            raise ValueError(f"bad row {r!r}")
        out[key(r)] = r
    return out


def read_baseline(path=BASELINE):
    """(key -> entry, problem). A missing file is an empty baseline; one that cannot be read is a problem."""
    if not os.path.exists(path):
        return {}, ""
    try:
        with open(path) as f:
            return parse_baseline(f.read()), ""
    except ValueError as e:  # includes json.JSONDecodeError
        return {}, f"{BASELINE_NAME} cannot be read ({e}); it is left alone this run"


def baseline_at(ref, root=ROOT):
    """The baseline as committed at a git ref (the PR's base: a PR cannot list its own rows). Missing: empty."""
    try:
        text = subprocess.run(["git", "show", f"{ref}:{BASELINE_REL}"], cwd=root, check=True, capture_output=True,
                              text=True).stdout
    except subprocess.CalledProcessError:
        return {}
    return parse_baseline(text)


def write_baseline(rows, path=BASELINE):
    rows = sorted(rows, key=lambda r: (r["set"], r["shape"], r["range"], MODES.index(r["mode"])))
    data = {"about": "Shape x range x mode rows the daily A/B run measured above 2x Loki. Generated by "
                     "bench/ab/over2x.py seed and kept by bench/ab/history.py alerts: rows are removed after two "
                     "consecutive daily runs under 2x and never added automatically. See bench/ab/README.md.",
            "rule": RULE, "rows": rows}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(json.dumps(data, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def counts(rows):
    by_mode = {m: 0 for m in MODES}
    for r in rows:
        by_mode[r["mode"]] = by_mode.get(r["mode"], 0) + 1
    return by_mode


def count_line(rows):
    by_mode = counts(rows)
    return (f"above-2× baseline: {len(rows)} rows ({', '.join(f'{m} {n}' for m, n in by_mode.items())}); "
            "the gate becomes blocking at 0")


# --- measuring a summary ----------------------------------------------------------------------------------------

def measure(summary, target=None):
    """Every mode of every row of a report.py summary, judged for target (default: the candidate)."""
    target = target or summary["candidate"]
    out = []
    for row in summary["rows"]:
        for mode, pair in summary_pairs(summary, row, target).items():
            out.append(entry(summary["set"], row["shape"], row["range"], mode, pair[0], pair[1], state=judge(*pair)))
    return out


def seed(summaries, date="", commit=""):
    """The baseline rows of a set of daily summaries: every comparable row above 2× Loki."""
    rows = []
    for s in summaries:
        for m in measure(s):
            if m.pop("state") == "above":
                rows.append({**m, "since": date or s.get("date", ""), "commit": commit or s.get("commit", "")})
    return rows


# --- the PR gate --------------------------------------------------------------------------------------------------

def slower(old, new, noise, min_delta):
    """new is slower than old beyond the relative and the absolute noise."""
    return old is not None and new is not None and new > old * (1 + noise) and new - old > min_delta


def gate(summary, baseline, noise, min_delta):
    """Findings for one pr_smoke summary: each above-2× row of the PR build with its kind (and a note).

    known     the base build is above 2× too in the same run and the PR is within noise of it, listed or not
              ("as slow on base"); or the row is listed and the PR is not worsened ("listed"); or the base
              build is comparable, just under 2× (e.g. inside the absolute floor), and the PR is within noise of
              it ("within noise of base": the PR did not move it);
    new       above 2× on the PR, not listed, and slower than the base build beyond noise (or the base did not
              answer comparably), with the base not above 2×;
    worsened  listed, and the PR's time is worse beyond noise than both the listed ratio predicts and the base
              build's in the same run; or not listed, above 2× on the base build too, and the PR is worse than
              the base beyond noise.
    Listed rows the PR measures comparable and within 2× are "cleared"; rows where the PR answered 200 and Loki
    did not are "loki_failed". Ratios from the daily data are not the PR smoke's (a third of its line rate,
    longer ranges), so the same-run base build decides first.
    """
    b, c = summary["baseline"], summary["candidate"]
    out = []
    for row in summary["rows"]:
        bases = summary_pairs(summary, row, b)
        for mode, pr in summary_pairs(summary, row, c).items():
            k = (summary["set"], row["shape"], row["range"], mode)
            state, listed = judge(*pr), baseline.get(k)
            base = bases.get(mode) or (None, None, None, None, False)
            base_state = judge(*base)
            base_ratio = ratio(base[0], base[1]) if base_state in ("above", "within") else None
            found = entry(*k, pr[0], pr[1], base_ratio=base_ratio, baseline_ratio=listed["ratio"] if listed else None,
                          base_s=base[0], confirmed=bool(row.get("confirmed")))
            if state == "loki_failed" and pr[2] == "200":
                out.append({**found, "kind": "loki_failed"})
            if state == "within" and listed:
                out.append({**found, "kind": "cleared"})
            if state != "above":
                continue
            worse_than_base = base_ratio is not None and slower(base[0], pr[0], noise, min_delta)
            if base_state == "above" and not worse_than_base:
                kind, note = "known", "as slow on base"
            elif listed:
                worse = worse_than_base and slower(listed["ratio"] * pr[1], pr[0], noise, min_delta)
                kind, note = ("worsened", "listed") if worse else ("known", "listed")
            elif base_state == "above":
                kind, note = "worsened", "above 2× on base too"
            elif base_ratio is not None and not worse_than_base:
                kind, note = "known", "within noise of base"
            else:
                kind, note = "new", ""
            out.append({**found, "kind": kind, "note": note})
    return out


def failing(findings):
    return [f for f in findings if f["kind"] in ("new", "worsened")]


def remeasure(summary, baseline, noise, min_delta):
    """Shapes with a new or worsened cold or warm finding: re-measured like slower rows before they count.
    The confirmation pass has no refresh run, so a refresh finding stands on the first pass; a cold one does too
    when Loki's cache answered the re-run's cold (pr_smoke.confirm keeps the first pass's cold then)."""
    return sorted({f["shape"] for f in failing(gate(summary, baseline, noise, min_delta)) if f["mode"] != "refresh"})


def times(v):
    if v is None:
        return "—"
    return f"{v:.2f}×" if v < 10 else f"{v:.0f}×"


def secs(v):
    if v is None:
        return "—"
    return f"{v * 1000:.0f} ms" if v < 1 else f"{v:.2f} s"


def annotations(findings, mode=DEFAULT_GATE, cap=10):
    """GitHub workflow commands for the new and worsened rows (GitHub shows ten per step)."""
    level = "error" if mode == "fail" else "warning"
    rows = failing(findings)
    out = []
    for f in rows[:cap]:
        text = (f"{f['kind']}: {f['set']} / {f['shape']} / {f['range']} / {f['mode']}: PR ÷ Loki {times(f['ratio'])}"
                f" (base {times(f['base_ratio'])}, baseline list {times(f['baseline_ratio'])})")
        text = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        out.append(f"::{level} title=above 2x Loki::{text}")
    if len(rows) > cap:
        out.append(f"{len(rows) - cap} more above-2× Loki row(s) in the PR comment")
    return out


# --- command line ----------------------------------------------------------------------------------------------

def load(path):
    with open(path) as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--results", default=os.path.join(HERE, "results"))
    s.add_argument("--out", default=BASELINE)
    r = sub.add_parser("replay")
    r.add_argument("summaries", nargs="+")
    r.add_argument("--baseline", default=BASELINE)
    r.add_argument("--noise", type=float, default=0.30)
    r.add_argument("--min-delta", type=float, default=0.10)
    args = ap.parse_args()
    if args.cmd == "seed":
        summaries = [load(p) for p in sorted(glob.glob(os.path.join(args.results, "daily-*.json")))]
        rows = seed(summaries)
        write_baseline(rows, args.out)
        print(f"wrote {args.out}: {count_line(rows)}")
        return 0
    baseline, problem = read_baseline(args.baseline)
    if problem:
        print(problem, file=sys.stderr)
        return 1
    for path in args.summaries:
        found = gate(load(path), baseline, args.noise, args.min_delta)
        by_kind = {}
        for f in found:
            by_kind[f["kind"]] = by_kind.get(f["kind"], 0) + 1
        print(f"{os.path.basename(path)}: {by_kind or 'nothing above 2x Loki'}")
        for f in found:
            if f["kind"] in ("new", "worsened"):
                print(f"  {f['kind']}: {f['shape']} {f['range']} {f['mode']}: PR {secs(f['proxy_s'])} vs Loki "
                      f"{secs(f['loki_s'])} = {times(f['ratio'])} (base {times(f['base_ratio'])}, listed "
                      f"{times(f['baseline_ratio'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
