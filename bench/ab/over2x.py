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

Acceptance (owner 2026-10-28: at most 2× Loki in the worst case, close to Loki in general, below Loki for
most queries). Every registry item a shape covers carries a perf: block (conformance/scripts/registry_io.py
has the form): max_ratio (the hard bound, at most 2.0), target_ratio (1.0), modes, locked and locked_since,
and an optional effort_exception. A row (set, shape, range, mode) is locked when any item that covers its
shape is locked for that mode; its bound is the smallest max_ratio of those items. A locked row above its
bound is a regression: the daily run fails on it (history.py), perf-smoke lists it in its own section and
fails on it only in --over-2x-loki fail mode. A row between 1× and max_ratio (beyond noise) is accepted only
with an effort_exception on an item that covers it; without one it is listed, flagged, for review. A row that
was below Loki and is now above it beyond noise is flagged as "lost below-Loki" (never a failure). band() and
distribution() give the share of comparable rows below Loki, close to it, between 1× and 2× and above 2×;
history.py ratchets the share below Loki and the count above 2×.

  over2x.py locks seed [--results bench/ab/results]
      Writes a perf: block into every covered item that has none, locked when every comparable row of its
      shapes measured within max_ratio in every mode on the latest daily results.
  over2x.py locks lock ID [ID ...] [--results bench/ab/results]
      Locks items the latest daily results show within their bound (refuses the others).
  over2x.py locks status [--results bench/ab/results]
      Locked items and rows per mode, locked rows above their bound, rows between 1× and 2× and the
      distribution, for the given results (a replay).
"""
import argparse
import glob
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "conformance", "scripts"))
import registry_io  # noqa: E402
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


def above(proxy, loki, bound=RATIO):
    """The proxy's time is above bound (default 2×) Loki's beyond the absolute noise floor."""
    return proxy is not None and loki is not None and proxy > bound * loki and proxy - loki > MIN_GAP


def judge(proxy, loki, proxy_status, loki_status, cache_served, bound=RATIO):
    """'above', 'within', 'loki_failed' (Loki did not answer 200: not a proxy defect) or 'not_comparable'."""
    if loki_status != "200":
        return "loki_failed"
    if proxy_status != "200" or cache_served or proxy is None or loki is None:
        return "not_comparable"
    return "above" if above(proxy, loki, bound) else "within"


# --- acceptance tiers -----------------------------------------------------------------------------------------

# The relative noise of history.py and perf_evidence.py; MIN_GAP is the absolute one.
DIST_NOISE = 0.25
BANDS = ("below", "close", "between", "above")
BAND_LABEL = {"below": "below Loki", "close": "close to Loki", "between": "1–2× Loki", "above": "above 2× Loki"}
# The distribution ratchet: the share below Loki may not fall, the count above 2× may not rise, beyond both
# margins (percentage points of the rows compared, and rows). Replayed on the daily history of 2026-10-21 to
# 2026-10-27: day-to-day moves of the share below were at most 2.0 points (6 rows). history.py counts a move to a
# worse band only when the proxy's own time got slower beyond noise (history.settled).
SHARE_NOISE_PP, SHARE_NOISE_ROWS = 3.0, 5
OVER_NOISE_PP, OVER_NOISE_ROWS = 2.0, 5


def band(proxy, loki, noise=DIST_NOISE, min_delta=MIN_GAP):
    """Where a comparable pair sits: 'below' (Loki slower beyond noise), 'close' (within noise of 1×),
    'between' (the proxy slower beyond noise, not above 2×) or 'above' (above 2×, the definition above)."""
    if above(proxy, loki):
        return "above"
    if proxy > loki * (1 + noise) and proxy - loki > min_delta:
        return "between"
    if loki > proxy * (1 + noise) and loki - proxy > min_delta:
        return "below"
    return "close"


def distribution(pairs):
    """mode -> {rows, below, close, between, above, share_below (percent)} from (mode, band) pairs."""
    out = {}
    for mode, b in pairs:
        d = out.setdefault(mode, {"rows": 0, **{k: 0 for k in BANDS}})
        d["rows"] += 1
        d[b] += 1
    for d in out.values():
        d["share_below"] = round(100 * d["below"] / d["rows"], 1) if d["rows"] else 0.0
    return {m: out[m] for m in MODES if m in out}


def ratchet_moves(old, new):
    """What the distribution ratchet reports between two distributions of the same rows (mode -> counts):
    a list of (mode, kind, detail) with kind 'share_below' (the share below Loki fell) or 'over_2x' (the count
    above 2× rose), each beyond both its margins."""
    out = []
    for mode in MODES:
        a, b = old.get(mode), new.get(mode)
        if not a or not b or not a["rows"] or a["rows"] != b["rows"]:
            continue
        n = a["rows"]
        fell = a["below"] - b["below"]
        if fell > SHARE_NOISE_ROWS and 100 * fell / n > SHARE_NOISE_PP:
            out.append((mode, "share_below", f"share below Loki {a['share_below']:.1f}% → {b['share_below']:.1f}% "
                        f"({a['below']} → {b['below']} of the {n} rows compared on both)"))
        rose = b["above"] - a["above"]
        if rose > OVER_NOISE_ROWS and 100 * rose / n > OVER_NOISE_PP:
            out.append((mode, "over_2x", f"rows above 2× Loki {a['above']} → {b['above']} of the {n} rows compared "
                        "on both"))
    return out


# --- registry bounds (the perf: blocks) ------------------------------------------------------------------------

SHAPES = os.path.join(HERE, "shapes.json")


def shape_covers(spec):
    """(set, shape) -> the registry ids the shape covers."""
    return {(s, sh["name"]): list(sh.get("covers") or []) for s, v in spec["sets"].items() for sh in v.get("shapes", [])}


def make_bounds(blocks, spec):
    """The bounds the gates use: the perf: blocks (id -> block) and the shapes' covers."""
    return {"blocks": blocks, "covers": shape_covers(spec)}


def read_bounds(root=ROOT, spec=None):
    """The bounds in the working tree (the daily run reads main's)."""
    if spec is None:
        with open(SHAPES) as f:
            spec = json.load(f)
    reg = os.path.join(root, registry_io.REGISTRY_ROOT)
    texts = {os.path.relpath(p, root): registry_io.read_text(p)
             for p in glob.glob(os.path.join(reg, "**", "*.yaml"), recursive=True)}
    blocks = registry_io.perf_blocks(registry_io.REGISTRY_ROOT, texts)
    return make_bounds({k: v[0] for k, v in blocks.items() if v[0] is not None}, spec)


def bounds_at(ref, root=ROOT):
    """The bounds as committed at a git ref (the PR's base: a PR cannot unlock its own rows). None when the
    ref cannot be read."""
    def git(*args, data=None):
        return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, input=data).stdout
    try:
        spec = json.loads(git("show", f"{ref}:bench/ab/shapes.json"))
        names = [n for n in git("ls-tree", "-r", "--name-only", ref, "--", registry_io.REGISTRY_ROOT).decode().splitlines()
                 if n.endswith(".yaml")]
        raw = git("cat-file", "--batch", data="".join(f"{ref}:{n}\n" for n in names).encode())
    except (subprocess.CalledProcessError, ValueError):
        return None
    texts, pos = {}, 0
    for name in names:
        end = raw.index(b"\n", pos)
        size = int(raw[pos:end].split()[2])
        texts[name] = raw[end + 1:end + 1 + size].decode()
        pos = end + 1 + size + 1
    blocks = registry_io.perf_blocks(registry_io.REGISTRY_ROOT, texts)
    return make_bounds({k: v[0] for k, v in blocks.items() if v[0] is not None}, spec)


def bound_for(bounds, set_name, shape, mode):
    """A row's acceptance: locked (an item covering it is locked for the mode), max_ratio (the smallest of the
    locked items', else 2×), locked_by and exception (the covering items with an effort_exception)."""
    out = {"locked": False, "max_ratio": RATIO, "locked_by": [], "exception": []}
    if not bounds:
        return out
    for item in bounds["covers"].get((set_name, shape), []):
        block = bounds["blocks"].get(item)
        if not block or mode not in block["modes"]:
            continue
        if block["locked"] is True:
            out["locked_by"].append(item)
            out["max_ratio"] = min(out["max_ratio"], block["max_ratio"])
        if block.get("effort_exception"):
            out["exception"].append(item)
    out["locked"] = bool(out["locked_by"])
    return out


def lock_state(summaries, bounds, ids=None):
    """id -> {"within": n, "above": [rows above its max_ratio], "lockable": bool} over every mode of the
    item's block, from report.py summaries (the daily results). An item is lockable when at least one of its
    rows is comparable and none is above its max_ratio."""
    ids = set(ids) if ids is not None else {i for v in bounds["covers"].values() for i in v}
    out = {i: {"within": 0, "above": []} for i in ids}
    for s in summaries:
        for row in s["rows"]:
            for mode, pair in summary_pairs(s, row, s["candidate"]).items():
                for item in bounds["covers"].get((s["set"], row["shape"]), []):
                    block = bounds["blocks"].get(item) or registry_io.PERF_DEFAULTS
                    if item not in out or mode not in block["modes"]:
                        continue
                    state = judge(*pair, bound=block["max_ratio"])
                    if state == "within":
                        out[item]["within"] += 1
                    elif state == "above":
                        out[item]["above"].append(entry(s["set"], row["shape"], row["range"], mode, pair[0], pair[1]))
    for v in out.values():
        v["lockable"] = v["within"] > 0 and not v["above"]
    return out


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


def gate(summary, baseline, noise, min_delta, bounds=None):
    """Findings for one pr_smoke summary: each above-2× row of the PR build with its kind (and a note).

    bounds (bounds_at the base): a locked row is judged at its own max_ratio instead of 2× and its findings
    carry locked=True (a new or worsened one is a locked-row regression); a row whose base build was below Loki
    and whose PR build is above Loki, both beyond noise, is "lost_below" (flagged, never failing).

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
            bound = bound_for(bounds, *k[:2], mode)
            limit = bound["max_ratio"] if bound["locked"] else RATIO
            state, listed = judge(*pr, bound=limit), baseline.get(k)
            base = bases.get(mode) or (None, None, None, None, False)
            base_state = judge(*base, bound=limit)
            base_ratio = ratio(base[0], base[1]) if base_state in ("above", "within") else None
            found = entry(*k, pr[0], pr[1], base_ratio=base_ratio, baseline_ratio=listed["ratio"] if listed else None,
                          base_s=base[0], confirmed=bool(row.get("confirmed")),
                          **({"locked": True, "bound": limit, "locked_by": bound["locked_by"]} if bound["locked"] else {}))
            if (base_ratio is not None and state in ("above", "within") and band(base[0], base[1], noise, min_delta) == "below"
                    and band(pr[0], pr[1], noise, min_delta) in ("between", "above")):
                out.append({**found, "kind": "lost_below"})
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


def locked_failing(findings):
    """The locked-row regressions among the failing findings: a locked row above its bound."""
    return [f for f in failing(findings) if f.get("locked")]


def remeasure(summary, baseline, noise, min_delta, bounds=None):
    """Shapes with a new or worsened cold or warm finding: re-measured like slower rows before they count.
    The confirmation pass has no refresh run, so a refresh finding stands on the first pass; a cold one does too
    when Loki's cache answered the re-run's cold (pr_smoke.confirm keeps the first pass's cold then)."""
    return sorted({f["shape"] for f in failing(gate(summary, baseline, noise, min_delta, bounds))
                   if f["mode"] != "refresh"})


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
    for f in sorted(rows, key=lambda f: not f.get("locked"))[:cap]:
        text = (f"{f['kind']}: {f['set']} / {f['shape']} / {f['range']} / {f['mode']}: PR ÷ Loki {times(f['ratio'])}"
                f" (base {times(f['base_ratio'])}, baseline list {times(f['baseline_ratio'])})")
        if f.get("locked"):
            text += f"; locked at {times(f['bound'])} by {', '.join(f['locked_by'])}"
        text = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        title = "locked row above its bound" if f.get("locked") else "above 2x Loki"
        out.append(f"::{level} title={title}::{text}")
    if len(rows) > cap:
        out.append(f"{len(rows) - cap} more above-2× Loki row(s) in the PR comment")
    return out


# --- command line ----------------------------------------------------------------------------------------------

def load(path):
    with open(path) as f:
        return json.load(f)


def daily_summaries(results):
    return [load(p) for p in sorted(glob.glob(os.path.join(results, "daily-*.json")))]


def locks(cmd, ids, results, root=ROOT, spec=None):
    """The `locks` command: seed, lock or status (see the module docstring). Returns the exit status."""
    summaries = daily_summaries(results)
    date = max((s.get("date", "") for s in summaries), default="")
    bounds = read_bounds(root, spec)
    covered = {i for v in bounds["covers"].values() for i in v}
    state = lock_state(summaries, bounds, covered)
    files = registry_io.item_files(os.path.join(root, registry_io.REGISTRY_ROOT))

    def write(item, block):
        path, text = files[item]
        with open(path, "w") as f:
            f.write(registry_io.with_perf(text, block))

    if cmd == "seed":
        added = {True: 0, False: 0}
        for item in sorted(covered - set(bounds["blocks"])):
            if item not in files:
                print(f"{item}: no registry file (bench/ab/shapes.json covers an unknown id)", file=sys.stderr)
                return 1
            lockable = state[item]["lockable"]
            write(item, {**registry_io.PERF_DEFAULTS, "locked": lockable, "locked_since": date if lockable else None})
            added[lockable] += 1
        print(f"perf: blocks written: {added[True]} locked, {added[False]} not locked (daily results of {date})")
        return 0
    if cmd == "lock":
        refused = [i for i in ids if i not in bounds["blocks"] or not state.get(i, {}).get("lockable")]
        for i in refused:
            why = ("no perf: block (run `locks seed`)" if i not in bounds["blocks"] else
                   f"{len(state.get(i, {}).get('above', []))} row(s) above its bound, {state.get(i, {}).get('within', 0)} within")
            print(f"{i}: not locked: {why}", file=sys.stderr)
        for i in ids:
            if i not in refused and bounds["blocks"][i]["locked"] is not True:
                write(i, {**bounds["blocks"][i], "locked": True, "locked_since": date})
                print(f"{i}: locked since {date}")
        return 1 if refused else 0
    # status: what the bounds say about the given results.
    locked_rows, over, between, pairs = {m: 0 for m in MODES}, [], [], []
    for s in summaries:
        for row in s["rows"]:
            for mode, pair in summary_pairs(s, row, s["candidate"]).items():
                b = bound_for(bounds, s["set"], row["shape"], mode)
                st = judge(*pair, bound=b["max_ratio"] if b["locked"] else RATIO)
                if st not in ("above", "within"):
                    continue
                where = f"{s['set']} / {row['shape']} / {row['range']} / {mode}: {times(ratio(*pair[:2]))}"
                pairs.append((mode, band(*pair[:2])))
                locked_rows[mode] += b["locked"]
                if b["locked"] and st == "above":
                    over.append(where)
                if band(*pair[:2]) == "between":
                    between.append(where + (f" (exception: {', '.join(b['exception'])})" if b["exception"] else
                                            " (no exception: flagged)") + (" locked" if b["locked"] else ""))
    blocks = bounds["blocks"]
    print(f"items with a perf: block: {len(blocks)} of {len(covered)} covered; locked "
          f"{sum(1 for b in blocks.values() if b['locked'] is True)}; with an effort exception "
          f"{sum(1 for b in blocks.values() if b.get('effort_exception'))}")
    print("locked comparable rows: " + ", ".join(f"{m} {n}" for m, n in locked_rows.items()))
    print(f"locked rows above their bound: {len(over)}")
    for line in over:
        print("  " + line)
    print(f"rows between 1× and 2× Loki: {len(between)}")
    for line in between:
        print("  " + line)
    for mode, d in distribution(pairs).items():
        print(f"{mode}: {d['rows']} comparable: " + ", ".join(f"{BAND_LABEL[k]} {d[k]}" for k in BANDS) +
              f"; share below Loki {d['share_below']:.1f}%")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--results", default=os.path.join(HERE, "results"))
    s.add_argument("--out", default=BASELINE)
    lk = sub.add_parser("locks")
    lk.add_argument("action", choices=("seed", "lock", "status"))
    lk.add_argument("ids", nargs="*")
    lk.add_argument("--results", default=os.path.join(HERE, "results"))
    r = sub.add_parser("replay")
    r.add_argument("summaries", nargs="+")
    r.add_argument("--baseline", default=BASELINE)
    r.add_argument("--noise", type=float, default=0.30)
    r.add_argument("--min-delta", type=float, default=0.10)
    args = ap.parse_args()
    if args.cmd == "locks":
        return locks(args.action, args.ids, args.results)
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
