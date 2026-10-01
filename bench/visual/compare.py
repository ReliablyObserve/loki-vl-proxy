#!/usr/bin/env python3
"""Compare the Grafana backend traffic captured by capture.spec.ts.

  compare.py OUT [--loki-seconds 5400]

For every page and range, the /api/ds/query frames and datasource resource
responses of the main proxy, the PR proxy and Loki are matched by request
(refId, expr, query type, or resource URL) and compared: series set, per-point
values (relative tolerance 1e-9) and timestamps for metric frames, a content
hash for everything else. main vs PR must be identical; vs Loki is compared
only for ranges Loki holds (--loki-seconds). Writes OUT/compare.md and
OUT/compare.json.
"""
import argparse
import glob
import hashlib
import json
import math
import os
import re
import sys
from collections import defaultdict

from vio import dump_json, load_json, write_text


# Request fields that differ between the three datasources or between page loads without changing the question.
VOLATILE = {"datasource", "datasourceId", "requestId", "key", "uid", "queryCachingTTL"}
# Frame meta that says how the answer was produced rather than what it is (stats, the executed query text).
META_STABLE = ("type", "typeVersion", "preferredVisualisationType", "custom", "notices")


def strip(obj):
    """The request body without its volatile fields, at any depth."""
    if isinstance(obj, dict):
        return {k: strip(v) for k, v in obj.items() if k not in VOLATILE}
    if isinstance(obj, list):
        return [strip(v) for v in obj]
    return obj


def digest(obj):
    return hashlib.sha1(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:12]


def signature(sch):
    """Field names, types, labels and interval plus the stable parts of the frame meta: the shape a panel is built from."""
    fields = [(f.get("name"), f.get("type"), f.get("labels") or {}, (f.get("config") or {}).get("interval")) for f in sch["fields"]]
    meta = {k: (sch.get("meta") or {}).get(k) for k in META_STABLE if (sch.get("meta") or {}).get(k) is not None}
    return digest([fields, meta])


def frames_of(resp):
    out = {}
    for ref, res in (resp or {}).get("results", {}).items():
        if res.get("error"):
            out[(ref, "error")] = ("error", res["error"])
        for fr in res.get("frames", []):
            sch, data = fr["schema"], fr["data"]["values"]
            fields = sch["fields"]
            labels = json.dumps((fields[1].get("labels") if len(fields) > 1 else None) or {}, sort_keys=True)
            ident = (ref, sch.get("name") or "", labels)
            if len(fields) == 2 and fields[0].get("type") == "time" and fields[1].get("type") == "number":
                out[ident] = ("metric", dict(zip(data[0], data[1])), signature(sch))
            else:
                out[ident] = ("other", digest([signature(sch), data]), len(data[0]) if data else 0)
    return out


def records(path):
    d = load_json(path)
    recs = defaultdict(list)
    for r in d["records"]:
        if "/api/ds/query" in r["url"]:
            for q in (r["request"] or {}).get("queries", []):
                key = ("query", q.get("refId"), q.get("expr"), q.get("queryType"), r["request"].get("from"), r["request"].get("to"),
                       digest(strip(q)))
                recs[key].append(frames_of(r["response"]) if r["status"] == 200 else {("status", str(r["status"])): ("error", r["status"])})
        else:
            key = ("resource", re.sub(r"/api/datasources/uid/[^/]+", "/api/datasources/uid/*", r["url"].split("&_=")[0]))
            body = r["response"]
            if "/resources/patterns" in r["url"] and isinstance(body, dict):
                # mined from a sample of the rows: compare the pattern set, not the sample counts
                body = sorted(p.get("pattern", "") for p in body.get("data", []))
            rows = len(body.get("data") or []) if isinstance(body, dict) and isinstance(body.get("data"), list) else (len(body) if isinstance(body, list) else 0)
            recs[key].append({"body": ("other", hashlib.sha1(json.dumps(body, sort_keys=True).encode()).hexdigest(), rows)} if r["status"] == 200 else {("status", ""): ("error", r["status"])})
    return d.get("settled", False), recs


def errors(recs, allowed=()):
    """Error answers (an error in a result, a non-200 status) minus the allow-listed ones, as short strings."""
    out = []
    for lst in recs.values():
        for fr in lst:
            for k, x in fr.items():
                if x[0] == "error":
                    text = str(x[1])[:120]
                    if not any(a and a in text for a in allowed):
                        out.append(text)
    return out


def points(recs):
    """Non-zero metric points plus rows of the other frames: what the page has to draw."""
    n = 0
    for lst in recs.values():
        for fr in lst:
            for x in fr.values():
                if x[0] == "metric":
                    n += sum(1 for v in x[1].values() if v)
                elif x[0] == "other":
                    n += x[2]
    return n


def ui_of(d, name):
    path = os.path.join(d, f"{name}.json")
    return (load_json(path).get("ui") or {}) if os.path.exists(path) else {}


def same(a, b):
    """None when equal, else a short reason."""
    if set(a) != set(b):
        only_a, only_b = sorted(map(str, set(a) - set(b))), sorted(map(str, set(b) - set(a)))
        return f"series sets differ: {len(set(a) - set(b))} only left {only_a[:2]}, {len(set(b) - set(a))} only right {only_b[:2]}"
    for k in a:
        x, y = a[k], b[k]
        if x[0] != y[0]:
            return f"{k}: kind {x[0]} vs {y[0]}"
        if x[0] == "metric":
            if x[2] != y[2]:
                return f"{k[2]}: frame schema or meta differs"
            if set(x[1]) != set(y[1]):
                return f"{k[2]}: timestamps differ ({len(x[1])} vs {len(y[1])} points)"
            for t in x[1]:
                u, v = x[1][t], y[1][t]
                if u is None or v is None:
                    if u != v:
                        return f"{k[2]}: null at {t}"
                elif not math.isclose(u, v, rel_tol=1e-9, abs_tol=1e-12):
                    return f"{k[2]}: value at {t}: {u} vs {v}"
        elif x[1:] != y[1:]:
            return f"{k}: content differs (rows {x[2]} vs {y[2]})"
    return None


def nonzero(fr):
    return {k: ("metric", {t: v for t, v in x[1].items() if v}, x[2]) if x[0] == "metric" and any(x[1].values()) else x
            for k, x in fr.items() if not (x[0] == "metric" and not any(x[1].values()))}


def pairs(a, b):
    """Pair the answers of one request by content, then the leftovers in order; (x, y) with None for a missing side."""
    left, right, out = list(a), list(b), []
    seen = {}
    for j, y in enumerate(right):
        seen.setdefault(digest(repr(sorted(y.items(), key=str))), []).append(j)
    taken, rest = set(), []
    for x in left:
        idx = seen.get(digest(repr(sorted(x.items(), key=str))))
        if idx:
            j = idx.pop(0)
            taken.add(j)
            out.append((x, right[j]))
        else:
            rest.append(x)
    rest_right = [y for j, y in enumerate(right) if j not in taken]
    for i in range(max(len(rest), len(rest_right))):
        out.append((rest[i] if i < len(rest) else None, rest_right[i] if i < len(rest_right) else None))
    return out


def compare(left, right, lenient=False):
    keys = sorted(set(left) | set(right), key=str)
    n = ok = missing = 0
    diffs, series = [], 0
    for k in keys:
        for xa, xb in pairs(left.get(k, []), right.get(k, [])):
            if xa is None or xb is None:
                missing += 1
                diffs.append(f"{k[0]} {str(k[2:] or k[1])[:90]}: request only on one side ({'left' if xb is None else 'right'})")
                continue
            n += 1
            x, y = (nonzero(xa), nonzero(xb)) if lenient else (xa, xb)
            series += len(xa)
            why = same(x, y)
            if why:
                diffs.append(f"{k[0]} {str(k[2:] or k[1])[:90]}: {why}")
            else:
                ok += 1
    return n, ok, diffs, series, missing


def tail_entries(path):
    """{(ts, line): label keys} of every entry the Live tail websockets delivered."""
    out = {}
    for f in load_json(path).get("frames", []):
        if "/loki/api/v1/tail" not in f["url"]:
            continue
        try:
            msg = json.loads(f["payload"])
        except ValueError:
            continue
        for st in msg.get("streams", []):
            for v in st.get("values", []):
                out[(v[0], v[1])] = tuple(sorted(st["stream"]))
    return out


def compare_tail(d):
    ent = {n: tail_entries(os.path.join(d, f"{n}.json")) for n in ("main", "pr", "loki")}
    # Same window for all three: from the latest first entry to the earliest last entry.
    lo = max(min(k[0] for k in e) for e in ent.values() if e)
    hi = min(max(k[0] for k in e) for e in ent.values() if e)
    win = {n: {k: v for k, v in e.items() if lo <= k[0] <= hi} for n, e in ent.items()}
    m, p, l = win["main"], win["pr"], win["loki"]
    diffs = []
    if set(m) != set(p) or m != p:
        diffs.append(f"tail entries differ: main {len(m)}, PR {len(p)}, only main {len(set(m) - set(p))}, only PR {len(set(p) - set(m))}")
    both = set(p) & set(l)
    lk = [f"entries in the window: PR {len(p)}, Loki {len(l)}, in both {len(both)}"]
    if both:
        k = next(iter(both))
        if p[k] != l[k]:
            lk.append(f"label keys differ: PR only {sorted(set(p[k]) - set(l[k]))[:6]}, Loki only {sorted(set(l[k]) - set(p[k]))[:6]}")
    return len(p), diffs, lk


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--loki-seconds", type=int, default=5400)
    a = ap.parse_args()
    spec = load_json(os.path.join(os.path.dirname(os.path.abspath(__file__)), "spec.json"))
    rows, report = [], []
    allowed = spec.get("allowed_errors") or []  # substrings of error answers that are expected (documented in the README)
    for d in sorted(glob.glob(os.path.join(a.out, "data", "*", "*"))):
        page, rng = d.split(os.sep)[-2:]
        if not all(os.path.exists(os.path.join(d, f"{n}.json")) for n in ("main", "pr")):
            continue
        if rng == "live":
            n, diffs, lk = compare_tail(d)
            rows.append(dict(page=page, range=rng, requests=n, series=0, main_vs_pr="identical" if not diffs else "DIFFERS",
                             pr_vs_loki="; ".join(lk), settled=True, main_pr_diffs=diffs, loki_diffs=[], loki_new=[],
                             loki_compared=True, points_main=n, points_pr=n, points_loki=n, ui_main=ui_of(d, "main"), ui_pr=ui_of(d, "pr"),
                             settled_pr=True, errors_pr=[], errors_main=[], loki_missing=False))
            continue
        sm, m = records(os.path.join(d, "main.json"))
        sp, p = records(os.path.join(d, "pr.json"))
        settle = {n: round(load_json(os.path.join(d, f"{n}.json")).get("settle_ms", 0) / 1000, 1) for n in ("main", "pr")}
        has_loki = os.path.exists(os.path.join(d, "loki.json"))
        sl, l = records(os.path.join(d, "loki.json")) if has_loki else (True, {})
        n, ok, diffs, series, miss = compare(m, p)
        ok_all = ok == n and miss == 0
        loki_ok = has_loki and spec["ranges"][rng] <= a.loki_seconds
        if loki_ok:
            ln, lok, ldiffs, _, lmiss = compare(p, l)
            _, lok2, ldiffs2, _, _ = compare(p, l, lenient=True)
            _, _, mdiffs, _, _ = compare(m, l, lenient=True)
            vs = f"{lok2}/{ln} identical" + (f" ({lok}/{ln} counting zero-filled points)" if lok != lok2 else "") + (f", {lmiss} request(s) on one side only" if lmiss else "")
        else:
            ln, lok, ldiffs, ldiffs2, mdiffs, vs = 0, 0, [], [], [], ("n/a (Loki holds 1.5h)" if has_loki else "n/a (Loki not captured)")
        rows.append(dict(page=page, range=rng, requests=n, series=series, main_vs_pr=f"{ok}/{n}" + (f", {miss} one-sided" if miss else ""), pr_vs_loki=vs,
                         settled=all((sm, sp, sl)), settle_s=settle, main_pr_diffs=diffs, loki_diffs=ldiffs2 if loki_ok else [],
                         loki_compared=bool(loki_ok), loki_main_n=len(mdiffs), loki_new=[x for x in ldiffs2 if x not in set(mdiffs)] if loki_ok else [],
                         points_main=points(m), points_pr=points(p), points_loki=points(l), ui_main=ui_of(d, "main"), ui_pr=ui_of(d, "pr"),
                         settled_pr=bool(sp), errors_pr=errors(p, allowed), errors_main=errors(m, allowed),
                         loki_missing=bool(not has_loki and spec["ranges"][rng] <= a.loki_seconds)))
    md = ["| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |", "|---|---|---|---|---|---|"]
    for r in rows:
        md.append(f"| {r['page']} | {r['range']} | {r['requests']} | {r['main_vs_pr']} | {r['pr_vs_loki']} | {'yes' if r['settled'] else 'NO'} |")
    for r in rows:
        if r["main_pr_diffs"] or r["loki_diffs"]:
            md += ["", f"### {r['page']} {r['range']}"]
            md += [f"- main vs PR: {x}" for x in r["main_pr_diffs"][:8]] + [f"- PR vs Loki: {x}" for x in r["loki_diffs"][:8]]
    write_text(os.path.join(a.out, "compare.md"), "\n".join(md) + "\n")
    dump_json(os.path.join(a.out, "compare.json"), rows)
    print("\n".join(md[:200]))
    bad = [r for r in rows if r["main_pr_diffs"]]
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
