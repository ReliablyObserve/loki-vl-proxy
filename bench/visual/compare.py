#!/usr/bin/env python3
"""Compare the Grafana backend traffic captured by capture.spec.ts.

  compare.py OUT [--loki-seconds 5400]

For every page and range, the /api/ds/query frames and datasource resource
responses of the main proxy, the PR proxy and Loki are matched by request
(refId, expr, query type, or resource URL) and compared: series set, per-point
values (relative tolerance 1e-9) and timestamps for metric frames, a content
hash for everything else. main vs PR must be identical; vs Loki is compared
only for ranges Loki holds (--loki-seconds), and a difference from Loki that is
by design is listed as explained (explain_vs_loki) instead of counted. Writes
OUT/compare.md and OUT/compare.json.
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
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:12]


def signature(sch):
    """Field names, types, labels and interval plus the stable parts of the frame meta: the shape a panel is built from."""
    fields = [(f.get("name"), f.get("type"), f.get("labels") or {}, (f.get("config") or {}).get("interval")) for f in sch["fields"]]
    meta = {k: (sch.get("meta") or {}).get(k) for k in META_STABLE if (sch.get("meta") or {}).get(k) is not None}
    return digest([fields, meta])


# Loki's pipeline error names the first failing line its shards meet, so two answers of the same failing query
# (Loki's, and a build that answers Loki's error) name different lines: the series is set aside, the rest compared.
PIPELINE_ERROR_SERIES = re.compile(r"for series: '\{.*\}'\.", re.S)


def error_text(text):
    return PIPELINE_ERROR_SERIES.sub("for series: '{...}'.", str(text))


def frames_of(resp):
    out = {}
    for ref, res in (resp or {}).get("results", {}).items():
        if res.get("error"):
            out[(ref, "error")] = ("error", error_text(res["error"]))
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


def records(path, raw=None):
    """(settled, {request key: [answer digests]}); raw, when given, gets the answers themselves in the same order."""
    d = load_json(path)
    recs = defaultdict(list)
    for r in d["records"]:
        if "/api/ds/query" in r["url"]:
            for q in (r["request"] or {}).get("queries", []):
                key = ("query", q.get("refId"), q.get("expr"), q.get("queryType"), r["request"].get("from"), r["request"].get("to"),
                       digest(strip(q)))
                recs[key].append(frames_of(r["response"]) if r["status"] == 200 else {("status", str(r["status"])): ("error", r["status"])})
                if raw is not None:
                    raw[key].append(((r["response"] or {}).get("results") or {}).get(q.get("refId")) if r["status"] == 200 else None)
        else:
            key = ("resource", re.sub(r"/api/datasources/uid/[^/]+", "/api/datasources/uid/*", r["url"].split("&_=")[0]))
            body = r["response"]
            if raw is not None:
                raw[key].append(body if r["status"] == 200 else None)
            if "/resources/patterns" in r["url"] and isinstance(body, dict):
                # mined from a sample of the rows: compare the pattern set, not the sample counts
                body = sorted(p.get("pattern", "") for p in body.get("data", []))
            lists = [body.get(k) for k in ("data", "fields", "detectedLabels")] if isinstance(body, dict) else [body]
            rows = next((len(x) for x in lists if isinstance(x, list)), 0)
            recs[key].append({"body": ("other", hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest(), rows)} if r["status"] == 200 else {("status", ""): ("error", r["status"])})
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
    """Pair the answers of one request by content, then the leftovers in order; (i, j) indices, None for a missing side."""
    out, seen = [], {}
    for j, y in enumerate(b):
        seen.setdefault(digest(repr(sorted(y.items(), key=str))), []).append(j)
    taken, rest = set(), []
    for i, x in enumerate(a):
        idx = seen.get(digest(repr(sorted(x.items(), key=str))))
        if idx:
            j = idx.pop(0)
            taken.add(j)
            out.append((i, j))
        else:
            rest.append(i)
    rest_right = [j for j in range(len(b)) if j not in taken]
    for n in range(max(len(rest), len(rest_right))):
        out.append((rest[n] if n < len(rest) else None, rest_right[n] if n < len(rest_right) else None))
    return out


# Drilldown's patterns come from the patterns-autodetect proxy, which mines them from
# the queries that proxy has already served: two processes running the same code
# answer differently depending on their request history, so a patterns difference
# between base and PR is reported, never gated.
NONDETERMINISTIC = ("/resources/patterns",)
# Resources whose answer is the proxy's deployment configuration (its
# -tenant-default-limits / -tenant-limits flags), not data: a base-vs-PR
# difference there comes from the stack's flags, so it is reported, never gated.
CONFIGURATION = ("/resources/drilldown-limits",)


def configuration(diff):
    return any(s in diff for s in CONFIGURATION)


def nondeterministic(diff):
    return any(s in diff for s in NONDETERMINISTIC)


def compare(left, right, lenient=False, explain=None):
    """(requests, identical, differences, series, one-sided, explained) of two sides' records.

    explain: (raw left, raw right) of the records, to ask explain_vs_loki why a difference is by design; such a
    difference goes to `explained` with its reason instead of the differences."""
    keys = sorted(set(left) | set(right), key=str)
    n = ok = missing = 0
    diffs, explained, series = [], [], 0
    for k in keys:
        la, lb = left.get(k, []), right.get(k, [])
        for i, j in pairs(la, lb):
            if i is None or j is None:
                missing += 1
                text = f"{k[0]} {str(k[2:] or k[1])[:90]}: request only on one side ({'left' if j is None else 'right'})"
                reason = explain_one_sided(k) if explain else None
                if reason:
                    explained.append(f"{text} -- explained: {reason}")
                else:
                    diffs.append(text)
                continue
            n += 1
            xa, xb = la[i], lb[j]
            x, y = (nonzero(xa), nonzero(xb)) if lenient else (xa, xb)
            series += len(xa)
            why = same(x, y)
            if not why:
                ok += 1
                continue
            text = f"{k[0]} {str(k[2:] or k[1])[:90]}: {why}"
            reason = explain_vs_loki(k, explain[0][k][i], explain[1][k][j]) if explain else None
            if reason:
                explained.append(f"{text} -- explained: {reason}")
            else:
                diffs.append(text)
    return n, ok, diffs, series, missing, explained


# Differences from Loki that are by design. Each rule takes the PR's and Loki's answer to one request, removes only
# the documented difference from both and requires the rest to match exactly; it returns the reason, or None.
EXTRACTED = ("Loki's _extracted suffix for a key named like a stream label, where the build does not return it "
             "(detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision)")


def _extracted(label):
    return str(label or "").endswith("_extracted")


# Loki's parse-error labels: a parser stage that rejects a line (| json on a
# logfmt line) adds them as parsed labels; the proxy does not report them.
PARSE_ERROR = ("Loki's __error__ / __error_details__ labels for a line a parser stage rejects (not reported by the "
               "proxy; open, profiles/stage-field-exposure)")
PARSE_ERROR_LABELS = ("__error__", "__error_details__")


def _envelope(a, b):
    """Keys only the proxy's answer carries (status and data mirrors), as a note."""
    extra = sorted(set(a) - set(b)) if isinstance(a, dict) and isinstance(b, dict) else []
    return f"the proxy also returns {', '.join(extra)}" if extra else ""


def _join(*notes):
    return "; ".join(x for x in notes if x) or None


def _detected_fields(a, b):
    def view(body):
        fields = [json.dumps(f, sort_keys=True) for f in (body.get("fields") or []) if not _extracted(f.get("label"))]
        return sorted(fields), body.get("limit")
    if view(a) != view(b):
        return None
    loki_fields = [f.get("label") for f in (b.get("fields") or [])]
    order = [f.get("label") for f in (a.get("fields") or [])] != loki_fields
    return _join(EXTRACTED if any(_extracted(x) for x in loki_fields) else "", _envelope(a, b),
                 "Loki lists fields in no fixed order" if order else "")


def _detected_labels(a, b):
    def labels(body):
        return {x.get("label"): x.get("cardinality") for x in (body.get("detectedLabels") or [])}
    pa, lb = labels(a), labels(b)
    if not lb or sorted(pa) != sorted(lb):
        return None
    cardinality = "" if pa == lb else ("sampled cardinality: Loki counts every stream its ingesters hold, not the requested window "
                                      "(pkg/ingester/instance.go LabelsWithValues), the proxy the window's last 5 minutes "
                                      "(metadataMaxFieldNamesWindow); the label names match")
    order = [x.get("label") for x in (a.get("detectedLabels") or [])] != [x.get("label") for x in (b.get("detectedLabels") or [])]
    return _join(cardinality, _envelope(a, b), "Loki lists labels in no fixed order" if order else "")


def _index_stats(a, b):
    if {k: a.get(k) for k in ("streams", "entries")} != {k: b.get(k) for k in ("streams", "entries")}:
        return None
    return "bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match"


def _index_volume(a, b):
    def series(body):
        return sorted(json.dumps(x.get("metric"), sort_keys=True) for x in ((body.get("data") or {}).get("result") or []))
    if not series(b) or series(a) != series(b):
        return None
    return ("volume bytes: VictoriaLogs sums stored line lengths, Loki its chunks' ingested bytes with structured metadata "
            "(registry loki_api_v1_index_volume, vl_cannot); the series match")


def _drilldown_limits(a, b):
    if sorted((a.get("limits") or {})) != sorted((b.get("limits") or {})):
        return None
    return "configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)"


RESOURCE_RULES = (("/resources/detected_fields", _detected_fields), ("/resources/detected_labels", _detected_labels),
                  ("/resources/index/stats", _index_stats), ("/resources/index/volume", _index_volume),
                  ("/resources/drilldown-limits", _drilldown_limits))


def _log_frames(a, b):
    """Log frames that match once Loki's _extracted and parse-error labels are set aside (and the row id, which Grafana
    derives from the labels)."""
    fa, fb = a.get("frames") or [], b.get("frames") or []
    if not fa or len(fa) != len(fb):
        return None
    changed = errors = False
    for x, y in zip(fa, fb):
        names = [f.get("name") for f in x["schema"]["fields"]]
        if names != [f.get("name") for f in y["schema"]["fields"]] or not {"labels", "labelTypes"} <= set(names):
            return None
        if signature(x["schema"]) != signature(y["schema"]):
            return None
        cx, cy = dict(zip(names, x["data"]["values"])), dict(zip(names, y["data"]["values"]))
        if any(cx[name] != cy[name] for name in names if name not in ("labels", "labelTypes", "id")):
            return None
        for lx, tx, ly, ty in zip(cx["labels"], cx["labelTypes"], cy["labels"], cy["labelTypes"]):
            lx, tx, ly, ty = dict(lx or {}), dict(tx or {}), dict(ly or {}), dict(ty or {})
            for k in [k for k in ly if _extracted(k) and k not in lx]:  # an _extracted label the PR returns is compared
                base = k[: -len("_extracted")]
                ly.pop(k)
                ty.pop(k, None)
                if tx.get(base) in ("S", "P") and ty.get(base) == "I":
                    tx[base] = "I"  # the proxy types the stream label as the metadata or parsed key it collides with
                changed = True
            for k in [k for k in ly if k in PARSE_ERROR_LABELS and k not in lx]:
                ly.pop(k)
                ty.pop(k, None)
                errors = True
            if (lx, tx) != (ly, ty):
                return None
    return _join(EXTRACTED if changed else "", PARSE_ERROR if errors else "")


def explain_one_sided(key):
    """Why a request only one side issued is by design, or None. Logs Drilldown breaks a field down by the name
    detected_fields gives it, so a field only Loki lists under its _extracted name is broken down on Loki only."""
    if key[0] == "query" and re.search(r"\bby \(\w+_extracted\)", str(key[2] or "")):
        return EXTRACTED
    return None


def explain_vs_loki(key, a, b):
    """Why the PR's answer a differs from Loki's answer b by design, or None (a real difference)."""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return None
    if key[0] == "query":
        return _log_frames(a, b)
    for fragment, rule in RESOURCE_RULES:
        if fragment in key[1]:
            return rule(a, b)
    return None


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
                             loki_compared=True, points_main=n, points_pr=n, points_loki=n, ui_main=ui_of(d, "main"), ui_pr=ui_of(d, "pr"), ui_loki=ui_of(d, "loki"),
                             settled_pr=True, errors_pr=[], errors_main=[], loki_missing=False))
            continue
        raw = {n: defaultdict(list) for n in ("main", "pr", "loki")}
        sm, m = records(os.path.join(d, "main.json"), raw["main"])
        sp, p = records(os.path.join(d, "pr.json"), raw["pr"])
        settle = {n: round(load_json(os.path.join(d, f"{n}.json")).get("settle_ms", 0) / 1000, 1) for n in ("main", "pr")}
        has_loki = os.path.exists(os.path.join(d, "loki.json"))
        sl, l = records(os.path.join(d, "loki.json"), raw["loki"]) if has_loki else (True, {})
        n, ok, diffs, series, miss, _ = compare(m, p)
        loki_ok = has_loki and spec["ranges"][rng] <= a.loki_seconds
        explained, lnondet = [], []
        if loki_ok:
            ln, lok, _, _, _, _ = compare(p, l)
            _, lok2, ldiffs2, _, _, explained = compare(p, l, lenient=True, explain=(raw["pr"], raw["loki"]))
            _, _, mdiffs, _, _, mexplained = compare(m, l, lenient=True, explain=(raw["main"], raw["loki"]))
            # A difference the base needs a by-design explanation for and the PR does not (the PR now answers like
            # Loki there) still counts against the base, so a fix towards Loki reads as improved.
            def what(x):  # the request and its reasons, without window timestamps
                return re.sub(r"\d{10,}", "#", x)
            pr_explained = {what(x) for x in explained}
            mdiffs += [x for x in mexplained if what(x) not in pr_explained]
            # Patterns are mined from the queries the proxy served (and this Loki has no pattern answer): reported, not counted.
            lnondet = [x for x in ldiffs2 if nondeterministic(x)]
            ldiffs2 = [x for x in ldiffs2 if not nondeterministic(x)]
            mdiffs = [x for x in mdiffs if not nondeterministic(x)]
            vs = (f"{lok2}/{ln} identical" + (f" ({lok}/{ln} counting zero-filled points)" if lok != lok2 else "")
                  + (f", {len(explained)} explained" if explained else "") + (f", {len(lnondet)} history-dependent" if lnondet else "")
                  + (f", {len(ldiffs2)} unexplained" if ldiffs2 else "")
                  + (f", {lmiss} request(s) on one side only" if (lmiss := sum("on one side" in x for x in ldiffs2)) else ""))
        else:
            ln, lok, ldiffs2, mdiffs, vs = 0, 0, [], [], ("n/a (Loki holds 1.5h)" if has_loki else "n/a (Loki not captured)")
        rows.append(dict(page=page, range=rng, requests=n, series=series, main_vs_pr=f"{ok}/{n}" + (f", {miss} one-sided" if miss else ""), pr_vs_loki=vs,
                         settled=all((sm, sp, sl)), settle_s=settle, main_pr_diffs=[x for x in diffs if not nondeterministic(x) and not configuration(x)], main_pr_nondet=[x for x in diffs if nondeterministic(x)], main_pr_config=[x for x in diffs if configuration(x)], loki_diffs=ldiffs2 if loki_ok else [],
                         loki_explained=explained, loki_nondet=lnondet,
                         loki_compared=bool(loki_ok), loki_main_n=len(mdiffs), loki_new=[x for x in ldiffs2 if x not in set(mdiffs)] if loki_ok else [],
                         points_main=points(m), points_pr=points(p), points_loki=points(l), ui_main=ui_of(d, "main"), ui_pr=ui_of(d, "pr"), ui_loki=ui_of(d, "loki"),
                         # An error the PR answers exactly as Loki answers it (a query Loki rejects) is Loki's answer.
                         settled_pr=bool(sp), errors_pr=[e for e in errors(p, allowed) if not loki_ok or e not in errors(l, allowed)],
                         errors_main=errors(m, allowed), errors_loki=errors(l, allowed) if loki_ok else [],
                         loki_missing=bool(not has_loki and spec["ranges"][rng] <= a.loki_seconds)))
    md = ["| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |", "|---|---|---|---|---|---|"]
    for r in rows:
        md.append(f"| {r['page']} | {r['range']} | {r['requests']} | {r['main_vs_pr']} | {r['pr_vs_loki']} | {'yes' if r['settled'] else 'NO'} |")
    for r in rows:
        if r["main_pr_diffs"] or r.get("main_pr_nondet") or r.get("main_pr_config") or r["loki_diffs"] or r.get("loki_explained") or r.get("loki_nondet"):
            md += ["", f"### {r['page']} {r['range']}"]
            md += [f"- main vs PR: {x}" for x in r["main_pr_diffs"][:8]] + [f"- main vs PR (history-dependent, not gated): {x}" for x in r.get("main_pr_nondet", [])[:4]] + [f"- main vs PR (deployment configuration, not gated): {x}" for x in r.get("main_pr_config", [])[:4]] + [f"- PR vs Loki: {x}" for x in r["loki_diffs"][:8]]
            md += [f"- PR vs Loki (explained): {x}" for x in r.get("loki_explained", [])[:12]] + [f"- PR vs Loki (history-dependent, not counted): {x}" for x in r.get("loki_nondet", [])[:4]]
    write_text(os.path.join(a.out, "compare.md"), "\n".join(md) + "\n")
    dump_json(os.path.join(a.out, "compare.json"), rows)
    print("\n".join(md[:200]))
    bad = [r for r in rows if r["main_pr_diffs"]]
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
