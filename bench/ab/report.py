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

  Add --refresh REFRAW.json (a perf_matrix.py --refresh-s run of the same set)
  to report the refresh p50 per target next to cold and warm.

A row records Loki's own cache counters ("cache") and the targets whose cold
run Loki says its results cache answered ("cold_cache_served"); statsResult, the
index-stats cache Loki uses for query planning, is not an answer and is ignored.
Rows from older runs carry none of these: they are left out, meaning unknown,
never false.

Results are compared with the reference only on the runs it computed. A reference
run its results cache answered whole (every requested entry found) is Loki's
cached extents put together for a window moved by a minute, and a ranked
(topk/bottomk) reference run with more points than k per step
("ranked_max_points", written by perf_matrix.py) cannot be a computed answer:
both are left out and counted ("reference_skipped"). A row with no computed
reference run has no result verdict (n/a). Timing is unchanged.

Timing verdicts use warm p50 (runs after the first); the first run is reported
as cold. A shape is slower or faster only when the p50 moves by more than
--noise (relative, default 25%) and --min-delta seconds (default 0.05).
"""
import argparse
import datetime
import json
import math
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reuse  # noqa: E402

SIG_RE = re.compile(r"series=(\d+|ranked) points=(\d+) sum=(\S+)")
NOT_NUMBERS_RE = re.compile(r"not_numbers=(\d+)")
SKIP_REASONS = {"cache": "run(s) its results cache answered whole",
                "ranked": "ranked run(s) with more points than k per step"}


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


def not_numbers(signature):
    """The count of samples that were not numbers, "0" when the signature has none."""
    found = NOT_NUMBERS_RE.search(signature or "")
    return found.group(1) if found else "0"


def cache_served(stats):
    """True when Loki's counters say a results cache answered (part of) the request.

    statsResult is the index-stats cache used for query planning: it is hit on
    almost every request and is not a cached answer, so it never counts. Every
    other sub-cache (result, volumeResult, seriesResult, labelResult,
    instantMetricResult) does.
    """
    return any((sub.get("entriesFound") or 0) > 0 or (sub.get("bytesReceived") or 0) > 0
               for name, sub in (stats or {}).items() if name != "statsResult")


def computed_correctness_cache(stats):
    """Strict evidence for EXTRA correctness reads only; measured cache policies stay unchanged."""
    if not isinstance(stats, dict) or not stats or "result" not in stats:
        return False

    def counter(value):
        return type(value) is int and value >= 0 or (
            type(value) is float and math.isfinite(value) and value >= 0 and value.is_integer())

    for name, bucket in stats.items():
        if not isinstance(name, str):
            return False
        if name == "statsResult" or name != "result" and not name.endswith("Result"):
            continue  # planning cache hits are not result reuse
        if (not isinstance(bucket, dict)
                or not all(counter(bucket.get(key)) for key in ("entriesFound", "entriesRequested"))
                or "bytesReceived" in bucket and not counter(bucket["bytesReceived"])
                or bucket["entriesFound"] != 0 or bucket.get("bytesReceived", 0) != 0):
            return False
    return True


def correctness_cache_projection(stats):
    """Validate raw correctness evidence, then project only legacy result-cache fields."""
    if not computed_correctness_cache(stats):
        return None
    return {name: {key: bucket[key] for key in ("entriesFound", "entriesRequested", "bytesReceived") if key in bucket}
            for name, bucket in stats.items()
            if (name == "result" or name.endswith("Result")) and isinstance(bucket, dict)}


def correctness_parity(shape, facets):
    """Only the registry's existing, narrowly tagged API deviations are accepted visibly."""
    if not isinstance(facets, list) or any(not isinstance(f, dict) for f in facets):
        return "differs"
    if not facets:
        return "same"
    from answers import correctness_endpoint
    from bench.parity import differ
    from conformance.scripts import parity_gaps
    if not hasattr(correctness_parity, "statuses"):
        correctness_parity.statuses = {c["id"]: c["status"] for c in parity_gaps.cases(
            str(Path(__file__).resolve().parents[2] / "conformance/registry"))[0]}
    endpoint = correctness_endpoint(shape)
    allowed = {("index_stats", "stats", "bytes differ"): differ.DOC_INDEX_STATS_BYTES,
               ("index_stats", "stats", "chunks differ"): differ.DOC_INDEX_STATS_BYTES,
               ("detected_labels", "cardinality", "label cardinality differs"): differ.DOC_DETECTED_LABELS_CARDINALITY,
               ("detected_fields", "field", "loki-only service *_extracted"): differ.DOC_DETECTED_FIELDS_SERVICE}
    for facet in facets:
        expected = differ.DOC_VOLUME_BYTES if endpoint in ("volume", "volume_range") and facet.get("kind") == "values" and facet.get("detail", "").startswith("values differ (") else allowed.get((endpoint, facet.get("kind"), facet.get("detail")))
        if (not expected and shape.get("documented_values") and endpoint in ("query_range", "query") and facet.get("kind") == "values"
                and facet.get("detail", "").startswith("values differ (")):
            expected = shape["documented_values"]  # the shape names the registry deviation its values differ by
        if not expected or facet.get("documented") != expected or correctness_parity.statuses.get(expected) not in ("documented", "owner-kept", "upstream"):
            return "differs"
    return "documented"


def metadata_observation(shape, receipt):
    from answers import metadata_endpoint
    return (metadata_endpoint(shape) and receipt.get("reference_kind") == "metadata-api-response"
            and receipt.get("cache_status") == "unavailable" and receipt.get("cache_absent") is True
            and receipt.get("cache_raw") is None and receipt.get("cache") is None)


def cache_answered_whole(stats):
    """True when Loki's counters say its results cache held every entry the request asked for: the answer is
    Loki's cached extents put together for a moved window, not one Loki computed.

    A shifted window answered this way is not a reference for the answer: merged extents can repeat a step with
    other ranked members (a topk with more than k series at a step) or drop a step's sample. statsResult, the
    index-stats cache, is never an answer and is ignored; a request with no counters (older runs, endpoints that
    report none) counts as computed."""
    asked = [sub for name, sub in (stats or {}).items() if name != "statsResult" and (sub.get("entriesRequested") or 0) > 0]
    return bool(asked) and all((sub.get("entriesFound") or 0) >= sub["entriesRequested"] for sub in asked)


def points_of(signature):
    """The point count of a result signature, or None."""
    found = SIG_RE.match(signature or "")
    return int(found.group(2)) if found else None


def reference_skip(row):
    """Why a reference (Loki) run is no reference for the answer, or "": its results cache answered it whole, or
    its ranked (topk/bottomk) answer holds more points than k per step, which no computed answer can."""
    if cache_answered_whole(row.get("cache")):
        return "cache"
    limit, points = row.get("ranked_max_points"), points_of(row.get("signature"))
    if limit is not None and points is not None and points > limit:
        return "ranked"
    return ""


def sum_cache(rows):
    """Loki's cache counters summed over several requests, per sub-cache."""
    total = {}
    for r in rows:
        for name, sub in (r.get("cache") or {}).items():
            acc = total.setdefault(name, {})
            for k, v in sub.items():
                acc[k] = acc.get(k, 0) + (v or 0)
    return total or None


def refresh_timings(raw, reference="loki", candidate="pr", tolerance=0):
    """(shape, range) -> {target: p50 over the runs after the first} and the statuses of a refresh run."""
    grouped = {}
    for r in raw["rows"]:
        grouped.setdefault((r["shape"], r["range"]), {}).setdefault(r["target"], []).append(r)
    out = {}
    for key, by_target in grouped.items():
        p50, status = {}, {}
        for t, rs in by_target.items():
            rs.sort(key=lambda r: r["run"])
            p50[t] = round(pct([r["seconds"] for r in rs[1:]] or [rs[0]["seconds"]], 0.5), 3)
            status[t] = "/".join(sorted({str(r["status"]) for r in rs}))
        metered = {t: reuse.median([r.get("reuse") for r in rs]) for t, rs in by_target.items()}
        out[key] = {"p50": p50, "status": status, "reuse": {t: d for t, d in metered.items() if d is not None}}
        if raw.get("runtime_proof"):
            out[key]["samples"] = request_samples(by_target, raw["runs"], reference, candidate, tolerance)
            out[key]["original_window_correctness"] = original_window(by_target, reference, raw.get("target_urls"))
    return out


def parity(a, b, tolerance):
    """Compare two result signatures of the same window."""
    if a == b:
        return "same"
    ma, mb = SIG_RE.match(a or ""), SIG_RE.match(b or "")
    # A sample that is not a number is a wrong answer whatever the sums say.
    if not_numbers(a) != not_numbers(b):
        return "differs"
    # Sum tolerance never excuses missing samples, including ranked answers.
    if ma and mb and ma.group(1) == mb.group(1) and ma.group(2) == mb.group(2):
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


def request_samples(by_target, runs, reference, candidate, tolerance=0):
    """Check every measured request before aggregating. A later run cannot fill an earlier hole."""
    refs = {r["run"]: r for r in by_target.get(reference, [])}
    candidates = {r["run"]: r for r in by_target.get(candidate, [])}
    evidence = {}
    for target, rows in by_target.items():
        good, starts, ends = [], [], []
        resource_runs = 0
        corrected_rejections = []
        for r in rows:
            seconds = r.get("seconds")
            proof = r.get("answer_proof")
            ref = (candidates if target == reference else refs).get(r["run"], {})
            matched_error = (isinstance(proof, str) and proof.startswith("rejection:") and
                             (proof, r["status"]) == (ref.get("answer_proof"), ref.get("status")))
            cand = candidates.get(r["run"], {})
            # A base that rejects the query, or fails on it with an internal error, while the candidate and the reference
            # both answer alike: a fix towards the reference. An internal error is accepted below only as a whole
            # (every run, one fingerprint), so a transient failure of the base is never mistaken for one.
            corrected = (target not in (reference, candidate) and isinstance(proof, str)
                         and (proof.startswith("rejection:") and r["status"] in (400, 422, 500)
                              or proof.startswith("failure:") and r["status"] == 500)
                         and all(s.get("status") == 200 and s.get("answer_proof") == "answer" and s.get("signature")
                                 and not s.get("skipped") for s in (ref, cand))
                         and parity(ref["signature"], cand["signature"], tolerance) != "differs")
            if (not r.get("skipped") and type(seconds) in (int, float) and math.isfinite(seconds) and seconds >= 0
                    and type(r.get("response_bytes")) is int and r["response_bytes"] > 0
                    and type(r.get("history_start")) is int and type(r.get("end")) is int
                    and (r["history_start"], r["end"]) == (ref.get("history_start"), ref.get("end"))
                    and (proof == "answer" and r["status"] == 200 or matched_error or corrected)):
                good.append(r["run"])
                if corrected:
                    corrected_rejections.append({"run": r["run"], "status": r["status"], "proof": proof,
                                                 "signature": r["signature"]})
                starts.append(r["history_start"])
                ends.append(r["end"])
            cost = r.get("reuse") or {}
            resource_runs += int(cost.get("cpu_samples") == cost.get("rss_samples") == 2 and
                                 all(type(cost.get(k)) in (int, float) and math.isfinite(cost[k]) and cost[k] >= 0
                                     for k in ("cpu_seconds", "rss_max_sampled_bytes")))
        failures = [c for c in corrected_rejections if c["proof"].startswith("failure:")]
        if failures and (len(failures) != len(rows) or len({c["proof"] for c in failures}) != 1):
            # Not one deterministic failure on every run: the base may only have been unwell for a run.
            good = [run for run in good if run not in {c["run"] for c in failures}]
        evidence[target] = {"complete": sorted(good) == list(range(runs)) and len(rows) == runs,
                            "measured": len(good), "resource_runs": resource_runs,
                            "start": min(starts, default=None), "end": max(ends, default=None)}
        if corrected_rejections:
            evidence[target]["corrected_rejections"] = corrected_rejections
    return evidence


def original_window(by_target, reference, target_urls=None):
    first = {t: next((r for r in rows if r["run"] == 0), {}) for t, rows in by_target.items()}
    proof = first.get(reference, {}).get("original_window_correctness")
    return {"proof": proof, "target_urls": target_urls, "measured": {t: {k: row.get(k) for k in
            ("status", "end", "history_start", "response_bytes", "body_sha256", "answer_proof", "request_url")}
            for t, row in first.items()}} if proof is not None else None


def summarize(args):
    raw = load(args.raw)
    rows = raw["rows"]
    refresh_raw = load(args.refresh) if getattr(args, "refresh", "") else None
    refresh = refresh_timings(refresh_raw, args.reference, args.candidate, args.tolerance) if refresh_raw else {}
    keys = []
    for r in rows:
        k = (r["shape"], r["range"])
        if k not in keys:
            keys.append(k)
    # Targets that did not run the setting of the Loki profile the pass was made against (perf_matrix.py
    # --not-like-for-like): their timings are shown, marked, and never compared.
    unlike = set(raw.get("not_like_for_like") or [])
    # perf_matrix.py --exact-repeat: what the second of two identical asks at a settled window cost, per target.
    exact = {(e["shape"], e["range"], e["target"]): e for e in raw.get("exact_repeats") or []}
    out = []
    for shape, rng in keys:
        entry = {"shape": shape, "range": rng, "p50": {}, "p95": {}, "cold": {}, "status": {}, "vl_cpu_max": {}}
        by_target = {}
        for r in rows:
            if (r["shape"], r["range"]) == (shape, rng):
                by_target.setdefault(r["target"], []).append(r)
        if raw.get("runtime_proof"):
            entry["runtime_samples"] = {"matrix": request_samples(by_target, raw["runs"], args.reference, args.candidate, args.tolerance)}
            if (original := original_window(by_target, args.reference, raw.get("target_urls"))) is not None:
                entry["original_window_correctness"] = {"matrix": original}
        for t, rs in by_target.items():
            rs.sort(key=lambda r: r["run"])
            warm = [r["seconds"] for r in rs[1:]] or [rs[0]["seconds"]]
            entry["cold"][t] = rs[0]["seconds"]
            entry["p50"][t] = round(pct(warm, 0.5), 3)
            entry["p95"][t] = round(pct(warm, 0.95), 3)
            if args.cold:
                entry["p50"][t] = rs[0]["seconds"]
            entry["status"][t] = "/".join(sorted({str(r["status"]) for r in rs}))
            sizes = [r["response_bytes"] for r in rs if isinstance(r.get("response_bytes"), (int, float))]
            if sizes:
                entry.setdefault("response_bytes", {})[t] = max(sizes)
            entry["vl_cpu_max"][t] = max((r["vl_cpu_max"] or 0) for r in rs)
            # The proxy's own counters (perf_matrix.py --proxy-metrics): the first run's cost, the later runs' median.
            if any(r.get("reuse") is not None for r in rs):
                entry.setdefault("reuse", {})[t] = {"cold": rs[0].get("reuse"), "repeat": reuse.median([r.get("reuse") for r in rs[1:]])}
            cold_stats = rs[0].get("cache")
            if cold_stats or any(r.get("cache") for r in rs[1:]):
                entry.setdefault("cache", {})[t] = {"cold": cold_stats, "warm": sum_cache(rs[1:])}
            if cache_served(cold_stats):
                entry.setdefault("cold_cache_served", []).append(t)
            # Each run moves the window by a minute while logs keep arriving, so a
            # correct answer changes between runs. Identical answers for different
            # windows mean a cache or a stale view answered: not a latency reference.
            ok = [r for r in rs if r["status"] == 200 and r.get("end") is not None]
            if len({r["end"] for r in ok}) > 1 and len({r["signature"] for r in ok}) == 1:
                entry.setdefault("stale", []).append(t)

        def sig_by_run(t):
            return {r["run"]: r["signature"] for r in by_target.get(t, [])}

        cand, base, ref = sig_by_run(args.candidate), sig_by_run(args.baseline), sig_by_run(args.reference)
        # Only the reference runs Loki computed are compared; the others are counted by reason.
        skipped = {}
        for r in by_target.get(args.reference, []):
            reason = reference_skip(r)
            if reason:
                skipped[reason] = skipped.get(reason, 0) + 1
                ref.pop(r["run"], None)
        if skipped:
            entry["reference_skipped"] = skipped
        vs_ref = [parity(ref[run], cand[run], args.tolerance) for run in cand if run in ref]
        entry["parity_vs_reference"] = "differs" if "differs" in vs_ref else (max(vs_ref, key=len) if vs_ref else "n/a")
        base_vs_ref = [parity(ref[run], base[run], args.tolerance) for run in base if run in ref]
        entry["baseline_parity_vs_reference"] = "differs" if "differs" in base_vs_ref else ("same" if base_vs_ref else "n/a")
        vs_base = [parity(base[run], cand[run], args.tolerance) for run in cand if run in base
                   and not base[run].startswith("ERR")]
        entry["parity_vs_baseline"] = "differs" if "differs" in vs_base else (max(vs_base, key=len) if vs_base else "n/a")

        b_ok = entry["status"].get(args.baseline) == "200"
        c_ok = entry["status"].get(args.candidate) == "200"
        # A candidate that now fails exactly as the reference (Loki) does - same
        # status, same error on every run - is a fix towards Loki, not a break:
        # the baseline answered a query Loki rejects.
        c_matches_ref_error = (not c_ok and entry["status"].get(args.candidate) == entry["status"].get(args.reference)
                               and bool(vs_ref) and all(v == "same" for v in vs_ref))
        if not b_ok and c_ok:
            verdict = "fixed"
            evidence = entry.get("runtime_samples", {}).get("matrix", {}).get(args.baseline, {})
            if evidence.get("complete") and len(evidence.get("corrected_rejections", [])) == raw.get("runs"):
                failed = any(c["proof"].startswith("failure:") for c in evidence["corrected_rejections"])
                entry["base_comparison_note"] = ("Base-relative cost and speedup unavailable: base failed on the query with an internal error."
                                                 if failed else
                                                 "Base-relative cost and speedup unavailable: base rejected the query.")
        elif b_ok and c_matches_ref_error:
            verdict = "fixed"
            entry["note"] = "now rejected exactly as the reference rejects it"
        elif b_ok and not c_ok:
            verdict = "broken"
        elif unlike & {args.baseline, args.candidate}:
            verdict = "n/a"
            entry["note"] = "not like-for-like with the Loki profile: " + ", ".join(
                sorted(unlike & {args.baseline, args.candidate}))
        else:
            verdict = timing_verdict(entry["p50"].get(args.baseline), entry["p50"].get(args.candidate),
                                     args.noise, args.min_delta)
        entry["verdict"] = verdict
        for (e_shape, e_rng, t), e in exact.items():
            if (e_shape, e_rng) != (shape, rng):
                continue
            slot = entry.setdefault("reuse", {}).setdefault(t, {})
            if e.get("problem"):
                slot["exact_repeat_problem"] = e["problem"]
            else:
                # The fill is the settled window's cold cost; the repeat is what the cache is expected to answer alone.
                slot["exact_fill"], slot["exact_repeat"] = e["fill"]["reuse"], e["repeat"]["reuse"]
        if (shape, rng) in refresh:
            if refresh_raw.get("runtime_proof"):
                entry.setdefault("runtime_samples", {})["refresh"] = refresh[(shape, rng)]["samples"]
                entry.setdefault("original_window_correctness", {})["refresh"] = refresh[(shape, rng)].get("original_window_correctness")
            entry["refresh"], entry["refresh_status"] = refresh[(shape, rng)]["p50"], refresh[(shape, rng)]["status"]
            for t, d in refresh[(shape, rng)]["reuse"].items():
                entry.setdefault("reuse", {}).setdefault(t, {})["refresh"] = d
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
               "reference": args.reference, "profile": raw.get("profile") or None,
               "not_like_for_like": sorted(unlike), "runs": raw.get("runs"), "valid": raw.get("valid", True) and (refresh_raw.get("valid", True) if refresh_raw else True),
               "restart_before": raw.get("restart_before"), "restart_after": raw.get("restart_after"),
               "noise": args.noise, "refresh_s": refresh_raw.get("refresh_s") if refresh_raw else None,
               "refresh_runs": refresh_raw.get("runs") if refresh_raw else None, "timing": "cold" if args.cold else "warm", "verdicts": counts, "result_differs_from_reference": differs,
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
    # A non-200 answer still has a time: the time is shown, with the status code in parentheses.
    text = "—" if v is None else f"{v:.2f}s" + ("†" if stale else "")
    if status and str(status) != "200":
        return f"{text} (**{status}**)"
    return text


def markdown(s):
    b, c, r = s["baseline"], s["candidate"], s["reference"]
    has_refresh = any("refresh" in e for e in s["rows"])
    unlike = set(s.get("not_like_for_like") or [])
    mark = lambda t, text: text + (" ‡" if t in unlike else "")  # noqa: E731 - a timing of a build that is not like-for-like
    lines = [f"**{s['set']}** — {s['label']} ({s['date']}, {s['runs']} runs"
             f"{', Loki profile ' + s['profile'] if s.get('profile') else ''}, "
             f"{'cold (first run)' if s.get('timing') == 'cold' else 'warm p50'}; "
             f"VictoriaLogs restarts {s['restart_before']}→{s['restart_after']}{'' if s['valid'] else ', INVALID'})", "",
             f"| shape | range | {b} | {c} | {r} | change | result vs {r} |", "|---|---|---|---|---|---|---|"]
    if has_refresh:
        lines[2] += f" refresh {b} | refresh {c} | refresh {r} |"
        lines[3] += "---|---|---|"
    for e in s["rows"]:
        change = {"fixed": "fixed", "broken": "**broken**", "slower": "**slower**", "n/a": "n/a"}.get(e["verdict"])
        if change is None:
            change = f"{e['speedup']}×" if e["speedup"] and e["verdict"] == "faster" else "same"
        st = e.get("stale", [])
        lines.append(f"| {cell(e['shape'])} | {e['range']} | {mark(b, fmt(e['p50'].get(b), e['status'].get(b), b in st))} | "
                     f"{mark(c, fmt(e['p50'].get(c), e['status'].get(c), c in st))} | "
                     f"{fmt(e['p50'].get(r), e['status'].get(r), r in st)} | {change} | {e['parity_vs_reference']} |"
                     + "".join(f" {fmt(e.get('refresh', {}).get(t), e.get('refresh_status', {}).get(t))} |"
                               for t in (b, c, r) if has_refresh))
    stale = {t: sum(1 for e in s["rows"] if t in e.get("stale", [])) for t in (b, c, r)}
    served = sum(1 for e in s["rows"] if r in e.get("cold_cache_served", []))
    if served:
        lines += ["", f"{served} of {len(s['rows'])} {r} cold runs were answered from its results cache "
                  "(its own counters say so): not a computed cold time."]
    v = s["verdicts"]
    if unlike:
        lines += ["", f"‡ {', '.join(sorted(unlike))}: this build did not run the setting of the Loki profile "
                  f"({s.get('profile')}), so its timings are not like-for-like with Loki's and are never compared."]
    skipped = {}
    for e in s["rows"]:
        for reason, n in (e.get("reference_skipped") or {}).items():
            skipped[reason] = skipped.get(reason, 0) + n
    if skipped:
        unchecked = sum(1 for e in s["rows"] if e["parity_vs_reference"] == "n/a" and e.get("reference_skipped"))
        lines += ["", f"Results are compared only with the {r} runs {r} computed: "
                  + ", ".join(f"{n} {SKIP_REASONS[k]}" for k, n in sorted(skipped.items()))
                  + " left out" + (f"; {unchecked} row(s) had no computed {r} run, so their result is n/a." if unchecked else ".")]
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
    s.add_argument("--refresh", default="",
                   help="raw rows of a perf_matrix.py --refresh-s run of the same set; adds the refresh p50")
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
