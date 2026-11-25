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
from functools import lru_cache
from pathlib import Path

from vio import dump_json, error_text, load_json, write_text

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from bench.ab.answers import query_rejection  # noqa: E402


# Request fields that differ between the three datasources or between page loads without changing the question.
VOLATILE = {"datasource", "datasourceId", "requestId", "key", "uid", "queryCachingTTL"}
# Frame meta that says how the answer was produced rather than what it is (stats, the executed query text).
META_STABLE = ("type", "typeVersion", "preferredVisualisationType", "custom", "notices")


def runtime_problems(main, pr, required=False, comparable=True, client_error=None):
    """Existing captures supply runtime proof; absent measurements are never zero.

    Resource/settlement changes beyond both noise guards enter the existing
    re-capture and verdict path, rather than introducing another benchmark run.
    """
    if not required:
        return []
    fields = ("browser_thread_seconds", "browser_task_seconds", "js_heap_max_sampled_bytes",
              "metric_samples", "api_requests", "api_encoded_bytes", "page_encoded_bytes")
    problems = []
    for side, capture in (("main", main), ("PR", pr)):
        runtime = capture.get("runtime") or {}
        settle = capture.get("settle_ms")
        if isinstance(settle, bool) or not isinstance(settle, (int, float)) or not math.isfinite(settle) or settle < 0:
            problems.append(f"runtime proof: {side} missing/invalid settle_ms")
        if capture.get("settled") is not True:
            problems.append(f"runtime proof: {side} did not settle")
        # Banners are matched on the page text, which includes rendered log lines. The crash of a panel in Chrome
        # reads "Cannot read properties of undefined"; the generator's seeded line "TypeError: Cannot read
        # property" is log content, and it is on Loki's page as much as on the proxy's.
        banners = (capture.get("ui") or {}).get("banners", [])
        exceptions = capture.get("client_exceptions")
        has_exception = (any(str(error).startswith("pageerror:") for error in capture.get("errors", []))
                         or isinstance(exceptions, dict) and bool(exceptions.get("events") or exceptions.get("playwright_page_error_count")))
        if has_exception and (not isinstance(client_error, _ExpectedClientError) or not client_error.matches(capture)):
            problems.append(f"runtime proof: {side} Grafana JavaScript page error")
        if exceptions is not None and (not isinstance(exceptions, dict) or exceptions.get("complete") is not True
                                       or exceptions.get("overflow") is not False):
            problems.append(f"runtime proof: {side} JavaScript exception capture is incomplete")
        if any(re.search(r"plugin (?:failed|unavailable|not found)|bad gateway|internal server error|"
                         r"page state unreadable|cannot read properties of|unexpected error", str(b), re.I) for b in banners):
            problems.append(f"runtime proof: {side} Grafana UI infrastructure error")
        nonempty = any(record.get("status") == 200 and has_data(record.get("response"))
                       for record in capture.get("records", []))
        for key in (*fields, *(("first_api_data_ms",) if nonempty else ())):
            value = runtime.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                problems.append(f"runtime proof: {side} missing/invalid {key}")
        first = runtime.get("first_api_data_ms")
        if isinstance(first, (float, int)) and isinstance(settle, (float, int)) and first > settle:
            problems.append(f"runtime proof: {side} first API data after settlement")
        if runtime.get("metric_samples") in (0, 1) or runtime.get("js_heap_max_sampled_bytes") == 0:
            problems.append(f"runtime proof: {side} browser heap was not sampled")
        if runtime.get("api_requests") == 0 or runtime.get("sampling_error"):
            problems.append(f"runtime proof: {side} capture did not measure API work successfully")
    if problems or not comparable:
        return problems
    for key, minimum, label in (
        ("browser_thread_seconds", 0.25, "browser thread CPU"),
        ("js_heap_max_sampled_bytes", 32 << 20, "sampled browser heap"),
        ("api_encoded_bytes", 64 << 10, "API encoded bytes"),
    ):
        before, after = main["runtime"][key], pr["runtime"][key]
        if after > before * 1.25 and after - before > minimum:
            problems.append(f"runtime regression: {label} {before:g} -> {after:g}")
    before, after = main.get("settle_ms"), pr.get("settle_ms")
    if isinstance(before, (int, float)) and isinstance(after, (int, float)) and after > before * 1.25 and after - before > 500:
        problems.append(f"runtime regression: Grafana settlement {before:g} -> {after:g} ms")
    return problems


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


# Grafana names each "Show context" query with a random suffix (log-row-context-query-_0.4374…), so the same
# query from two datasources would never pair up: the suffix is dropped (the direction field keeps the two apart).
RANDOM_REF = re.compile(r"^(log-row-context-query)-_?[0-9.]+$")


def stable_ref(ref):
    return RANDOM_REF.sub(r"\1", ref) if isinstance(ref, str) else ref


def frames_of(resp):
    out = {}
    for ref, res in (resp or {}).get("results", {}).items():
        ref = stable_ref(ref)
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


# A request the page itself cancelled (Chrome net::ERR_ABORTED) has no answer. Whether the answer or the cancel
# comes first is a race of the page, so a request cancelled on one side and answered on the other is reported, never
# gated (the page does the same on Loki direct); cancelled on both sides it is equal.
CANCELLED = "request cancelled by the page"


def query_key(r, q):
    sq = {**q, "refId": stable_ref(q.get("refId"))}
    return ("query", sq["refId"], q.get("expr"), q.get("queryType"), r["request"].get("from"),
            r["request"].get("to"), digest(strip(sq)))


def resource_key(r):
    return ("resource", re.sub(r"(/api/datasources/(?:proxy/uid|uid|proxy))/[^/?]+", r"\1/*",
                               r["url"].split("&_=")[0]))


CLIENT_ERROR_CONTRACT = "loki-metric-supporting-error-v1"
# A query Loki rejects itself (a 400 on the primary and on its supporting log-volume query, the same on all three builds)
# whose rejection the stock page throws uncaught; see _rejection_evidence.
QUERY_REJECTION_CONTRACT = "loki-query-rejection-v1"
CLIENT_ERROR_CONTRACTS = (CLIENT_ERROR_CONTRACT, QUERY_REJECTION_CONTRACT)
CLIENT_ERROR_PROOF_BYTES = 16 << 20


def capture_fingerprint(capture):
    return hashlib.sha256(json.dumps(capture, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def client_request_shape(request):
    """Only the absolute window can move; its length and every nonvolatile request field stay recorded."""
    if any(not isinstance(request.get(k), str) or not request[k].isdigit() for k in ("from", "to")):
        raise ValueError("client contract requires numeric Grafana window bounds")
    span = int(request["to"]) - int(request["from"])
    if span <= 0:
        raise ValueError("client contract requires a positive window")
    return {**strip(request), "from": "0", "to": str(span)}


def client_console_error(text):
    # The CI and local Grafana origins differ; do not normalize messages, status, paths, versions or stacks.
    return re.sub(r"https?://[^/\s]+", "", text)


class _ExpectedClientError:
    """A receipt bound to complete captures, not an exception-message allowlist."""
    def __init__(self, case_id, page, rng, captures, events, point_counts, contract=CLIENT_ERROR_CONTRACT):
        self.receipt = dict(registry_case=case_id, contract=contract,
                            state="upstream_matches_loki", scene=page, range=rng,
                            capture_sha256={side: capture_fingerprint(cap) for side, cap in captures.items()},
                            exceptions=events, primary_points=point_counts)
        self.payload = dict(receipt=self.receipt, captures=captures)
        self.payload_bytes = len(json.dumps(self.payload).encode())

    def matches(self, capture):
        return capture_fingerprint(capture) in self.receipt["capture_sha256"].values()


@lru_cache(maxsize=1)
def client_error_contracts():
    """Read the reviewed registry and its bounded proof; no extra query catalog."""
    from conformance.scripts.registry_io import load_cases
    root = Path(__file__).resolve().parents[2]
    contracts = []
    for row in load_cases(str(root / "conformance/registry")):
        case = row["doc"]
        values = case.get("compare", {}).get("values", {})
        if values.get("client_error_contract") not in CLIENT_ERROR_CONTRACTS or case.get("gap", {}).get("status") != "upstream":
            continue
        path = (root / values.get("client_error_proof", "")).resolve()
        if not path.is_relative_to(root / "bench/ab/results/oneoff") or not path.is_file() or path.stat().st_size > 256 << 10:
            raise ValueError("client error proof must be a bounded existing oneoff artifact")
        proof = load_json(path)
        if proof.get("registry_case") != case["id"]:
            raise ValueError("client error proof belongs to a different registry case")
        contracts.append((case, proof))
    return contracts


def _thrown_supporting_events(captures):
    """The one uncaught `{message, refId, status}` object of a log-volume-A rejection on every build, or None.

    The event must be complete CDP evidence (version 1, no overflow, exactly one page error whose raw index and
    identity agree) with a stack inside the stock client, so an old `pageerror: Object` string, a missing capture
    or any other exception never qualifies."""
    events = {}
    for side, cap in captures.items():
        state = cap.get("client_exceptions") or {}
        if (cap.get("settled") is not True or state.get("complete") is not True
                or state.get("version") != 1
                or state.get("overflow") is not False or type(state.get("playwright_page_error_count")) is not int
                or state["playwright_page_error_count"] != 1
                or len(state.get("events", [])) != 1):
            return None
        event = state["events"][0]
        raw = cap.get("errors", [])
        index, exception_id = event.get("raw_error_index"), event.get("exception_id")
        if (type(index) is not int or type(exception_id) is not int or exception_id <= 0
                or event.get("event_id") != f"runtime:{exception_id}"
                or [i for i, error in enumerate(raw) if isinstance(error, str) and error.startswith("pageerror:")] != [index]
                or raw[index] != "pageerror: Object" or event.get("kind") != "uncaught-object"
                or event.get("class_name") != "Object" or event.get("own_property_names") != ["message", "refId", "status"]
                or event.get("ref_id") != "log-volume-A" or type(event.get("status")) is not int or event["status"] != 400
                or not isinstance(event.get("message"), str) or len(event["message"].encode()) > 4096
                or not 1 <= len(event.get("stack", [])) <= 8):
            return None
        if any(not isinstance(frame.get("url"), str) or "/public/" not in frame["url"]
               or type(frame.get("line")) is not int or type(frame.get("column")) is not int for frame in event["stack"]):
            return None
        events[side] = event
    return events


def _supporting_error_evidence(page, rng, entry, captures, contracts=None):
    """Recognize only a recorded client failure whose complete primary answers equal Loki's.

    Old pageerror:Object strings, missing CDP evidence and any unknown event stay failures.
    All existing API, pixel and resource checks still run on the unmodified captures.
    """
    try:
        if set(captures) != {"main", "pr", "loki"} or entry.get("kind") != "explore":
            return None
        events = _thrown_supporting_events(captures)
        if events is None:
            return None
        contracts = client_error_contracts() if contracts is None else contracts
        for case, proof in contracts:
            if (case.get("gap", {}).get("status") != "upstream"
                    or case.get("compare", {}).get("values", {}).get("client_error_contract") != CLIENT_ERROR_CONTRACT
                    or proof.get("registry_case") != case.get("id") or case["id"] not in entry.get("covers", [])
                    or entry.get("id") != page):
                continue
            expected = [c for c in proof["ci"]["captures"] if c["scene_id"] == page and c["range"] == rng]
            if len(expected) != 3 or {c["side"] for c in expected} != {"main", "pr", "loki"}:
                continue
            primary, requests, supporting, supporting_requests, windows = {}, {}, [], {}, {}
            for side, cap in captures.items():
                recorded = next(c for c in expected if c["side"] == side)
                expected_queries = {q["ref_id"]: q for q in recorded["queries"]}
                if entry.get("query") != expected_queries["A"]["expression"]:
                    return None
                console = [e for e in cap["errors"] if not e.startswith("pageerror:")]
                if sorted(map(client_console_error, console)) != sorted(map(client_console_error, recorded.get("console_errors", []))):
                    return None
                seen = set()
                for record in cap["records"]:
                    if record.get("aborted") or record.get("response") is None:
                        return None
                    if "/api/ds/query" not in record["url"]:
                        if record.get("status") != 200:
                            return None
                        continue
                    queries = record["request"]["queries"]
                    if len(queries) != 1 or queries[0].get("refId") not in {"A", "log-volume-A"}:
                        return None
                    query = queries[0]
                    ref = query["refId"]
                    if ref in seen or query["expr"] != expected_queries[ref]["expression"]:
                        return None
                    if client_request_shape(record["request"]) != client_request_shape(expected_queries[ref]["request"]):
                        return None
                    windows.setdefault(side, set()).add((record["request"]["from"], record["request"]["to"]))
                    seen.add(ref)
                    if set(record["response"].get("results", {})) != {ref}:
                        return None
                    result = record["response"]["results"][ref]
                    if ref == "log-volume-A":
                        if (record["status"] != 400 or record["response"] != expected_queries[ref]["response"]
                                or result.get("status") != events[side]["status"] or result.get("error") != events[side]["message"]):
                            return None
                        supporting.append(record["response"])
                        supporting_requests[side] = query_key(record, query)
                    else:
                        if record["status"] != 200 or result.get("status", 200) != 200 or result.get("error"):
                            return None
                        for frame in result.get("frames", []):
                            columns = frame["data"]["values"]
                            if len(columns) != 2 or len(columns[0]) != len(columns[1]) or len(set(columns[0])) != len(columns[0]):
                                return None
                        primary[side] = frames_of(record["response"])
                        if len(primary[side]) != len(result.get("frames", [])):
                            return None
                        requests[side] = query_key(record, query)
                if seen != {"A", "log-volume-A"} or len(windows[side]) != 1 or not primary[side] or any(x[0] != "metric" for x in primary[side].values()):
                    return None
            if len(set(requests.values())) != 1 or len(set(supporting_requests.values())) != 1 or not supporting[0] == supporting[1] == supporting[2]:
                return None
            point_counts = {side: sum(len(v[1]) for v in data.values()) for side, data in primary.items()}
            if (not all(point_counts.values())
                    or any(not any(point is not None for series in data.values() for point in series[1].values()) for data in primary.values())
                    or same(primary["main"], primary["pr"])):
                return None
            compare_primary = same_ranked if is_ranked(requests["pr"]) else same
            if compare_primary(primary["main"], primary["loki"]) or compare_primary(primary["pr"], primary["loki"]):
                return None
            evidence = _ExpectedClientError(case["id"], page, rng, captures, events, point_counts)
            return evidence if evidence.payload_bytes <= CLIENT_ERROR_PROOF_BYTES else None
    except (AttributeError, KeyError, IndexError, TypeError, ValueError, OSError, RuntimeError):
        # A malformed/missing contract or capture does not turn an unproved exception into a match.
        return None
    return None



def _rejection_evidence(page, rng, entry, captures, contracts=None):
    """Recognize only a query Loki itself rejects, identically on all three builds, whose supporting rejection is thrown.

    Both the primary query and the stock supporting log-volume query answer 400 with the recorded complete DTO on main,
    PR and Loki, no frames are returned, the only page error is the CDP-identified uncaught object carrying the
    supporting rejection verbatim, and the page spec, request shapes and console errors equal the recorded proof. Any
    other status, DTO, event or extra request leaves the page error a failure."""
    try:
        if set(captures) != {"main", "pr", "loki"} or entry.get("kind") != "explore":
            return None
        events = _thrown_supporting_events(captures)
        if events is None:
            return None
        contracts = client_error_contracts() if contracts is None else contracts
        for case, proof in contracts:
            if (case.get("gap", {}).get("status") != "upstream"
                    or case.get("compare", {}).get("values", {}).get("client_error_contract") != QUERY_REJECTION_CONTRACT
                    or proof.get("registry_case") != case.get("id") or case["id"] not in entry.get("covers", [])
                    or entry.get("id") != page):
                continue
            expected = [c for c in proof["ci"]["captures"] if c["scene_id"] == page and c["range"] == rng]
            if len(expected) != 3 or {c["side"] for c in expected} != {"main", "pr", "loki"}:
                continue
            answers = {}
            for side, cap in captures.items():
                recorded = next(c for c in expected if c["side"] == side)
                want = {q["ref_id"]: q for q in recorded["queries"]}
                if set(want) != {"A", "log-volume-A"} or entry.get("query") != want["A"]["expression"]:
                    return None
                console = [e for e in cap["errors"] if not e.startswith("pageerror:")]
                if sorted(map(client_console_error, console)) != sorted(map(client_console_error, recorded.get("console_errors", []))):
                    return None
                seen, windows = {}, set()
                for record in cap["records"]:
                    if record.get("aborted") or record.get("response") is None:
                        return None
                    if "/api/ds/query" not in record["url"]:
                        if record.get("status") != 200:
                            return None
                        continue
                    queries = record["request"]["queries"]
                    if len(queries) != 1 or queries[0].get("refId") not in want:
                        return None
                    query = queries[0]
                    ref = query["refId"]
                    if ref in seen or query["expr"] != want[ref]["expression"]:
                        return None
                    if client_request_shape(record["request"]) != client_request_shape(want[ref]["request"]):
                        return None
                    results = record["response"].get("results", {})
                    if (record["status"] != 400 or record["response"] != want[ref]["response"] or set(results) != {ref}
                            or results[ref].get("status") != 400 or results[ref].get("frames")):
                        return None
                    windows.add((record["request"]["from"], record["request"]["to"]))
                    seen[ref] = record["response"]
                if set(seen) != set(want) or len(windows) != 1:
                    return None
                supporting = seen["log-volume-A"]["results"]["log-volume-A"]
                if events[side]["message"] != supporting.get("error") or events[side]["status"] != supporting.get("status"):
                    return None
                answers[side] = seen
            if not (answers["main"] == answers["pr"] == answers["loki"]):
                return None
            evidence = _ExpectedClientError(case["id"], page, rng, captures, events, {}, contract=QUERY_REJECTION_CONTRACT)
            return evidence if evidence.payload_bytes <= CLIENT_ERROR_PROOF_BYTES else None
    except (AttributeError, KeyError, IndexError, TypeError, ValueError, OSError, RuntimeError):
        return None
    return None


def client_error_evidence(page, rng, entry, captures, contracts=None):
    """A recorded client failure that Loki's own page shows identically; None for anything unproved."""
    return (_supporting_error_evidence(page, rng, entry, captures, contracts)
            or _rejection_evidence(page, rng, entry, captures, contracts))


def has_data(body):
    if isinstance(body, list):
        return bool(body)
    if not isinstance(body, dict):
        return False
    if "results" in body:
        return any(not res.get("error") and any(any(col for col in frame.get("data", {}).get("values", []))
                   for frame in res.get("frames", [])) for res in body["results"].values())
    if body.get("status") == "error":
        return False
    data = body.get("data", body)
    if isinstance(data, dict) and any(type(data.get(k)) in (int, float) and data[k] > 0 for k in ("entries", "streams")):
        return True
    if isinstance(data, list):
        return bool(data)
    return isinstance(data, dict) and any(isinstance(data.get(k), list) and data[k]
                                          for k in ("result", "fields", "detectedLabels", "values", "series", "volumes"))


# A body above the capture's retention cap (capture.spec.ts, BodyDigest) is kept as its SHA-256, length and a bounded head and
# tail, read from the whole body once. Such a record answers the request only when it looks like a successful answer: a
# well-formed digest, a JSON object that starts like one and does not start with an error, and ends like one.
DIGEST_HEAD = re.compile(r'\A\s*\{\s*"(?:results|status|data|fields|detectedLabels|values|volumes|series)"')


def body_digest(record):
    """(sha256, bytes) of a 200 record whose body was not retained but was read and hashed whole; None otherwise."""
    d = record.get("body_digest")
    if not isinstance(d, dict) or record.get("status") != 200 or record.get("response") is not None:
        return None
    sha, size, head, tail = d.get("sha256"), d.get("bytes"), d.get("head"), d.get("tail")
    if not (isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha) and isinstance(size, int) and not isinstance(size, bool)
            and size > 0 and isinstance(head, str) and isinstance(tail, str)):
        return None
    if not DIGEST_HEAD.match(head) or re.search(r'"error"\s*:', head[:2048]) or not tail.rstrip().endswith(("}", "]")):
        return None
    return sha, size


def answer_evidence(capture):
    """Classify each requested answer by its normalized identity, including error bodies on non-200s."""
    rejected, problems, answered = defaultdict(list), [], set()
    for record in capture.get("records", []):
        if record.get("aborted"):
            continue
        status, body = record.get("status"), record.get("response")
        if "/api/ds/query" in record["url"]:
            queries = (record.get("request") or {}).get("queries", [])
            results = body.get("results", {}) if isinstance(body, dict) else {}
            answers = [(query_key(record, q), results.get(q.get("refId")), True) for q in queries]
        else:
            answers = [(resource_key(record), body, False)]
        for key, result, query in answers:
            response = result if result is not None else body
            detail = (response.get("error") or response.get("message") or response.get("messageId")
                      if isinstance(response, dict) else response if isinstance(response, str) else None)
            code = result.get("status", status) if isinstance(result, dict) and query else status
            explicit_error = any(isinstance(value, dict) and value.get("status") == "error" for value in (body, result))
            if explicit_error or detail or code != 200 or status != 200:
                if (code in (200, 400, 422, 500) and status in (200, 400, 422, 500)
                        and query_rejection(detail)):
                    rejected[key].append((status, code, error_text(detail)))
                    answered.add(key)
                else:
                    problems.append("infrastructure error or unusable error response")
            elif query and isinstance(result, dict) and isinstance(result.get("frames"), list):
                answered.add(key)
            elif response is None and body_digest(record):
                answered.add(key)
            elif not query and isinstance(body, (dict, list)):
                if isinstance(body, dict):
                    data = body.get("data", body)
                    if not (isinstance(data, list) or isinstance(data, dict) and
                            (any(isinstance(data.get(k), list) for k in ("result", "fields", "detectedLabels", "values", "volumes", "series"))
                             or any(k in data for k in ("entries", "streams"))
                             or "detected_" in record["url"] and set(data) <= {"limit"})):
                        # Ancillary requests such as drilldown-limits do not establish the page's data answer.
                        if re.search(r"/(?:labels?|detected_fields|detected_labels|values|index/stats|index/volume|patterns)(?:/|\?|$)",
                                     record["url"]):
                            problems.append("unusable API resource answer")
                        continue
                answered.add(key)
            else:
                problems.append("unusable API answer")
    if not answered:
        problems.append("no usable recorded API answer")
    # What a request was rejected with, not how often the page asked: Grafana re-issues a rejected query once more on
    # one build and not on another from timing alone (explore-unwrap-conversion-error at 15m: twice on the proxies,
    # once on Loki, the same 400 and text). A different status, code or text still differs.
    return {k: sorted(set(v)) for k, v in rejected.items()}, answered, problems


def records(path, raw=None, gaps=None):
    """(settled, {request key: [answer digests]}); raw, when given, gets the answers themselves in the same order.

    gaps, when given, is filled with how the capture ended: `settled` (Playwright saw the page go quiet), `keys` (the
    requests whose 200 answer had no readable body) and `reasons` (why, from capture.spec.ts). The returned `settled`
    is false for either; gaps lets the caller tell an unfinished page from a body the browser could not hand over.
    """
    d = load_json(path)
    recs = defaultdict(list)
    unreadable = False
    keys, reasons = set(), []
    for r in d["records"]:
        # A request the page cancelled (capture.spec.ts: `aborted`) has no answer and is not a gap of the capture.
        cancelled = bool(r.get("aborted"))
        digest = None if cancelled else body_digest(r)
        missing_body = not cancelled and r["status"] == 200 and r["response"] is None and not digest
        unreadable |= missing_body
        if missing_body and r.get("body_unavailable"):
            reasons.append(str(r["body_unavailable"])[:160])
        if "/api/ds/query" in r["url"]:
            for q in (r["request"] or {}).get("queries", []):
                key = query_key(r, q)
                if missing_body:
                    keys.add(key)
                recs[key].append({("capture", ""): ("capture", CANCELLED)} if cancelled else
                                 {("capture", ""): ("capture", "body unreadable")} if missing_body else
                                 {("digest", ""): ("digest", *digest)} if digest else
                                 frames_of(r["response"]) if r["status"] == 200 else
                                 {("status", str(r["status"])): ("error", r["status"])})
                if raw is not None:
                    raw[key].append(((r["response"] or {}).get("results") or {}).get(q.get("refId")) if r["status"] == 200 else None)
        else:
            key = resource_key(r)
            body = r["response"]
            if raw is not None:
                raw[key].append(body if r["status"] == 200 else None)
            if "/resources/patterns" in r["url"] and isinstance(body, dict):
                # mined from a sample of the rows: compare the pattern set, not the sample counts
                body = sorted(p.get("pattern", "") for p in body.get("data", []))
            if isinstance(body, dict) and isinstance(body.get("data"), dict) and "stats" in body["data"]:
                # Loki's query statistics (index/volume, like query_range) carry each run's timings: compare the answer
                body = {**body, "data": {k: v for k, v in body["data"].items() if k != "stats"}}
            lists = [body.get(k) for k in ("data", "fields", "detectedLabels")] if isinstance(body, dict) else [body]
            rows = next((len(x) for x in lists if isinstance(x, list)), 0)
            if missing_body:
                keys.add(key)
            recs[key].append({("capture", ""): ("capture", CANCELLED)} if cancelled else
                             {("capture", ""): ("capture", "body unreadable")} if missing_body else
                             {("digest", ""): ("digest", *digest)} if digest else
                             {"body": ("other", hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest(), rows)}
                             if r["status"] == 200 else {("status", ""): ("error", r["status"])})
    if gaps is not None:
        gaps.update(settled=bool(d.get("settled", False)), keys=keys, reasons=sorted(set(reasons)))
    return d.get("settled", False) and not unreadable, recs


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
    if ("capture", "") in a or ("capture", "") in b:
        why = {x.get(("capture", ""), ("capture", ""))[1] for x in (a, b)}
        if why == {CANCELLED}:
            return None  # the page cancelled this request on both sides
        return f"capture {CANCELLED}" if CANCELLED in why else "capture body unreadable"
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


def is_ranked(key):
    """True for a topk or bottomk query request (its expression starts with the operator, as in bench/ab's signature)."""
    return key[0] == "query" and str(key[2] or "").lstrip().lower().startswith(("topk", "bottomk"))


def same_ranked(a, b):
    """None when two topk/bottomk answers hold the same ranked values at every step, else a short reason.

    When several series tie across the k-th place at a step, Loki keeps the tied one its map yields
    (a tied series does not replace the heap root, and the inner aggregation hands the series over in
    map order), so the union of series over
    the range differs between runs of Loki itself, while the ranked values never do. The proxy breaks
    the tie on the label key. The series sets are therefore not compared: every step's sorted values are,
    per query ref, and so are the label names the series carry and the frame schema and meta of the series
    both sides have. A wrong value, a missing step, an extra point, a dropped or renamed label still differs."""
    if not a or not b or any(x[0] != "metric" for x in (*a.values(), *b.values())):
        return same(a, b)
    names = lambda fr: sorted({n for ident in fr for n in json.loads(ident[2])})
    if names(a) != names(b):
        return f"label names differ: {names(a)} vs {names(b)}"
    for ident in set(a) & set(b):
        if a[ident][2] != b[ident][2]:
            return f"{ident[2]}: frame schema or meta differs"

    def steps(fr):
        out = {}
        for ident, x in fr.items():
            for t, v in x[1].items():
                out.setdefault(ident[0], {}).setdefault(t, []).append(v)
        return {ref: {t: sorted(vs, key=lambda v: (v is None, v or 0)) for t, vs in by_t.items()} for ref, by_t in out.items()}

    sa, sb = steps(a), steps(b)
    if set(sa) != set(sb):
        return f"query refs differ: {sorted(map(str, sa))} vs {sorted(map(str, sb))}"
    for ref in sa:
        if set(sa[ref]) != set(sb[ref]):
            return f"{ref}: timestamps differ ({len(sa[ref])} vs {len(sb[ref])} steps)"
        for t in sa[ref]:
            u, v = sa[ref][t], sb[ref][t]
            if len(u) != len(v):
                return f"{ref}: {len(u)} vs {len(v)} ranked series at {t}"
            for x, y in zip(u, v):
                if x is None or y is None:
                    if x != y:
                        return f"{ref}: null at {t}"
                elif not math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-12):
                    return f"{ref}: ranked values at {t}: {u} vs {v}"
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


# A capture whose body was unreadable or cancelled says nothing about the build: it is
# reported, never gated. Drilldown's patterns are compared as their set of pattern texts
# (above): the proxy mines them from the lines VictoriaLogs holds, as Loki's ingester
# does, so two processes on the same data answer alike.
NONDETERMINISTIC = ("capture body unreadable", f"capture {CANCELLED}")
# Resources whose answer is the proxy's deployment configuration (its
# -tenant-default-limits / -tenant-limits flags), not data: a base-vs-PR
# difference there comes from the stack's flags, so it is reported, never gated.
CONFIGURATION = ("/resources/drilldown-limits",)


def configuration(diff):
    return any(s in diff for s in CONFIGURATION)


def nondeterministic(diff):
    return any(s in diff for s in NONDETERMINISTIC)


class Diff(str):
    """A difference text that also carries the question it is about (`qid`), so a gate can match the same query at
    another range without parsing the (truncated) text. None for a resource request."""
    qid = None


def tagged(text, key):
    d = Diff(text)
    d.qid = f"{key[2]}|{key[3]}" if key[0] == "query" else None  # the expression and query type: the window and ids vary by range
    return d


def qids(diffs):
    return [getattr(x, "qid", None) for x in diffs]


# Bodies of differing resource requests kept in compare.json: the run's artifact holds no raw captures.
BODY_CHARS = 2000
BODIES_PER_ROW = 12


def bounded(body):
    """A resource answer as JSON text of at most BODY_CHARS characters."""
    text = json.dumps(body, sort_keys=True, default=str)
    return text if len(text) <= BODY_CHARS else text[:BODY_CHARS] + f"... ({len(text)} characters)"


def stats_numbers(body):
    """An index/stats answer's four numbers, as compare.md lists them."""
    if not isinstance(body, dict):
        return str(body)[:80]
    return ", ".join(f"{k} {body.get(k)}" for k in ("streams", "chunks", "entries", "bytes"))


def compare(left, right, lenient=False, explain=None, vs_loki=False, raw=None, bodies=None):
    """(requests, identical, differences, series, one-sided, explained) of two sides' records.

    explain: (raw left, raw right[, Loki's first-pass answers by request]) of the records, to ask explain_vs_loki why a difference is by design; such a
    difference goes to `explained` with its reason instead of the differences.
    vs_loki: a topk or bottomk answer is compared by its ranked values per step (same_ranked), not its series.
    raw: (raw left, raw right) when explain is not given, for bodies.
    bodies: a list that gets {request, left, right} (bounded; index/stats also `numbers`) for every resource request
    that differs, at most BODIES_PER_ROW."""
    raw = explain or raw
    keys = sorted(set(left) | set(right), key=str)
    n = ok = missing = 0
    diffs, explained, series = [], [], 0
    for k in keys:
        la, lb = left.get(k, []), right.get(k, [])
        for i, j in pairs(la, lb):
            if i is None or j is None:
                # An answer the other side also gave, one more time, is Grafana asking again (it re-issues a rejected
                # or slow query from timing alone), not a different answer: the data are the same. What it costs
                # shows in the runtime evidence (requests, bytes), not here.
                other, extra = (lb, la[i]) if j is None else (la, lb[j])
                if any(digest(repr(sorted(o.items(), key=str))) == digest(repr(sorted(extra.items(), key=str))) for o in other):
                    n += 1
                    ok += 1
                    continue
                missing += 1
                text = f"{k[0]} {str(k[2:] or k[1])[:90]}: request only on one side ({'left' if j is None else 'right'})"
                reason = explain_one_sided(k) if explain else None
                if reason:
                    explained.append(tagged(f"{text} -- explained: {reason}", k))
                else:
                    diffs.append(tagged(text, k))
                continue
            n += 1
            xa, xb = la[i], lb[j]
            x, y = (nonzero(xa), nonzero(xb)) if lenient else (xa, xb)
            series += len(xa)
            if (("digest", "") in xa) != (("digest", "") in xb):
                # one side's body was hashed, the other's retained (or from Loki): the bytes are not comparable
                why = "capture body unreadable"
            else:
                why = (same_ranked if vs_loki and is_ranked(k) else same)(x, y)
            if not why:
                ok += 1
                continue
            if raw and bodies is not None and k[0] == "resource" and len(bodies) < BODIES_PER_ROW:
                ba, bb = raw[0][k][i], raw[1][k][j]
                bodies.append({"request": k[1], "left": bounded(ba), "right": bounded(bb),
                               **({"numbers": f"{stats_numbers(ba)} vs {stats_numbers(bb)}"}
                                  if "/resources/index/stats" in k[1] else {})})
            text = f"{k[0]} {str(k[2:] or k[1])[:90]}: {why}"
            reason = explain_vs_loki(k, explain[0][k][i], explain[1][k][j], (explain[2].get(k) or ()) if len(explain) > 2 else ()) if explain and why != "capture body unreadable" else None
            if reason:
                explained.append(tagged(f"{text} -- explained: {reason}", k))
            else:
                diffs.append(tagged(text, k))
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
                                      "(registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes "
                                      "(metadataMaxFieldNamesWindow); the label names match")
    order = [x.get("label") for x in (a.get("detectedLabels") or [])] != [x.get("label") for x in (b.get("detectedLabels") or [])]
    return _join(cardinality, _envelope(a, b), "Loki lists labels in no fixed order" if order else "")


def _index_stats(a, b, earlier=()):
    """Loki's bytes (uncompressed chunk KB, registry quality/index-stats-bytes-and-chunks) and chunk count have no
    VictoriaLogs equivalent. Its streams and entries are
    estimates too (chunks pro-rated by time, sources summed: registry quality/index-stats-loki-chunk-accounting), but
    exact for the visual stack's flushed, uncached seed, so a difference there stays counted.

    One difference of that kind is named by the registry case itself: "every copy of a chunk Loki reads is added".
    When Loki reads the same chunks from its ingesters and from its store, its entries and its chunk count are both an
    exact whole multiple of the proxy's while its streams stay equal. That signature alone would also fit a proxy that
    undercounts by exactly 1/k, so it is explained only with evidence that Loki's own count moved and the proxy's did
    not: `earlier`, Loki's answers to the same request in the first pass of this run, must include one whose streams,
    entries and chunks equal the proxy's current numbers. Without that evidence the difference stays counted, as does
    any other difference in streams or entries, including entries that are not a multiple of the proxy's."""
    if {k: a.get(k) for k in ("streams", "entries")} != {k: b.get(k) for k in ("streams", "entries")}:
        copies = _chunk_copies(a, b)
        agreed = any(isinstance(e, dict) and all(e.get(k) == a.get(k) for k in ("streams", "entries", "chunks")) for e in earlier)
        if not copies or not agreed:
            return None
        return (f"Loki read every chunk {copies} times (its ingesters and its store both answered): earlier in this run Loki "
                f"answered this request with the proxy's streams, entries and chunks, now entries and chunks are exactly "
                f"{copies}x with streams equal (registry quality/index-stats-loki-chunk-accounting); bytes have no VictoriaLogs equivalent")
    return "bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match"


def _chunk_copies(proxy, loki):
    """k when Loki's entries and chunks are both exactly k times the proxy's (k >= 2) with equal streams, else 0."""
    numbers = {}
    for name, body in (("proxy", proxy), ("loki", loki)):
        values = [body.get(key) for key in ("streams", "entries", "chunks")]
        if not all(isinstance(v, int) and not isinstance(v, bool) and v > 0 for v in values):
            return 0
        numbers[name] = values
    (ps, pe, pc), (ls, le, lc) = numbers["proxy"], numbers["loki"]
    if ls != ps or le % pe or lc % pc:
        return 0
    k = le // pe
    return k if k >= 2 and lc // pc == k else 0


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


def explain_vs_loki(key, a, b, earlier=()):
    """Why the PR's answer a differs from Loki's answer b by design, or None (a real difference). earlier: Loki's
    answers to the same request in the first pass of this run (empty in the first pass itself)."""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return None
    if key[0] == "query":
        return _log_frames(a, b)
    for fragment, rule in RESOURCE_RULES:
        if fragment in key[1]:
            return rule(a, b, earlier) if rule is _index_stats else rule(a, b)
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


def render_md(rows):
    """compare.md: the table and the detail of every capture that differs; a pure function of the rows,
    so a run split into shards renders the same file from the merged rows (merge.py)."""
    md = ["| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |", "|---|---|---|---|---|---|"]
    for r in rows:
        md.append(f"| {r['page']} | {r['range']} | {r['requests']} | {r['main_vs_pr']} | {r['pr_vs_loki']} | {'yes' if r['settled'] else 'NO'} |")
    for r in rows:
        if r["main_pr_diffs"] or r.get("main_pr_nondet") or r.get("main_pr_config") or r["loki_diffs"] or r.get("loki_explained") or r.get("loki_nondet") or r.get("expected_client_errors"):
            md += ["", f"### {r['page']} {r['range']}"]
            md += [f"- main vs PR: {x}" for x in r["main_pr_diffs"][:8]] + [f"- main vs PR (history-dependent, not gated): {x}" for x in r.get("main_pr_nondet", [])[:4]] + [f"- main vs PR (deployment configuration, not gated): {x}" for x in r.get("main_pr_config", [])[:4]] + [f"- PR vs Loki: {x}" for x in r["loki_diffs"][:8]]
            md += [f"- PR vs Loki (explained): {x}" for x in r.get("loki_explained", [])[:12]] + [f"- PR vs Loki (history-dependent, not counted): {x}" for x in r.get("loki_nondet", [])[:4]]
            md += [f"- {side} index/stats: {b['numbers']}" for side, key in (("main vs PR", "main_pr_bodies"), ("PR vs Loki", "loki_bodies"))
                   for b in r.get(key, []) if "numbers" in b]
            md += [f"- Expected upstream client error matches Loki: {e['registry_case']}; "
                   + ("the primary and the supporting query are rejected with the identical 400 DTOs on all three builds and the "
                      if e.get("contract") == QUERY_REJECTION_CONTRACT else
                      "primary points match on all three builds, the supporting 400 DTO and ")
                   + "actual uncaught exception match. The upstream gap remains open; raw captures are retained."
                   for e in r.get("expected_client_errors", [])]
    return md


UNREADABLE_BODIES = "runtime proof: unreadable API bodies leave data completeness unproved"


def base_cannot_answer(row, gap, pr_judged):
    """Main-side runtime-proof problems of a page this pull request fixes, when the PR answers the page completely.

    A fix for a request the base cannot answer (an oversize answer the browser cannot read, an error answer that is
    no query rejection) has no usable base to measure against, and the page's own fix proof decides whether the PR is
    right. The problems are set aside, and recorded in row["base_unusable"], only when every one of these holds: each
    problem is about the main side; the PR settled; no body of the PR side is unreadable; the PR answered without an
    error; the PR shows no difference from Loki that the base lacks (the rule that makes a regression) wherever Loki was
    compared; and no other runtime problem remains (a PR-side
    problem, a missing Loki reference, a resource regression). Returns the problems set aside."""
    problems = row["runtime_problems"]
    main_only_gap = bool(gap["main"]["keys"]) and not gap["pr"]["keys"]
    moved = [x for x in problems if x.startswith("runtime proof: main ") or (x == UNREADABLE_BODIES and main_only_gap)]
    if not moved or [x for x in problems if x not in moved]:
        return []
    if not pr_judged or gap["pr"]["keys"] or row.get("errors_pr"):
        return []
    if row.get("loki_compared") and row.get("loki_new"):
        return []
    row["runtime_problems"] = []
    row["base_unusable"] = sorted(set(moved))
    row["base_comparison_note"] = ("The base's answer to this page, which the pull request fixes, is unusable as a baseline "
                                   "for the page's data; the PR's resource use is still compared with the base's.")
    return moved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--loki-seconds", type=int, default=5400)
    a = ap.parse_args()
    # A failed recapture must not leave the prior successful comparison to be gated.
    for name in ("compare.json", "compare.md", "expected-client-error-captures.json"):
        path = os.path.join(a.out, name)
        if os.path.exists(path):
            os.unlink(path)
    from plan import load_spec
    spec = load_spec()
    plan_path = os.path.join(a.out, "plan.json")
    plan = load_json(plan_path) if os.path.exists(plan_path) else {}
    runtime_required = bool(plan.get("runtime_proof", {}).get("required"))
    # Pages whose registry case this pull request turns to fixed (plan.py: fix_cases): a base that cannot answer them
    # is the very defect being fixed.
    fix_pages = {entry for case in (plan.get("fix_cases") or {}).values() if case.get("status") == "fixed"
                 for entry in case.get("entries") or []}
    rows, report, client_proofs = [], [], []
    client_proof_bytes = 4096  # Include the enclosing JSON/list metadata in the total retention bound.
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
        gap = {"main": {}, "pr": {}, "loki": {}}
        sm, m = records(os.path.join(d, "main.json"), raw["main"], gap["main"])
        sp, p = records(os.path.join(d, "pr.json"), raw["pr"], gap["pr"])
        settle = {n: round(load_json(os.path.join(d, f"{n}.json")).get("settle_ms", 0) / 1000, 1) for n in ("main", "pr")}
        has_loki = os.path.exists(os.path.join(d, "loki.json"))
        # Loki's answers of the first pass of this run (a recapture is taken later, by which time Loki's own chunk
        # accounting may have moved): evidence for _index_stats, empty when there was no earlier pass.
        earlier = defaultdict(list)
        first_loki = os.path.join(a.out, "first-pass", "data", page, rng, "loki.json")
        if os.path.exists(first_loki):
            records(first_loki, earlier, {})
        sl, l = records(os.path.join(d, "loki.json"), raw["loki"], gap["loki"]) if has_loki else (True, {})
        # A body the browser could not hand over (a very large answer) is a gap of the capture, not of the build. The PR
        # side is blamed for it only where the base read that request: a request unreadable on both sides is reported
        # (unreadable_both) and left uncompared; the PR still fails when it never settled or lost a body the base had.
        unreadable_only_pr = gap["pr"]["keys"] - gap["main"]["keys"]
        sp_judged = gap["pr"]["settled"] and not unreadable_only_pr
        mp_bodies, loki_bodies = [], []
        n, ok, diffs, series, miss, _ = compare(m, p, raw=(raw["main"], raw["pr"]), bodies=mp_bodies)
        loki_ok = has_loki and spec["ranges"][rng] <= a.loki_seconds
        explained, lnondet = [], []
        if loki_ok:
            ln, lok, _, _, _, _ = compare(p, l, vs_loki=True)
            _, lok2, ldiffs2, _, _, explained = compare(p, l, lenient=True, explain=(raw["pr"], raw["loki"], earlier), vs_loki=True,
                                                        bodies=loki_bodies)
            _, _, mdiffs, _, _, mexplained = compare(m, l, lenient=True, explain=(raw["main"], raw["loki"], earlier), vs_loki=True)
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
        gated = [x for x in diffs if not nondeterministic(x) and not configuration(x)]
        rows.append(dict(page=page, range=rng, requests=n, series=series, main_vs_pr=f"{ok}/{n}" + (f", {miss} one-sided" if miss else ""), pr_vs_loki=vs,
                         settled=all((sm, sp, sl)), settle_s=settle, main_pr_diffs=gated, main_pr_qids=qids(gated), loki_qids=qids(ldiffs2) if loki_ok else [], main_pr_nondet=[x for x in diffs if nondeterministic(x)], main_pr_config=[x for x in diffs if configuration(x)], loki_diffs=ldiffs2 if loki_ok else [],
                         loki_explained=explained, loki_nondet=lnondet, main_pr_bodies=mp_bodies, loki_bodies=loki_bodies,
                         loki_compared=bool(loki_ok), loki_main_n=len(mdiffs), loki_new=[x for x in ldiffs2 if x not in set(mdiffs)] if loki_ok else [],
                         points_main=points(m), points_pr=points(p), points_loki=points(l), ui_main=ui_of(d, "main"), ui_pr=ui_of(d, "pr"), ui_loki=ui_of(d, "loki"),
                         # An error the PR answers exactly as Loki answers it (a query Loki rejects) is Loki's answer.
                         # An error the PR answers exactly as Loki answers it (a query Loki rejects) is Loki's answer, at any
                         # range Loki was captured: an error does not depend on how much history Loki holds.
                         settled_pr=bool(sp_judged), settled_main=bool(sm), unreadable_main=len(gap["main"]["keys"]), unreadable_pr=len(gap["pr"]["keys"]),
                         unreadable_both=len(gap["pr"]["keys"] & gap["main"]["keys"]), unreadable_why=sorted(set(gap["main"]["reasons"] + gap["pr"]["reasons"]))[:3],
                         errors_pr=[e for e in errors(p, allowed) if not has_loki or e not in errors(l, allowed)],
                         errors_main=errors(m, allowed), errors_loki=errors(l, allowed) if has_loki else [],
                         loki_missing=bool(not has_loki and spec["ranges"][rng] <= a.loki_seconds)))
        captures = {side: load_json(os.path.join(d, f"{side}.json"))
                    for side in ("main", "pr", "loki") if os.path.exists(os.path.join(d, f"{side}.json"))}
        row = rows[-1]
        row["settle_s"] = {side: cap["settle_ms"] / 1000 for side, cap in captures.items()
                           if isinstance(cap.get("settle_ms"), (int, float)) and not isinstance(cap["settle_ms"], bool)
                           and math.isfinite(cap["settle_ms"]) and cap["settle_ms"] >= 0
                           and (side != "loki" or row["loki_compared"])}
        row["runtime_main"] = captures["main"].get("runtime")
        row["runtime_pr"] = captures["pr"].get("runtime")
        row["runtime_loki"] = captures.get("loki", {}).get("runtime") if row["loki_compared"] else None
        row["runtime_problems"] = []
        entry = next((p for p in spec["pages"] if p["id"] == page), {})
        client_error = client_error_evidence(page, rng, entry, captures) if runtime_required else None
        if client_error and client_proof_bytes + client_error.payload_bytes > CLIENT_ERROR_PROOF_BYTES:
            client_error = None  # No error is accepted without retaining its complete bounded evidence.
        if client_error:
            row["expected_client_errors"] = [client_error.receipt]
            client_proofs.append(client_error.payload)
            client_proof_bytes += client_error.payload_bytes
        if runtime_required and (gap["main"]["keys"] or gap["pr"]["keys"]):
            row["runtime_problems"].append("runtime proof: unreadable API bodies leave data completeness unproved")
        if runtime_required and spec["ranges"][rng] <= a.loki_seconds:
            if "loki" not in captures:
                row["runtime_problems"].append("runtime proof: required smaller-range Loki reference is missing")
            else:
                row["runtime_problems"].extend("Loki reference: " + p for p in runtime_problems(captures["loki"], captures["loki"], required=True,
                                                                                           client_error=client_error))
                if not sl or not gap["loki"]["settled"] or gap["loki"]["keys"]:
                    row["runtime_problems"].append("runtime proof: required Loki reference is incomplete or unreadable")
        if runtime_required:
            rejections, answered = {}, {}
            for side, cap in captures.items():
                rejections[side], answered[side], problems = answer_evidence(cap)
                if side != "loki" or row["loki_compared"]:
                    row["runtime_problems"].extend(f"runtime proof: {side} {message}" for message in set(problems))
            if row["loki_compared"] and answered["pr"] - answered["loki"]:
                row["runtime_problems"].append("runtime proof: required Loki reference lacks a corresponding API answer")
            reference = rejections.get("loki", {})
            # Rejecting a query and returning its successful result are different workloads.
            # Compare PR and Loki on the actual request before accepting a correction.
            corrected = set()
            if row["loki_compared"]:
                for key in rejections["main"].keys() & answered["pr"] & answered["loki"]:
                    if key in rejections["pr"] or key in reference:
                        continue
                    _, _, different, _, _, _ = compare({key: p[key]}, {key: l[key]}, vs_loki=True, lenient=True)
                    if not different:
                        corrected.add(key)
            for side in ("main", "pr"):
                if any(reference.get(k) != v and (side != "main" or k not in corrected)
                       for k, v in rejections[side].items()):
                    row["runtime_problems"].append(f"runtime proof: {side} query rejection differs from Loki")
            if row["loki_compared"] and any(rejections["pr"].get(k) != v for k, v in reference.items()):
                row["runtime_problems"].append("runtime proof: required Loki reference rejection differs from PR")
            if corrected:
                row["corrected_base_rejections"] = [{"request": k, "errors": rejections["main"][k]}
                                                    for k in sorted(corrected, key=str)]
                row["base_comparison_note"] = "Base-relative resource and settlement changes unavailable: base rejected a query."
            row["runtime_problems"].extend(runtime_problems(captures["main"], captures["pr"], required=True,
                                                             comparable=not corrected, client_error=client_error))
        if runtime_required and page in fix_pages:
            base_cannot_answer(row, gap, sp_judged)
    md = render_md(rows)
    write_text(os.path.join(a.out, "compare.md"), "\n".join(md) + "\n")
    dump_json(os.path.join(a.out, "compare.json"), rows)
    if client_proofs:
        dump_json(os.path.join(a.out, "expected-client-error-captures.json"), client_proofs, indent=None)
    print("\n".join(md[:200]))
    bad = [r for r in rows if r["main_pr_diffs"] or r.get("runtime_problems")]
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
