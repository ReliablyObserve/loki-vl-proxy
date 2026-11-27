#!/usr/bin/env python3
"""A/B latency and parity matrix: two or more proxy builds against Loki direct.

Runs every shape of a set (bench/ab/shapes.json) over each range against every
target, interleaved per (shape, range, run) so data growth and cache state do
not favour one target. All targets share the same pinned, step-aligned window
per run; each run shifts the window by one minute (or the shape's "shift") so
repeated runs are not response-cache hits. VictoriaLogs CPU is sampled with `docker stats` and the
container's RestartCount is recorded before and after: a run that spans a
restart is marked invalid and stops. The sampler runs in its own thread and the
CPU figures of every row are read from its samples once the loop has finished,
so no request waits for a sample.

A request to the reference target (`loki`) that times out is made once per
shape and range: the later runs of that shape and range, in this pass and in
every pass given the raw file through --known-timeouts, are not sent. Their rows
are written with the first timeout's seconds and signature and `"skipped":
"loki_timeout"`, so every consumer sees the same Loki status on each run. The
raw file lists each first timeout under "timeouts".

Cache handling, so a cold number is a computed one:
  - No Cache-Control header is sent to any target. Loki 3.7.7 does not honour
    `Cache-Control: no-cache` on query_range or index/volume_range: it still
    looks up and stores results-cache entries with it, so the header would only
    claim a bypass that does not happen.
  - Whether Loki's cache answered a request is read from Loki's own counters
    (`data.stats.cache.result` and the other *Result sub-caches), recorded per
    request. `statsResult` is the index-stats cache Loki uses for query planning;
    it is hit on almost every request, is not a cached answer and is ignored.
  - Each set gets its own block of windows (plan_windows). Loki's results cache
    reuses overlapping time extents of the same query string, so a query that
    two sets share can still be answered from the earlier set's extents; the
    counters above detect it and pr_smoke takes the first set's computed run as
    Loki's cold reference for that query.
  - --refresh-s N is the refresh mode: instead of moving the window backwards
    by a shift each run, run k ends at end + k*N (so the last run ends at --end
    and no window is in the future), like a Grafana dashboard that refreshes.
    N must be a multiple of every selected range's step. Its first run fills
    Loki's cache the way the first load of a dashboard does, and the refresh
    timing is taken from the runs after it.

Writes the raw per-request rows to --out. Reduce them with report.py.

  perf_matrix.py --set control --runs 5 \\
      --target main=http://127.0.0.1:13190 \\
      --target branch=http://127.0.0.1:13192 \\
      --target loki=http://127.0.0.1:13101 \\
      --out /tmp/control.raw.json

Run each proxy build on a port you have verified you own and with a unique
-admin-listen; see bench/ab/README.md.
"""
import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from answers import correctness_endpoint, correctness_spelling, metadata_endpoint, metadata_schema, query_rejection, strict_json  # noqa: E402
import report  # noqa: E402
from bench.parity import differ  # noqa: E402
from selection import load_shapes, query_lookback  # noqa: E402
from bench.visual.vio import error_text  # noqa: E402
import reuse  # noqa: E402
import stack  # noqa: E402


SAMPLE_WINDOW = 1.5  # seconds around a request in which a sample belongs to it (CPUSampler.between)
REFERENCE_TARGET = "loki"  # the target whose timeouts are not repeated (the README names the Loki reference this)


class CPUSampler(threading.Thread):
    def __init__(self, container):
        super().__init__(daemon=True)
        self.container, self.samples, self.stop = container, [], False
        self.failed = 0

    def run(self):
        while not self.stop:
            try:
                out = subprocess.check_output(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", self.container],
                                              text=True, timeout=20).strip()
                self.samples.append((time.time(), float(out.rstrip("%"))))
            except (subprocess.SubprocessError, ValueError, OSError):
                # A missed sample only narrows the CPU figures; it is counted
                # and reported with the run instead of failing it.
                self.failed += 1
            time.sleep(0.5)

    def drain(self, until, timeout=10.0):
        """Wait, at most `timeout` seconds, for a sample taken at or after `until`."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.samples and self.samples[-1][0] >= until:
                return True
            time.sleep(0.1)
        return False

    def between(self, t0, t1):
        vals = [c for t, c in self.samples if t0 - SAMPLE_WINDOW <= t <= t1 + SAMPLE_WINDOW]
        return (max(vals), sum(vals) / len(vals)) if vals else (None, None)


DEFAULT_SHIFT = 60


def shape_shift(shape, rname):
    """Seconds a shape moves its window per run: "shift" is a number or a per-range dict."""
    shift = shape.get("shift", DEFAULT_SHIFT)
    if isinstance(shift, dict):
        shift = shift.get(rname, DEFAULT_SHIFT)
    return int(shift)


def run_end_for(end, shape, rname, run, runs, refresh_s=0):
    """The end of run `run`'s window.

    Shifted mode (default): backwards, `shift` seconds per run, so repeated runs
    are not response-cache hits. Refresh mode: forwards by refresh_s per run,
    ending at `end` on the last run. "end_offset" moves the whole window back.
    """
    base = end - int(shape.get("end_offset", 0))
    if refresh_s:
        return base - (runs - 1 - run) * refresh_s
    return base - shape_shift(shape, rname) * run


# An exact repeat is asked at a window that ended this long before the matrix's end: older than the proxy's
# -query-range-freshness (10m, after which a window is historical and kept for hours), with a minute to spare, so
# nothing about it is "recent" and its answer is the cache's to keep. The seed reaches back past it (daily.py).
EXACT_REPEAT_AGE = 660


def exact_repeat_end(end, shape, step):
    """The settled end of a shape's exact repeat: EXACT_REPEAT_AGE before the shape's own end, on the range's step."""
    settled = end - int(shape.get("end_offset", 0)) - EXACT_REPEAT_AGE
    return settled - settled % step if step else settled


def shape_query(shape, step):
    """The shape's query with $__auto set to the range's step."""
    return shape.get("query", "").replace("$__auto", f"{step}s")


INTERNAL_SERVER_ERROR = "Internal Server Error"


def answer_proof(status, body, shape):
    """An actual Loki answer, or a fingerprint of its semantic query rejection."""
    try:
        parsed = strict_json(body)
    except (ValueError, UnicodeError):
        parsed = None
    if status != 200:
        if isinstance(parsed, dict) and (parsed.get("status") not in (None, "error") or "errorType" in parsed and not isinstance(parsed["errorType"], str)):
            return None
        message = parsed.get("error") if isinstance(parsed, dict) else body.decode(errors="replace").strip()
        if status in (400, 422, 500) and query_rejection(message):
            return "rejection:" + hashlib.sha256(error_text(message).encode()).hexdigest()
        if status == 500 and isinstance(message, str) and message.strip() == INTERNAL_SERVER_ERROR:
            # The Go server's own body for an unhandled failure of the target (http.Error with 500): not a Loki answer
            # and not a rejection. Infrastructure bodies (a plugin or gateway text, an HTML page, a refused connection)
            # are none of these and stay unproved. report.py accepts this only as a whole: every run, alone.
            return "failure:" + hashlib.sha256(error_text(message).encode()).hexdigest()
        return None
    if not isinstance(parsed, dict) or parsed.get("status") not in (None, "success") or parsed.get("error"):
        return None
    data = parsed.get("data", parsed)
    endpoint = correctness_endpoint(shape)
    if endpoint is None:
        return None
    valid = metadata_schema(parsed, endpoint) if metadata_endpoint(shape) else None
    if valid is None:
        kind = data.get("resultType") if isinstance(data, dict) else None
        result = data.get("result") if isinstance(data, dict) else None
        def point(value, numeric=True):
            if not isinstance(value, list) or len(value) != 2 or type(value[0]) not in (int, float) or not isinstance(value[1], str):
                return False
            try:
                return math.isfinite(value[0]) and (not numeric or differ.valid_sample_value(value[1]))
            except (ValueError, OverflowError):
                return False
        if kind in ("matrix", "vector"):
            valid = differ.valid_metric_result(parsed)
        elif kind == "streams":
            valid = isinstance(result, list) and all(isinstance(s, dict) and isinstance(s.get("stream"), dict)
                    and all(isinstance(k, str) and isinstance(v, str) for k, v in s["stream"].items())
                    and isinstance(s.get("values"), list) and all(isinstance(p, list) and 2 <= len(p) <= 3
                    and isinstance(p[0], str) and re.fullmatch(r"-?[0-9]{1,19}", p[0]) and -(1 << 63) <= int(p[0]) < (1 << 63)
                    and isinstance(p[1], str) and (len(p) == 2 or isinstance(p[2], dict)) for p in s["values"]) for s in result)
        else:
            valid = kind in ("scalar", "string") and point(result, kind == "scalar")
    return "answer" if valid else None


def request_url(base, shape, query, secs, step, run_end):
    """The URL of one request of a shape: an instant query at run_end, a metadata endpoint (labels, label
    values, detected labels or fields, index volume) over the range, or a query_range over it."""
    if shape.get("instant"):
        return f"{base}/loki/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": run_end})
    if shape.get("path"):
        # The shape's params are passed through and its query, if any, becomes the selector.
        params = {"start": f"{run_end - secs}000000000", "end": f"{run_end}000000000", **shape.get("params", {})}
        if shape.get("query"):
            params["query"] = query
        return f"{base}{shape['path']}?" + urllib.parse.urlencode(params)
    params = {"query": query, "start": run_end - secs, "end": run_end, "step": step,
              "limit": shape.get("limit", 1000), "direction": "backward"}
    return f"{base}/loki/api/v1/query_range?" + urllib.parse.urlencode(params)


def target_order(targets, k):
    """The targets in the order run k asks them, k counting the (shape, range) pairs and the runs within a pair: the
    builds (every target but the reference) swap places on odd k (ABAB over the pairs and over the runs of a pair),
    the reference keeps its place. Without this the first-listed build always goes first and the second meets its
    post-request background work. Swapping per run too keeps a build's gap to its own next request from being fixed
    for a whole pair: with one order a build's gap is the other build's request plus Loki's, so a build slowed once
    gives the other a long gap and itself a short one, run after run."""
    builds = [t for t in targets if t[0] != REFERENCE_TARGET]
    if k % 2 and len(builds) > 1:
        swapped = iter(builds[::-1])
        return [next(swapped) if t[0] != REFERENCE_TARGET else t for t in targets]
    return list(targets)


def quiet_before(targets, tname, logs, quiet, cap):
    """Seconds spent waiting for the other builds' logs to go quiet before a cold request to `tname`.

    The other builds, not the one asked: its own work is part of what it costs. A build with no log given is not
    waited for. cap 0 turns the wait off.
    """
    paths = [logs[t] for t, _ in targets if t != tname and t in logs]
    if not paths or cap <= 0:
        return 0.0
    t0 = time.time()
    stack.logs_quiet(paths, quiet, cap)
    return time.time() - t0


def request_headers(shape):
    """Headers of one request: the shape's own. No target gets a Cache-Control header."""
    return dict(shape.get("headers", {}))


def loki_cache_stats(body):
    """Loki's per-request result-cache counters, or None when the answer carries none.

    Loki reports them as data.stats.cache.result (entriesFound / entriesRequested /
    bytesReceived ...) and, for other query kinds, in sibling *Result sub-caches
    (statsResult, volumeResult, ...). Every such sub-cache is kept under its own name.
    """
    if b'"cache"' not in body:
        return None
    try:
        cache = json.loads(body)["data"]["stats"]["cache"]
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(cache, dict):
        return None
    stats = {}
    for name, sub in cache.items():
        if (name == "result" or name.endswith("Result")) and isinstance(sub, dict):
            stats[name] = {k: sub[k] for k in ("entriesFound", "entriesRequested", "bytesReceived") if k in sub}
    return stats or None


def refresh_problem(refresh_s, runs, steps):
    """Why a refresh interval cannot be used, or None: it must divide every step, with two runs at least."""
    if not refresh_s:
        return None
    bad = sorted({st for st in steps if st and refresh_s % st})
    if runs < 2 or bad:
        return (f"--refresh-s {refresh_s} needs --runs >= 2 and a multiple of every selected range's step "
                f"(not a multiple of: {bad}); a step that does not divide it snaps consecutive refresh windows "
                "onto the same one")
    return None


def query_key(shape):
    """What Loki's results cache keys a shape on, or None for an instant query (no results cache)."""
    if shape.get("instant"):
        return None
    return (shape.get("path") or "", shape.get("query"))


def plan_windows(spec, sets, ranges, runs, confirm_runs, end, refresh_s=0, refresh_runs=0, margin=60, cells=None):
    """Non-overlapping window blocks, one per set, so no request repeats across sets.

    `sets` is [(set name, wanted shape names or empty for all)]. A set's block
    holds, from its top downwards: the refresh pass when refresh_s is set, the
    first pass (shift*runs) and the confirm pass (shift*confirm_runs), where
    shift is the largest per-run shift of any selected shape and range. The
    refresh pass is on top because it is the one that models a dashboard
    refresh, whose window ends at now: Loki recomputes the newest part of a
    window (max_cache_freshness_per_query), so the refresh pass must be the
    nearest to now of the three (the first pass follows it by a few minutes).
    The next set starts below the block (plus margin), rounded up to a minute.
    Every end is floored to the minute, as perf_matrix floors --end.

    Separate blocks keep a later set from meeting the proxy's exact-URL caches
    or a window an earlier set sent. They do not keep Loki's results cache from
    answering a query that two sets share, because it reuses overlapping time
    extents of the same query string; pr_smoke.reuse_first_cold handles that
    from Loki's own cache counters.

    Returns one dict per set: end (first pass), confirm_end, refresh_end (or
    None; the block's top), span (the block's length) and offset from the first set.
    """
    plans, offset = [], 0
    for name, wanted in sets:
        shapes = [s for s in spec["sets"][name]["shapes"] if not wanted or s["name"] in wanted]
        shift = max([shape_shift(s, r) for s in shapes for r in
                     (cells or {}).get(f"{name}/{s['name']}", ["instant"] if s.get("instant") else ranges)]
                    or [DEFAULT_SHIFT])
        # perf_matrix floors --end to the minute, so the plan records ends that are really sent.
        top = (end - offset) // 60 * 60
        used = shift * (runs + confirm_runs)
        refresh_end, first = None, top
        if refresh_s:
            refresh_end = top
            refresh_used = margin + (refresh_runs - 1) * refresh_s
            first = (top - refresh_used) // 60 * 60
            used += refresh_used
        span = -(-(used + margin) // 60) * 60
        plans.append({"set": name, "end": first, "confirm_end": (first - shift * runs) // 60 * 60,
                      "refresh_end": refresh_end, "shift": shift, "span": span, "offset": offset})
        offset += span
    return plans


def request_full(url, headers, tenant, timeout, max_body_bytes=None):
    """(status, body, seconds, started, calls): calls is the proxy's X-Loki-VL-Backend-Calls for this request (a proxy run
    with -debug-vl-calls-header), None when the answer carries none."""
    req = urllib.request.Request(url, headers={"X-Scope-OrgID": tenant, **headers})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body, status, calls = r.read() if max_body_bytes is None else r.read(max_body_bytes + 1), r.status, r.headers.get(reuse.CALLS_HEADER)
    except urllib.error.HTTPError as e:
        body, status, calls = e.read() if max_body_bytes is None else e.read(max_body_bytes + 1), e.code, e.headers.get(reuse.CALLS_HEADER)
    except (urllib.error.URLError, OSError) as e:
        return 0, str(e).encode()[:200], time.time() - t0, t0, None
    return status, body, time.time() - t0, t0, reuse.calls_of(calls)


CORRECTNESS_BODY_CAP = 32 * 1024 * 1024


def correctness_reference(base, shape, query, secs, step, end, tenant, timeout, measured=None, phase="original-window"):
    actual, mode = (query, "measured") if measured is not None else correctness_spelling(query, shape)
    url = request_url(base, shape, actual, secs, step, end)
    headers = {k: v for k, v in request_headers(shape).items() if k.lower() != "cache-control"}
    headers["Cache-Control"] = "no-cache"
    status, body, dt, at, _ = measured if measured is not None else request_full(
        url, headers, tenant, timeout, max_body_bytes=CORRECTNESS_BODY_CAP)
    try:
        decoded = strict_json(body) if len(body) <= CORRECTNESS_BODY_CAP else None
    except (ValueError, UnicodeError):
        decoded = body.decode(errors="replace") if status != 200 else None
    proof = answer_proof(status, body, shape) if len(body) <= CORRECTNESS_BODY_CAP else None
    data = decoded.get("data", decoded) if isinstance(decoded, dict) else None
    stats = data.get("stats") if isinstance(data, dict) else None
    raw = stats.get("cache") if isinstance(stats, dict) else None
    cache_absent = isinstance(decoded, dict) and all("cache" not in container and (
        "stats" not in container or isinstance(container["stats"], dict) and "cache" not in container["stats"])
        for container in (decoded, data) if isinstance(container, dict))
    observation = metadata_endpoint(shape) and proof == "answer" and cache_absent
    cache = report.correctness_cache_projection(raw)
    rejection = isinstance(proof, str) and proof.startswith("rejection:")
    record = dict(phase=phase, end=end, history_start=end-secs-query_lookback(query), at=at, seconds=round(dt, 3),
                  status=status, response_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest(), answer_proof=proof,
                  original_query=query, actual_query=actual, correctness_query_mode=mode, url=url,
                  correctness_cache_control=None if measured is not None else "no-cache", cache_raw=raw, cache=cache,
                  reference_kind="metadata-api-response" if observation else "computed-query", cache_status="unavailable" if observation else "observed", cache_absent=cache_absent,
                  signature=signature(status, body, shape.get("logs", False), query.lstrip().startswith(("topk", "bottomk"))) if proof else "unavailable",
                  ranked_max_points=ranked_max_points(query, secs, step, shape.get("instant", False)))
    record["reference_skip"] = "" if rejection or observation else report.reference_skip(record) if cache is not None else "cache-status-invalid-or-hit"
    record["complete"] = bool(proof and decoded is not None and math.isfinite(dt) and dt >= 0
                              and not (isinstance(decoded, dict) and decoded.get("warnings")) and not record["reference_skip"])
    return record, decoded


# Stitched Loki references (AGENTS.md, "AST/planner changes require runtime API and Grafana proof"): when Loki cannot
# answer a whole query_range (its request timed out), its answer is put together from requests over consecutive pieces
# of the same evaluation grid, asked one after another. The pieces are as long as the stack Loki's own
# split_queries_by_interval (test/e2e-compat/loki-local-config.yaml), so each one is the work of one of the whole
# request's own splits. Only the answer is taken from the pieces: the Loki timing of such a row stays its timeout.
STITCH_PARTITION_S = 3600
STITCHED_KIND = "stitched-query-range"


def timed_out_row(row):
    """True for a Loki row whose whole request timed out (is_timeout), sent or repeated by skipped_row."""
    return isinstance(row, dict) and row.get("status") == 0 and "timed out" in str(row.get("signature") or "")


def stitch_plan(start, end, step, part=STITCH_PARTITION_S):
    """The pieces [(start, end), ...] of the grid start, start+step, ..., end (Loki's query_range includes its end).

    The first piece starts at `start`, the last ends at `end` and every next piece starts one step after the previous
    one ends, so each grid point is in exactly one piece: no hole, no overlap. A cut is the first grid point at or after
    a multiple of `part`. Empty when the grid is not one: a start off the step (Loki's step alignment would then move
    the pieces' starts and not the whole request's) or an end off the grid."""
    if type(start) is not int or type(end) is not int or type(step) is not int or step <= 0 or part <= 0:
        return []
    if end < start or start % step or (end - start) % step:
        return []
    cuts, multiple = [], start // part + 1
    while multiple * part <= end:
        cut = start + -(-(multiple * part - start) // step) * step
        if start < cut <= end and (not cuts or cut > cuts[-1]):
            cuts.append(cut)
        multiple += 1
    bounds = [start, *cuts]
    return [(s, bounds[i + 1] - step if i + 1 < len(bounds) else end) for i, s in enumerate(bounds)]


def stitch_join(parts, step):
    """Loki's whole matrix answer from (piece, decoded answer) pairs of stitch_plan, or (None, problem).

    A series is one label set across the pieces; each point must sit on its piece's own grid and no timestamp may
    repeat within a series. The joined answer lists the series by label set and their points by time."""
    series = {}
    for (start, end), decoded in parts:
        data = decoded.get("data") if isinstance(decoded, dict) else None
        if (not isinstance(data, dict) or data.get("resultType") != "matrix" or not isinstance(data.get("result"), list)
                or decoded.get("warnings")):
            return None, "piece answer is not a matrix"
        for item in data["result"]:
            metric, values = item.get("metric") if isinstance(item, dict) else None, item.get("values") if isinstance(item, dict) else None
            if not isinstance(metric, dict) or not isinstance(values, list):
                return None, "piece answer is not a matrix"
            points = series.setdefault(json.dumps(metric, sort_keys=True), (metric, []))[1]
            for value in values:
                if (not isinstance(value, list) or len(value) != 2 or type(value[0]) not in (int, float)
                        or not start <= value[0] <= end or (value[0] - start) % step):
                    return None, "piece point off its grid"
                points.append(value)
    result = []
    for key in sorted(series):
        metric, points = series[key]
        points.sort(key=lambda p: p[0])
        if any(a[0] == b[0] for a, b in zip(points, points[1:])):
            return None, "a point repeats across pieces"
        result.append({"metric": metric, "values": points})
    return {"status": "success", "data": {"resultType": "matrix", "result": result}}, ""


def stitched_answer(base, shape, query, secs, step, end, tenant, timeout, spelling=None):
    """(record, decoded): Loki's answer of the window [end-secs, end] put together from stitch_plan's pieces.

    With `spelling` (an (actual query, mode) pair from correctness_spelling) every piece is a correctness read: that
    spelling and Cache-Control: no-cache; without it the pieces are asked like the measured request. The pieces are
    asked one after another, so Loki works on one piece at a time; the first piece that fails ends the read."""
    actual, mode = spelling or (query, "measured")
    headers = request_headers(shape)
    if spelling:
        headers = {k: v for k, v in headers.items() if k.lower() != "cache-control"}
        headers["Cache-Control"] = "no-cache"
    plan = stitch_plan(end - secs, end, step) if not shape.get("instant") and not shape.get("path") and not shape.get("logs") else []
    record = dict(kind=STITCHED_KIND, partition_s=STITCH_PARTITION_S, end=end, history_start=end-secs-query_lookback(query),
                  original_query=query, actual_query=actual, correctness_query_mode=mode,
                  correctness_cache_control="no-cache" if spelling else None, partitions=[], complete=False, problem="")
    if len(plan) < 3:  # the whole-request check asks two pieces at once: a smaller window than the whole needs three
        return {**record, "problem": "no stitch plan for this request"}, None
    parts = []
    for start, stop in plan:
        url = request_url(base, shape, actual, stop - start, step, stop)
        status, body, dt, at, _ = request_full(url, headers, tenant, timeout, max_body_bytes=CORRECTNESS_BODY_CAP)
        try:
            decoded = strict_json(body) if status == 200 and len(body) <= CORRECTNESS_BODY_CAP else None
        except (ValueError, UnicodeError):
            decoded = None
        data = decoded.get("data") if isinstance(decoded, dict) else None
        stats = data.get("stats") if isinstance(data, dict) else None
        raw = stats.get("cache") if isinstance(stats, dict) else None
        piece = dict(start=start, end=stop, url=url, status=status, seconds=round(dt, 3), at=int(at),
                     response_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest(),
                     answer_proof=answer_proof(status, body, shape) if len(body) <= CORRECTNESS_BODY_CAP else None,
                     warnings=bool(isinstance(decoded, dict) and decoded.get("warnings")), cache_raw=raw,
                     cache=report.correctness_cache_projection(raw),
                     signature=signature(status, body, False, query.lstrip().startswith(("topk", "bottomk"))))
        piece["reference_skip"] = report.reference_skip(piece) if piece["cache"] is not None else "cache-status-invalid-or-hit"
        record["partitions"].append(piece)
        if status != 200 or piece["answer_proof"] != "answer" or piece["warnings"] or decoded is None:
            return {**record, "problem": f"piece {start}-{stop} not answered"}, None
        parts.append(((start, stop), decoded))
    joined, problem = stitch_join(parts, step)
    if joined is None:
        return {**record, "problem": problem}, None
    body = json.dumps(joined, separators=(",", ":"), sort_keys=True).encode()
    # The pieces' cache counters summed: report.reference_skip reads them like one request's.
    cache = report.sum_cache([{"cache": p["cache"]} for p in record["partitions"]])
    record.update(complete=True, status=200, response_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest(),
                  answer_proof=answer_proof(200, body, shape), cache=cache,
                  signature=signature(200, body, False, query.lstrip().startswith(("topk", "bottomk"))),
                  seconds=round(sum(p["seconds"] for p in record["partitions"]), 3))
    if record["answer_proof"] != "answer" or len(body) > CORRECTNESS_BODY_CAP:
        return {**record, "complete": False, "problem": "joined answer unusable"}, None
    return record, joined


def stitched_pieces_computed(record):
    """True when Loki computed every piece: a usable cache projection that does not say its results cache held the
    whole piece."""
    return bool(record.get("partitions")) and all(
        p.get("status") == 200 and p.get("answer_proof") == "answer" and p.get("cache") is not None
        and p.get("reference_skip") == "" for p in record["partitions"])


def restrict(joined, start, end):
    """The joined answer's points in [start, end], series without one there left out."""
    result = [{"metric": s["metric"], "values": [v for v in s["values"] if start <= v[0] <= end]}
              for s in joined["data"]["result"]]
    return {"status": "success", "data": {"resultType": "matrix", "result": [s for s in result if s["values"]]}}


def stitched_reference(targets, shape, query, secs, step, end, tenant, timeout, measured):
    """(reference, decoded): the original window's stitched Loki answer, and the whole-request check that proves the
    stitching on this query.

    `measured` is the (record, decoded) of the run's stitched answer. It is the reference when Loki computed every
    piece of it; otherwise every piece is read again with a correctness spelling (extra read). The check asks Loki the
    whole window of the two pieces around the middle cut, itself a correctness read, and compares that answer with the
    stitched points of that window: a hole or an overlap at a cut would show there."""
    record, joined = measured if measured else ({}, None)
    extra = 0
    # A stitch that failed is not tried again (stitch_row): only a complete one with a cached piece is read again.
    if record.get("complete") and not stitched_pieces_computed(record):
        extra = 1
        record, joined = stitched_answer(targets["loki"], shape, query, secs, step, end, tenant, timeout,
                                         spelling=correctness_spelling(query, shape))
    ref = dict(record, phase="original-window", reference_kind=STITCHED_KIND, extra_reads=extra,
               url=request_url(targets["loki"], shape, record.get("actual_query", query), secs, step, end),
               reference_skip="", complete=False)
    if not record.get("complete") or not stitched_pieces_computed(record):
        return {**ref, "problem": record.get("problem") or "a piece was not computed"}, None
    pieces = record["partitions"]
    middle = len(pieces) // 2
    start, stop = pieces[middle - 1]["start"], pieces[middle]["end"]
    whole_ref, whole = correctness_reference(targets["loki"], shape, query, stop - start, step, stop, tenant, timeout)
    facets = (differ.diff(correctness_endpoint(shape), whole_ref["status"], whole, 200, restrict(joined, start, stop),
                          query=query) if whole_ref["complete"] and whole_ref["status"] == 200 else None)
    ref["whole_check"] = dict(start=start, end=stop, url=whole_ref["url"], status=whole_ref["status"],
                              seconds=whole_ref["seconds"], response_bytes=whole_ref["response_bytes"],
                              body_sha256=whole_ref["body_sha256"], answer_proof=whole_ref["answer_proof"],
                              cache_raw=whole_ref["cache_raw"], cache=whole_ref["cache"],
                              reference_skip=whole_ref["reference_skip"], actual_query=whole_ref["actual_query"],
                              correctness_query_mode=whole_ref["correctness_query_mode"],
                              correctness_cache_control=whole_ref["correctness_cache_control"],
                              facets=facets, matches=facets == [])
    if facets != []:
        return {**ref, "problem": "whole-request check differs from the stitched points"}, None
    ref["complete"] = True
    return ref, joined


def first_window_correctness(responses, shape, rng, query, secs, step, end, targets, tenant, timeout, stitched=None):
    proof = dict(complete=False, end=end, history_start=end-secs-query_lookback(query), query=query,
                 range=rng, run=0, extra_reads=0, targets={}, measured_reference=None)
    loki = responses.get("loki") or {}
    # Loki's whole request timed out (sent now, or in an earlier pass: loki_timeout): the reference is stitched.
    timed_out = stitched is not None and timed_out_row(loki.get("row"))
    if (set(responses) != set(targets) or "loki" not in responses
            or any(v.get("reply") is None for name, v in responses.items() if not (name == "loki" and timed_out))):
        return {**proof, "problem": "measured body unavailable or retention cap exceeded"}
    if (sum(len(v["reply"][1]) for v in responses.values() if v.get("reply")) > 3 * CORRECTNESS_BODY_CAP
            or any(len(v["reply"][1]) > CORRECTNESS_BODY_CAP or not answer_proof(v["reply"][0], v["reply"][1], shape)
                   for name, v in responses.items() if name != "loki")):
        return {**proof, "problem": "measured proxy answer unavailable"}
    if timed_out:
        row = loki["row"]
        proof["measured_reference"] = {k: row.get(k) for k in ("status", "seconds", "signature", "response_bytes",
                                                               "body_sha256", "answer_proof", "skipped")}
        proof["measured_reference"].update(phase="original-window", end=end, history_start=proof["history_start"],
                                           url=request_url(targets["loki"], shape, query, secs, step, end))
        ref, decoded = stitched_reference(targets, shape, query, secs, step, end, tenant, timeout, stitched)
        proof["extra_reads"] = ref["extra_reads"]
    else:
        ref, decoded = correctness_reference(targets["loki"], shape, query, secs, step, end, tenant, timeout,
                                             measured=responses["loki"]["reply"])
        proof["measured_reference"] = ref
        if not ref["complete"] and ref["status"] == 200 and (not metadata_endpoint(shape) or not ref["cache_absent"]):
            ref, decoded = correctness_reference(targets["loki"], shape, query, secs, step, end, tenant, timeout)
            proof["extra_reads"] = 1
    proof["reference"] = ref
    if not ref["complete"]:
        return {**proof, "problem": "computed reference unavailable"}
    endpoint = correctness_endpoint(shape)
    for name, value in responses.items():
        if name == "loki":
            continue
        status, body, dt, at, _ = value["reply"]
        answer = answer_proof(status, body, shape)
        try:
            parsed = strict_json(body)
        except (ValueError, UnicodeError):
            parsed = body.decode(errors="replace") if status != 200 else None
        if not answer or parsed is None or isinstance(parsed, dict) and parsed.get("warnings"):
            return {**proof, "problem": "measured proxy answer incomplete"}
        facets = differ.diff(endpoint, ref["status"], decoded, status, parsed,
                             limit=shape.get("limit", 0), direction=shape.get("direction", "backward"), query=query,
                             documented_values=shape.get("documented_values"))
        proof["targets"][name] = dict(status=status, response_bytes=len(body), body_sha256=hashlib.sha256(body).hexdigest(),
                                      answer_proof=answer, parity_vs_reference=report.correctness_parity(shape, facets), facets=facets)
    proof["complete"] = True
    return proof


def request(url, headers, tenant, timeout):
    status, body, seconds, started, _ = request_full(url, headers, tenant, timeout)
    return status, body, seconds, started


def is_timeout(status, body):
    """True when a request failed on its timeout (request() answers status 0 and the error text)."""
    return status == 0 and b"timed out" in body


def skipped_row(first, shape, rname, run, run_end, tname):
    """The row of a request that was not sent because `first`, the same target's timeout on this shape and range, was.

    It repeats the timeout's status, seconds and signature, so a summary reads the same Loki status on every run,
    and carries no CPU figure (nothing ran).
    """
    return {"shape": shape, "range": rname, "run": run, "end": run_end, "target": tname, "status": first["status"],
            "seconds": first["seconds"], "signature": first["signature"], "vl_cpu_max": None, "vl_cpu_avg": None,
            "skipped": "loki_timeout"}


def stitch_row(row, base, shape, query, secs, step, run_end, args, failed):
    """Put Loki's answer of a timed-out (or not sent) run together from pieces (stitched_answer) and record it on the
    row as "stitched"; the row's own status and seconds stay the timeout's. Returns (record, decoded).

    `failed` holds the shapes and ranges whose stitch failed in this pass: like a timeout, a failed stitch is not
    tried again on the later runs (their rows record why)."""
    key = (shape["name"], row["range"])
    if key in failed:
        record, decoded = {"kind": STITCHED_KIND, "complete": False, "partitions": [],
                           "problem": "not tried: an earlier run's stitch of this shape and range failed"}, None
    else:
        record, decoded = stitched_answer(base, shape, query, secs, step, run_end, args.tenant, args.timeout)
        if not record["complete"]:
            failed.add(key)
    row["stitched"] = record
    print(f"{shape['name'][:44]:44} {row['range']:>7} run{row['run']} {row['target']:8} stitched from "
          f"{len(record['partitions'])} pieces in {sum(p['seconds'] for p in record['partitions']):.2f}s: "
          f"{record.get('signature') or record['problem']}", flush=True)
    return record, decoded


def read_known_timeouts(path):
    """(target, shape, range) -> the first timeout row, from the "timeouts" of an earlier pass's raw file."""
    if not path:
        return {}
    with open(path) as f:
        return {(t["target"], t["shape"], t["range"]): t for t in json.load(f).get("timeouts", [])}


def flush_cache(url, token=""):
    """POST a proxy's /admin/cache/flush, so its next request meets empty cache tiers.

    It clears the response caches, the kept stats buckets and the parse-risk
    verdicts in memory and on disk. Fails the run when the proxy refuses.
    """
    headers = {"X-Admin-Token": token} if token else {}
    req = urllib.request.Request(url, data=b"", method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status = r.status
    except urllib.error.HTTPError as e:
        status = e.code
    except (urllib.error.URLError, OSError) as e:
        raise SystemExit(f"cache flush {url}: {e}")
    if status != 200:
        raise SystemExit(f"cache flush {url}: HTTP {status}")


def name_set_signature(kind, names):
    names = sorted(names)
    return f"{kind}={len(names)} set={hashlib.sha256(chr(10).join(names).encode()).hexdigest()[:12]}"


RANKED_RE = re.compile(r"\s*(?:topk|bottomk)\s*\(\s*(\d+)\s*,")


def ranked_max_points(query, secs, step, instant=False):
    """The most points a topk or bottomk answer can hold, or None for any other query: k per step (Loki keeps at most k
    series per step), over the steps of [end - secs, end] or the one step of an instant query."""
    found = RANKED_RE.match(query or "")
    if not found:
        return None
    k = int(found.group(1))
    if instant:
        return k
    if not step:
        return None
    return k * (int(secs / step + 1e-9) + 1)


def signature(status, body, logs, ranked=False):
    """A result fingerprint: equal signatures mean equal answers.

    A topk or bottomk answer (ranked) names no series count: when several series tie at a step,
    Loki keeps the tied one its map yields (the union of series over the range grows with
    it), so the count says nothing about the answer, while the points and the sum, which hold
    every step's ranked values, do.
    """
    if status != 200:
        try:
            return "ERR " + error_text(json.loads(body).get("error", ""))[:90]
        except (ValueError, AttributeError):
            return "ERR " + error_text(body.decode(errors="replace"))[:90]
    try:
        parsed = json.loads(body)
        # Loki answers detected_labels and detected_fields without a data wrapper.
        data = parsed["data"] if isinstance(parsed, dict) and "data" in parsed else parsed
    except (ValueError, KeyError, TypeError):
        return "unparseable"
    if isinstance(data, list) and data and isinstance(data[0], dict) and "pattern" in data[0]:
        # /patterns: Loki and the proxy mine different patterns (registry drilldown/patterns-mined-differently);
        # what both answer is how many lines they counted, per step.
        lines = sum(int(s[1]) for p in data for s in p.get("samples") or [] if len(s) > 1)
        steps = len({s[0] for p in data for s in p.get("samples") or [] if s})
        return f"patterns lines={lines} steps={steps}"
    if isinstance(data, list):
        # /labels, /label/<name>/values: the sorted set of names is the answer.
        # detected_fields answers a list of objects: the field names are the
        # answer (cardinalities come from a sample and vary between calls).
        return name_set_signature("names", [str(v.get("label")) if isinstance(v, dict) else str(v) for v in data])
    if isinstance(data, dict) and "detectedLabels" in data:
        return name_set_signature("labels", [str(d.get("label")) for d in data.get("detectedLabels") or []])
    if isinstance(data, dict) and "fields" in data:
        return name_set_signature("fields", [str(d.get("label")) for d in data.get("fields") or []])
    if data.get("resultType") in ("scalar", "string"):
        return data["resultType"] + "=" + hashlib.sha256(json.dumps(data.get("result"), separators=(",", ":")).encode()).hexdigest()
    result = data.get("result", [])
    if logs or data.get("resultType") == "streams":
        return f"streams={len(result)} lines={sum(len(s.get('values', [])) for s in result)}"
    total, points, not_numbers = 0.0, 0, 0
    for series in result:
        values = series.get("values") or ([series["value"]] if "value" in series else [])
        for _, v in values:
            try:
                total += float(v)
            except ValueError:
                # A sample that is not a number ("" or "86282s" from an unwrap
                # over a missing label or a unit string) is a wrong answer to
                # report, not a reason to stop the run.
                not_numbers += 1
            points += 1
    sig = f"series={'ranked' if ranked else len(result)} points={points} sum={total:.6g}"
    return f"{sig} not_numbers={not_numbers}" if not_numbers else sig


def exact_repeat_pass(targets, shape, rname, query, secs, step, end, args):
    """The exact repeat of one shape and range on every proxy target: the same request twice in a row at a settled window,
    each proxy's two asks back to back (before another target's ask and before its cache entry could age out).

    The first ask fills (what it cost is kept as the settled window's cold cost); the second is the repeat, whose
    VictoriaLogs calls are the request's own count when the proxy gives one, and whose answer must be the first's. A
    pair whose answers differ, or that did not answer 200 twice, carries a problem and no cost."""
    run_end = exact_repeat_end(end, shape, step)
    url_for = lambda base: request_url(base, shape, query, secs, step, run_end)  # noqa: E731
    headers = request_headers(shape)
    out = []
    for tname, base in targets:
        if tname == REFERENCE_TARGET:
            continue
        asks = []
        for _ in range(2):
            before = reuse.scrape(base)
            status, body, dt, _, calls = request_full(url_for(base), headers, args.tenant, args.timeout)
            after = reuse.scrape(base) if before is not None else None
            cost = reuse.delta(before, after) if after is not None else None
            if cost is not None and calls is not None:
                cost = reuse.with_request_calls(cost, calls)
            asks.append({"status": status, "seconds": round(dt, 3), "reuse": cost, "vl_calls_header": calls,
                         "signature": signature(status, body, shape.get("logs", False),
                                                query.lstrip().startswith(("topk", "bottomk")))})
        fill, repeat = asks
        row = {"target": tname, "shape": shape["name"], "range": rname, "end": run_end,
               "fill": {k: v for k, v in fill.items() if k != "signature"},
               "repeat": {k: v for k, v in repeat.items() if k != "signature"}}
        if fill["status"] != 200 or repeat["status"] != 200:
            row["problem"] = f"status {fill['status']} then {repeat['status']}"
        elif fill["signature"] != repeat["signature"]:
            row["problem"] = "the repeat answered differently"
        elif repeat["reuse"] is None:
            row["problem"] = "the proxy's metrics could not be read"
        out.append(row)
        print(f"{shape['name'][:44]:44} {rname:>7} exact {tname:8} {repeat['status']} {repeat['seconds']:7.2f}s "
              f"calls {repeat['vl_calls_header']} {row.get('problem', '')}", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", required=True, help="shape set name in shapes.json")
    ap.add_argument("--target", action="append", required=True, metavar="NAME=URL",
                    help="repeatable; name the Loki reference 'loki'")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--ranges", default="", help="comma-separated override of the set's ranges")
    ap.add_argument("--shapes", default="", help="comma-separated shape name prefixes (default all)")
    ap.add_argument("--shape", action="append", default=[], metavar="NAME",
                    help="repeatable exact shape name (names may contain commas)")
    ap.add_argument("--end", type=int, default=0,
                    help="unix seconds the first run's window ends at (default: a minute ago); floored to the minute")
    ap.add_argument("--shapes-file", default=os.path.join(HERE, "shapes.json"))
    ap.add_argument("--tenant", default="0")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--container", default="e2e-victorialogs")
    ap.add_argument("--vl-health", default="http://127.0.0.1:19428/health")
    ap.add_argument("--refresh-s", type=int, default=0, metavar="N",
                    help="refresh mode: run k ends at end + k*N seconds (the last run at --end) instead of moving "
                         "backwards; a multiple of every selected range's step")
    ap.add_argument("--long-runs", default="", metavar="NAME=N",
                    help="cap runs of ranges >= 24h for a target, e.g. main=1 when an old build scans raw rows "
                         "(repeated long raw scans have killed the shared backend)")
    ap.add_argument("--profile", default="",
                    help="the Loki profile this pass ran against (stack.PROFILES); recorded in the raw file")
    ap.add_argument("--not-like-for-like", action="append", default=[], metavar="TARGET",
                    help="repeatable: a target that did not run the setting the profile compares with (a proxy build "
                         "without -align-queries-with-step in the aligned profile); recorded in the raw file, and "
                         "report.py leaves its timings out of every comparison")
    ap.add_argument("--known-timeouts", default="", metavar="RAW.json",
                    help="raw file of an earlier pass of the same set: a shape and range whose Loki request timed out "
                         "there is not sent to Loki again (its rows repeat the timeout, marked skipped)")
    ap.add_argument("--pause", type=float, default=0.0, metavar="SECONDS",
                    help="wait this long after every request (default 0: sampling does not need it)")
    ap.add_argument("--flush-before-run0", action="append", default=[], metavar="URL",
                    help="POST this proxy admin URL (http://HOST:ADMIN_PORT/admin/cache/flush) before run 0 of every "
                         "shape and range, so run 0 is a cold first request for that proxy; repeatable; the admin "
                         "token, if any, is read from $AB_ADMIN_TOKEN. Loki's results cache is not flushed: read its "
                         "run-0 cache counters")
    ap.add_argument("--quiet-log", action="append", default=[], metavar="TARGET=PATH",
                    help="repeatable: the log file of a proxy build; before a cold request (run 0) the other builds' logs "
                         "are waited on until they have not grown for --quiet-s seconds (at most --quiet-cap-s), so the "
                         "request does not meet the work the previous one started")
    ap.add_argument("--quiet-s", type=float, default=1.0)
    ap.add_argument("--quiet-cap-s", type=float, default=4.0, help="0 = no wait")
    ap.add_argument("--proxy-metrics", action="store_true",
                    help="read every proxy target's /metrics around each request: the cache reuse of the row (reuse.py)")
    ap.add_argument("--range-plan", default="", help="existing selection.json with required per-shape runtime cells")
    ap.add_argument("--cells-plan", default="",
                    help="existing selection.json whose \"cells\" give some shapes their own ranges (selection.range_cells); "
                         "the other shapes run at --ranges")
    ap.add_argument("--exact-repeat", action="store_true",
                    help="with --proxy-metrics: for every shape and range ask each proxy target the same request twice in a "
                         "row at a settled window (EXACT_REPEAT_AGE before the end) and keep what the second one cost, "
                         "apart from the runs that move the window: an exact repeat is the cache's to answer, 0 VictoriaLogs "
                         "calls expected. Recorded as exact_repeats in the raw file")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    range_plan = {}
    if args.range_plan:
        with open(args.range_plan) as f:
            range_plan = json.load(f).get("runtime_proof", {}).get("cells", {})
    own_ranges = {}
    if args.cells_plan and not range_plan:
        with open(args.cells_plan) as f:
            own_ranges = json.load(f).get("cells", {})
    logs = dict(x.split("=", 1) for x in args.quiet_log)

    with open(args.shapes_file) as f:
        spec = load_shapes(f.read())
    shape_set = spec["sets"][args.set]
    range_defs = spec["ranges"]
    ranges = [r for r in (args.ranges.split(",") if args.ranges else shape_set["ranges"]) if r]
    targets = [t.split("=", 1) for t in args.target]
    long_runs = dict(x.split("=", 1) for x in args.long_runs.split(",") if x)
    wanted = [s for s in args.shapes.split(",") if s]
    exact = set(args.shape)

    def restarts():
        return subprocess.check_output(["docker", "inspect", args.container, "--format", "{{.RestartCount}}"], text=True).strip()

    def healthy_and_unrestarted(ref):
        last_error = None
        for _ in range(120):
            try:
                with urllib.request.urlopen(args.vl_health, timeout=5) as r:
                    if r.status == 200:
                        break
            except (urllib.error.URLError, OSError) as e:
                last_error = e  # still starting up; retry until the deadline
            time.sleep(1)
        else:
            print(f"VictoriaLogs did not become healthy: {last_error}", flush=True)
        return restarts() == ref

    end = (args.end or int(time.time()) - 60) // 60 * 60  # step-aligned for every range's step
    problem = refresh_problem(args.refresh_s, args.runs, [range_defs[r][1] for r in {*ranges, *(
        r for values in own_ranges.values() for r in values)} if r in range_defs])
    if problem:
        raise SystemExit(problem)
    sampler = CPUSampler(args.container)
    sampler.start()
    timed_out = read_known_timeouts(args.known_timeouts)
    restart0, rows, timings, timeouts, aborted = restarts(), [], [], [], False
    exact_repeats, stitch_failed = [], set()
    pair, quiet_waited = 0, 0.0
    for shape in shape_set["shapes"]:
        if wanted and not any(shape["name"].startswith(w) for w in wanted):
            continue
        if exact and shape["name"] not in exact:
            continue
        shape_ranges = ["instant"] if shape.get("instant") else ranges
        if range_plan:
            shape_ranges = [r for r in shape_ranges if r in range_plan.get(f"{args.set}/{shape['name']}", [])]
        elif f"{args.set}/{shape['name']}" in own_ranges:
            shape_ranges = own_ranges[f"{args.set}/{shape['name']}"]
        for rname in shape_ranges:
            pair += 1
            secs, step = range_defs.get(rname, (0, 0))
            query = shape_query(shape, step)
            for run in range(args.runs):
                source_bodies, retained_bytes = {}, 0
                if run == 0:
                    for flush_url in args.flush_before_run0:
                        flush_cache(flush_url, os.environ.get("AB_ADMIN_TOKEN", ""))
                # A shape may move its window by more than a minute per run, per range
                # ("shift": {"7d": 21617}) or for all ranges, e.g. far enough to miss the
                # response cache's time bucket the way a Grafana refresh eventually does;
                # "end_offset" (seconds) ends the whole window that long before the run's
                # end, e.g. a historical control next to near-now shapes. Refresh mode
                # moves forward instead (run_end_for).
                run_end = run_end_for(end, shape, rname, run, args.runs, args.refresh_s)
                # "pause" (seconds) waits before every run after the first, e.g. to
                # outlast a cache's max staleness so each run meets an aged entry.
                if run and shape.get("pause"):
                    time.sleep(float(shape["pause"]))
                for tname, base in target_order(targets, pair - 1 + run):
                    if run == 0 and tname != REFERENCE_TARGET:  # Loki runs on its own host process: nothing to wait for
                        quiet_waited += quiet_before(targets, tname, logs, args.quiet_s, args.quiet_cap_s)
                    if secs >= 86400 and tname in long_runs and run >= int(long_runs[tname]):
                        continue
                    key = (tname, shape["name"], rname)
                    url = request_url(base, shape, query, secs, step, run_end)
                    if tname == REFERENCE_TARGET and key in timed_out:
                        row = skipped_row(timed_out[key], shape["name"], rname, run, run_end, tname)
                        rows.append(row)
                        print(f"{shape['name'][:44]:44} {rname:>7} run{run} {tname:8} {row['status']} not sent: "
                              f"{tname} timed out on this shape and range", flush=True)
                        if args.range_plan:
                            row.update(request_url=url, answer_proof=None,
                                       history_start=run_end - secs - query_lookback(query))
                            stitched = stitch_row(row, base, shape, query, secs, step, run_end, args, stitch_failed)
                            if run == 0:
                                source_bodies[tname] = {"row": row, "reply": None, "stitched": stitched}
                        continue
                    headers = request_headers(shape)
                    metered = args.proxy_metrics and tname != REFERENCE_TARGET
                    before = reuse.scrape(base) if metered else None
                    status, body, dt, t0, calls = request_full(url, headers, args.tenant, args.timeout)
                    after = reuse.scrape(base) if before is not None else None
                    if args.pause:
                        time.sleep(args.pause)
                    # vl_cpu_max / vl_cpu_avg are filled from the sampler's samples after the loop.
                    row = {"shape": shape["name"], "range": rname, "run": run, "end": run_end, "target": tname, "status": status,
                           "seconds": round(dt, 3), "signature": signature(status, body, shape.get("logs", False),
                                                  query.lstrip().startswith(("topk", "bottomk"))),
                           "vl_cpu_max": None, "vl_cpu_avg": None, "at": int(t0)}
                    row["response_bytes"] = len(body)
                    if args.range_plan and run == 0:
                        row["body_sha256"] = hashlib.sha256(body).hexdigest()
                        row["request_url"] = url
                        retained_bytes += len(body)
                        source_bodies[tname] = {"row": row, "reply": (status, body, dt, t0, calls)
                                               if len(body) <= CORRECTNESS_BODY_CAP and retained_bytes <= 3 * CORRECTNESS_BODY_CAP else None}
                    if args.range_plan:
                        row["answer_proof"] = answer_proof(status, body, shape)
                        row["history_start"] = run_end - secs - query_lookback(query)
                    limit = ranked_max_points(query, secs, step, shape.get("instant", False))
                    if limit is not None:
                        row["ranked_max_points"] = limit
                    if after is not None:
                        row["reuse"] = reuse.delta(before, after)
                        if calls is not None:
                            # The request's own count: the proxy's background calls in the metrics are not its.
                            row["reuse"] = reuse.with_request_calls(row["reuse"], calls)
                    if calls is not None:
                        row["vl_calls_header"] = calls
                    timings.append((row, t0, dt))
                    cache = loki_cache_stats(body) if status == 200 else None
                    if cache:
                        row["cache"] = cache
                    rows.append(row)
                    print(f"{shape['name'][:44]:44} {rname:>7} run{run} {tname:8} {status} {dt:7.2f}s {row['signature'][:70]}", flush=True)
                    if tname == REFERENCE_TARGET and is_timeout(status, body):
                        timed_out[key] = row
                        timeouts.append({k: row[k] for k in ("target", "shape", "range", "run", "status", "seconds", "signature")})
                        if args.range_plan:
                            stitched = stitch_row(row, base, shape, query, secs, step, run_end, args, stitch_failed)
                            if tname in source_bodies:
                                source_bodies[tname]["stitched"] = stitched
                    if status in (0, 502) and not healthy_and_unrestarted(restart0):
                        print(f"ABORT: {args.container} RestartCount changed; this run is invalid", flush=True)
                        aborted = True
                        break
                if aborted:
                    break
                if args.range_plan and run == 0:
                    original = first_window_correctness(source_bodies, shape, rname, query, secs, step, run_end,
                                                        dict(targets), args.tenant, args.timeout,
                                                        stitched=source_bodies.get(REFERENCE_TARGET, {}).get("stitched"))
                    for value in source_bodies.values():
                        value["row"]["original_window_correctness"] = original
                    source_bodies.clear()
            if args.exact_repeat and args.proxy_metrics and not aborted:
                exact_repeats.extend(exact_repeat_pass(targets, shape, rname, query, secs, step, end, args))
            if aborted:
                break
        if aborted:
            break
    if timings:
        sampler.drain(max(t0 + dt for _, t0, dt in timings) + SAMPLE_WINDOW)
    sampler.stop = True
    for row, t0, dt in timings:
        row["vl_cpu_max"], row["vl_cpu_avg"] = sampler.between(t0, t0 + dt)
    restart1 = restarts()
    result = {"set": args.set, "description": shape_set.get("description", ""), "end": end, "runs": args.runs,
               "runtime_proof": bool(args.range_plan), "targets": [t for t, _ in targets], "target_urls": dict(targets), "restart_before": restart0, "restart_after": restart1,
              "valid": not aborted and restart0 == restart1, "cpu_samples_missed": sampler.failed,
              "mode": "refresh" if args.refresh_s else "shifted", "refresh_s": args.refresh_s,
              "profile": args.profile, "not_like_for_like": args.not_like_for_like,
              "quiet_wait_s": round(quiet_waited, 1),
              "exact_repeat_age_s": EXACT_REPEAT_AGE if args.exact_repeat else None, "exact_repeats": exact_repeats,
              "timeouts": timeouts, "rows": rows}
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    print(f"\nwrote {args.out}; VictoriaLogs RestartCount before={restart0} after={restart1}")


if __name__ == "__main__":
    main()
