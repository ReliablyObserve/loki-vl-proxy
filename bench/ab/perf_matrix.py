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
import os
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


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


def plan_windows(spec, sets, ranges, runs, confirm_runs, end, refresh_s=0, refresh_runs=0, margin=60):
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
        shift = max([shape_shift(s, r) for s in shapes for r in (["instant"] if s.get("instant") else ranges)]
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


def request(url, headers, tenant, timeout):
    req = urllib.request.Request(url, headers={"X-Scope-OrgID": tenant, **headers})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    except (urllib.error.URLError, OSError) as e:
        return 0, str(e).encode()[:200], time.time() - t0, t0
    return status, body, time.time() - t0, t0


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


def signature(status, body, logs, ranked=False):
    """A result fingerprint: equal signatures mean equal answers.

    A topk or bottomk answer (ranked) names no series count: when several series tie at a step,
    Loki keeps the tied one its map yields (the union of series over the range grows with
    it), so the count says nothing about the answer, while the points and the sum, which hold
    every step's ranked values, do.
    """
    if status != 200:
        try:
            return "ERR " + json.loads(body).get("error", "")[:90]
        except (ValueError, AttributeError):
            return "ERR " + body[:90].decode(errors="replace")
    try:
        parsed = json.loads(body)
        # Loki answers detected_labels and detected_fields without a data wrapper.
        data = parsed["data"] if isinstance(parsed, dict) and "data" in parsed else parsed
    except (ValueError, KeyError, TypeError):
        return "unparseable"
    if isinstance(data, list):
        # /labels, /label/<name>/values: the sorted set of names is the answer.
        # detected_fields answers a list of objects: the field names are the
        # answer (cardinalities come from a sample and vary between calls).
        return name_set_signature("names", [str(v.get("label")) if isinstance(v, dict) else str(v) for v in data])
    if isinstance(data, dict) and "detectedLabels" in data:
        return name_set_signature("labels", [str(d.get("label")) for d in data.get("detectedLabels") or []])
    if isinstance(data, dict) and "fields" in data:
        return name_set_signature("fields", [str(d.get("label")) for d in data.get("fields") or []])
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
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.shapes_file) as f:
        spec = json.load(f)
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
    problem = refresh_problem(args.refresh_s, args.runs, [range_defs[r][1] for r in ranges if r in range_defs])
    if problem:
        raise SystemExit(problem)
    sampler = CPUSampler(args.container)
    sampler.start()
    timed_out = read_known_timeouts(args.known_timeouts)
    restart0, rows, timings, timeouts, aborted = restarts(), [], [], [], False
    for shape in shape_set["shapes"]:
        if wanted and not any(shape["name"].startswith(w) for w in wanted):
            continue
        if exact and shape["name"] not in exact:
            continue
        shape_ranges = ["instant"] if shape.get("instant") else ranges
        for rname in shape_ranges:
            secs, step = range_defs.get(rname, (0, 0))
            query = shape.get("query", "").replace("$__auto", f"{step}s")
            for run in range(args.runs):
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
                for tname, base in targets:
                    if secs >= 86400 and tname in long_runs and run >= int(long_runs[tname]):
                        continue
                    key = (tname, shape["name"], rname)
                    if tname == REFERENCE_TARGET and key in timed_out:
                        row = skipped_row(timed_out[key], shape["name"], rname, run, run_end, tname)
                        rows.append(row)
                        print(f"{shape['name'][:44]:44} {rname:>7} run{run} {tname:8} {row['status']} not sent: "
                              f"{tname} timed out on this shape and range", flush=True)
                        continue
                    if shape.get("instant"):
                        url = f"{base}/loki/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": run_end})
                    elif shape.get("path"):
                        # A metadata endpoint (labels, label values, detected labels or
                        # fields, index volume) over the range; the shape's params are
                        # passed through and its query, if any, becomes the selector.
                        params = {"start": f"{run_end - secs}000000000", "end": f"{run_end}000000000", **shape.get("params", {})}
                        if shape.get("query"):
                            params["query"] = query
                        url = f"{base}{shape['path']}?" + urllib.parse.urlencode(params)
                    else:
                        params = {"query": query, "start": run_end - secs, "end": run_end, "step": step,
                                  "limit": shape.get("limit", 1000), "direction": "backward"}
                        url = f"{base}/loki/api/v1/query_range?" + urllib.parse.urlencode(params)
                    headers = request_headers(shape)
                    status, body, dt, t0 = request(url, headers, args.tenant, args.timeout)
                    if args.pause:
                        time.sleep(args.pause)
                    # vl_cpu_max / vl_cpu_avg are filled from the sampler's samples after the loop.
                    row = {"shape": shape["name"], "range": rname, "run": run, "end": run_end, "target": tname, "status": status,
                           "seconds": round(dt, 3), "signature": signature(status, body, shape.get("logs", False),
                                                  query.lstrip().startswith(("topk", "bottomk"))),
                           "vl_cpu_max": None, "vl_cpu_avg": None, "at": int(t0)}
                    timings.append((row, t0, dt))
                    cache = loki_cache_stats(body) if status == 200 else None
                    if cache:
                        row["cache"] = cache
                    rows.append(row)
                    print(f"{shape['name'][:44]:44} {rname:>7} run{run} {tname:8} {status} {dt:7.2f}s {row['signature'][:70]}", flush=True)
                    if tname == REFERENCE_TARGET and is_timeout(status, body):
                        timed_out[key] = row
                        timeouts.append({k: row[k] for k in ("target", "shape", "range", "run", "status", "seconds", "signature")})
                    if status in (0, 502) and not healthy_and_unrestarted(restart0):
                        print(f"ABORT: {args.container} RestartCount changed; this run is invalid", flush=True)
                        aborted = True
                        break
                if aborted:
                    break
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
               "targets": [t for t, _ in targets], "restart_before": restart0, "restart_after": restart1,
              "valid": not aborted and restart0 == restart1, "cpu_samples_missed": sampler.failed,
              "mode": "refresh" if args.refresh_s else "shifted", "refresh_s": args.refresh_s,
              "profile": args.profile, "not_like_for_like": args.not_like_for_like,
              "timeouts": timeouts, "rows": rows}
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    print(f"\nwrote {args.out}; VictoriaLogs RestartCount before={restart0} after={restart1}")


if __name__ == "__main__":
    main()
