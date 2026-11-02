"""Cold timings that one bad sample cannot fail: a warm-up before the first measured request, and a cold re-measure.

A cold cell (run 0 of a shape x range) is one sample per build, taken right after the other build's request,
on a VictoriaLogs that may be busy with the builds' own background work (window warming, label refreshes). Two
pull requests failed `perf-smoke` on such a sample while the base and the PR made the same upstream calls and
every other run of the row was equal. The confirm pass cannot clear it: its windows share the first pass's
1-hour split buckets, so the proxies answer its run 0 from their window cache and its "cold" is a warm time.

Two steps, both bounded:

  warm_up      after a build starts and before its first measured request: labels, label values and metric/log
               queries on a window no shape uses, repeated until the time stops moving, then the build's log
               goes quiet. It removes the start-up state from the first cold cell of every set.
  recold       for the shape x range pairs whose first-pass cold is a new or worse above-2× finding (the gate's
               own call, bounds and locks included): n more cold samples of each build on windows below every
               block, each taken after the build's caches were flushed (/admin/cache/flush) and both builds' logs
               went quiet, the build asked first alternating (ABAB). The cell becomes the median of the first-pass
               sample and the n new ones; the gate then judges it exactly as before. A real cold regression is
               slower in every sample and stays a finding; an outlier is outvoted. When a sample cannot be taken
               (flush refused, an error status, a shape on a moved window) the first-pass cold stands and so
               does its finding.

Nothing here changes a threshold, a lock or the above-2× rule: gate() and judge() read the median as they read
any cold.
"""
import concurrent.futures
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import over2x  # noqa: E402
import perf_matrix  # noqa: E402
import stack  # noqa: E402

COLD_SAMPLES = 3     # new cold samples per flagged row (the first pass is the fourth)
WARM_ROUNDS = 3      # at most; a build whose times stop moving earlier stops earlier
SAMPLE_SHIFT = 60    # seconds between two samples' windows
WARM_SPAN = 300      # the warm-up's windows are 5 minutes wide
SPARE_WINDOWS = 10   # windows the spare region (below the last block) holds before the seeded margin ends
QUIET_CAP_S = 10.0   # most seconds one wait for the builds' logs to go quiet takes
BUDGET_S = 300       # most seconds the cold re-measures of a whole run take; rows past it keep the first pass's cold


def median(values):
    values = sorted(values)
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


def flagged(findings):
    """The (shape, range) pairs with a new or worsened cold finding: the rows whose cold needs more samples."""
    return sorted({(f["shape"], f["range"]) for f in over2x.failing(findings) if f["mode"] == "cold"})


def spare_region(end, blocks, range_secs):
    """Where the warm-up and the cold samples send their requests: below the last set's block, which ends at
    end - blocks. The seed reaches range_secs + 900 s below it (pr_smoke.window_plan), so SPARE_WINDOWS windows
    of range_secs fit. The warm-up sits one range lower, so its short windows touch data no set's window does."""
    return {"cold_end": end - blocks - SAMPLE_SHIFT, "warm_end": end - blocks - range_secs - SAMPLE_SHIFT}


def decide(row, first, resampled, base, cand):
    """Replace a row's cold of both builds by the median of the first-pass sample and the new ones.

    first: {target: first-pass cold}; resampled: {target: [new colds]} or None when a sample could not be taken.
    Records the samples on the row (cold_samples) and returns True when the row was changed. A target with no
    first-pass time, or with fewer new samples than the other, leaves the row as it was.
    """
    if not resampled:
        return False
    n = min(len(resampled.get(base, [])), len(resampled.get(cand, [])))
    if n < 1 or any(first.get(t) is None for t in (base, cand)):
        return False
    samples = {t: [first[t], *resampled[t][:n]] for t in (base, cand)}
    for t, values in samples.items():
        row["cold"][t] = round(median(values), 3)
    row["cold_samples"] = samples
    return True


def quiet_all(proxies, cap=QUIET_CAP_S):
    """Wait for every build's log to go quiet, all at once (the longest wait counts, not their sum)."""
    procs = list(proxies.values())
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(procs) or 1) as pool:
        return all(pool.map(lambda p: p.wait_quiet(cap=cap), procs))


def sample_once(proxies, order, request, flush):
    """One cold sample of each build in `order`, or None when a flush was refused or an answer was not a 200.

    Before each request the build's caches are flushed and both builds' logs go quiet, so the build asked second
    does not meet the first one's background work and no build meets the work its own flush started.
    """
    out = {}
    for name in order:
        try:
            flush(proxies[name])
        except (SystemExit, OSError):
            return None
        quiet_all(proxies)
        status, secs = request(proxies[name])
        if status != 200:
            return None
        out[name] = secs
    return out


def resample(proxies, shape, rng, ranges, cold_end, n, base, cand, tenant="0", timeout=90):
    """{target: [n cold seconds]} for one shape x range, or None when any sample could not be taken."""
    if shape.get("end_offset"):
        return None  # a historical window: the spare region is not under it
    secs, step = ranges.get(rng, (0, 0))
    query = perf_matrix.shape_query(shape, step)
    headers = perf_matrix.request_headers(shape)
    got = {base: [], cand: []}
    for i in range(n):
        end = cold_end - i * SAMPLE_SHIFT

        def request(proxy, end=end):
            status, _, dt, _ = perf_matrix.request(perf_matrix.request_url(proxy.url, shape, query, secs, step, end),
                                                   headers, tenant, timeout)
            return status, round(dt, 3)

        one = sample_once(proxies, (base, cand) if i % 2 == 0 else (cand, base), request, flush_proxy)
        if one is None:
            return None
        for t, dt in one.items():
            got[t].append(dt)
    return got


def flush_proxy(proxy):
    url, token = proxy.flush_target()
    perf_matrix.flush_cache(url, token)


def recold(summary, first_cold, pairs, proxies, spec, cold_end, n=COLD_SAMPLES, log=print, deadline=None):
    """Re-measure the cold of `pairs` (shape, range) in a summary; returns one record per pair for the run's meta.

    first_cold: {(shape, range): {target: the first pass's cold}}, taken before the confirm pass replaced rows.
    A pair that cannot be re-measured (a sample failed, the time budget `deadline` is used up) gets the first
    pass's cold back for every target: the confirm pass replaced the row, and its run 0 is a window-cache answer
    for the proxies, so leaving it would clear a finding on a measurement that was never cold.
    """
    base, cand = summary["baseline"], summary["candidate"]
    shapes = {s["name"]: s for s in spec["sets"][summary["set"]]["shapes"]}
    rows = {(r["shape"], r["range"]): r for r in summary["rows"]}
    records = []
    for shape_name, rng in pairs:
        row, shape = rows.get((shape_name, rng)), shapes.get(shape_name)
        if row is None or shape is None:
            continue
        first, t0 = first_cold.get((shape_name, rng), {}), time.time()
        if deadline is not None and t0 > deadline:
            got, why = None, "the time budget for cold re-measures was used up"
        else:
            log(f"re-measuring the cold of '{shape_name}' {rng}: {n} flushed samples of each build, ABAB")
            got, why = resample(proxies, shape, rng, spec["ranges"], cold_end, n, base, cand), "a sample could not be taken"
        changed = decide(row, first, got, base, cand)
        if not changed and first:
            row["cold"] = dict(first)
        records.append({"set": summary["set"], "profile": summary.get("profile"), "shape": shape_name, "range": rng,
                        "applied": changed, "seconds": round(time.time() - t0, 1),
                        "samples": row.get("cold_samples") if changed else None,
                        **({} if changed else {"why": f"{why}; the first-pass cold stands"})})
    if records:
        quiet_all(proxies)  # the flushes' own refreshes finish before the next set's first request
    return records


def warm_round(proxy, end, tenant, timeout):
    """Seconds the warm-up requests of one round took on one build, on the window ending at `end`."""
    start = end - WARM_SPAN
    ns = {"start": f"{start}000000000", "end": f"{end}000000000"}
    urls = [perf_matrix.request_url(proxy.url, {"path": "/loki/api/v1/labels", "params": ns}, "", WARM_SPAN, 0, end),
            perf_matrix.request_url(proxy.url, {"path": "/loki/api/v1/label/env/values", "params": ns}, "", WARM_SPAN,
                                    0, end),
            perf_matrix.request_url(proxy.url, {"query": f"sum(count_over_time({stack.SELECTOR}[1m]))"},
                                    f"sum(count_over_time({stack.SELECTOR}[1m]))", WARM_SPAN, 15, end),
            perf_matrix.request_url(proxy.url, {"query": stack.SELECTOR, "logs": True, "limit": 10}, stack.SELECTOR,
                                    WARM_SPAN, 15, end)]
    return round(sum(perf_matrix.request(u, {}, tenant, timeout)[2] for u in urls), 3)


def warm_up(proxies, warm_end, rounds=WARM_ROUNDS, noise=0.30, min_delta=0.10, tenant="0", timeout=60):
    """Warm every build, one at a time: rounds of requests on windows no shape uses, until a round is not slower
    beyond the noise than the one before it, then until its log goes quiet. Never fails the run: what happened
    is returned (per build: the rounds' seconds and whether it went quiet) for the run's meta."""
    out = {}
    for name, proxy in proxies.items():
        times = []
        for i in range(rounds):
            times.append(warm_round(proxy, warm_end - i * SAMPLE_SHIFT, tenant, timeout))
            if i and not over2x.slower(times[-2], times[-1], noise, min_delta):
                break
        out[name] = {"rounds_s": times, "quiet": proxy.wait_quiet(cap=QUIET_CAP_S)}
    return out
