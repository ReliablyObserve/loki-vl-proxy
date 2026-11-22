#!/usr/bin/env python3
"""The proxy's cache reuse as its own counters show it, around each A/B request.

perf_matrix.py --proxy-metrics reads a proxy target's /metrics before and after every request it sends and keeps
the delta on the row (`reuse`). The keys are the ones test/caseverify reads:

  vl_calls, vl_calls:<endpoint>   VictoriaLogs requests (loki_vl_proxy_backend_duration_seconds_count), in all and
                                  per VictoriaLogs endpoint (query, stats_query_range, stats_query, hits, ...)
  vl_bytes                        decoded VictoriaLogs body bytes (loki_vl_proxy_backend_read_bytes_total)
  <layer>:<tier>:requests|hits    cache lookups and hits per semantic layer (loki: the Loki-facing answers,
                                  vl: the VictoriaLogs-facing results) and tier (l1_memory, l2_disk, l3_peer)
  cpu_seconds                     the proxy's CPU (process_cpu_seconds_total)
  rss_before_bytes, rss_after_bytes, rss_max_sampled_bytes
                                  absolute proxy RSS at the existing two scrapes, not a request allocation,
                                  a counter delta or a continuously sampled peak; rss_samples records availability
  translations                    LogQL translations (loki_vl_proxy_translations_total)

report.py puts the first run's delta (cold), the median of the later runs' (repeat: the same shape with its window
moved by a minute, as the A/B runs it) and the refresh pass's on each summary row.

The VictoriaLogs calls of a request are the proxy's own count of that request (the X-Loki-VL-Backend-Calls header of
a proxy run with -debug-vl-calls-header) when the answer carries it: `vl_calls` of the row's reuse is that count and
`background_vl_calls` what the process-wide counters saw beyond it (health probes, the stream field inventory's
refreshes, other clients). A proxy that sends no header (an older build) leaves `vl_calls` the process-wide counter's
delta, which includes its background work. The counters still give bytes, hit ratios and CPU. Not measured, for want
of a metric: allocations per request, plan and translation cache hits, and the reuse of proxy-evaluated operators.
"""
import math
import re
import statistics
import urllib.request

# The proxy's own count of the VictoriaLogs calls one request started before its response headers were written
# (-debug-vl-calls-header, internal/proxy vl_calls_header.go).
CALLS_HEADER = "X-Loki-VL-Backend-Calls"

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def keys_of(name, labels):
    if name == "loki_vl_proxy_backend_duration_seconds_count":
        return ["vl_calls", "vl_calls:" + labels.get("endpoint", "")]
    if name == "loki_vl_proxy_backend_read_bytes_total":
        return ["vl_bytes"]
    if name == "loki_vl_proxy_cache_layer_requests_total":
        return [f"{labels.get('layer', '')}:{labels.get('tier', '')}:requests"]
    if name == "loki_vl_proxy_cache_layer_hits_total":
        return [f"{labels.get('layer', '')}:{labels.get('tier', '')}:hits"]
    if name == "process_cpu_seconds_total":
        return ["cpu_seconds"]
    if name == "process_resident_memory_bytes":
        return ["rss_bytes"]
    if name == "loki_vl_proxy_translations_total":
        return ["translations"]
    return []


def parse(text):
    """The Prometheus text exposition as a snapshot: counters and the process RSS gauge."""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = LINE.match(line)
        if not m:
            continue
        try:
            value = float(m.group(4))
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        labels = dict(LABEL.findall(m.group(3) or ""))
        for key in keys_of(m.group(1), labels):
            out[key] = out.get(key, 0.0) + value
    return out


def delta(before, after):
    """Counter changes, plus available absolute RSS samples from the same two scrapes."""
    out = {k: round(v - before.get(k, 0.0), 6) for k, v in after.items()
           if k not in ("rss_bytes", "cpu_seconds") and v - before.get(k, 0.0) != 0}
    rss = []
    for name, snapshot in (("before", before), ("after", after)):
        value = snapshot.get("rss_bytes")
        if value is not None and math.isfinite(value) and value >= 0:
            out[f"rss_{name}_bytes"] = value
            rss.append(value)
    if rss:
        out.update(rss_max_sampled_bytes=max(rss), rss_samples=len(rss))
    cpu = [s.get("cpu_seconds") for s in (before, after)]
    valid = [v for v in cpu if isinstance(v, (int, float)) and math.isfinite(v) and v >= 0]
    if valid:
        out["cpu_samples"] = len(valid)
    if len(valid) == 2 and valid[1] >= valid[0]:
        out["cpu_seconds"] = round(valid[1] - valid[0], 6)
    return out


def hit_ratio(d):
    """Hits over lookups per layer and tier of a delta."""
    out = {}
    for k, req in d.items():
        if k.endswith(":requests") and req > 0:
            prefix = k[: -len(":requests")]
            out[prefix] = round(d.get(prefix + ":hits", 0.0) / req, 6)
    return out


def scrape(base, timeout=10):
    """A proxy's snapshot, or None when its /metrics cannot be read."""
    try:
        with urllib.request.urlopen(base.rstrip("/") + "/metrics", timeout=timeout) as r:
            return parse(r.read().decode("utf-8", "replace"))
    except (OSError, ValueError):
        return None


def median(deltas):
    """Counter medians include unchanged zeroes; absent RSS samples remain unmeasured."""
    original = list(deltas)
    deltas = [d for d in original if d is not None]
    if not deltas:
        return None
    cpu = [d["cpu_seconds"] for d in deltas if d.get("cpu_samples") == 2 and
           type(d.get("cpu_seconds")) in (int, float) and math.isfinite(d["cpu_seconds"]) and d["cpu_seconds"] >= 0]
    keys = sorted({k for d in deltas for k in d} - {"cpu_seconds"})
    out = {k: round(statistics.median(d[k] for d in deltas if k in d), 6) if k.startswith("rss_") or k.startswith("cpu_")
           else round(statistics.median(d.get(k, 0.0) for d in deltas), 6) for k in keys}
    if cpu:
        out["cpu_seconds"] = round(statistics.median(cpu), 6)
    if any("rss_samples" in d or "cpu_samples" in d for d in deltas):
        out.update(metric_runs_expected=len(original),
                   rss_runs_measured=sum(d.get("rss_samples") == 2 for d in deltas),
                   cpu_runs_measured=len(cpu))
    return {k: v for k, v in out.items() if v != 0 or k.startswith(("cpu_", "rss_"))}


def calls_of(value):
    """The calls of a X-Loki-VL-Backend-Calls header value: a non-negative integer, else None (absent is not 0)."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def with_request_calls(d, calls):
    """A delta whose vl_calls is the request's own count; what the counters saw beyond it is background_vl_calls."""
    out = dict(d)
    total = out.pop("vl_calls", 0.0)
    if calls:
        out["vl_calls"] = float(calls)
    background = round(max(0.0, total - calls), 6)
    if background:
        out["background_vl_calls"] = background
    return out
