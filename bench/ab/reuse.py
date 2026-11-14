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
  translations                    LogQL translations (loki_vl_proxy_translations_total)

report.py puts the first run's delta (cold), the median of the later runs' (repeat: the same shape with its window
moved by a minute, as the A/B runs it) and the refresh pass's on each summary row. Not measured, for want of a
metric: allocations per request, plan and translation cache hits, the reuse of proxy-evaluated operators, and a
per-request attribution of the counters (the proxy's background work counts too).
"""
import re
import statistics
import urllib.request

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
    if name == "loki_vl_proxy_translations_total":
        return ["translations"]
    return []


def parse(text):
    """The Prometheus text exposition as a snapshot: counters summed per key."""
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
        labels = dict(LABEL.findall(m.group(3) or ""))
        for key in keys_of(m.group(1), labels):
            out[key] = out.get(key, 0.0) + value
    return out


def delta(before, after):
    """after less before, the keys that moved only."""
    return {k: round(v - before.get(k, 0.0), 6) for k, v in after.items() if v - before.get(k, 0.0) != 0}


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
    """The per-key median of several deltas (a key missing from one delta counts as zero there)."""
    deltas = [d for d in deltas if d is not None]
    if not deltas:
        return None
    keys = sorted({k for d in deltas for k in d})
    out = {k: round(statistics.median(d.get(k, 0.0) for d in deltas), 6) for k in keys}
    return {k: v for k, v in out.items() if v != 0}
