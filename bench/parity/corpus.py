#!/usr/bin/env python3
"""Build the differential query corpus: every request the runner sends to Loki and the proxy.

  corpus.py --out corpus.json [--loki-src DIR] [--captures DIR ...] [--seeded URL --end UNIX]

Sources (each entry records its `source`):
  loki-tests   LogQL strings from Loki's own test suites (--loki-src: a checkout of
               grafana/loki at the reference version, pkg/logql at least). Stream
               selectors are rewritten onto the seeded data (a JSON, logfmt, plain
               or mixed stream, picked by the parser the query uses); sharding and
               template fixtures are dropped, and ranges longer than the seeded
               hour are clamped to [1h].
  repo         the repository's own parity cases: LogQL in test/e2e-compat/*_test.go,
               test/e2e-compat/query-semantics-matrix.json, bench/ab/shapes.json and
               every conformance/registry case's request. Selectors naming data the
               seeded stack does not hold are rewritten the same way.
  seeded       generated from what Loki itself reports for the seeded window
               (--seeded LOKI_URL): every label and its values, every service's
               detected fields, and per field the stages Explore and Logs Drilldown
               build (filters, keep/drop, extraction lists, line_format, grouping,
               unwrap), plus the metadata endpoints (labels, values, series,
               index/stats, volume, volume_range, detected_*, patterns,
               format_query, tail).
  grafana      every request Grafana Explore and Logs Drilldown sent on the
               bench/visual pages (--captures: capture output directories); they are
               replayed through Grafana's own API against the Loki and proxy
               datasources, so the corpus covers the exact datasource path.

Times are placeholders ($start, $end, $step, $start_ns, $end_ns) that run.py fills
from the seeded window.
"""
import argparse
import glob
import hashlib
import json
import os
import re
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

# The stream the rewritten query lands on, by the parser it uses (test/e2e-compat/log-generator.py).
SELECTORS = {
    "json": '{service_name="api-gateway"}',
    "logfmt": '{service_name="payment-service"}',
    "pattern": '{service_name="nginx-ingress"}',
    "regexp": '{service_name="nginx-ingress"}',
    "unpack": '{service_name="api-gateway"}',
    "otel": '{service_name="otel-collector"}',
    "mixed": '{namespace="prod"}',
}
# Label values the seeded data holds; a selector using anything else is rewritten.
SEEDED_LABELS = {"app", "service_name", "namespace", "cluster", "env", "level", "pod", "container", "version",
                 "component", "role", "db_name", "db", "gpu", "instrumentation"}
SEEDED_VALUES = {
    "namespace": {"prod", "data", "batch", "ml", "ingress-nginx", "monitoring"},
    "env": {"production"},
    "cluster": {"us-east-1", "us-west-2"},
}
MATCHER = re.compile(r'\s*([A-Za-z_][A-Za-z0-9_.]*)\s*(=~|!~|!=|=)\s*("(?:[^"\\]|\\.)*"|`[^`]*`)\s*')
LOGQL_START = re.compile(
    r'^\s*(\{|(sum|avg|min|max|count|topk|bottomk|stddev|stdvar|sort|sort_desc|rate|count_over_time|bytes_rate|'
    r'bytes_over_time|absent_over_time|label_replace|quantile_over_time|avg_over_time|sum_over_time|max_over_time|'
    r'min_over_time|first_over_time|last_over_time|stddev_over_time|stdvar_over_time|rate_counter|approx_topk)'
    r'\s*(by|without)?\s*[\(\{])')
STRING = re.compile(r'`([^`]*)`|"((?:[^"\\\n]|\\.)*)"')
# Loki test fixtures that are not queries a client sends: sharding plans, templates, JSON lines.
NOT_A_QUERY = ("downstream<", "++", "shard=", "<nil>", "{{", "__count_min_sketch__", "__quantile_sketch",
               "variants(", "approx_topk")


def scan_selectors(query):
    """(start, end) spans of every stream selector in a LogQL string, skipping string literals."""
    spans, i, n = [], 0, len(query)
    while i < n:
        c = query[i]
        if c in "\"`":
            close = query.find(c, i + 1)
            while c == '"' and close != -1 and query[close - 1] == "\\" and query[close - 2] != "\\":
                close = query.find(c, close + 1)
            i = n if close == -1 else close + 1
            continue
        if c == "{":
            j, ok = i + 1, False
            while True:
                m = MATCHER.match(query, j)
                if not m:
                    break
                ok, j = True, m.end()
                if j < n and query[j] == ",":
                    j += 1
                    continue
                break
            k = j
            while k < n and query[k] == " ":
                k += 1
            if ok and k < n and query[k] == "}":
                spans.append((i, k + 1))
                i = k + 1
                continue
        i += 1
    return spans


def selector_matchers(text):
    return [(m.group(1), m.group(2), m.group(3)[1:-1]) for m in MATCHER.finditer(text.strip("{} "))]


def selector_seeded(text):
    """True when every matcher names data the seeded stack holds."""
    matchers = selector_matchers(text)
    if not matchers:
        return False
    for name, op, value in matchers:
        if name not in SEEDED_LABELS:
            return False
        if op == "=" and name in SEEDED_VALUES and value not in SEEDED_VALUES[name]:
            return False
        if op == "=" and name in ("app", "service_name") and not re.fullmatch(r"[a-z0-9-]+", value):
            return False
        if op == "=" and name in ("app", "service_name") and re.search(r"\d{6,}|compat|test|<", value):
            return False
    return True


def parser_of(query):
    for name in ("json", "logfmt", "pattern", "regexp", "unpack"):
        if re.search(rf"\|\s*{name}\b", query):
            return name
    return "mixed"


def rewrite(query, force=False):
    """The query with every stream selector moved onto the seeded data (unless already seeded)."""
    spans = scan_selectors(query)
    if not spans:
        return None
    target = SELECTORS[parser_of(query)]
    out, last = [], 0
    for start, end in spans:
        out.append(query[last:start])
        sel = query[start:end]
        out.append(sel if (not force and selector_seeded(sel)) else target)
        last = end
    out.append(query[last:])
    return "".join(out)


def is_metric(query):
    return not query.lstrip("( ").startswith("{")


def extract_strings(paths):
    found = set()
    for path in paths:
        with open(path, errors="ignore") as handle:
            text = handle.read()
        for m in STRING.finditer(text):
            raw = m.group(1) if m.group(1) is not None else m.group(2)
            if m.group(2) is not None:
                try:
                    raw = json.loads('"' + m.group(2) + '"')
                except ValueError:
                    continue
            if not raw or len(raw) > 600 or "\n\n" in raw or any(x in raw for x in NOT_A_QUERY):
                continue
            if "%" in raw and re.search(r"%[sdvq]", raw):
                continue  # a Go format string, filled at test time
            if LOGQL_START.match(raw) and scan_selectors(raw):
                found.add(" ".join(raw.split()))
    return found


def entry(source, endpoint, params, origin="", headers=None, transport="api", **extra):
    body = json.dumps([endpoint, params, headers or {}, transport, extra], sort_keys=True)
    out = {"id": hashlib.sha256(body.encode()).hexdigest()[:12], "source": source, "endpoint": endpoint,
           "params": params, "origin": origin, "transport": transport}
    if headers:
        out["headers"] = headers
    out.update(extra)
    return out


def query_entries(source, query, origin=""):
    """The requests one LogQL query becomes: a log range query, or a metric range and instant query."""
    if is_metric(query):
        return [entry(source, "query_range", {"query": query, "start": "$start", "end": "$end", "step": "$step"}, origin),
                entry(source, "query", {"query": query, "time": "$end"}, origin)]
    return [entry(source, "query_range", {"query": query, "start": "$start", "end": "$end", "limit": "100",
                                          "direction": "backward"}, origin)]


RANGE = re.compile(r"\[(\d+)([smhdwy])\]")
UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800, "y": 31536000}


def clamp_ranges(query, limit_s=3600):
    """Range selectors longer than the seeded window read every line once per step on Loki and prove nothing more."""
    return RANGE.sub(lambda m: m.group(0) if int(m.group(1)) * UNIT_S[m.group(2)] <= limit_s else "[1h]", query)


def loki_tests(src):
    paths = glob.glob(os.path.join(src, "pkg", "logql", "**", "*_test.go"), recursive=True)
    out, seen = [], set()
    for raw in sorted(extract_strings(paths)):
        query = rewrite(raw, force=True)
        query = clamp_ranges(query) if query else query
        if not query or query in seen:
            continue
        seen.add(query)
        out += query_entries("loki-tests", query, origin=raw)
    return out


# Grafana template variables in the repository's shapes, replaced by what Grafana sends for an hour at 60s steps.
GRAFANA_VARS = (("$__auto", "1m"), ("$__interval", "1m"), ("$__range", "1h"))


def repo_queries():
    out, seen = [], set()

    def add(source, raw, origin):
        for var, value in GRAFANA_VARS:
            raw = raw.replace(var, value)
        query = rewrite(raw)
        if query and query not in seen:
            seen.add(query)
            out.extend(query_entries(source, query, origin))

    for raw in sorted(extract_strings(glob.glob(os.path.join(ROOT, "test", "e2e-compat", "*_test.go")))):
        add("repo", raw, "test/e2e-compat")
    matrix = os.path.join(ROOT, "test", "e2e-compat", "query-semantics-matrix.json")
    for case in json.load(open(matrix))["cases"]:
        add("repo", case["query"], f"query-semantics-matrix:{case['id']}")
    shapes = json.load(open(os.path.join(ROOT, "bench", "ab", "shapes.json")))
    for name, group in shapes["sets"].items():
        for shape in group["shapes"]:
            if shape.get("query"):
                add("repo", shape["query"], f"shapes:{name}:{shape['name']}")
    for path in sorted(glob.glob(os.path.join(ROOT, "conformance", "registry", "cases", "**", "*.yaml"), recursive=True)):
        text = open(path).read()
        found = re.search(r"^\s+query:\s*'(.*)'\s*$", text, re.M) or re.search(r'^\s+query:\s*"(.*)"\s*$', text, re.M)
        if found:
            add("repo", found.group(1).replace("''", "'"), "registry:" + os.path.relpath(path, ROOT))
    return out


# ---------------------------------------------------------------- seeded

def loki_get(base, path, params, timeout=60):
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"X-Scope-OrgID": "0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def quote(value):
    return json.dumps(value)


def field_queries(selector, field, parsers, numeric):
    """The stages Explore and Logs Drilldown build on one detected field."""
    parser = "logfmt" if parsers == ["logfmt"] else "json" if parsers else ""
    stage = f" | {parser}" if parser else ""
    q = [f"{selector}{stage} | {field}!=\"\"",
         f"{selector}{stage} | {field}=~\".+\"",
         f"{selector}{stage} | keep {field}",
         f"{selector}{stage} | drop {field}",
         f"{selector}{stage} | line_format \"{{{{.{field}}}}}\"",
         f"sum by ({field}) (count_over_time({selector}{stage} [5m]))",
         f"sum by ({field}) (count_over_time({selector}{stage} | {field}!=\"\" [5m]))",
         f"sum(count_over_time({selector}{stage} | {field}!=\"\" [5m]))"]
    if parser == "json":
        q.append(f"{selector} | json {field}=\"{field}\"")
    if parser == "logfmt":
        q.append(f"{selector} | logfmt {field}")
    if numeric:
        q += [f"avg_over_time({selector}{stage} | unwrap {field} | __error__=\"\" [5m])",
              f"sum by (level) (max_over_time({selector}{stage} | unwrap {field} | __error__=\"\" [5m]))",
              f"quantile_over_time(0.9, {selector}{stage} | unwrap {field} | __error__=\"\" [5m]) by (service_name)"]
    return q


def seeded(loki, end, window=3600, max_fields=14):
    start = end - window
    ns = {"start": str(start * 10 ** 9), "end": str(end * 10 ** 9)}
    out = []

    def add(endpoint, params, origin, **extra):
        out.append(entry("seeded", endpoint, params, origin, **extra))

    labels = loki_get(loki, "/loki/api/v1/labels", ns).get("data") or []
    add("labels", {"start": "$start_ns", "end": "$end_ns"}, "all labels")
    add("labels", {"start": "$start_ns", "end": "$end_ns", "query": '{namespace="prod"}'}, "labels with query")
    services = []
    for name in labels:
        add("label_values", {"name": name, "start": "$start_ns", "end": "$end_ns"}, f"values of {name}")
        add("label_values", {"name": name, "start": "$start_ns", "end": "$end_ns", "query": '{env="production"}'},
            f"values of {name} with query")
        if name == "service_name":
            services = loki_get(loki, f"/loki/api/v1/label/{name}/values", ns).get("data") or []
    for match in ('{namespace="prod"}', '{service_name="api-gateway"}', '{env="production", level="error"}'):
        add("series", {"match[]": match, "start": "$start_ns", "end": "$end_ns"}, "series")
    add("detected_labels", {"query": '{env="production"}', "start": "$start_ns", "end": "$end_ns"}, "detected_labels")
    add("index_stats", {"query": '{env="production"}', "start": "$start_ns", "end": "$end_ns"}, "index/stats")
    for target in ("service_name", "level", "namespace", "detected_level", "pod", "cluster"):
        add("volume", {"query": '{env="production"}', "start": "$start_ns", "end": "$end_ns", "limit": "100",
                       "targetLabels": target, "aggregateBy": "series"}, f"volume by {target}")
        add("volume_range", {"query": '{env="production"}', "start": "$start_ns", "end": "$end_ns", "step": "$step",
                             "limit": "100", "targetLabels": target, "aggregateBy": "series"}, f"volume_range by {target}")
    for query in ('{env="production"} |= "error"', '{namespace="prod"} | json | status>=500'):
        add("format_query", {"query": query}, "format_query")
    for service in services:
        selector = "{" + f"service_name={quote(service)}" + "}"
        add("index_stats", {"query": selector, "start": "$start_ns", "end": "$end_ns"}, f"index/stats {service}")
        add("volume", {"query": selector, "start": "$start_ns", "end": "$end_ns", "limit": "100",
                       "targetLabels": "detected_level", "aggregateBy": "series"}, f"volume detected_level {service}")
        add("detected_labels", {"query": selector, "start": "$start_ns", "end": "$end_ns"}, f"detected_labels {service}")
        add("patterns", {"query": selector, "start": "$start_ns", "end": "$end_ns", "step": "$step"}, f"patterns {service}")
        add("tail", {"query": selector, "start": "$start_ns", "limit": "100"}, f"tail history {service}")
        for level in ("info", "warn", "error", "debug"):
            out.extend(query_entries("seeded", f"{selector} | detected_level={quote(level)}", f"detected_level {service}"))
        out.extend(query_entries("seeded", f"sum by (detected_level) (count_over_time({selector}[5m]))",
                                 f"level volume {service}"))
        out.extend(query_entries("seeded", f"{selector} | json | __error__!=\"\"", f"json errors {service}"))
        out.extend(query_entries("seeded", f"{selector} | logfmt | __error__!=\"\"", f"logfmt errors {service}"))
        out.extend(query_entries("seeded", f"{selector} | json | keep service_name, level", f"keep {service}"))
        out.extend(query_entries("seeded", f"{selector} | json msg=\"msg\"", f"json msg {service}"))
        add("detected_fields", {"query": selector, "start": "$start_ns", "end": "$end_ns"}, f"detected_fields {service}")
        try:
            fields = loki_get(loki, "/loki/api/v1/detected_fields", dict(ns, query=selector)).get("fields") or []
        except (OSError, ValueError) as error:
            print(f"[corpus] detected_fields {service}: {error}", file=sys.stderr)
            continue
        for field in fields[:max_fields]:
            name = field.get("label")
            add("detected_field_values", {"name": name, "query": selector, "start": "$start_ns", "end": "$end_ns"},
                f"field values {service}.{name}")
            numeric = field.get("type") in ("int", "float", "duration", "bytes")
            for query in field_queries(selector, name, field.get("parsers") or [], numeric):
                out.extend(query_entries("seeded", query, f"field {service}.{name}"))
    return out


# ---------------------------------------------------------------- grafana captures

def grafana_requests(capture_dirs, loki_uid="vp-loki"):
    """Every request the Loki datasource answered in bench/visual captures, as Grafana API calls."""
    out, seen = [], set()
    for directory in capture_dirs:
        for path in sorted(glob.glob(os.path.join(directory, "**", "*.json"), recursive=True)):
            try:
                doc = json.load(open(path))
            except (OSError, ValueError):
                continue
            if not isinstance(doc, dict) or "records" not in doc:
                continue
            page = os.path.relpath(os.path.dirname(path), directory).replace(os.sep, " ")
            for record in doc["records"]:
                url = record.get("url", "")
                if "/api/ds/query" in url:
                    request = record.get("request") or {}
                    queries = [q for q in request.get("queries") or []
                               if (q.get("datasource") or {}).get("uid") == loki_uid]
                    if not queries:
                        continue
                    body = {"queries": queries, "from": request.get("from"), "to": request.get("to")}
                    key = json.dumps(body, sort_keys=True)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(entry("grafana", "ds_query", {}, page, transport="grafana", body=body))
                elif f"/api/datasources/uid/{loki_uid}/resources/" in url:
                    resource = url.split("/resources/", 1)[1].split("&_=")[0]
                    if resource in seen:
                        continue
                    seen.add(resource)
                    out.append(entry("grafana", "resource", {}, page, transport="grafana", resource=resource))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--loki-src", default="", help="checkout of grafana/loki at the reference version")
    ap.add_argument("--captures", action="append", default=[], help="bench/visual capture output directory")
    ap.add_argument("--seeded", default="", help="Loki URL of the seeded stack, to generate the seeded corpus")
    ap.add_argument("--end", type=int, default=0, help="end of the seeded window (unix seconds)")
    ap.add_argument("--no-repo", action="store_true")
    a = ap.parse_args()
    entries = []
    if a.loki_src:
        entries += loki_tests(a.loki_src)
    if not a.no_repo:
        entries += repo_queries()
    if a.seeded:
        if not a.end:
            ap.error("--seeded needs --end")
        entries += seeded(a.seeded, a.end)
    if a.captures:
        entries += grafana_requests(a.captures)
    unique = {}
    for item in entries:
        unique.setdefault(item["id"], item)
    with open(a.out, "w") as handle:
        json.dump({"entries": list(unique.values())}, handle, indent=1, sort_keys=True)
    by_source = {}
    for item in unique.values():
        by_source[item["source"]] = by_source.get(item["source"], 0) + 1
    print(f"corpus: {len(unique)} requests " + json.dumps(by_source, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
