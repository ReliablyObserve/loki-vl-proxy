#!/usr/bin/env python3
"""Differential runner: send every corpus request to Loki and to the proxy, diff the answers.

  run.py --state STATE.json --corpus corpus.json --out RUN_DIR [--workers 4] [--window 3600]
  run.py --loki URL --proxy URL --vl URL --end UNIX --corpus corpus.json --out RUN_DIR

STATE.json is the state file of `bench/visual/stack.py up` (an isolated stack:
Loki, VictoriaLogs, the proxy builds, Grafana with the vp-pr / vp-loki
datasources). The proxy compared is the `pr` build (`--side main` for the other).

Before any comparison the run proves both sides healthy and the data equal,
and stops otherwise (health.json):
  * Loki /ready, VictoriaLogs /health, proxy /ready answer 200;
  * Loki and VictoriaLogs count the same lines in every 10-minute slice of the
    window (bench/ab/stack.py counts), and the total is not zero;
  * Loki's range-metric path is out of its fresh-stack blank window: a grouped
    count over the window returns one series per service Loki lists;
  * the VictoriaLogs and Loki containers' restart counts are recorded before and
    after the run, must be known (--vl-container, --loki-container) and must not
    change (a Loki that restarted answers range metrics empty for minutes);
  * the proxy log (--proxy-log) must not show VictoriaLogs failing during the
    run: 5xx answers, or a fallback or partial answer after one (a 4xx from
    VictoriaLogs is a query the proxy translated wrongly, so it is counted and
    reported but is the difference being measured, not a broken comparison).
A request whose Loki answer carries warnings or whose proxy answer is marked
partial is `blocked`, never a difference, and up to three requests of every
difference signature are asked for a second time: a signature none of whose
samples reproduces is `blocked` (not reproducible). Query requests run twice: as
Grafana sends them (`X-Loki-Response-Encoding-Flags: categorize-labels`) and
as a plain API client does.

Outputs RUN_DIR/results.jsonl (one row per request and encoding), health.json,
and, through cluster.py, clusters.json and report.md.
"""
import argparse
import base64
import concurrent.futures
import gzip
import importlib.util
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cluster  # noqa: E402
import differ  # noqa: E402

PATHS = {
    "query_range": "/loki/api/v1/query_range", "query": "/loki/api/v1/query", "labels": "/loki/api/v1/labels",
    "label_values": "/loki/api/v1/label/{name}/values", "series": "/loki/api/v1/series",
    "index_stats": "/loki/api/v1/index/stats", "volume": "/loki/api/v1/index/volume",
    "volume_range": "/loki/api/v1/index/volume_range", "detected_fields": "/loki/api/v1/detected_fields",
    "detected_field_values": "/loki/api/v1/detected_field/{name}/values",
    "detected_labels": "/loki/api/v1/detected_labels", "patterns": "/loki/api/v1/patterns",
    "format_query": "/loki/api/v1/format_query", "tail": "/loki/api/v1/tail",
}
CATEGORIZE = {"X-Loki-Response-Encoding-Flags": "categorize-labels"}
TENANT = {"X-Scope-OrgID": "0"}


def load_ab_stack():
    spec = importlib.util.spec_from_file_location("ab_stack", os.path.join(HERE, "..", "ab", "stack.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fill(params, times):
    out = {}
    for key, value in params.items():
        if isinstance(value, str) and value.startswith("$"):
            value = str(times[value[1:]])
        out[key] = value
    return out


def http(method, url, headers=None, body=None, timeout=150):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=dict(headers or {}, **({"Content-Type": "application/json"} if data else {})))
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw, status, hdrs = response.read(), response.status, dict(response.headers)
    except urllib.error.HTTPError as error:
        raw, status, hdrs = error.read(), error.code, dict(error.headers or {})
    except (urllib.error.URLError, OSError) as error:
        return 0, {"_text": f"transport error: {error}"}, {}, time.time() - t0
    try:
        payload = json.loads(raw) if raw else {}
    except ValueError:
        payload = {"_text": raw.decode(errors="replace")[:2000]}
    return status, payload, hdrs, time.time() - t0


# ---------------------------------------------------------------- tail (minimal websocket client)

def ws_tail(base, params, headers, seconds=4.0):
    """Frames a /tail websocket sends in `seconds`, merged into one streams answer."""
    parsed = urllib.parse.urlparse(base)
    path = PATHS["tail"] + "?" + urllib.parse.urlencode(params)
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [f"GET {path} HTTP/1.1", f"Host: {parsed.hostname}:{parsed.port}", "Upgrade: websocket",
             "Connection: Upgrade", f"Sec-WebSocket-Key: {key}", "Sec-WebSocket-Version: 13"]
    lines += [f"{k}: {v}" for k, v in headers.items()]
    try:
        sock = socket.create_connection((parsed.hostname, parsed.port), timeout=seconds)
    except OSError as error:
        return 0, {"_text": f"transport error: {error}"}
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
    buf = b""
    deadline = time.time() + seconds
    try:
        while b"\r\n\r\n" not in buf and time.time() < deadline:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
        head, _, buf = buf.partition(b"\r\n\r\n")
        status = int(head.split(b" ")[1]) if head else 0
        if status != 101:
            return status, {"_text": (head + buf).decode(errors="replace")[:500]}
        streams, dropped = [], 0
        sock.settimeout(0.5)
        while time.time() < deadline:
            frame, buf = read_frame(sock, buf, deadline)
            if frame is None:
                continue
            if frame == b"":
                break
            message = json.loads(frame)
            streams += message.get("streams") or []
            dropped += len(message.get("dropped_entries") or [])
        return 200, {"data": {"resultType": "streams", "result": streams}, "dropped": dropped}
    except (OSError, ValueError) as error:
        return 0, {"_text": f"tail error: {error}"}
    finally:
        sock.close()


def read_frame(sock, buf, deadline):
    """(payload, rest): one text frame; None when nothing arrived yet (or the deadline passed mid-frame), b'' on close."""
    def need(n):
        nonlocal buf
        while len(buf) < n:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                return False
            if not chunk:
                raise OSError("closed")
            buf += chunk
        return True
    if not need(2):
        return None, buf
    opcode, length = buf[0] & 0x0F, buf[1] & 0x7F
    offset = 2
    if length == 126:
        if not need(4):
            return None, buf
        length, offset = int.from_bytes(buf[2:4], "big"), 4
    elif length == 127:
        if not need(10):
            return None, buf
        length, offset = int.from_bytes(buf[2:10], "big"), 10
    while not need(offset + length):
        if time.time() >= deadline:
            return None, buf
        time.sleep(0.05)
    payload, rest = buf[offset:offset + length], buf[offset + length:]
    if opcode == 8:
        return b"", rest
    if opcode != 1:
        return None, rest
    return payload, rest


# ---------------------------------------------------------------- Grafana frames

def frames_body(result):
    """A Grafana /api/ds/query result (one refId) as a Loki-shaped answer, so differ.py compares it."""
    if not result:
        return {}
    if result.get("error"):
        return {"error": result["error"]}
    streams, series = [], []
    for frame in result.get("frames") or []:
        fields = frame["schema"]["fields"]
        values = frame["data"]["values"]
        names = [f.get("name") for f in fields]
        if "Line" in names and "labels" in names:
            col = dict(zip(names, values))
            ts = col.get("tsNs") or [str(int(t) * 10 ** 6) for t in col.get("Time", [])]
            types = col.get("labelTypes") or [None] * len(ts)
            for t, line, labels, kinds in zip(ts, col["Line"], col["labels"], types):
                labels = labels or {}
                stream = {k: v for k, v in labels.items() if not kinds or kinds.get(k) == "I"}
                meta = {}
                if kinds:
                    meta = {"structuredMetadata": {k: v for k, v in labels.items() if kinds.get(k) == "S"},
                            "parsed": {k: v for k, v in labels.items() if kinds.get(k) == "P"}}
                streams.append({"stream": stream, "values": [[t, line, meta]]})
        elif len(fields) >= 2 and fields[0].get("type") == "time":
            for index in range(1, len(fields)):
                labels = fields[index].get("labels") or {}
                points = [[t / 1000, str(v)] for t, v in zip(values[0], values[index]) if v is not None]
                series.append({"metric": labels, "values": points})
    if streams:
        return {"data": {"resultType": "streams", "result": streams}}
    return {"data": {"resultType": "matrix", "result": series}}


RESOURCE_ENDPOINTS = (("detected_field/", "detected_field_values"), ("detected_fields", "detected_fields"),
                      ("detected_labels", "detected_labels"), ("index/stats", "index_stats"),
                      ("index/volume_range", "volume_range"), ("index/volume", "volume"), ("patterns", "patterns"),
                      ("label/", "label_values"), ("labels", "labels"), ("series", "series"))


def resource_endpoint(resource):
    for prefix, name in RESOURCE_ENDPOINTS:
        if resource.startswith(prefix):
            return name
    return "resource"


# ---------------------------------------------------------------- one request

def run_one(item, cfg):
    times = cfg["times"]
    rows = []
    if item["transport"] == "grafana":
        rows.append(run_grafana(item, cfg))
        return rows
    endpoint = item["endpoint"]
    params = fill(item["params"], times)
    path = PATHS[endpoint]
    if "{name}" in path:
        path = path.replace("{name}", urllib.parse.quote(params.pop("name"), safe=""))
    encodings = ("categorize", "plain") if endpoint in ("query_range", "query", "tail") else ("plain",)
    for encoding in encodings:
        headers = dict(TENANT, **(CATEGORIZE if encoding == "categorize" else {}), **(item.get("headers") or {}))
        if endpoint == "tail":
            ls, lb = ws_tail(cfg["loki"], params, headers)
            ps, pb = ws_tail(cfg["proxy"], params, headers)
            lh = ph = {}
            lt = pt = 0
        else:
            url = "?" + urllib.parse.urlencode(params)
            ls, lb, lh, lt = http("GET", cfg["loki"] + path + url, headers)
            ps, pb, ph, pt = http("GET", cfg["proxy"] + path + url, headers)
        row = verdict(item, endpoint, encoding, params, ls, lb, lh, ps, pb, ph, cfg, lt, pt)
        loki_own = None
        if endpoint == "index_stats" and row["verdict"] == "diff":
            loki_own = loki_own_counts(params, cfg)
            apply_loki_own_counts(row, pb, loki_own)
        keep_body(cfg, row, {"endpoint": endpoint, "params": params, "loki": [ls, lb, lh], "proxy": [ps, pb, ph],
                             "loki_own": loki_own})
        rows.append(row)
    return rows


def keep_body(cfg, row, record):
    """With --keep-bodies, store both answers so the diff can be recomputed offline (--rediff)."""
    sink = cfg.get("bodies")
    if sink is not None:
        sink.append(dict(record, id=row["id"], encoding=row.get("encoding")))


def rediff(item, record, cfg):
    """The row a stored pair of answers gives under the current diff rules (no request is sent)."""
    if item["transport"] == "grafana" and item["endpoint"] == "ds_query":
        (ls, lb, _), (ps, pb, _) = record["loki"], record["proxy"]
        return grafana_query_verdict(item, ls, lb, ps, pb, cfg, 0, 0)
    (ls, lb, lh), (ps, pb, ph) = record["loki"], record["proxy"]
    row = verdict(item, record["endpoint"], record.get("encoding") or "plain", record["params"], ls, lb, lh, ps, pb,
                  ph, cfg)
    if item["transport"] == "grafana":
        row["resource"], row["resource_endpoint"] = item["resource"], record["endpoint"]
    if record["endpoint"] == "index_stats" and row["verdict"] == "diff" and record.get("loki_own"):
        apply_loki_own_counts(row, pb, record["loki_own"])
    return row


def loki_own_counts(params, cfg):
    """Loki's own answers for an index/stats window: count_over_time (entries) and /series (streams)."""
    start, end = int(params["start"]) // 10 ** 9, int(params["end"]) // 10 ** 9
    query = f"sum(count_over_time({params['query']}[{end - start}s]))"
    status, body, _, _ = http("GET", cfg["loki"] + PATHS["query"] + "?" + urllib.parse.urlencode(
        {"query": query, "time": end}), TENANT)
    result = ((body.get("data") or {}).get("result") or []) if status == 200 else []
    entries = int(float(result[0]["value"][1])) if result else None
    status, body, _, _ = http("GET", cfg["loki"] + PATHS["series"] + "?" + urllib.parse.urlencode(
        {"match[]": params["query"], "start": params["start"], "end": params["end"]}), TENANT)
    streams = len(body.get("data") or []) if status == 200 else None
    return {"entries": entries, "streams": streams}


def apply_loki_own_counts(row, proxy, own):
    """Loki's index/stats reads chunk metadata: a chunk it holds twice (flushed and retained in an ingester) counts
    twice. A streams or entries difference is Loki's chunk accounting only when Loki's own /series or count_over_time
    over the window equals the proxy's number; otherwise it stays a gap."""
    row["loki_own_counts"] = own
    for f in row["facets"]:
        key = f["detail"].split(" ")[0]
        if f["kind"] == "stats" and not f.get("documented") and own.get(key) is not None \
                and own.get(key) == proxy.get(key):
            f["documented"] = differ.DOC_INDEX_STATS_LOKI_CHUNKS
            f["example"] += f"; Loki's own {'count_over_time' if key == 'entries' else '/series'}: {own[key]}"


def run_grafana(item, cfg):
    g, uids = cfg["grafana"], cfg["uids"]
    if item["endpoint"] == "ds_query":
        def body(uid):
            out = json.loads(json.dumps(item["body"]))
            for q in out["queries"]:
                q["datasource"]["uid"] = uid
            return out
        ls, lb, _, lt = http("POST", f"{g}/api/ds/query", {}, body(uids["loki"]))
        ps, pb, _, pt = http("POST", f"{g}/api/ds/query", {}, body(uids["proxy"]))
        row = grafana_query_verdict(item, ls, lb, ps, pb, cfg, lt, pt)
        keep_body(cfg, row, {"endpoint": "ds_query", "params": {}, "loki": [ls, lb, {}], "proxy": [ps, pb, {}]})
        return row
    resource = item["resource"]
    endpoint = resource_endpoint(resource)
    ls, lb, _, lt = http("GET", f"{g}/api/datasources/uid/{uids['loki']}/resources/{resource}")
    ps, pb, _, pt = http("GET", f"{g}/api/datasources/uid/{uids['proxy']}/resources/{resource}")
    row = verdict(item, endpoint, "plain", {"query": resource}, ls, lb, {}, ps, pb, {}, cfg, lt, pt)
    row["resource"], row["resource_endpoint"] = resource, endpoint
    keep_body(cfg, row, {"endpoint": endpoint, "params": {"query": resource}, "loki": [ls, lb, {}],
                         "proxy": [ps, pb, {}]})
    return row


def grafana_query_verdict(item, ls, lb, ps, pb, cfg, lt, pt):
    """The row of one replayed /api/ds/query: every refId's frames compared as a Loki answer."""
    facets, vac = [], True
    for ref in sorted(set((lb.get("results") or {})) | set((pb.get("results") or {}))):
        la = frames_body((lb.get("results") or {}).get(ref))
        pa = frames_body((pb.get("results") or {}).get(ref))
        facets += differ.diff("query_range", 200 if "error" not in la else 400, la,
                              200 if "error" not in pa else 400, pa, cfg["stream_labels"])
        vac = vac and differ.vacuous("query_range", 200, la, 200, pa)
    query = " ; ".join(q.get("expr", "") for q in item["body"]["queries"])
    row = base_row(item, "categorize", {"query": query}, ls, ps, lt, pt)
    if ls != ps or ls != 200:
        facets = [differ.facet("status", f"loki {ls} proxy {ps}", differ.short(lb, 160))] if ls != ps else facets
    row.update(verdict="diff" if facets else ("vacuous" if vac else "same"), facets=differ.dedupe(facets))
    return row


def base_row(item, encoding, params, ls, ps, lt, pt):
    return {"id": item["id"], "source": item["source"], "endpoint": item["endpoint"], "origin": item.get("origin", ""),
            "encoding": encoding, "params": params, "loki_status": ls, "proxy_status": ps,
            "loki_s": round(lt, 3), "proxy_s": round(pt, 3)}


def verdict(item, endpoint, encoding, params, ls, lb, lh, ps, pb, ph, cfg, lt=0, pt=0):
    row = base_row(dict(item, endpoint=endpoint), encoding, params, ls, ps, lt, pt)
    warnings = (lb.get("warnings") if isinstance(lb, dict) else None)
    partial = {k.lower(): v for k, v in (ph or {}).items()}.get("x-loki-vl-partial-response")
    if ls == 0 or ps == 0:
        row.update(verdict="blocked", why="transport error", facets=[])
    elif warnings:
        row.update(verdict="blocked", why=f"Loki warnings: {differ.short(warnings, 160)}", facets=[])
    elif partial:
        row.update(verdict="blocked", why=f"proxy partial response: {partial}", facets=[])
    elif ls >= 500:
        # Loki failing (timeout, limit, internal) is no reference answer.
        row.update(verdict="blocked", why=f"Loki failed ({ls}): {differ.short(differ.error_text(lb), 160)}", facets=[])
    else:
        facets = differ.diff(endpoint, ls, lb, ps, pb, cfg["stream_labels"],
                             limit=int(params.get("limit") or 0), direction=params.get("direction", "backward"))
        if facets:
            row.update(verdict="diff", facets=facets)
        else:
            row.update(verdict="vacuous" if differ.vacuous(endpoint, ls, lb, ps, pb) else "same", facets=[])
    return row


# ---------------------------------------------------------------- confirmation

def confirm(rows, corpus, cfg, workers, per_cluster=3):
    """Ask both sides again for up to `per_cluster` requests of every difference signature. A signature none of whose
    samples reproduces (Loki's answer moved between two identical requests: sharding, splitting, results cache) is
    `blocked` (not reproducible), never a gap. Returns the number of rows blocked."""
    by_id = {e["id"]: e for e in corpus}
    members = {}
    for row in rows:
        if row.get("verdict") == "diff" and row["id"] in by_id:
            members.setdefault(cluster_key(row), []).append(row)
    sample = {}
    for key, group in members.items():
        for row in group[:per_cluster]:
            sample.setdefault(row["id"], set()).add(key)
    ids = sorted(sample)
    reproduced = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for item_id, redone in zip(ids, pool.map(lambda i: run_one(by_id[i], cfg), ids)):
            for row in redone:
                if row.get("verdict") == "diff":
                    reproduced.add(cluster_key(row))
    blocked = 0
    for key, group in members.items():
        for row in group:
            if key in reproduced:
                row["confirmed"] = True
            else:
                row["verdict"] = "blocked"
                row["why"] = f"not reproducible: no sampled request of {key} differed again"
                blocked += 1
    return blocked


def cluster_key(row):
    return " / ".join(cluster.signature_of(row))


# ---------------------------------------------------------------- health

def restart_count(container):
    if not container:
        return None
    out = subprocess.run(["docker", "inspect", container, "--format", "{{.RestartCount}}"], capture_output=True, text=True)
    return int(out.stdout.strip()) if out.returncode == 0 and out.stdout.strip().isdigit() else None


# Proxy log counters that make a comparison invalid when they rise during a run: VictoriaLogs failing (5xx; 499 is
# the proxy cancelling a call its client gave up on) and a proxy answer produced by a fallback or a partial
# response after such a failure. A 4xx from VictoriaLogs is a query the proxy translated wrongly: it is the
# difference being measured, so it is counted but does not invalidate the run.
GATING = ("upstream_5xx", "fallback_after_5xx", "partial_after_5xx")


def scan_log(path):
    """Counters from the proxy's JSON log (one object per line); text that is not JSON is skipped."""
    out = {"upstream_5xx": 0, "upstream_4xx": 0, "upstream_499": 0, "fallback_after_5xx": 0,
           "fallback_after_4xx": 0, "partial_after_5xx": 0, "lines": 0}
    if not path or not os.path.exists(path):
        return {}
    for line in open(path, errors="ignore"):
        out["lines"] += 1
        try:
            record = json.loads(line)
        except ValueError:
            continue
        body = str(record.get("body") or "")
        status = record.get("http.response.status_code") or record.get("status")
        error = str(record.get("err") or record.get("error") or "")
        upstream_failed = bool(re.search(r"\b5\d\d\b|timeout|deadline", error))
        if body == "upstream_request" and isinstance(status, int):
            if status == 499:
                out["upstream_499"] += 1
            elif status >= 500:
                out["upstream_5xx"] += 1
            elif status >= 400:
                out["upstream_4xx"] += 1
        elif "falling back" in body or "fallback" in body:
            out["fallback_after_5xx" if upstream_failed else "fallback_after_4xx"] += 1
        elif "partial" in body.lower() and upstream_failed:
            out["partial_after_5xx"] += 1
    return out


def log_rose(before, after):
    """The gating counters that rose between two scans of the same log (a shorter log was replaced: all count)."""
    if not after:
        return {}
    restarted = after.get("lines", 0) < before.get("lines", 0)
    return {k: after.get(k, 0) - (0 if restarted else before.get(k, 0)) for k in GATING
            if after.get(k, 0) - (0 if restarted else before.get(k, 0)) > 0}


def health(cfg):
    out = {"checked": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    for name, url in (("loki", cfg["loki"] + "/ready"), ("proxy", cfg["proxy"] + "/ready"), ("vl", cfg["vl"] + "/health")):
        status, _, _, _ = http("GET", url, TENANT, timeout=10)
        out[f"{name}_ready"] = status
    ab = load_ab_stack()
    loki_port = int(urllib.parse.urlparse(cfg["loki"]).port)
    vl_port = int(urllib.parse.urlparse(cfg["vl"]).port)
    st = ab.Stack("parity", loki_port, vl_port)
    start, end = cfg["times"]["start"], cfg["times"]["end"]
    loki_counts, vl_counts = st.counts(start, end, 600)
    out["slices"] = len(vl_counts)
    out["lines_loki"], out["lines_vl"] = sum(loki_counts.values()), sum(vl_counts.values())
    out["slices_differing"] = sorted(t for t in vl_counts if loki_counts.get(t, 0) != vl_counts[t])
    services_status, services, _, _ = http("GET", cfg["loki"] + "/loki/api/v1/label/service_name/values?" +
                                           urllib.parse.urlencode({"start": start * 10 ** 9, "end": end * 10 ** 9}), TENANT)
    params = {"query": 'sum by (service_name) (count_over_time({env=~".+"}[5m]))', "start": start, "end": end, "step": 300}
    qs, qb, _, _ = http("GET", cfg["loki"] + "/loki/api/v1/query_range?" + urllib.parse.urlencode(params),
                        dict(TENANT, **{"Cache-Control": "no-cache"}))
    out["services_listed"] = len(services.get("data") or []) if services_status == 200 else None
    out["services_in_loki_metric"] = len(((qb.get("data") or {}).get("result") or [])) if qs == 200 else None
    out["vl_restarts"] = restart_count(cfg.get("vl_container"))
    # A Loki that restarted mid-run answers range metrics empty for minutes (its fresh-stack blank window).
    out["loki_restarts"] = restart_count(cfg.get("loki_container"))
    out["ok"] = healthy(out)
    return out


def healthy(out):
    """Both sides ready, equal non-empty data, Loki out of its blank window, restart counts known (an unknown
    restart count proves nothing: the containers must be named and inspectable)."""
    return bool(out.get("loki_ready") == out.get("proxy_ready") == out.get("vl_ready") == 200
                and (out.get("lines_vl") or 0) > 0 and out.get("lines_loki") == out.get("lines_vl")
                and not out.get("slices_differing") and out.get("services_listed")
                and out.get("services_in_loki_metric") == out.get("services_listed")
                and out.get("vl_restarts") is not None and out.get("loki_restarts") is not None)


def stream_labels(cfg):
    times = cfg["times"]
    status, body, _, _ = http("GET", cfg["loki"] + "/loki/api/v1/labels?" + urllib.parse.urlencode(
        {"start": times["start_ns"], "end": times["end_ns"]}), TENANT)
    return sorted(body.get("data") or []) if status == 200 else []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", default="", help="bench/visual/stack.py state.json")
    ap.add_argument("--side", default="pr", choices=["pr", "main"])
    ap.add_argument("--loki", default="")
    ap.add_argument("--proxy", default="")
    ap.add_argument("--vl", default="")
    ap.add_argument("--grafana", default="")
    ap.add_argument("--end", type=int, default=0)
    ap.add_argument("--window", type=int, default=3600, help="seconds before --end that queries cover")
    ap.add_argument("--step", type=int, default=60)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--source", action="append", default=[], help="only these corpus sources")
    ap.add_argument("--endpoint", action="append", default=[], help="only these endpoints (query_range, ds_query, ...)")
    ap.add_argument("--limit", type=int, default=0, help="first N corpus entries (smoke run)")
    ap.add_argument("--vl-container", default="")
    ap.add_argument("--loki-container", default="")
    ap.add_argument("--proxy-log", default="")
    ap.add_argument("--skip-health", action="store_true", help="debugging only: the report says so")
    ap.add_argument("--no-confirm", action="store_true", help="do not ask twice for differing requests")
    ap.add_argument("--keep-bodies", action="store_true",
                    help="store both answers of every request in RUN_DIR/bodies.jsonl.gz for --rediff")
    ap.add_argument("--rediff", action="store_true",
                    help="recompute RUN_DIR/results.jsonl from RUN_DIR/bodies.jsonl.gz under the current diff "
                         "rules; sends no request")
    ap.add_argument("--confirm-only", action="store_true",
                    help="confirm the differences of an existing RUN_DIR/results.jsonl instead of running the corpus")
    a = ap.parse_args()
    if a.state:
        state = json.load(open(a.state))
        ports = state["ports"]
        a.loki = a.loki or f"http://127.0.0.1:{ports['loki']}"
        a.vl = a.vl or f"http://127.0.0.1:{ports['vl']}"
        a.proxy = a.proxy or f"http://127.0.0.1:{ports[a.side]}"
        a.grafana = a.grafana or state.get("grafana", "")
        a.end = a.end or state["end"]
        a.vl_container = a.vl_container or f"{state['project']}-victorialogs"
        a.loki_container = a.loki_container or f"{state['project']}-loki"
        a.proxy_log = a.proxy_log or os.path.join(os.path.dirname(os.path.abspath(a.state)), f"proxy-{a.side}.log")
    if not (a.loki and a.proxy and a.vl and a.end):
        ap.error("--state, or --loki/--proxy/--vl/--end")
    os.makedirs(a.out, exist_ok=True)
    if a.rediff:
        return rediff_run(a)
    start = a.end - a.window
    cfg = {"loki": a.loki, "proxy": a.proxy, "vl": a.vl, "grafana": a.grafana, "vl_container": a.vl_container,
           "loki_container": a.loki_container,
           "uids": {"loki": "vp-loki", "proxy": "vp-" + a.side},
           "times": {"start": start, "end": a.end, "step": a.step, "start_ns": start * 10 ** 9, "end_ns": a.end * 10 ** 9}}
    before = {} if a.skip_health else health(cfg)
    before["log_before"] = scan_log(a.proxy_log)
    if not a.skip_health and not before["ok"]:
        json.dump({"before": before}, open(os.path.join(a.out, "health.json"), "w"), indent=1)
        print("both sides are not proven healthy and equal; nothing compared: " + json.dumps(before), file=sys.stderr)
        return 2
    cfg["stream_labels"] = stream_labels(cfg)
    corpus = json.load(open(a.corpus))["entries"]
    if a.source:
        corpus = [e for e in corpus if e["source"] in a.source]
    if a.endpoint:
        corpus = [e for e in corpus if e["endpoint"] in a.endpoint]
    if not a.grafana:
        corpus = [e for e in corpus if e["transport"] != "grafana"]
    if a.limit:
        corpus = corpus[: a.limit]
    t0 = time.time()
    results = os.path.join(a.out, "results.jsonl")
    if a.keep_bodies:
        cfg["bodies"] = []
    if not a.confirm_only:
        with open(results, "w") as handle, concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool:
            done = 0
            for rows in pool.map(lambda e: run_one(e, cfg), corpus):
                for row in rows:
                    handle.write(json.dumps(row, sort_keys=True) + "\n")
                handle.flush()
                done += 1
                if done % 200 == 0:
                    print(f"[run] {done}/{len(corpus)} ({time.time() - t0:.0f}s)", file=sys.stderr, flush=True)
    if a.keep_bodies:
        with gzip.open(os.path.join(a.out, "bodies.jsonl.gz"), "wt") as handle:
            for record in cfg.pop("bodies"):
                handle.write(json.dumps(record) + "\n")
    unconfirmed = None
    if not a.no_confirm:
        rows = cluster.load_rows(results)
        unconfirmed = confirm(rows, corpus, cfg, a.workers)
        with open(results, "w") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        print(f"[run] confirmation: {unconfirmed} differences did not reproduce", file=sys.stderr, flush=True)
    after = {} if a.skip_health else health(cfg)
    after["log_after"] = scan_log(a.proxy_log)
    report = {"before": before, "after": after, "skip_health": a.skip_health, "corpus_entries": len(corpus),
              "seconds": round(time.time() - t0), "loki": a.loki, "proxy": a.proxy, "window": [start, a.end],
              "not_reproducible": unconfirmed, "stream_labels": cfg.get("stream_labels") or []}
    report["vl_restarts_unchanged"] = before.get("vl_restarts") is not None and \
        before.get("vl_restarts") == after.get("vl_restarts")
    report["loki_restarts_unchanged"] = before.get("loki_restarts") is not None and \
        before.get("loki_restarts") == after.get("loki_restarts")
    report["proxy_log_rose"] = log_rose(before.get("log_before") or {}, after.get("log_after") or {})
    json.dump(report, open(os.path.join(a.out, "health.json"), "w"), indent=1, sort_keys=True)
    rows = cluster.load_rows(os.path.join(a.out, "results.jsonl"))
    clusters = cluster.cluster(rows)
    stats = cluster.summary(rows)
    json.dump({"summary": stats, "clusters": clusters}, open(os.path.join(a.out, "clusters.json"), "w"), indent=1,
              sort_keys=True)
    with open(os.path.join(a.out, "report.md"), "w") as handle:
        handle.write(cluster.markdown(clusters, stats, report))
    print(f"[run] {len(rows)} requests in {report['seconds']}s; "
          f"{sum(1 for c in clusters if not c['documented'])} gap signatures; {a.out}/report.md")
    if not a.skip_health and not (after.get("ok") and report["vl_restarts_unchanged"]
                                  and report["loki_restarts_unchanged"] and not report["proxy_log_rose"]):
        print("health changed during the run (or the proxy log shows VictoriaLogs failures): results are not "
              "proof; see health.json", file=sys.stderr)
        return 3
    return 0


def rediff_run(a):
    """--rediff: the stored answers under the current rules; the confirmation and health of the original run stand."""
    corpus = {e["id"]: e for e in json.load(open(a.corpus))["entries"]}
    previous = {(r["id"], r.get("encoding")): r for r in cluster.load_rows(os.path.join(a.out, "results.jsonl"))}
    health_path = os.path.join(a.out, "health.json")
    cfg = {"stream_labels": (json.load(open(health_path)).get("stream_labels") or [])
           if os.path.exists(health_path) else []}
    rows = []
    with gzip.open(os.path.join(a.out, "bodies.jsonl.gz"), "rt") as handle:
        for line in handle:
            record = json.loads(line)
            row = rediff(corpus[record["id"]], record, cfg)
            old = previous.get((record["id"], record.get("encoding")), {})
            if old.get("verdict") == "blocked":  # blocked stays blocked (transport, Loki failure, not reproducible)
                row.update(verdict="blocked", why=old.get("why"), facets=[])
            rows.append(row)
    with open(os.path.join(a.out, "results.jsonl"), "w") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    clusters, stats = cluster.cluster(rows), cluster.summary(rows)
    json.dump({"summary": stats, "clusters": clusters}, open(os.path.join(a.out, "clusters.json"), "w"), indent=1,
              sort_keys=True)
    print(f"[rediff] {len(rows)} rows, {sum(1 for c in clusters if not c['documented'])} gap signatures")
    return 0


if __name__ == "__main__":
    sys.exit(main())
