#!/usr/bin/env python3
"""Record the Loki HTTP surface for a release into the conformance registry, from Loki's public sources only.

Two sources, and no Loki source code:
  - the published HTTP API reference (grafana.com/docs/loki/<docs version>/reference/loki-http-api/): every
    `METHOD /path` it lists;
  - Loki's recorded HTTP answers (internal/lokierr/testdata/loki_error_answers.jsonl and
    internal/proxy/testdata/loki_request_param_answers.jsonl, recorded over HTTP from a live Loki of the pinned
    release): a method Loki answered with anything but 404 or 405 is a method the route serves, and a route the
    reference does not list but Loki answered is a route Loki serves (Logs Drilldown's endpoints).

  - a live probe of the pinned release (live-routes.json beside the output, written by --record-live URL): every
    route and method, and what Loki answered.

A route or method the previous file holds and none of these sources confirms is kept, marked unconfirmed and
listed under "unconfirmed", until a live probe confirms or drops it.

Writes conformance/registry/generated/loki/<version>/endpoints.json: every client route with its methods and the
sources of each method. --check-live URL asks a live Loki every listed route and method instead and fails when
Loki's answer differs from the file (the weekly parity refresh runs it on its stack). Never edit the generated tree
by hand; owner state lives in conformance/registry/state/.

Usage: sync_loki.py --version v3.7.7 [--docs-version v3.7.x] [--docs-file FILE] [--out conformance/registry/generated]
       sync_loki.py --version v3.7.7 --record-live http://127.0.0.1:3100 [--org 0]
       sync_loki.py --version v3.7.7 --check-live http://127.0.0.1:3100 [--org 0]
"""
import argparse, html, json, os, re, sys, urllib.error, urllib.parse, urllib.request

DOCS = "https://grafana.com/docs/loki/{version}/reference/loki-http-api/"
RECORDINGS = ["internal/lokierr/testdata/loki_error_answers.jsonl",
              "internal/proxy/testdata/loki_request_param_answers.jsonl"]
CLIENT_PREFIXES = ("/loki/api/", "/prometheus/api/", "/api/prom")
METHODS = ("GET", "POST", "PUT", "DELETE")
ROUTE_LINE = re.compile(r"\b(GET|POST|PUT|DELETE|PATCH)\s+(/[A-Za-z0-9_./{}<>-]+)")


def fetch(url):
    request = urllib.request.Request(url, headers={"User-Agent": "loki-vl-proxy-conformance"})
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise SystemExit(f"{url}: HTTP {response.status}")
        return response.read().decode()


def endpoint_id(path):
    slug = path.strip("/").replace("loki/api/v1/", "").replace("/", "_")
    slug = re.sub(r"[{}]", "", slug)
    return re.sub(r"[^a-z0-9_]+", "_", slug.lower()) or "root"


def normalise(path):
    """A documented path with its parameters as {name}, without a trailing slash."""
    path = re.sub(r"<([A-Za-z_]+)>", r"{\1}", path).rstrip(".")
    return path.rstrip("/") or "/"


def documented_routes(text):
    """route -> methods the HTTP API reference lists."""
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    routes = {}
    for method, path in ROUTE_LINE.findall(text):
        path = normalise(path)
        if path.startswith(CLIENT_PREFIXES):
            routes.setdefault(path, set()).add(method)
    return routes


def recorded_routes(root):
    """route -> {method: [row ids]} that a live Loki served (not 404 or 405) in the recorded answers."""
    routes = {}
    for rel in RECORDINGS:
        path = os.path.join(root, rel)
        if not os.path.exists(path):
            continue
        with open(path) as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                target, method, loki = row.get("target"), row.get("method"), row.get("loki")
                if not target or not method or not isinstance(loki, dict):
                    continue
                route = urllib.parse.urlsplit(target).path
                if not route.startswith(CLIENT_PREFIXES) or loki.get("status") in (404, 405, 301, None):
                    continue
                if row.get("route") == "label_values" or "/label/" in route:
                    route = re.sub(r"/label/[^/]+/values$", "/label/{name}/values", route)
                if "/detected_field/" in route:
                    route = re.sub(r"/detected_field/[^/]+/values$", "/detected_field/{name}/values", route)
                if route.startswith(("/loki/api/v1/rules/", "/api/prom/rules/")):
                    parts = route.split("/")
                    base = "/".join(parts[:5]) if route.startswith("/loki") else "/".join(parts[:4])
                    depth = len(parts) - len(base.split("/"))
                    route = base + ("/{namespace}" if depth >= 1 else "") + ("/{groupName}" if depth >= 2 else "")
                routes.setdefault(route, {}).setdefault(method, []).append(row.get("id", ""))
    return routes


def build(version, docs_version, docs_file, root, previous, live):
    text = open(docs_file).read() if docs_file else fetch(DOCS.format(version=docs_version))
    docs = documented_routes(text)
    if not docs:
        raise SystemExit("the HTTP API reference lists no route: the page layout changed")
    recorded = recorded_routes(root)
    endpoints = {}
    for path in sorted(set(docs) | set(recorded) | set(live)):
        methods = {}
        for method in sorted(docs.get(path, ())):
            methods[method] = ["docs"]
        for method, ids in sorted(recorded.get(path, {}).items()):
            methods.setdefault(method, []).append("recorded:" + ",".join(sorted(ids)[:3]))
        for method, status in sorted(live.get(path, {}).items()):
            if status not in (404, 405):
                methods.setdefault(method, []).append(f"live:{status}")
        if methods:
            endpoints[path] = {"id": endpoint_id(path), "path": path, "methods": sorted(methods),
                               "documented": path in docs, "sources": methods}
    unconfirmed = []
    for e in previous:
        path = e["path"]
        probed = live.get(path, {})
        for method in e.get("methods", []):
            if method in probed:
                continue  # the live probe decided it
            entry = endpoints.setdefault(path, {"id": endpoint_id(path), "path": path, "methods": [],
                                                "documented": path in docs, "sources": {}})
            if method not in entry["sources"]:
                entry["sources"][method] = ["unconfirmed"]
                entry["methods"] = sorted(entry["sources"])
                unconfirmed.append(f"{method} {path}")
    return {"version": version, "docs_version": docs_version,
            "source": DOCS.format(version=docs_version) + ", Loki's recorded answers and a live probe",
            "endpoints": [endpoints[p] for p in sorted(endpoints)], "unconfirmed": sorted(unconfirmed)}


def probe(base, path, method, org):
    """Loki's status for one route and method (a transport failure raises)."""
    path = path.replace("{name}", "app").replace("{namespace}", "ns").replace("{groupName}", "grp")
    request = urllib.request.Request(base.rstrip("/") + path, method=method, headers={"X-Scope-OrgID": org})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as err:
        return err.code


def record_live(target_dir, base, org, paths):
    """Ask a live Loki every route and method; write live-routes.json (route -> method -> status)."""
    out = {}
    for path in sorted(paths):
        if path.endswith("/tail"):
            continue  # a websocket route
        out[path] = {method: probe(base, path, method, org) for method in METHODS}
    with open(os.path.join(target_dir, "live-routes.json"), "w") as handle:
        json.dump(out, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return out


def check_live(target, base, org):
    """Ask a live Loki every route and method of the file: a listed method must not answer 404 or 405, and a
    method of the route the file does not list must (Loki's mux answers it 405). Returns the differences."""
    payload = json.load(open(target))
    problems = []
    for e in payload["endpoints"]:
        if e["path"].endswith("/tail"):
            continue  # a websocket route
        for method in METHODS:
            try:
                status = probe(base, e["path"], method, org)
            except Exception as err:  # noqa: BLE001 - a transport failure is a difference to report
                problems.append(f"{method} {e['path']}: {err}")
                continue
            served = status not in (404, 405)
            if served != (method in e["methods"]):
                problems.append(f"{method} {e['path']}: Loki answered {status}, the file lists {e['methods']}")
    return problems


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--docs-version", default="", help="documentation version, such as v3.7.x (default: the minor of --version)")
    parser.add_argument("--docs-file", default="", help="read the HTTP API reference page from this file instead of fetching it")
    parser.add_argument("--out", default="conformance/registry/generated")
    parser.add_argument("--check-live", default="", help="a live Loki's base URL: compare its answers with the file")
    parser.add_argument("--record-live", default="", help="a live Loki's base URL: record its answers to live-routes.json, then regenerate")
    parser.add_argument("--org", default="0")
    args = parser.parse_args()
    target = os.path.join(args.out, "loki", args.version, "endpoints.json")
    if args.check_live:
        problems = check_live(target, args.check_live, args.org)
        for p in problems:
            print(p)
        print(f"{target}: {len(problems)} difference(s) from the live Loki at {args.check_live}")
        return 1 if problems else 0
    docs_version = args.docs_version or re.sub(r"^(v\d+\.\d+)\..*$", r"\1.x", args.version)
    directory = os.path.dirname(target)
    os.makedirs(directory, exist_ok=True)
    previous = json.load(open(target))["endpoints"] if os.path.exists(target) else []
    live_path = os.path.join(directory, "live-routes.json")
    if args.record_live:
        paths = {e["path"] for e in previous}
        live = record_live(directory, args.record_live, args.org, paths)
    else:
        live = json.load(open(live_path)) if os.path.exists(live_path) else {}
    payload = build(args.version, docs_version, args.docs_file, os.getcwd(), previous, live)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    documented = sum(1 for e in payload["endpoints"] if e["documented"])
    print(f"{target}: {len(payload['endpoints'])} client endpoints ({documented} in the HTTP API reference), "
          f"{len(payload['unconfirmed'])} method(s) no public source confirms yet")
    for item in payload["unconfirmed"]:
        print(f"  unconfirmed: {item}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
