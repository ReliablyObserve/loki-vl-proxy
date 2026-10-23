#!/usr/bin/env python3
"""Build the coverage half of the conformance registry from this repository.

Records, per Loki endpoint:
  * whether the proxy registers a route for it (internal/proxy route table),
  * which tests exercise it, split into unit, e2e (Go) and ui (Playwright),
  * which of those tests compare the proxy against Loki directly (differential),
  * the proxy handler name.

Writes conformance/registry/generated/proxy/coverage.json. Owner state and
waivers live in conformance/registry/state/ and are never written here.

Usage: sync_proxy.py [--endpoints conformance/registry/generated/loki/<ver>/endpoints.json]
"""
import argparse, json, os, re, subprocess, sys
from registry_io import load_json, read_text

# Every wrapper RegisterProxyRoutes registers Loki routes with: the full chain
# (p.routeHandler), the tenant-free chain (rlNoTenant) and the ruler chain
# (alertRead). A bare mux.HandleFunc is a route too.
ROUTE_RE = re.compile(r'mux\.Handle\(\s*"([^"]+)"\s*,\s*(?:p\.routeHandler|rlNoTenant|alertRead)\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*p\.(\w+)')
HANDLEFUNC_RE = re.compile(r'mux\.HandleFunc\(\s*"([^"]+)"\s*,\s*p\.(\w+)\s*\)')
# Handlers that answer a route only to refuse it: the route exists, the endpoint
# is not implemented.
REFUSING_HANDLERS = {"handleWriteBlocked"}
# Path parameters in a Loki route template, e.g. {name} or {namespace}.
PARAM_RE = re.compile(r"\{[A-Za-z]+\}")
# A test that names both the proxy and Loki base URL compares the two.
DIFFERENTIAL_MARKERS = ("lokiURL", "lokiBase", "LOKI_URL", "lokiDatasource", "P158F1C6922A12716")
# Only test files count: a production file that names a route is not a test.
KINDS = (
    ("unit", ["internal"], ("_test.go",)),
    ("e2e", ["test/e2e-compat", "test/e2e-fleet", "test/e2e-pipeline", "test/integration"], ("_test.go",)),
    ("ui", ["test/e2e-ui"], (".spec.ts",)),
)


def proxy_routes(root):
    routes = {}
    source = read_text(os.path.join(root, "internal/proxy/proxy.go"))
    for pattern, metric, template, handler in ROUTE_RE.findall(source):
        routes[template] = {"pattern": pattern, "route": metric, "handler": handler}
    for pattern, handler in HANDLEFUNC_RE.findall(source):
        if handler in REFUSING_HANDLERS:
            continue
        routes.setdefault(pattern, {"pattern": pattern, "route": handler, "handler": handler})
    return routes


def path_regex(path):
    """An extended regexp matching the path in a test, with each {param}
    standing for one path segment. The path must end there: /loki/api/v1/label
    does not match /loki/api/v1/labels or /loki/api/v1/label/app/values."""
    parts = PARAM_RE.split(path)
    body = "[^/\"'?` ]+".join(re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", part) for part in parts)
    return body + "([\"'?` )]|$)"


def grep(root, needle, directories, suffixes):
    hits = []
    for directory in directories:
        base = os.path.join(root, directory)
        if not os.path.isdir(base):
            continue
        try:
            output = subprocess.run(["grep", "-rnE", "--include=*" + suffixes[0], needle, base],
                                    capture_output=True, text=True).stdout
        except OSError:
            continue
        for line in output.splitlines():
            path, _, rest = line.partition(":")
            number, _, text = rest.partition(":")
            hits.append({"file": os.path.relpath(path, root), "line": int(number), "text": text.strip()[:160]})
    return hits


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoints", required=True)
    parser.add_argument("--out", default="conformance/registry/generated/proxy/coverage.json")
    args = parser.parse_args()
    root = os.getcwd()
    endpoints = load_json(args.endpoints)
    routes = proxy_routes(root)
    covered = []
    for endpoint in endpoints["endpoints"]:
        path = endpoint["path"]
        route = routes.get(path)
        needle = path_regex(path)
        entry = {
            "id": endpoint["id"],
            "path": path,
            "methods": endpoint["methods"],
            "proxy_route": route,
            "tests": {},
            "differential_tests": {},
        }
        for kind, directories, suffixes in KINDS:
            hits = grep(root, needle, directories, suffixes)
            files = sorted({hit["file"] for hit in hits})
            entry["tests"][kind] = files
            differential = sorted({
                hit["file"] for hit in hits
                if any(marker in read_text(os.path.join(root, hit["file"])) for marker in DIFFERENTIAL_MARKERS)
            })
            entry["differential_tests"][kind] = differential
        covered.append(entry)
    payload = {"loki_version": endpoints["version"], "endpoints": covered}
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    implemented = sum(1 for e in covered if e["proxy_route"])
    tested = sum(1 for e in covered if any(e["tests"].values()))
    differential = sum(1 for e in covered if any(e["differential_tests"].values()))
    print(f"{args.out}: {len(covered)} endpoints, {implemented} routed by the proxy, "
          f"{tested} referenced by tests, {differential} compared against Loki")
    return 0


if __name__ == "__main__":
    sys.exit(main())
