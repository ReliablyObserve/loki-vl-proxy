#!/usr/bin/env python3
"""Extract the Loki HTTP surface for a release tag into the conformance registry.

Reads the Loki source at the given tag and writes
conformance/registry/generated/loki/<version>/endpoints.json: every registered
route with its methods, the constant or literal it came from, and the source
reference. Never edit the generated tree by hand; owner state lives in
conformance/registry/state/.

Usage: sync_loki.py --version v3.7.7 [--out conformance/registry/generated]
"""
import argparse, json, os, re, sys, urllib.request

RAW = "https://raw.githubusercontent.com/grafana/loki/{version}/{path}"
# Files that register HTTP routes in the single-binary and target modules.
SOURCES = [
    "pkg/loki/modules.go",
    "pkg/loki/loki.go",
    "pkg/util/constants/api_paths.go",
]
ROUTE_RE = re.compile(r'HTTP\.Path(?:Prefix)?\(\s*([^)]+?)\s*\)(.*?)(?:\n\n|\n\t*t\.)', re.S)
METHODS_RE = re.compile(r'\.Methods\(([^)]*)\)')
CONST_RE = re.compile(r'(Path[A-Za-z0-9_]+)\s*=\s*"([^"]+)"')


def fetch(version, path):
    url = RAW.format(version=version, path=path)
    with urllib.request.urlopen(url, timeout=60) as response:
        if response.status != 200:
            raise SystemExit(f"{url}: HTTP {response.status}")
        return response.read().decode()


def endpoint_id(path):
    slug = path.strip("/").replace("/loki/api/v1/", "").replace("/", "_")
    slug = re.sub(r"[{}]", "", slug)
    return re.sub(r"[^a-z0-9_]+", "_", slug.lower()) or "root"


def extract(version):
    sources = {path: fetch(version, path) for path in SOURCES}
    constants = dict(CONST_RE.findall(sources["pkg/util/constants/api_paths.go"]))
    routes = {}
    for path, text in sources.items():
        if path.endswith("constants/api_paths.go"):
            continue
        for line_number, line in enumerate(text.splitlines(), 1):
            match = re.search(r'HTTP\.Path(?:Prefix)?\(\s*(constants\.[A-Za-z0-9_]+|"[^"]+")\s*\)', line)
            if not match:
                continue
            raw = match.group(1)
            if raw.startswith('"'):
                route, source = raw.strip('"'), "literal"
            else:
                name = raw.split(".", 1)[1]
                if name not in constants:
                    continue
                route, source = constants[name], name
            methods = METHODS_RE.search(line)
            verbs = sorted(set(re.findall(r'"([A-Z]+)"', methods.group(1)))) if methods else []
            entry = routes.setdefault(route, {
                "id": endpoint_id(route), "path": route, "methods": [],
                "constant": source, "source_refs": [],
            })
            entry["methods"] = sorted(set(entry["methods"]) | set(verbs))
            ref = f"{path}:{line_number}"
            if ref not in entry["source_refs"]:
                entry["source_refs"].append(ref)
    return routes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--out", default="conformance/registry/generated")
    args = parser.parse_args()
    routes = extract(args.version)
    client_routes = {path: entry for path, entry in routes.items()
                     if path.startswith(("/loki/api/", "/prometheus/api/", "/api/prom"))}
    payload = {
        "version": args.version,
        "source": "github.com/grafana/loki",
        "endpoints": [client_routes[path] for path in sorted(client_routes)],
        "other_routes": sorted(set(routes) - set(client_routes)),
    }
    directory = os.path.join(args.out, "loki", args.version)
    os.makedirs(directory, exist_ok=True)
    target = os.path.join(directory, "endpoints.json")
    with open(target, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(f"{target}: {len(payload['endpoints'])} client endpoints, {len(payload['other_routes'])} internal routes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
