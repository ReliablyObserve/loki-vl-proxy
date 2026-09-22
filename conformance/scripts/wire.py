#!/usr/bin/env python3
"""Wire tests and code to the conformance registry, both directions.

A test declares what it proves:
  Go:         // conformance: loki_api_v1_index_volume, drilldown/index_volume/single-sample-returns-vector
  Playwright: test('landing volume @cov:loki_api_v1_index_volume', ...)

This script builds the two-way map:
  item  -> the tests that prove it, and the code that implements it
  test  -> the registry items it claims

and reports what is not wired: registry items with no test, tests claiming
unknown ids, code routes with no registry item, and items whose recorded
implementation site no longer exists.

Writes conformance/registry/generated/wiring.json.
Exit code 1 with --check when anything is inconsistent.

Usage: wire.py [--check]
"""
import argparse, json, os, re, sys
from registry_io import load_json, read_text

ROOT = "conformance/registry"
GO_MARKER = re.compile(r'//\s*conformance:\s*(.+)')
TS_MARKER = re.compile(r'@cov:([A-Za-z0-9_/\-]+)')
TEST_DIRS = ["cmd", "internal", "test"]


def registry_ids():
    ids = {}
    for kind, directory in (("endpoint", "loki/endpoints"), ("error", "loki/errors"),
                            ("logql", "loki/logql"), ("behaviour", "behaviours"),
                            ("capability", "vl/capabilities")):
        base = os.path.join(ROOT, directory)
        if os.path.isdir(base):
            for name in sorted(os.listdir(base)):
                if name.endswith(".yaml"):
                    ids[name[:-5]] = kind
    cases = os.path.join(ROOT, "cases")
    for current, _, files in os.walk(cases):
        for name in files:
            if name.endswith(".yaml"):
                identifier = os.path.relpath(os.path.join(current, name), cases)[:-5]
                ids[identifier] = "case"
    return ids


def scan_tests():
    claims = {}
    for directory in TEST_DIRS:
        for current, _, files in os.walk(directory):
            for name in files:
                path = os.path.join(current, name)
                if name.endswith("_test.go"):
                    pattern, marker = GO_MARKER, None
                elif name.endswith(".spec.ts"):
                    pattern, marker = TS_MARKER, "ts"
                else:
                    continue
                text = read_text(path, errors="ignore")
                found = []
                for match in pattern.finditer(text):
                    if marker == "ts":
                        found.append(match.group(1))
                    else:
                        found += [piece.strip() for piece in match.group(1).split(",") if piece.strip()]
                if found:
                    claims[path] = sorted(set(found))
    return claims


def code_routes():
    source = read_text("internal/proxy/proxy.go")
    return re.findall(r'p\.routeHandler\(\s*"[^"]+"\s*,\s*"([^"]+)"', source)


def endpoint_id(path):
    slug = re.sub(r"[{}]", "", path.strip("/")).replace("/", "_")
    return re.sub(r"[^a-z0-9_]+", "_", slug.lower())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    ids = registry_ids()
    claims = scan_tests()
    item_tests = {}
    unknown = {}
    for path, claimed in claims.items():
        for identifier in claimed:
            if identifier in ids:
                item_tests.setdefault(identifier, []).append(path)
            else:
                unknown.setdefault(path, []).append(identifier)

    implementation = {}
    impl_path = os.path.join(ROOT, "generated/proxy/implementation.json")
    if os.path.exists(impl_path):
        implementation = {entry["id"]: entry for entry in load_json(impl_path)["endpoints"]}
    missing_code = []
    for identifier, entry in implementation.items():
        for where in entry.get("where", []):
            file_path = where.split(":")[0]
            if not os.path.exists(file_path):
                missing_code.append((identifier, where))

    routes_without_item = [route for route in code_routes() if endpoint_id(route) not in ids]
    unwired = sorted(identifier for identifier, kind in ids.items()
                     if kind in ("endpoint", "case", "behaviour") and identifier not in item_tests)

    payload = {
        "registry_items": len(ids),
        "tests_with_markers": len(claims),
        "wired_items": len(item_tests),
        "item_to_tests": {k: sorted(v) for k, v in sorted(item_tests.items())},
        "test_to_items": {k: v for k, v in sorted(claims.items())},
        "unknown_ids": unknown,
        "routes_without_registry_item": routes_without_item,
        "implementation_sites_missing": missing_code,
        "unwired_items": unwired,
    }
    os.makedirs(os.path.join(ROOT, "generated"), exist_ok=True)
    with open(os.path.join(ROOT, "generated/wiring.json"), "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(f"registry items: {len(ids)}; tests declaring coverage: {len(claims)}; "
          f"items wired to a test: {len(item_tests)}; unwired: {len(unwired)}")
    problems = 0
    if unknown:
        problems += 1
        print("tests claiming unknown registry ids:")
        for path, identifiers in sorted(unknown.items()):
            print(f"  {path}: {', '.join(identifiers)}")
    if routes_without_item:
        problems += 1
        print("proxy routes without a registry item: " + ", ".join(routes_without_item))
    if missing_code:
        problems += 1
        print("registry implementation sites that no longer exist: " +
              ", ".join(f"{i} {w}" for i, w in missing_code))
    if args.check and problems:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
