#!/usr/bin/env python3
"""Seed the conformance registry from the generated Loki and proxy inventories.

Writes one small YAML file per Loki endpoint under
conformance/registry/loki/endpoints/ and one state file per endpoint under
conformance/registry/state/loki/. Existing files are never overwritten: the
curated prose (description, context, examples, edge cases) and owner state are
kept, and only the `generated` block is refreshed.

Usage: seed_registry.py --endpoints <generated endpoints.json> --coverage <generated coverage.json>
"""
import argparse, os, sys
from registry_io import load_json, write_text

# Which client drives an endpoint. Grafana Explore and Logs Drilldown send
# different parameter combinations, so cases are written per consumer.
CONSUMERS = {
    "loki_api_v1_query_range": ["explore", "drilldown", "api"],
    "loki_api_v1_query": ["explore", "drilldown", "api"],
    "loki_api_v1_labels": ["explore", "drilldown", "datasource", "api"],
    "loki_api_v1_label_name_values": ["explore", "drilldown", "datasource", "api"],
    "loki_api_v1_series": ["explore", "datasource", "api"],
    "loki_api_v1_index_volume": ["drilldown", "api"],
    "loki_api_v1_index_volume_range": ["drilldown", "api"],
    "loki_api_v1_detected_fields": ["explore", "drilldown", "api"],
    "loki_api_v1_detected_field_name_values": ["drilldown", "api"],
    "loki_api_v1_detected_labels": ["drilldown", "api"],
    "loki_api_v1_patterns": ["drilldown", "api"],
    "loki_api_v1_tail": ["explore", "api"],
    "loki_api_v1_index_stats": ["explore", "datasource", "api"],
    "loki_api_v1_status_buildinfo": ["datasource", "api"],
    "loki_api_v1_format_query": ["explore", "api"],
}


def yaml_escape(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def render(entry, consumers):
    route = entry.get("proxy_route") or {}
    lines = [
        "# Loki API surface item. Curated prose below the generated block is kept",
        "# by scripts/conformance/seed_registry.py; only `generated` is refreshed.",
        f"id: {entry['id']}",
        f"path: {yaml_escape(entry['path'])}",
        f"methods: [{', '.join(entry['methods'])}]",
        "generated:",
        f"  loki_version: {entry['loki_version']}",
        f"  proxy_route: {yaml_escape(route.get('route', '')) if route else 'null'}",
        f"  proxy_handler: {yaml_escape(route.get('handler', '')) if route else 'null'}",
        "  tests:",
    ]
    for kind in ("unit", "e2e", "ui"):
        files = entry["tests"].get(kind, [])
        differential = entry["differential_tests"].get(kind, [])
        lines.append(f"    {kind}: {len(files)}")
        lines.append(f"    {kind}_vs_loki: {len(differential)}")
    impl = entry.get("implementation") or {}
    lines += ["  implementation:",
              f"    implemented: {str(bool(impl.get('implemented'))).lower()}",
              f"    where: {yaml_escape(impl['where'][0]) if impl.get('where') else 'null'}",
              f"    execution: {impl.get('execution', 'not_implemented')}",
              "    vl_endpoints: [" + ", ".join(impl.get("vl_endpoints", [])) + "]",
              "    vl_pipes: [" + ", ".join(impl.get("vl_pipes", [])) + "]",
              "    proxy_side_work:"]
    for marker, reason in sorted((impl.get("proxy_side_work") or {}).items()):
        lines.append(f"      - {marker}: {yaml_escape(reason)}")
    if not impl.get("proxy_side_work"):
        lines.append("      []")
    lines += [
        "",
        "description: |",
        "  TODO: what this endpoint returns and the Loki behaviour a client depends on.",
        "context:",
        "  consumers:",
    ]
    for consumer in consumers:
        lines.append(f"    - {consumer}")
    lines += [
        "  notes: |",
        "    TODO: how each consumer calls it, and how their parameter combinations differ.",
        "examples:",
        "  - consumer: api",
        "    request: |",
        "      TODO: a real request, with the exact parameters a client sends.",
        "    expectation: |",
        "      TODO: Loki's response shape and the values that must match.",
        "victorialogs:",
        "  # Confirmed by the differential runner at runtime; the generated block above",
        "  # is a static approximation. Say what VictoriaLogs answers natively, what the",
        "  # proxy has to compute, and why VictoriaLogs cannot do that part.",
        "  native: []",
        "  transformed: []",
        "  vl_cannot: |",
        "    TODO: the VictoriaLogs limitation that forces proxy-side work, if any.",
        "edge_cases: []    # case ids under conformance/registry/cases/",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoints", required=True)
    parser.add_argument("--coverage", required=True)
    parser.add_argument("--implementation", default="conformance/registry/generated/proxy/implementation.json")
    parser.add_argument("--root", default="conformance/registry")
    args = parser.parse_args()
    endpoints = load_json(args.endpoints)
    coverage = {entry["id"]: entry for entry in load_json(args.coverage)["endpoints"]}
    implementation = {}
    if os.path.exists(args.implementation):
        implementation = {entry["id"]: entry for entry in load_json(args.implementation)["endpoints"]}
    written, kept = 0, 0
    for endpoint in endpoints["endpoints"]:
        entry = dict(coverage.get(endpoint["id"], endpoint))
        entry["loki_version"] = endpoints["version"]
        entry.setdefault("tests", {})
        entry.setdefault("differential_tests", {})
        entry["implementation"] = implementation.get(endpoint["id"], {})
        target = os.path.join(args.root, "loki/endpoints", endpoint["id"] + ".yaml")
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if os.path.exists(target):
            kept += 1
        else:
            write_text(target, render(entry, CONSUMERS.get(endpoint["id"], ["api"])))
            written += 1
        state_path = os.path.join(args.root, "state/loki", endpoint["id"] + ".yaml")
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        if not os.path.exists(state_path):
            routed = bool(entry.get("proxy_route"))
            differential = sum(len(v) for v in entry.get("differential_tests", {}).values())
            state = "proven" if routed and differential else "partial" if routed else "gap"
            write_text(state_path,
                f"id: {endpoint['id']}\nstate: {state}\n"
                f"reason: |\n  Seeded from the repository: "
                f"{'the proxy routes it' if routed else 'no proxy route'}, "
                f"{differential} test files compare it against Loki.\n"
                "owner_decision: null\nwaiver: null\n")
    print(f"registry: {written} endpoint files written, {kept} kept")
    return 0


if __name__ == "__main__":
    sys.exit(main())
