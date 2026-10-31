#!/usr/bin/env python3
"""Generate the coverage map from the conformance registry.

Writes:
  * docs/compatibility-coverage.md - the published coverage map: every Loki
    endpoint with its state, how it is served, which consumers depend on it and
    what evidence exists; the VictoriaLogs surface the proxy uses; the scores.
  * conformance/registry/generated/summary.json - the same numbers, machine
    readable, so CI can gate on them and the roadmap can rank the gaps.

Usage: coverage_map.py [--docs docs/compatibility-coverage.md]
"""
import argparse, json, os, re, sys
from registry_io import SCOPE_SENTENCE, WRITE_IN_SCOPE, load_json, out_of_scope, read_methods, read_text, write_text

ROOT = "conformance/registry"
STATE_LABEL = {"proven": "proven", "partial": "partial", "gap": "gap",
               "waived": "waived", "not-applicable": "n/a", "unknown": "unknown"}
EXECUTION_LABEL = {"native_vl": "VictoriaLogs native", "hybrid": "hybrid",
                   "proxy_side": "proxy-side", "not_implemented": "not implemented"}


def read_yaml_field(path, field):
    if not os.path.exists(path):
        return None
    match = re.search(rf'^{field}:\s*(.+)$', read_text(path), re.M)
    return match.group(1).strip() if match else None


def consumers(endpoint_id):
    path = os.path.join(ROOT, "loki/endpoints", endpoint_id + ".yaml")
    if not os.path.exists(path):
        return []
    block = re.search(r'consumers:\n((?:\s+- \w+\n)+)', read_text(path))
    return re.findall(r'- (\w+)', block.group(1)) if block else []


def collect():
    coverage = load_json(os.path.join(ROOT, "generated/proxy/coverage.json"))
    implementation = {entry["id"]: entry for entry in
                      load_json(os.path.join(ROOT, "generated/proxy/implementation.json"))["endpoints"]}
    vl = load_json(os.path.join(ROOT, "generated/vl/usage.json"))
    rows = []
    skipped = out_of_scope()
    for endpoint in coverage["endpoints"]:
        identifier = endpoint["id"]
        if identifier in skipped:
            continue
        state_path = os.path.join(ROOT, "state/loki", identifier + ".yaml")
        state = read_yaml_field(state_path, "state") or "unknown"
        optional = read_yaml_field(state_path, "priority") == "optional"
        reason = re.search(r'^reason: \|\n((?:[ \t]+.*\n)+)', read_text(state_path), re.M) if optional else None
        impl = implementation.get(identifier, {})
        rows.append({
            "id": identifier, "path": endpoint["path"], "methods": read_methods(identifier) or endpoint["methods"],
            "state": state, "execution": impl.get("execution", "not_implemented"),
            "optional": optional, "scope_note": " ".join(reason[1].split()) if reason else "",
            "where": (impl.get("where") or [None])[0],
            "consumers": consumers(identifier),
            "tests": sum(len(v) for v in endpoint["tests"].values()),
            "tests_vs_loki": sum(len(v) for v in endpoint["differential_tests"].values()),
            "vl_endpoints": impl.get("vl_endpoints", []),
            "proxy_side_work": sorted((impl.get("proxy_side_work") or {}).keys()),
        })
    return coverage["loki_version"], rows, vl


def summarize(rows):
    total = len(rows)
    proven = sum(1 for row in rows if row["state"] == "proven")
    differential = sum(1 for row in rows if row["tests_vs_loki"])
    implemented = sum(1 for row in rows if row["execution"] != "not_implemented")
    native = sum(1 for row in rows if row["execution"] == "native_vl")
    return {
        "endpoints": total, "implemented": implemented,
        "proven": proven, "with_differential_tests": differential,
        "execution": {key: sum(1 for row in rows if row["execution"] == key)
                      for key in EXECUTION_LABEL},
        "state": {key: sum(1 for row in rows if row["state"] == key)
                  for key in STATE_LABEL if key != "not-applicable"},
        "scores": {
            "coverage": round(differential / total, 4) if total else 0,
            "implementation": round(implemented / total, 4) if total else 0,
            "native_share_of_implemented": round(native / implemented, 4) if implemented else 0,
        },
    }


def render_api_status(rows):
    read_rows = [row for row in rows if row["id"] not in WRITE_IN_SCOPE]
    summary = summarize(read_rows)
    total = summary["endpoints"]
    lines = [SCOPE_SENTENCE, "",
             "| Read API signal | Registry evidence |", "|---|---|",
             f"| Routed endpoints | {summary['implemented']}/{total} |",
             f"| Endpoints with Loki differential tests | {summary['with_differential_tests']}/{total} |",
             f"| Endpoints declared `proven` | {summary['proven']}/{total} |", "",
             "Routing, linked tests and declared proof are different signals; none means",
             "every LogQL shape or every backend version is compatible. See the",
             "[coverage map](compatibility-coverage.md) and [ranked differences](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/parity-gaps.md).", "",
             "Read routes without an implementation:", "",
             "| Route | Current scope |", "|---|---|"]
    missing = sorted((row for row in read_rows if row["execution"] == "not_implemented"),
                     key=lambda row: (row["optional"], row["path"]))
    lines += [f"| `{row['path']}` | {'Optional planner gap; low priority, counted in scores' if row['optional'] else 'Not implemented'} |"
              for row in missing]
    if not missing:
        lines.append("| — | No unrouted read endpoints in this registry |")
    if any(row["path"] == "/loki/api/v1/index/shards" for row in missing):
        lines += ["", "`index/shards` is not required by the pinned Grafana/Drilldown request paths",
                  "or by proxy peer-cache lookup."]
    if any(row["path"] == "/loki/api/v1/cache/generation_numbers" for row in missing):
        lines += ["", "`cache/generation_numbers` is not implemented; manual cache flush and",
                  "eviction do not provide a distributed deletion generation."]
    lines += ["",
              "Rules reads under `/loki/api/v1/rules` and `/api/prom/rules`, including namespace",
              "and group lookups, are already routed. Configure `-ruler-backend` for vmalert;",
              "routing these reads does not implement rule writes or prove full ruler parity."]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", default="conformance/reports/coverage-map.md")
    parser.add_argument("--api-status", help="Refresh the api-status block of a curated docs page")
    args = parser.parse_args()
    loki_version, rows, vl = collect()
    read_rows = [row for row in rows if row["id"] not in WRITE_IN_SCOPE]
    management = [row for row in rows if row["id"] in WRITE_IN_SCOPE]
    read = summarize(read_rows)
    summary = {**summarize(rows), "loki_version": loki_version,
               "read_api": {**read, "scope": "loki-read-api",
                            "unrouted": [{"path": row["path"], "optional": row["optional"]}
                                         for row in sorted(read_rows, key=lambda row: (row["optional"], row["path"]))
                                         if row["execution"] == "not_implemented"]},
               "optional_management": summarize(management),
               "vl_surface": {"endpoints": len(vl["endpoints_used"]), "pipes": len(vl["pipes_used"]),
                              "stats_functions": len(vl["stats_functions_used"]), "version_gated": len(vl["version_gated"])}}
    with open(os.path.join(ROOT, "generated/summary.json"), "w") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    if args.api_status:
        text = read_text(args.api_status)
        start, end = "<!-- api-status:start -->", "<!-- api-status:end -->"
        if text.count(start) != 1 or text.count(end) != 1:
            parser.error("curated page needs exactly one api-status marker pair")
        prefix, rest = text.split(start, 1)
        _, suffix = rest.split(end, 1)
        write_text(args.api_status, prefix + start + "\n" + render_api_status(rows) + "\n" + end + suffix)
        print(f"{args.api_status}: API status refreshed")
        return 0

    total, implemented = read["endpoints"], read["implemented"]
    differential, proven, native = read["with_differential_tests"], read["proven"], read["execution"]["native_vl"]

    lines = [
        "---", "sidebar_label: Coverage Map",
        ("description: Which Loki endpoints the proxy implements, how each one is served "
         + "by VictoriaLogs, which clients depend on it, and what evidence proves it."),
        "---", "",
        "# Coverage Map", "",
        ("Generated from the conformance registry by "
         + "`conformance/scripts/coverage_map.py`. Do not edit by hand."), "",
        f"Loki surface: **{loki_version}**, extracted from the Loki source.", "",
        SCOPE_SENTENCE, "",
        "## Read API scores", "",
        "| Score | Value | Meaning |", "|---|---|---|",
        f"| Implementation | {implemented}/{total} | endpoints the proxy routes |",
        f"| Differential evidence | {differential}/{total} | endpoints with tests comparing the proxy against Loki |",
        f"| Proven | {proven}/{total} | endpoints whose registry state is `proven` |",
        f"| VictoriaLogs native share | {native}/{implemented} | implemented endpoints answered natively |",
        "",
        "Counts exclude optional deletion management as well as out-of-scope writes and deprecated aliases.",
        "A route or a linked test is not a full compatibility claim.", "",
        "## Read endpoints", "",
        "| Endpoint | Methods | State | Served by | Consumers | Tests | vs Loki | Implemented in |",
        "|---|---|---|---|---|---:|---:|---|",
    ]
    for row in sorted(read_rows, key=lambda r: (r["execution"] == "not_implemented", r["id"])):
        where = row["where"].split(" ")[0] if row["where"] else "—"
        lines.append(
            f"| `{row['path']}` | {', '.join(row['methods'])} | {STATE_LABEL.get(row['state'], row['state'])}{' (optional)' if row['optional'] else ''} | "
            f"{EXECUTION_LABEL.get(row['execution'], row['execution'])} | "
            f"{', '.join(row['consumers']) or '—'} | {row['tests']} | {row['tests_vs_loki']} | "
            f"{where} |")
    optional_rows = [row for row in rows if row["optional"]]
    if optional_rows:
        lines += ["", "Optional API gaps remain included in the totals and scores:", ""]
        lines += [f"- `{row['path']}`: {row['scope_note']}" for row in optional_rows]
    if management:
        lines += ["", "## Optional deletion management", "",
                  "Not included in the read API denominator. `/loki/api/v1/delete` is disabled",
                  "by default. Its opt-in handler has request safeguards but forwards to an",
                  "unsupported VictoriaLogs path; it does not implement VL's asynchronous deletion API.",
                  "See [Known Issues](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/docs/KNOWN_ISSUES.md)."]
    lines += ["", "## Proxy-side work", "",
              ("Where the proxy computes a result instead of passing a VictoriaLogs answer "
               + "through."), "",
              "| Endpoint | Proxy-side work |", "|---|---|"]
    for row in sorted(rows, key=lambda r: r["id"]):
        if row["proxy_side_work"]:
            lines.append(f"| `{row['path']}` | {', '.join(row['proxy_side_work'])} |")
    lines += ["", "## VictoriaLogs surface in use", "",
              f"- Endpoints: {', '.join('`' + e + '`' for e in sorted(vl['endpoints_used']))}",
              f"- Pipes: {', '.join('`' + p + '`' for p in sorted(vl['pipes_used']))}",
              f"- Stats functions: {', '.join('`' + s + '`' for s in sorted(vl['stats_functions_used']))}",
              "", "| Version-gated capability | Since | Used in |", "|---|---|---:|"]
    for name, meta in sorted(vl["version_gated"].items()):
        lines.append(f"| {name} | {meta['since']} | {len(meta['used_in'])} |")
    lines.append("")
    os.makedirs(os.path.dirname(args.docs), exist_ok=True)
    write_text(args.docs, "\n".join(lines))
    print(f"{args.docs}: {total} read endpoints, coverage {read['scores']['coverage']:.0%}, "
          f"implementation {read['scores']['implementation']:.0%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
