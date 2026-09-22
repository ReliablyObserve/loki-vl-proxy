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
from registry_io import load_json, read_text, write_text

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
    for endpoint in coverage["endpoints"]:
        identifier = endpoint["id"]
        state = read_yaml_field(os.path.join(ROOT, "state/loki", identifier + ".yaml"), "state") or "unknown"
        impl = implementation.get(identifier, {})
        rows.append({
            "id": identifier, "path": endpoint["path"], "methods": endpoint["methods"],
            "state": state, "execution": impl.get("execution", "not_implemented"),
            "where": (impl.get("where") or [None])[0],
            "consumers": consumers(identifier),
            "tests": sum(len(v) for v in endpoint["tests"].values()),
            "tests_vs_loki": sum(len(v) for v in endpoint["differential_tests"].values()),
            "vl_endpoints": impl.get("vl_endpoints", []),
            "proxy_side_work": sorted((impl.get("proxy_side_work") or {}).keys()),
        })
    return coverage["loki_version"], rows, vl


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", default="conformance/reports/coverage-map.md")
    args = parser.parse_args()
    loki_version, rows, vl = collect()
    total = len(rows)
    proven = sum(1 for row in rows if row["state"] == "proven")
    differential = sum(1 for row in rows if row["tests_vs_loki"])
    implemented = sum(1 for row in rows if row["execution"] != "not_implemented")
    native = sum(1 for row in rows if row["execution"] == "native_vl")
    summary = {
        "loki_version": loki_version, "endpoints": total, "implemented": implemented,
        "proven": proven, "with_differential_tests": differential,
        "execution": {key: sum(1 for row in rows if row["execution"] == key)
                      for key in EXECUTION_LABEL},
        "state": {key: sum(1 for row in rows if row["state"] == key) for key in STATE_LABEL},
        "vl_surface": {"endpoints": len(vl["endpoints_used"]), "pipes": len(vl["pipes_used"]),
                       "stats_functions": len(vl["stats_functions_used"]),
                       "version_gated": len(vl["version_gated"])},
        "scores": {
            "coverage": round(differential / total, 4),
            "implementation": round(implemented / total, 4),
            "native_share_of_implemented": round(native / implemented, 4) if implemented else 0,
        },
    }
    with open(os.path.join(ROOT, "generated/summary.json"), "w") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    lines = [
        "---", "sidebar_label: Coverage Map",
        ("description: Which Loki endpoints the proxy implements, how each one is served "
         + "by VictoriaLogs, which clients depend on it, and what evidence proves it."),
        "---", "",
        "# Coverage Map", "",
        ("Generated from the conformance registry by "
         + "`conformance/scripts/coverage_map.py`. Do not edit by hand."), "",
        f"Loki surface: **{loki_version}**, extracted from the Loki source.", "",
        "## Scores", "",
        "| Score | Value | Meaning |", "|---|---|---|",
        f"| Implementation | {implemented}/{total} | endpoints the proxy routes |",
        f"| Differential evidence | {differential}/{total} | endpoints with tests comparing the proxy against Loki |",
        f"| Proven | {proven}/{total} | endpoints whose registry state is `proven` |",
        f"| VictoriaLogs native share | {native}/{implemented} | implemented endpoints answered natively |",
        "",
        "## Endpoints", "",
        "| Endpoint | State | Served by | Consumers | Tests | vs Loki | Implemented in |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for row in sorted(rows, key=lambda r: (r["execution"] == "not_implemented", r["id"])):
        where = row["where"].split(" ")[0] if row["where"] else "—"
        lines.append(
            f"| `{row['path']}` | {STATE_LABEL.get(row['state'], row['state'])} | "
            f"{EXECUTION_LABEL.get(row['execution'], row['execution'])} | "
            f"{', '.join(row['consumers']) or '—'} | {row['tests']} | {row['tests_vs_loki']} | "
            f"{where} |")
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
    print(f"{args.docs}: {total} endpoints, coverage {summary['scores']['coverage']:.0%}, "
          f"implementation {summary['scores']['implementation']:.0%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
