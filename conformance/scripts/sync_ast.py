#!/usr/bin/env python3
"""Extract the LogQL syntax surface from Loki and record what the proxy supports.

Loki's operator constants (pkg/logql/syntax/ast.go) are the full LogQL
vocabulary: aggregations, range functions, parsers, label-filter and
line-filter operators, conversion functions and pipeline stages. For each one
this script records whether the proxy's own parser and translator handle it,
and where.

Writes conformance/registry/generated/loki/<version>/logql.json and seeds one
registry file per construct under conformance/registry/loki/logql/.

Usage: sync_ast.py --version v3.7.7
"""
import argparse, json, os, re, subprocess, sys, urllib.request
from registry_io import write_text

RAW = "https://raw.githubusercontent.com/grafana/loki/{version}/pkg/logql/syntax/ast.go"
GROUPS = {
    "OpType": "operator", "OpRangeType": "range_function", "OpParser": "parser",
    "OpFmt": "formatter", "OpFilter": "filter", "OpConv": "conversion",
    "OpLabel": "label_op", "OpPipe": "pipeline_stage",
}
SEARCH_DIRS = ["internal/logql", "internal/translator", "internal/proxy"]


def fetch(version):
    with urllib.request.urlopen(RAW.format(version=version), timeout=60) as response:
        return response.read().decode()


def supported(literal):
    """Where the proxy mentions this LogQL construct, outside tests."""
    hits = []
    for directory in SEARCH_DIRS:
        if not os.path.isdir(directory):
            continue
        output = subprocess.run(["grep", "-rn", "--include=*.go", f'"{literal}"', directory],
                                capture_output=True, text=True).stdout
        for line in output.splitlines():
            path = line.split(":", 1)[0]
            if not path.endswith("_test.go"):
                hits.append(path)
    return sorted(set(hits))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    source = fetch(args.version)
    constructs = []
    for name, literal in re.findall(r'\b(Op[A-Za-z]+)\s*=\s*"([^"]+)"', source):
        prefix = next((p for p in sorted(GROUPS, key=len, reverse=True) if name.startswith(p)), "OpType")
        where = supported(literal)
        constructs.append({
            "constant": name, "literal": literal, "kind": GROUPS.get(prefix, "operator"),
            "proxy_mentions": where, "supported": bool(where),
        })
    directory = f"conformance/registry/generated/loki/{args.version}"
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "logql.json"), "w") as handle:
        json.dump({"version": args.version, "constructs": sorted(constructs, key=lambda c: c["constant"])},
                  handle, indent=2, sort_keys=True)
        handle.write("\n")

    os.makedirs("conformance/registry/loki/logql", exist_ok=True)
    written = 0
    for construct in constructs:
        identifier = re.sub(r'[^a-z0-9]+', '-', construct["literal"].lower()).strip('-') or construct["constant"].lower()
        identifier = f"{construct['kind']}-{identifier}"
        target = f"conformance/registry/loki/logql/{identifier}.yaml"
        if os.path.exists(target):
            continue
        where = "\n".join(f"    - {path}" for path in construct["proxy_mentions"][:6]) or "    []"
        write_text(target,
            f"id: {identifier}\nkind: {construct['kind']}\n"
            f"logql: {json.dumps(construct['literal'])}\nloki_constant: {construct['constant']}\n"
            f"generated:\n  supported_by_proxy: {str(construct['supported']).lower()}\n"
            f"  seen_in:\n{where}\n\n"
            "semantics: |\n  TODO: Loki's exact semantics, including how it treats missing labels,\n"
            "  parse errors, ordering and empty input.\n"
            "victorialogs: |\n  TODO: the native LogsQL construct used, or the proxy-side\n"
            "  transformation and the VictoriaLogs limitation that forces it.\n"
            "consumers: []   # explore, drilldown, datasource, api\n"
            "cases: []       # case ids proving the semantics match\n")
        written += 1
    kinds = {}
    for construct in constructs:
        kinds.setdefault(construct["kind"], [0, 0])
        kinds[construct["kind"]][0] += 1
        kinds[construct["kind"]][1] += 1 if construct["supported"] else 0
    print(f"LogQL surface {args.version}: {len(constructs)} constructs ({written} files written)")
    for kind, (total, ok) in sorted(kinds.items()):
        print(f"  {kind:16} {ok}/{total} referenced in proxy code")
    return 0


if __name__ == "__main__":
    sys.exit(main())
