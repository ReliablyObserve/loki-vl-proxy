#!/usr/bin/env python3
"""Rank what is still missing, from the registry. Writes conformance/reports/gaps.md."""
import os, re, sys
from registry_io import load_json, read_text, write_text

ROOT = "conformance/registry"
WEIGHT = {"explore": 3, "drilldown": 3, "datasource": 2, "api": 1}


def consumers(identifier):
    path = f"{ROOT}/loki/endpoints/{identifier}.yaml"
    if not os.path.exists(path):
        return []
    block = re.search(r'consumers:\n((?:\s+- \w+\n)+)', read_text(path))
    return re.findall(r'- (\w+)', block.group(1)) if block else []


def main():
    coverage = load_json(f"{ROOT}/generated/proxy/coverage.json")
    implementation = {e["id"]: e for e in load_json(f"{ROOT}/generated/proxy/implementation.json")["endpoints"]}
    wiring = load_json(f"{ROOT}/generated/wiring.json")
    errors = load_json(f"{ROOT}/generated/proxy/errors.json")["summary"]["by_status"]
    rows = []
    for endpoint in coverage["endpoints"]:
        identifier = endpoint["id"]
        people = consumers(identifier)
        impl = implementation.get(identifier, {})
        execution = impl.get("execution", "not_implemented")
        differential = sum(len(v) for v in endpoint["differential_tests"].values())
        tests = sum(len(v) for v in endpoint["tests"].values())
        missing = []
        if execution == "not_implemented":
            missing.append("not implemented")
        if not differential:
            missing.append("no test compares it against Loki")
        if identifier in wiring["unwired_items"]:
            missing.append("no test wired to the registry")
        if execution == "proxy_side":
            missing.append("proxy-side: justify in the registry or push down to VictoriaLogs")
        score = (sum(WEIGHT.get(p, 1) for p in people) + (3 if not differential else 0)
                 + (2 if execution == "proxy_side" else 0))
        rows.append((score, endpoint["path"], people, execution, tests, differential, missing))
    rows.sort(reverse=True)
    lines = ["# Open gaps", "",
             "Generated from the registry by `conformance/scripts/gaps.py`. Do not edit by hand.", "",
             "Priority = consumer weight (explore/drilldown 3, datasource 2, api 1), plus 3 when no test",
             "compares the endpoint against Loki, plus 2 when the proxy computes the result itself.", "",
             "| Priority | Endpoint | Consumers | Served by | Tests | vs Loki | Missing |",
             "|---:|---|---|---|---:|---:|---|"]
    for score, path, people, execution, tests, differential, missing in rows:
        if not missing:
            continue
        lines.append(f"| {score} | `{path}` | {', '.join(people) or '—'} | {execution} | {tests} | "
                     f"{differential} | {'; '.join(missing)} |")
    lines += ["", "## Error surface", "",
              "Every dynamic message is a risk: it can carry VictoriaLogs or proxy-internal text that Loki",
              "would never emit. Each needs a registry entry stating Loki's own text.", "",
              "| Status | errorType | proxy call sites | literal messages | dynamic |", "|---|---|---:|---:|---:|"]
    for key, value in errors.items():
        if key.isdigit():
            lines.append(f"| {key} | {value['error_type'] or '—'} | {value['count']} | "
                         f"{len(value['messages'])} | {value['dynamic_sites']} |")
    behaviours = []
    base = f"{ROOT}/behaviours"
    for name in sorted(os.listdir(base)) if os.path.isdir(base) else []:
        identifier = name[:-5]
        text = read_text(os.path.join(base, name))
        title = re.search(r'^title:\s*(.+)$', text, re.M)
        kind = re.search(r'^kind:\s*(\S+)$', text, re.M)
        named = len(re.findall(r'^  - \S+$', text, re.M))
        state_path = f"{ROOT}/state/behaviours/{identifier}.yaml"
        state = "unknown"
        if os.path.exists(state_path):
            found = re.search(r'^state:\s*(\S+)$', read_text(state_path), re.M)
            state = found.group(1) if found else state
        wired = len(wiring["item_to_tests"].get(identifier, []))
        behaviours.append((kind.group(1) if kind else "-", identifier,
                           title.group(1) if title else "", named, wired, state))
    lines += ["", "## Behaviour tracks", "",
              "Semantics, severity, identity and data-quality behaviour the proxy must reproduce.",
              "`cases` counts the edge cases named in the item; `wired` counts tests declaring them.", "",
              "| Track | Item | Cases named | Wired | State |", "|---|---|---:|---:|---|"]
    for kind, identifier, title, named, wired, state in behaviours:
        lines.append(f"| {kind} | `{identifier}` — {title} | {named} | {wired} | {state} |")
    logql_dir = f"{ROOT}/generated/loki"
    for version in sorted(os.listdir(logql_dir)) if os.path.isdir(logql_dir) else []:
        path = os.path.join(logql_dir, version, "logql.json")
        if not os.path.exists(path):
            continue
        constructs = load_json(path)["constructs"]
        missing = [c for c in constructs if not c["supported"]]
        lines += ["", f"## LogQL surface ({version})", "",
                  f"{len(constructs) - len(missing)}/{len(constructs)} constructs are referenced in "
                  "proxy code. Not referenced anywhere:", ""]
        for construct in missing:
            lines.append(f"- `{construct['literal']}` ({construct['kind']}, {construct['constant']})")
    links_path = f"{ROOT}/generated/ast-links.json"
    if os.path.exists(links_path):
        links = load_json(links_path)
        lines += ["", "## VictoriaLogs reuse opportunities", "",
                  "VictoriaLogs already provides these, and the proxy does not emit them. Each is a",
                  "chance to move work off the proxy and onto the backend.", "",
                  "| LogQL | VictoriaLogs | Available since |", "|---|---|---|"]
        for item in links["reuse_opportunities"]:
            lines.append(f"| `{item['logql']}` | {', '.join(item['victorialogs'])} | {item['since']} |")
        lines += ["", "LogQL constructs the proxy supports with no VictoriaLogs equivalent "
                  f"({len(links['no_victorialogs_equivalent'])}): " +
                  ", ".join(f"`{x}`" for x in links["no_victorialogs_equivalent"]), ""]
    lines.append("")
    os.makedirs("conformance/reports", exist_ok=True)
    write_text("conformance/reports/gaps.md", "\n".join(lines))
    print(f"conformance/reports/gaps.md: {sum(1 for r in rows if r[6])} endpoints with gaps")
    return 0


if __name__ == "__main__":
    sys.exit(main())
