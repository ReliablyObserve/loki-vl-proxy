#!/usr/bin/env python3
"""Record which VictoriaLogs surface the proxy actually uses.

Scans the proxy source for the VictoriaLogs endpoints it calls, the LogsQL
pipes and stats functions it emits, and the version-gated capabilities in
internal/logsql/capabilities.go. Writes
conformance/registry/generated/vl/usage.json and seeds one registry file per
capability under conformance/registry/vl/capabilities/ (never overwriting).

The curated part of each capability file records the VictoriaLogs version that
introduced it, whether the proxy uses it natively, and, when it does not, the
transformation the proxy performs instead and why.

Usage: sync_vl.py
"""
import json, os, re, subprocess, sys
from registry_io import read_text, write_text

PIPES = ["unpack_json", "unpack_logfmt", "stats by", "stats count", "filter", "math", "sort",
         "limit", "top", "uniq", "hits", "field_names", "field_values", "stream_field_names",
         "stream_field_values", "extract", "format", "pack_json", "block_stats", "coalesce",
         "json_array_len", "json_values", "len", "replace", "running", "unroll", "drop", "keep"]
STATS_FUNCS = ["count()", "count_uniq(", "sum(", "sum_len(", "avg(", "min(", "max(", "quantile(",
               "rate(", "rate_sum(", "histogram(", "uniq_values(", "values(", "row_any("]


def run(pattern, directory="internal", extra=None):
    command = ["grep", "-rn", "--include=*.go", pattern, directory]
    output = subprocess.run(command, capture_output=True, text=True).stdout
    return [line for line in output.splitlines() if "_test.go" not in line.split(":")[0]]


def main():
    endpoints = {}
    for line in run(r'"/select/logsql/[a-z_]*"'):
        for path in re.findall(r'"(/select/logsql/[a-z_]+)"', line):
            file_path, number, _ = line.split(":", 2)
            endpoints.setdefault(path, []).append(f"{file_path}:{number}")
    pipes = {}
    for pipe in PIPES:
        hits = run(f'| {pipe}')
        if hits:
            pipes[pipe] = len(hits)
    stats = {}
    for func in STATS_FUNCS:
        hits = run(func.replace("(", r"("))
        if hits:
            stats[func.rstrip("(")] = len(hits)
    capabilities = {}
    source = read_text("internal/logsql/capabilities.go")
    for name, comment in re.findall(r'\n\t(\w+)\s+bool\s*//\s*(v[\d.]+)\+', source):
        capabilities[name] = {"since": comment, "used_in": [
            line.split(":", 1)[0] for line in run(f"caps.{name}") + run(f"Capabilities{{{name}")]}
    payload = {"endpoints_used": {k: sorted(set(v)) for k, v in sorted(endpoints.items())},
               "pipes_used": pipes, "stats_functions_used": stats, "version_gated": capabilities}
    os.makedirs("conformance/registry/generated/vl", exist_ok=True)
    with open("conformance/registry/generated/vl/usage.json", "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.makedirs("conformance/registry/vl/capabilities", exist_ok=True)
    written = 0
    for name, meta in capabilities.items():
        identifier = re.sub(r'(?<!^)(?=[A-Z])', '-', name).lower()
        target = f"conformance/registry/vl/capabilities/{identifier}.yaml"
        if os.path.exists(target):
            continue
        write_text(target,
            f"id: {identifier}\nkind: version_gated_capability\n"
            f"since: {meta['since']}\n"
            f"generated:\n  used_in: {len(meta['used_in'])}\n\n"
            "description: |\n  TODO: what the capability does in VictoriaLogs.\n"
            "proxy_usage: |\n  TODO: native use, or the transformation used instead and why.\n"
            "fallback: |\n  TODO: what happens on a backend older than `since`.\n"
            "proves: []  # case ids that exercise it\n")
        written += 1
    print(f"vl usage: {len(endpoints)} endpoints, {len(pipes)} pipes, {len(stats)} stats functions, "
          f"{len(capabilities)} version-gated capabilities ({written} files written)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
