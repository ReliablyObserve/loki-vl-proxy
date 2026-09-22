#!/usr/bin/env python3
"""Every proxy flag must be documented where flags are documented exhaustively.

`docs/configuration.md` and the chart's `values.yaml` list every flag; the
chart README, the env example and the k8s example are curated subsets. So a
missing flag in the first two is an error, and in the other three it is only
reported, so a reviewer can decide whether a new flag belongs in the curated
sets. Two PRs in a row shipped a flag that reached values.yaml and
configuration.md but nowhere else, which is what this makes visible.

Usage: python3 conformance/scripts/flag_docs.py [--check]
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import read_text  # noqa: E402

SOURCE = "cmd/proxy/main.go"
REQUIRED = {
    "chart values": "charts/loki-vl-proxy/values.yaml",
    "configuration docs": "docs/configuration.md",
}
CURATED = {
    "chart README": "charts/loki-vl-proxy/README.md",
    "env example": "examples/loki-vl-proxy-full.env",
    "k8s example": "examples/k8s/proxy-config-configmap.yaml",
}
# Flags an operator never sets: internal test seams and aliases.
EXEMPT = {"version"}


def flags():
    found = set()
    for match in re.finditer(r'fs\.(?:Bool|Int|Int64|Uint|Float64|Duration|String)(?:Var)?\(\s*(?:&\w+,\s*)?"([a-z0-9.\-]+)"', read_text(SOURCE)):
        found.add(match.group(1))
    return sorted(found - EXEMPT)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    required = {label: read_text(path) for label, path in REQUIRED.items() if os.path.exists(path)}
    curated = {label: read_text(path) for label, path in CURATED.items() if os.path.exists(path)}
    known = flags()
    missing, thin = {}, {}
    for flag in known:
        gaps = [label for label, text in required.items() if flag not in text]
        if gaps:
            missing[flag] = gaps
        absent = [label for label, text in curated.items() if flag not in text]
        if len(absent) == len(curated):
            thin[flag] = absent
    print(f"flag documentation: {len(known)} flags, {len(known) - len(missing)} in every "
          f"exhaustive place ({', '.join(REQUIRED)})")
    for flag, gaps in sorted(missing.items()):
        print(f"  ERROR -{flag}: missing from {', '.join(gaps)}")
    if thin:
        print(f"  {len(thin)} flags appear in no curated example "
              "(chart README, env example, k8s example) — fine for internal knobs, "
              "worth adding for anything an operator tunes")
    if args.check and missing:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
