#!/usr/bin/env python3
"""Summarise what a change touches in the conformance registry.

Given the files a pull request changes, this reports the registry items behind
them: the Loki endpoints whose handlers moved, the behaviours and LogQL
constructs those files carry, what state each item is in, which of them the
branch's tests declare coverage for, and what is still unproven. Paste the
output into the PR so a reviewer sees, without digging, which compatibility
contracts the change is standing on.

Usage:
  python3 conformance/scripts/pr_report.py                 # against origin/main
  python3 conformance/scripts/pr_report.py --base <ref>
  python3 conformance/scripts/pr_report.py --files a.go b.go
"""
import argparse
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import load_json, read_text  # noqa: E402

ROOT = "conformance/registry"


def changed_files(base):
    diff = subprocess.run(["git", "diff", "--name-only", f"{base}...HEAD"],
                          capture_output=True, text=True)
    return [line for line in diff.stdout.splitlines() if line]


def state_of(track, identifier):
    path = os.path.join(ROOT, "state", track, identifier + ".yaml")
    if not os.path.exists(path):
        return "unknown"
    found = re.search(r'^state:\s*(\S+)', read_text(path), re.M)
    return found.group(1) if found else "unknown"


def endpoints_for(files):
    path = os.path.join(ROOT, "generated/proxy/implementation.json")
    if not os.path.exists(path):
        return []
    touched = []
    for entry in load_json(path)["endpoints"]:
        for where in entry.get("where") or []:
            source = where.split(":")[0]
            if source in files:
                touched.append((entry["id"], entry.get("execution", "?"), source))
                break
    return touched


def items_mentioning(directory, track, files):
    base = os.path.join(ROOT, directory)
    if not os.path.isdir(base):
        return []
    touched = []
    for name in sorted(os.listdir(base)):
        if not name.endswith(".yaml"):
            continue
        text = read_text(os.path.join(base, name))
        if any(source in text for source in files):
            touched.append((name[:-5], track))
    return touched


def declared_coverage():
    path = os.path.join(ROOT, "generated/wiring.json")
    return load_json(path)["item_to_tests"] if os.path.exists(path) else {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--files", nargs="*")
    args = parser.parse_args()
    files = args.files or changed_files(args.base)
    sources = [f for f in files if f.endswith((".go", ".ts"))]
    wired = declared_coverage()

    endpoints = endpoints_for(sources)
    behaviours = items_mentioning("behaviours", "behaviour", sources)
    translations = items_mentioning("translations", "translation", sources)

    print("## Conformance registry")
    print()
    if not (endpoints or behaviours or translations):
        print("No registry item points at the files this change touches. If it changes "
              "client-visible behaviour, add the item first: see `conformance/README.md`.")
        return 0
    print("| Item | Kind | State | Served by | Tests declaring it |")
    print("|---|---|---|---|---|")
    for identifier, execution, source in endpoints:
        tests = len(wired.get(identifier, []))
        print(f"| `{identifier}` | endpoint | {state_of('loki', identifier)} | {execution} | "
              f"{tests or '**none**'} |")
    for identifier, track in behaviours + translations:
        tests = len(wired.get(identifier, []))
        print(f"| `{identifier}` | {track} | {state_of(track + 's', identifier)} | — | "
              f"{tests or '**none**'} |")
    print()
    unproven = [i for i, _, _ in endpoints if not wired.get(i)]
    unproven += [i for i, _ in behaviours + translations if not wired.get(i)]
    if unproven:
        print("Not yet proven by a test declaring it: " +
              ", ".join(f"`{i}`" for i in unproven) + ".")
        print("Add `// conformance: <ids>` above the Go test, or `@cov:<id>` in the "
              "Playwright title, so the coverage is measured rather than assumed.")
    else:
        print("Every item this change touches is declared by at least one test.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
