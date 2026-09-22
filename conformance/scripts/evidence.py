#!/usr/bin/env python3
"""Collect the evidence that a registry item actually holds.

A registry item is only `proven` when a test that declares it has run and
passed. This reads `go test -json` output (and Playwright's JSON reporter),
maps each result back to the items its test declares, and writes
conformance/registry/generated/evidence.json:

  item -> [{test, package, outcome, seconds, commit, recorded}]

`--check` fails when an item's state claims more than its evidence supports:
`proven` with no passing run, or with a run that failed.

Usage:
  go test ./internal/... -json > /tmp/unit.json
  python3 conformance/scripts/evidence.py --go /tmp/unit.json [--playwright pw.json] [--check]
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import dump_json, load_json, read_text  # noqa: E402

ROOT = "conformance/registry"
GO_MARKER = re.compile(r'//\s*conformance:\s*(.+)')
TS_MARKER = re.compile(r'@cov:([A-Za-z0-9_/\-]+)')


def declared_items():
    """test name -> [registry ids], read from the markers in the tree."""
    claims = {}
    for directory in ("internal", "test"):
        for current, _, files in os.walk(directory):
            for name in files:
                path = os.path.join(current, name)
                if name.endswith("_test.go"):
                    text = read_text(path, errors="ignore")
                    pending = []
                    for line in text.split("\n"):
                        marker = GO_MARKER.search(line)
                        if marker:
                            pending += [p.strip() for p in marker.group(1).split(",") if p.strip()]
                            continue
                        func = re.match(r'func (Test\w+|Fuzz\w+)\(', line)
                        if func and pending:
                            claims.setdefault(func.group(1), []).extend(pending)
                            pending = []
                        elif func:
                            pending = []
                elif name.endswith(".spec.ts"):
                    text = read_text(path, errors="ignore")
                    for line in text.split("\n"):
                        found = TS_MARKER.findall(line)
                        title = re.search(r"test\(\s*['\"]([^'\"]+)['\"]", line)
                        if found and title:
                            claims.setdefault(title.group(1), []).extend(found)
    return claims


def go_results(path):
    """test name -> (outcome, seconds, package) from `go test -json`."""
    results = {}
    for line in read_text(path, errors="ignore").split("\n"):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("Action") in ("pass", "fail", "skip") and event.get("Test"):
            name = event["Test"].split("/")[0]
            outcome = event["Action"]
            previous = results.get(name)
            # a failing subtest fails the whole test
            if previous and previous[0] == "fail":
                continue
            results[name] = (outcome, event.get("Elapsed", 0.0), event.get("Package", ""))
    return results


def playwright_results(path):
    data = load_json(path)
    results = {}

    def walk(suite):
        for spec in suite.get("specs", []):
            for test in spec.get("tests", []):
                status = test.get("status") or "unknown"
                results[spec.get("title", "")] = (
                    "pass" if status in ("expected", "passed") else "fail",
                    sum(r.get("duration", 0) for r in test.get("results", [])) / 1000.0,
                    suite.get("title", ""),
                )
        for child in suite.get("suites", []):
            walk(child)

    for suite in data.get("suites", []):
        walk(suite)
    return results


def state_of(identifier):
    for track in ("loki", "behaviours", "translations", "vl"):
        path = os.path.join(ROOT, "state", track, identifier + ".yaml")
        if os.path.exists(path):
            found = re.search(r'^state:\s*(\S+)', read_text(path), re.M)
            return track, (found.group(1) if found else "unknown")
    return None, "unknown"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", action="append", default=[])
    parser.add_argument("--playwright", action="append", default=[])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    recorded = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    claims = declared_items()
    results = {}
    for path in args.go:
        results.update(go_results(path))
    for path in args.playwright:
        results.update(playwright_results(path))

    evidence = {}
    for test, items in claims.items():
        outcome, seconds, package = results.get(test, ("not-run", 0.0, ""))
        for identifier in items:
            evidence.setdefault(identifier, []).append({
                "test": test, "package": package, "outcome": outcome,
                "seconds": round(seconds, 3), "commit": commit, "recorded": recorded,
            })

    os.makedirs(os.path.join(ROOT, "generated"), exist_ok=True)
    dump_json(os.path.join(ROOT, "generated/evidence.json"),
              {"commit": commit, "recorded": recorded, "items": evidence})

    proven = [i for i, runs in evidence.items() if any(r["outcome"] == "pass" for r in runs)]
    failing = [i for i, runs in evidence.items() if any(r["outcome"] == "fail" for r in runs)]
    print(f"evidence: {len(claims)} tests declare {len(evidence)} items; "
          f"{len(proven)} have a passing run, {len(failing)} a failing one")

    problems = []
    for identifier, runs in sorted(evidence.items()):
        _, state = state_of(identifier)
        if any(r["outcome"] == "fail" for r in runs):
            problems.append(f"{identifier}: a test declaring it failed")
        elif state == "proven" and not any(r["outcome"] == "pass" for r in runs):
            problems.append(f"{identifier}: state is proven but no declaring test ran")
    for line in problems:
        print("  " + line)
    if args.check and problems:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
