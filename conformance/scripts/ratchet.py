#!/usr/bin/env python3
"""Scores may go up, never down.

Reads the generated summary and compares it with the committed baseline in
conformance/registry/baseline.json. A drop in any score, a `proven` item that
falls back, or an expired waiver fails the check. `--accept` writes the current
numbers as the new baseline, which is how an improvement is recorded.

Usage:
  python3 conformance/scripts/ratchet.py [--check] [--accept]
"""
import argparse
import datetime
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import dump_json, load_json, read_text  # noqa: E402

ROOT = "conformance/registry"
BASELINE = os.path.join(ROOT, "baseline.json")
SUMMARY = os.path.join(ROOT, "generated/summary.json")


def states():
    found = {}
    base = os.path.join(ROOT, "state")
    for current, _, files in os.walk(base):
        for name in sorted(files):
            if not name.endswith(".yaml"):
                continue
            text = read_text(os.path.join(current, name))
            state = re.search(r'^state:\s*(\S+)', text, re.M)
            found[name[:-5]] = state.group(1) if state else "unknown"
    return found


def expired_waivers():
    today = datetime.date.today().isoformat()
    stale = []
    base = os.path.join(ROOT, "state")
    for current, _, files in os.walk(base):
        for name in sorted(files):
            if not name.endswith(".yaml"):
                continue
            text = read_text(os.path.join(current, name))
            expiry = re.search(r'^\s+expiry:\s*(\d{4}-\d{2}-\d{2})', text, re.M)
            if expiry and expiry.group(1) < today:
                stale.append((name[:-5], expiry.group(1)))
    return stale


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--accept", action="store_true")
    args = parser.parse_args()
    if not os.path.exists(SUMMARY):
        print("no summary; run conformance/scripts/coverage_map.py first")
        return 1
    summary = load_json(SUMMARY)
    current = {"scores": summary["scores"], "proven": summary["state"].get("proven", 0),
               "endpoints": summary["endpoints"]}
    current["states"] = states()

    if args.accept or not os.path.exists(BASELINE):
        dump_json(BASELINE, current)
        print(f"baseline recorded: coverage {current['scores']['coverage']:.0%}, "
              f"{current['proven']} proven items")
        return 0

    baseline = load_json(BASELINE)
    problems = []
    for name, value in current["scores"].items():
        was = baseline["scores"].get(name, 0)
        if value + 1e-9 < was:
            problems.append(f"score {name} fell from {was:.0%} to {value:.0%}")
    if current["proven"] < baseline.get("proven", 0):
        problems.append(f"proven items fell from {baseline['proven']} to {current['proven']}")
    rank = {"proven": 3, "partial": 2, "waived": 2, "gap": 1, "not-applicable": 3, "unknown": 0}
    for identifier, was in baseline.get("states", {}).items():
        now = current["states"].get(identifier, "unknown")
        if rank.get(now, 0) < rank.get(was, 0):
            problems.append(f"{identifier}: {was} -> {now}")
    for identifier, expiry in expired_waivers():
        problems.append(f"{identifier}: waiver expired on {expiry}; renew it or close the gap")

    print(f"ratchet: coverage {current['scores']['coverage']:.0%} "
          f"(baseline {baseline['scores'].get('coverage', 0):.0%}), "
          f"{current['proven']} proven (baseline {baseline.get('proven', 0)})")
    for line in problems:
        print("  " + line)
    if args.check and problems:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
