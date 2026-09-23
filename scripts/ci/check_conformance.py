#!/usr/bin/env python3
"""CI gate: the registry must stay true to the code, the docs and the evidence.

Fails when:
  * a test claims a registry id that does not exist,
  * a proxy route has no registry item,
  * a registry item points at code that no longer exists,
  * a bench/ab shape covers an unknown registry id, or a saved A/B result does
    not match the shape sets (performance evidence);
  * a generated report is stale (performance, coverage map, gaps, translation map, roadmap,
    compatibility matrix),
  * a flag is missing from the places that list every flag,
  * a score or an item's state regressed against the committed baseline, or a
    waiver expired,
  * evidence contradicts a state: `proven` without a passing declaring test,
    when a test-result file is supplied.

Usage:
  python3 scripts/ci/check_conformance.py [--go-results unit.json ...]
"""
import argparse
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile

SCRIPTS = "conformance/scripts"
REPORTS = (
    # perf_evidence.py runs first: gaps.py reads the evidence it writes.
    ("perf_evidence.py", "conformance/reports/performance.md", None),
    ("coverage_map.py", "conformance/reports/coverage-map.md", "--docs"),
    ("gaps.py", "conformance/reports/gaps.md", None),
    ("translation_map.py", "conformance/reports/translation-map.md", None),
    ("roadmap.py", "conformance/reports/roadmap.md", None),
    ("matrix_link.py", "conformance/reports/compatibility-matrix.md", None),
)


def run(script, *args):
    return subprocess.run([sys.executable, os.path.join(SCRIPTS, script), *args],
                          capture_output=True, text=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--go-results", action="append", default=[])
    args = parser.parse_args()
    failures = []

    wiring = run("wire.py", "--check")
    print(wiring.stdout.strip())
    if wiring.returncode:
        failures.append("registry wiring")

    performance = run("perf_evidence.py", "--check")
    print(performance.stdout.strip())
    if performance.returncode:
        failures.append("performance evidence")

    flags = run("flag_docs.py", "--check")
    print(flags.stdout.strip())
    if flags.returncode:
        failures.append("flag documentation")

    # A generated report must match what the generators produce right now.
    for script, target, docs_flag in REPORTS:
        with tempfile.TemporaryDirectory() as directory:
            backup = os.path.join(directory, os.path.basename(target))
            if os.path.exists(target):
                shutil.copyfile(target, backup)
            result = run(script, *( (docs_flag, target) if docs_flag else () ))
            if result.returncode:
                print(result.stdout.strip() or result.stderr.strip())
                failures.append(f"{script} failed")
                continue
            if os.path.exists(backup) and not filecmp.cmp(backup, target, shallow=False):
                print(f"{target} is stale; run python3 {SCRIPTS}/{script}")
                failures.append(f"{target} stale")

    ratchet = run("ratchet.py", "--check")
    print(ratchet.stdout.strip())
    if ratchet.returncode:
        failures.append("score ratchet")

    if args.go_results:
        evidence_args = []
        for path in args.go_results:
            evidence_args += ["--go", path]
        evidence = run("evidence.py", *evidence_args, "--check")
        print(evidence.stdout.strip())
        if evidence.returncode:
            failures.append("evidence")

    if failures:
        print("conformance gate FAILED: " + ", ".join(failures))
        return 1
    print("conformance gate ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
