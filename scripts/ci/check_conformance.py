#!/usr/bin/env python3
"""CI gate: the conformance registry must stay true to the code and the docs.

Fails when:
  * a test claims a registry id that does not exist,
  * a proxy route has no registry item,
  * a registry item points at an implementation site that no longer exists,
  * the generated coverage map in docs/ is out of date.

Usage: python3 scripts/ci/check_conformance.py
"""
import subprocess, sys, tempfile, os, filecmp

def main():
    failures = []
    wiring = subprocess.run([sys.executable, "conformance/scripts/wire.py", "--check"],
                            capture_output=True, text=True)
    print(wiring.stdout.strip())
    if wiring.returncode:
        failures.append("registry wiring")
    with tempfile.TemporaryDirectory() as directory:
        target = os.path.join(directory, "coverage.md")
        regenerate = subprocess.run([sys.executable, "conformance/scripts/coverage_map.py",
                                     "--docs", target], capture_output=True, text=True)
        if regenerate.returncode:
            print(regenerate.stderr.strip())
            failures.append("coverage map generation")
        elif not filecmp.cmp(target, "conformance/reports/coverage-map.md", shallow=False):
            print("docs/compatibility-coverage.md is stale; run "
                  "python3 conformance/scripts/coverage_map.py")
            failures.append("coverage map drift")
    if failures:
        print("conformance gate FAILED: " + ", ".join(failures))
        return 1
    print("conformance gate ok")
    return 0

if __name__ == "__main__":
    sys.exit(main())
