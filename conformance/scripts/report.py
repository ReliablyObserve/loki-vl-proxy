#!/usr/bin/env python3
"""Print the conformance registry state: coverage, implementation and evidence."""
import os, re, sys
from registry_io import load_json, read_text

ROOT = "conformance/registry"


def read_state(track, identifier):
    path = os.path.join(ROOT, "state", track, identifier + ".yaml")
    if not os.path.exists(path):
        return "unknown"
    match = re.search(r'^state:\s*(\S+)', read_text(path), re.M)
    return match.group(1) if match else "unknown"


def main():
    coverage = load_json(os.path.join(ROOT, "generated/proxy/coverage.json"))
    implementation = {entry["id"]: entry for entry in
                      load_json(os.path.join(ROOT, "generated/proxy/implementation.json"))["endpoints"]}
    rows, states, executions = [], {}, {}
    for endpoint in coverage["endpoints"]:
        state = read_state("loki", endpoint["id"])
        impl = implementation.get(endpoint["id"], {})
        execution = impl.get("execution", "not_implemented")
        states[state] = states.get(state, 0) + 1
        executions[execution] = executions.get(execution, 0) + 1
        rows.append((endpoint["id"], state, execution,
                     sum(len(v) for v in endpoint["tests"].values()),
                     sum(len(v) for v in endpoint["differential_tests"].values())))
    width = max(len(row[0]) for row in rows)
    print(f"{'endpoint'.ljust(width)}  {'state':8} {'execution':14} {'tests':>5} {'vs loki':>7}")
    for row in sorted(rows):
        print(f"{row[0].ljust(width)}  {row[1]:8} {row[2]:14} {row[3]:5} {row[4]:7}")
    total = len(rows)
    proven = states.get("proven", 0)
    print(f"\nLoki endpoints: {total}")
    print("state:      " + ", ".join(f"{k}={v}" for k, v in sorted(states.items())))
    print("execution:  " + ", ".join(f"{k}={v}" for k, v in sorted(executions.items())))
    print(f"proven share: {proven}/{total} = {proven / total:.0%} "
          "(seeded from repository evidence; the differential runner replaces this)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
