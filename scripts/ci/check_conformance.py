#!/usr/bin/env python3
"""CI gate: the registry must stay true to the code, the docs and the evidence.

Fails when:
  * a test claims a registry id that does not exist,
  * a proxy route has no registry item,
  * a registry item points at code that no longer exists,
  * a bench/ab shape covers an unknown registry id, or a saved A/B result does
    not match the shape sets (performance evidence);
  * a registry item a bench/ab shape covers has no valid `perf:` acceptance block (max_ratio at most
    2.0, target_ratio, modes, locked with locked_since, effort_exception with reason and follow_up), an
    item no shape covers has one, or a locked item covers a row on the above-2× list
    (bench/ab/history/over-2x-loki.json); see perf_acceptance_problems();
  * a bench/ab shape outside `control` covers no registry item that names code,
    so no change could select it for the per-PR A/B run (bench/ab/selection.py);
  * a bench/visual entry covers an unknown registry id, or (outside the core
    set) covers nothing that names code, so no change could select it for the
    per-PR visual smoke (bench/visual/plan.py);
  * a generated report is stale (performance, coverage map and its published copy
    docs/compatibility-coverage.md, gaps, translation map, roadmap, compatibility matrix,
    ranked parity gaps) or the "Quality and performance vs Loki" section of README.md and
    docs/real-window-compatibility-gaps.md (between its quality-summary markers),
  * the method-level scope is inconsistent: a write method of an endpoint is neither listed
    in its state's out_of_scope_methods (with the owner decision) nor allowed (delete), or
    an endpoint with every method out of scope is not not-applicable, or a deprecated-read
    exclusion does not name one of the five designated aliases with its reason and owner decision,
  * a cluster of the last differential run (bench/parity) is accounted for by
    no registry case, or a case names a cluster the run does not hold,
  * a flag is missing from the places that list every flag,
  * a score or an item's state regressed against the committed baseline, or a
    waiver expired,
  * evidence contradicts a state: `proven` without a passing declaring test,
    when a test-result file is supplied;
  * the error-layer known-leak list (conformance/registry/error-layer-known-leaks.json) grew, names
    an unknown or fixed case, or disagrees with its ceiling; with test results: the error-layer gate
    (TestErrorLayer_NoVictoriaLogsLeak) did not run, failed, or skipped more cells than the list holds;
    see error_layer_problems().

Usage:
  python3 scripts/ci/check_conformance.py [--go-results unit.json ...]
"""
import argparse
import filecmp
import json
import os
import shutil
import subprocess
import sys
import tempfile

SCRIPTS = "conformance/scripts"
sys.path.insert(0, SCRIPTS)
from registry_io import load_json, perf_problems, scope_problems  # noqa: E402
SHAPES = "bench/ab/shapes.json"
OVER_2X = "bench/ab/history/over-2x-loki.json"
REPORTS = (
    # perf_evidence.py runs first: gaps.py reads the evidence it writes.
    ("perf_evidence.py", "conformance/reports/performance.md", None),
    ("coverage_map.py", "conformance/reports/coverage-map.md", "--docs"),
    # The published copy of the same map (docs site); it went stale unchecked before.
    ("coverage_map.py", "docs/compatibility-coverage.md", "--docs"),
    ("coverage_map.py", "docs/api-reference.md", "--api-status"),
    ("coverage_map.py", "docs/compatibility-loki.md", "--api-status"),
    ("coverage_map.py", "docs/KNOWN_ISSUES.md", "--api-status"),
    ("coverage_map.py", "docs/roadmap.md", "--api-status"),
    ("gaps.py", "conformance/reports/gaps.md", None),
    ("translation_map.py", "conformance/reports/translation-map.md", None),
    ("roadmap.py", "conformance/reports/roadmap.md", None),
    ("matrix_link.py", "conformance/reports/compatibility-matrix.md", None),
    ("parity_gaps.py", "conformance/reports/parity-gaps.md", None),
    # The "Quality and performance vs Loki" section between the quality-summary markers: it reads the registry and
    # the evidence, so it runs after every generator above.
    ("perf_evidence.py", "README.md", "--quality-summary"),
    ("perf_evidence.py", "docs/real-window-compatibility-gaps.md", "--quality-summary"),
)


def perf_acceptance_problems(shapes=SHAPES, over_2x=OVER_2X):
    """The perf: blocks against the shapes that cover registry items and the above-2× list (schema and
    consistency only: measured values are the daily run's and perf-smoke's, never this gate's)."""
    spec = load_json(shapes)
    covers = {(s, sh["name"]): list(sh.get("covers") or []) for s, v in spec["sets"].items() for sh in v["shapes"]}
    rows = load_json(over_2x).get("rows", []) if os.path.exists(over_2x) else []
    return perf_problems({i for ids in covers.values() for i in ids}, rows, covers)


ERROR_LAYER_LEAKS = "conformance/registry/error-layer-known-leaks.json"
ERROR_LAYER_TEST = "TestErrorLayer_NoVictoriaLogsLeak"


def semantic_comparison_problems(matrix_path="test/e2e-compat/query-semantics-matrix.json",
                                 registry_root="conformance/registry"):
    """Availability-only cases must name an accepted difference in the existing registry."""
    from parity_gaps import cases
    registered, _ = cases(registry_root)
    accepted = {case["id"] for case in registered
                if case["status"] in ("owner-kept", "documented", "upstream")}
    problems = []
    for case in load_json(matrix_path)["cases"]:
        reference = case.get("known_deviation")
        if case.get("compare") == "any_non_empty":
            if not isinstance(reference, str) or reference not in accepted:
                problems.append(f"{case['id']}: availability-only comparison needs an existing accepted registry deviation")
        elif reference:
            problems.append(f"{case['id']}: a strict comparison cannot apply known_deviation")
    return problems


def error_layer_problems(go_results=(), leaks_path=ERROR_LAYER_LEAKS, base_text=None):
    """The ratchet of the single Loki-shaped error layer. Every route x failure-mode cell that still leaks
    VictoriaLogs text or a non-Loki status is listed with the registry cases it fails; the Go gate skips
    exactly those and fails on any other. The list may only shrink: its pair count must equal `ceiling`
    (a fix lowers both), never exceed the base branch's count when that is known (base_text, the file at
    the base ref), each case must exist with an open gap, and a supplied go test -json run must show the
    gate ran, passed and skipped no more cells than are listed."""
    problems = []
    doc = load_json(leaks_path)
    leaks = doc.get("leaks", {})
    pairs = sum(len(ids) for ids in leaks.values())
    if pairs != doc.get("ceiling"):
        problems.append(f"{leaks_path}: {pairs} listed leak(s) but ceiling {doc.get('ceiling')}; "
                        "the list may not grow, and a fix lowers the ceiling with it")
    for ident in sorted({i for ids in leaks.values() for i in ids}):
        path = os.path.join("conformance/registry/cases", ident + ".yaml")
        if not os.path.exists(path):
            problems.append(f"{leaks_path}: unknown registry case {ident}")
        else:
            with open(path, encoding="utf-8") as handle:
                case_text = handle.read()
            if "status: open" not in case_text:
                problems.append(f"{leaks_path}: {ident} is listed as leaking but its gap is not open")
    if base_text:
        base = json.loads(base_text).get("leaks", {})
        base_pairs = sum(len(ids) for ids in base.values())
        added = sorted(f"{cell}: {i}" for cell, ids in leaks.items() for i in ids if i not in base.get(cell, []))
        if pairs > base_pairs or added:
            problems.append(f"{leaks_path}: the known-leak list grew against the base ({base_pairs} -> {pairs}); "
                            f"new: {', '.join(added[:10])}")
    if go_results:
        ran, failed, skipped = False, False, 0
        for path in go_results:
            with open(path, encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    if not line.startswith("{"):
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    name = event.get("Test") or ""
                    if name.split("/")[0] != ERROR_LAYER_TEST:
                        continue
                    ran = True
                    if event.get("Action") == "fail":
                        failed = True
                    elif event.get("Action") == "skip" and name.count("/") == 2:
                        skipped += 1
        if not ran:
            problems.append(f"{ERROR_LAYER_TEST} did not run in the supplied test results")
        if failed:
            problems.append(f"{ERROR_LAYER_TEST} failed: a new leak, or a listed one that was fixed")
        if skipped > len(leaks):
            problems.append(f"{ERROR_LAYER_TEST} skipped {skipped} cells, the list holds {len(leaks)}")
    return problems


def base_known_leaks(ref="origin/main", path=ERROR_LAYER_LEAKS):
    """The known-leak list at the base ref, when the checkout has it (a full clone); None otherwise."""
    shown = subprocess.run(["git", "show", f"{ref}:{path}"], capture_output=True, text=True)
    return shown.stdout if shown.returncode == 0 else None


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

    # The per-PR A/B selection is derived from shape covers and registry
    # implementation sites; every shape must stay reachable from code.
    ab_selection = subprocess.run([sys.executable, "bench/ab/selection.py", "--check"], capture_output=True, text=True)
    print(ab_selection.stdout.strip() or ab_selection.stderr.strip())
    if ab_selection.returncode:
        failures.append("A/B shape selection")

    # The same for the visual-smoke entries (bench/visual/spec.json `covers`).
    visual_plan = subprocess.run([sys.executable, "bench/visual/plan.py", "--check"], capture_output=True, text=True)
    print(visual_plan.stdout.strip() or visual_plan.stderr.strip())
    if visual_plan.returncode:
        failures.append("visual-smoke entry selection")

    # Every difference the last differential run found is registered.
    parity = run("parity_gaps.py", "--check")
    print(parity.stdout.strip())
    if parity.returncode:
        failures.append("parity gap registration")

    acceptance = perf_acceptance_problems()
    for problem in acceptance:
        print(f"perf acceptance: {problem}")
    if acceptance:
        failures.append("perf acceptance blocks")
    else:
        print("perf acceptance: every covered registry item has a valid perf: block")

    scope = scope_problems()
    for problem in scope:
        print(f"endpoint scope: {problem}")
    if scope:
        failures.append("endpoint scope")

    semantics = semantic_comparison_problems()
    for problem in semantics:
        print(f"semantic comparison: {problem}")
    if semantics:
        failures.append("semantic comparison ownership")

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
                print(f"{target} is stale; run python3 {SCRIPTS}/{script}" + (f" {docs_flag} {target}" if docs_flag else ""))
                failures.append(f"{target} stale")

    ratchet = run("ratchet.py", "--check")
    print(ratchet.stdout.strip())
    if ratchet.returncode:
        failures.append("score ratchet")

    error_layer = error_layer_problems(args.go_results, base_text=base_known_leaks())
    for problem in error_layer:
        print(f"error layer: {problem}")
    if error_layer:
        failures.append("error-layer leak ratchet")
    else:
        leaks = load_json(ERROR_LAYER_LEAKS).get("leaks", {})
        print(f"error layer: {len(leaks)} known leaking cell(s), the list may only shrink")

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
