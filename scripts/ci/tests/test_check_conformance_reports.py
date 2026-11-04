"""The conformance gate diffs every generated report with what its generator renders now. The README.md and docs page
section between the quality-summary markers is one of them."""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]


def load_gate():
    """scripts/ci/check_conformance.py imported with the repository root as the working directory."""
    cwd = os.getcwd()
    os.chdir(REPO)
    try:
        spec = importlib.util.spec_from_file_location("check_conformance_under_test", REPO / "scripts/ci/check_conformance.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        os.chdir(cwd)


class QualitySummaryReportTest(unittest.TestCase):
    def test_both_published_copies_are_checked_after_the_generators_they_read(self):
        reports = load_gate().REPORTS
        entries = [(script, target, flag) for script, target, flag in reports if flag == "--quality-summary"]
        self.assertEqual(entries, [("perf_evidence.py", "README.md", "--quality-summary"),
                                   ("perf_evidence.py", "docs/real-window-compatibility-gaps.md", "--quality-summary")])
        order = [target for _, target, _ in reports]
        self.assertGreater(order.index("README.md"), order.index("conformance/reports/parity-gaps.md"))


class SemanticComparisonOwnershipTest(unittest.TestCase):
    def check_case(self, comparison="any_non_empty", reference="parsed-identity/dynamic-key-parsers", state="owner-kept"):
        gate = load_gate()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            matrix = root / "matrix.json"
            case = {"id": "fixture", "compare": comparison}
            if reference is not None:
                case["known_deviation"] = reference
            matrix.write_text(json.dumps({"cases": [case]}))
            registry = root / "registry"
            definition = registry / "cases/parsed-identity/dynamic-key-parsers.yaml"
            definition.parent.mkdir(parents=True)
            definition.write_text(f"id: parsed-identity/dynamic-key-parsers\ntitle: Existing deviation\ngap:\n  status: {state}\n")
            return gate.semantic_comparison_problems(str(matrix), str(registry))

    def test_availability_contract_requires_existing_accepted_ownership(self):
        for state in ("owner-kept", "documented", "upstream"):
            with self.subTest(state=state):
                self.assertEqual([], self.check_case(state=state))

    def test_unknown_and_open_cases_cannot_hide_a_value_gap(self):
        self.assertTrue(self.check_case(reference="parsed-identity/missing"))
        self.assertTrue(self.check_case(reference="../../other"))
        self.assertTrue(self.check_case(state="open"))
        self.assertTrue(self.check_case(state="fixed"))

    def test_availability_without_a_reference_is_rejected(self):
        self.assertTrue(self.check_case(reference=None))

    def test_strict_comparison_cannot_silently_apply_a_deviation(self):
        self.assertTrue(self.check_case(comparison="metric_points"))
        self.assertEqual([], self.check_case(comparison="metric_points", reference=None))


class ErrorLayerRatchetTest(unittest.TestCase):
    """The known-leak list of the error-layer gate may only shrink, and the gate must have run."""

    CASE = "semantics/error-layer-backend-text"

    def check(self, doc, go_lines=None, base=None):
        gate = load_gate()
        cwd = os.getcwd()
        os.chdir(REPO)
        try:
            with tempfile.TemporaryDirectory() as directory:
                leaks = os.path.join(directory, "leaks.json")
                with open(leaks, "w", encoding="utf-8") as handle:
                    json.dump(doc, handle)
                results = []
                if go_lines is not None:
                    results = [os.path.join(directory, "unit.json")]
                    with open(results[0], "w", encoding="utf-8") as handle:
                        handle.write("\n".join(json.dumps(line) for line in go_lines))
                return gate.error_layer_problems(results, leaks_path=leaks, base_text=json.dumps(base) if base else None)
        finally:
            os.chdir(cwd)

    def doc(self, *cells):
        return {"ceiling": len(cells), "leaks": {cell: [self.CASE] for cell in cells}}

    def test_a_consistent_list_passes(self):
        self.assertEqual(self.check(self.doc("labels vl500")), [])

    def test_ceiling_must_match_the_list(self):
        doc = self.doc("labels vl500")
        doc["ceiling"] = 2
        self.assertTrue(self.check(doc))

    def test_unknown_case_fails(self):
        self.assertTrue(self.check({"ceiling": 1, "leaks": {"labels vl500": ["semantics/no-such-case"]}}))

    def test_growth_against_the_base_fails(self):
        self.assertTrue(self.check(self.doc("labels vl500", "series vl500"), base=self.doc("labels vl500")))
        self.assertEqual(self.check(self.doc("labels vl500"), base=self.doc("labels vl500", "series vl500")), [])

    def test_go_results_must_show_the_gate(self):
        name = "TestErrorLayer_NoVictoriaLogsLeak"
        ran = [{"Action": "skip", "Test": name + "/labels/vl500"}, {"Action": "pass", "Test": name}]
        self.assertEqual(self.check(self.doc("labels vl500"), go_lines=ran), [])
        self.assertTrue(self.check(self.doc("labels vl500"), go_lines=[{"Action": "pass", "Test": "TestOther"}]))
        self.assertTrue(self.check(self.doc("labels vl500"), go_lines=ran + [{"Action": "fail", "Test": name + "/series/vl500"}]))
        extra = ran + [{"Action": "skip", "Test": name + "/series/vl500"}]
        self.assertTrue(self.check(self.doc("labels vl500"), go_lines=extra))


class ProfileDriftRatchetTest(unittest.TestCase):
    """The known-drift list of the Grafana and Logs Drilldown profile labels may only shrink."""

    def check(self, doc, base=None):
        gate = load_gate()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "drift.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(doc, handle)
            return gate.profile_drift_problems(path, json.dumps(base) if base else None)

    def doc(self, *ids):
        return {"ceiling": len(ids), "drift": {i: {"reason": "why"} for i in ids}}

    def test_a_consistent_list_passes(self):
        self.assertEqual(self.check(self.doc("datasource:a", "runtime:b")), [])
        self.assertEqual(self.check(self.doc()), [])

    def test_ceiling_must_match_the_list(self):
        doc = self.doc("datasource:a")
        doc["ceiling"] = 2
        self.assertTrue(self.check(doc))

    def test_an_entry_needs_a_reason(self):
        doc = self.doc("datasource:a")
        doc["drift"]["datasource:a"]["reason"] = " "
        self.assertTrue(self.check(doc))

    def test_growth_against_the_base_fails_and_shrinking_passes(self):
        self.assertTrue(self.check(self.doc("datasource:a", "drilldown:b"), base=self.doc("datasource:a")))
        self.assertTrue(self.check(self.doc("drilldown:b"), base=self.doc("datasource:a")))
        self.assertEqual(self.check(self.doc("datasource:a"), base=self.doc("datasource:a", "drilldown:b")), [])

    def test_the_committed_list_is_consistent(self):
        gate = load_gate()
        cwd = os.getcwd()
        os.chdir(REPO)
        try:
            self.assertEqual(gate.profile_drift_problems(), [])
        finally:
            os.chdir(cwd)


class ExecutedCombinationEvidenceTest(unittest.TestCase):
    """Exercise the existing collector, including its gate and retained JSON."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "go.mod").write_text("module example.com/proxy\n\ngo 1.27.3\n")

    def claim(self, package="internal/one", identifier="contract", proven=False):
        source = self.root / package / "matrix_test.go"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"package matrix\n// conformance: {identifier}\nfunc TestMatrix(t *testing.T) {{}}\n")
        if proven:
            state = self.root / "conformance/registry/state/behaviours" / (identifier + ".yaml")
            state.parent.mkdir(parents=True, exist_ok=True)
            state.write_text(f"id: {identifier}\nstate: proven\n")

    def event(self, test="TestMatrix", action="pass", package="internal/one", seconds=1.25):
        return {"Action": action, "Package": "example.com/proxy/" + package,
                "Test": test, "Elapsed": seconds}

    def collect(self, *logs, playwright=None):
        args = [sys.executable, str(REPO / "conformance/scripts/evidence.py"), "--check"]
        for index, events in enumerate(logs):
            path = self.root / f"events-{index}.json"
            path.write_text("not JSON\n" + "\n".join(json.dumps(e) for e in events) + "\n{broken\n")
            args += ["--go", str(path)]
        if playwright is not None:
            path = self.root / "playwright.json"
            path.write_text(json.dumps(playwright))
            args += ["--playwright", str(path)]
        result = subprocess.run(args, cwd=self.root, capture_output=True, text=True)
        output = self.root / "conformance/registry/generated/evidence.json"
        self.assertTrue(output.exists(), result.stderr)
        return result.returncode, json.loads(output.read_text())["items"]

    def test_retains_full_combination_paths_without_replacing_parent_summary(self):
        self.claim(proven=True)
        status, items = self.collect([
            self.event("TestMatrix/range/json/filtered", seconds=0.1),
            self.event("TestMatrix/range/logfmt/unfiltered", "skip", seconds=0.2),
            self.event(seconds=2.5),
        ])
        self.assertEqual(status, 0)
        row, = items["contract"]
        self.assertEqual((row["test"], row["package"], row["outcome"], row["seconds"]),
                         ("TestMatrix", "example.com/proxy/internal/one", "pass", 2.5))
        self.assertTrue(row["recorded"])
        self.assertIn("commit", row)
        self.assertEqual(row["subtests"], [
            {"test": "TestMatrix/range/json/filtered", "outcome": "pass", "seconds": 0.1},
            {"test": "TestMatrix/range/logfmt/unfiltered", "outcome": "skip", "seconds": 0.2},
        ])

    def test_same_test_name_in_other_package_cannot_supply_proof(self):
        self.claim(proven=True)
        status, items = self.collect([self.event(package="internal/two")])
        self.assertEqual(status, 1)
        self.assertEqual(items["contract"][0]["outcome"], "not-run")

    def test_same_named_tests_keep_separate_package_contracts(self):
        self.claim(identifier="one", proven=True)
        self.claim("internal/two", "two", proven=True)
        status, items = self.collect([self.event(), self.event(action="skip", package="internal/two")])
        self.assertEqual(status, 1)
        self.assertEqual(items["one"][0]["outcome"], "pass")
        self.assertEqual(items["two"][0]["outcome"], "skip")

    def test_failure_survives_a_later_success_in_another_input_file(self):
        self.claim(proven=True)
        status, items = self.collect([
            self.event("TestMatrix/range/json", "fail"), self.event(action="fail")
        ], [self.event("TestMatrix/range/json"), self.event()])
        self.assertEqual(status, 1)
        self.assertEqual(items["contract"][0]["outcome"], "fail")
        self.assertEqual(items["contract"][0]["subtests"][0]["outcome"], "fail")

    def test_child_failure_cannot_be_hidden_by_a_parent_pass(self):
        self.claim(proven=True)
        status, items = self.collect([self.event("TestMatrix/json", "fail"), self.event()])
        self.assertEqual(status, 1)
        self.assertEqual(items["contract"][0]["outcome"], "fail")

    def test_child_pass_without_terminal_parent_is_incomplete(self):
        self.claim(proven=True)
        status, items = self.collect([self.event("TestMatrix/json")])
        self.assertEqual(status, 1)
        self.assertEqual(items["contract"][0]["outcome"], "incomplete")

    def test_skipped_child_without_terminal_parent_is_not_passing(self):
        self.claim(proven=True)
        status, items = self.collect([self.event("TestMatrix/json", "skip")])
        self.assertEqual(status, 1)
        self.assertEqual(items["contract"][0]["outcome"], "incomplete")

    def test_later_skip_cannot_erase_passing_execution(self):
        self.claim(proven=True)
        status, items = self.collect([self.event()], [self.event(action="skip")])
        self.assertEqual(status, 0)
        self.assertEqual(items["contract"][0]["outcome"], "pass")

    def test_plain_run_can_prove_a_test_skipped_under_race(self):
        self.claim(proven=True)
        status, items = self.collect([self.event(action="skip", seconds=0)], [self.event(seconds=3)])
        self.assertEqual(status, 0)
        self.assertEqual((items["contract"][0]["outcome"], items["contract"][0]["seconds"]), ("pass", 3))

    def test_command_package_uses_the_existing_test_directory_scope(self):
        self.claim("cmd/proxy", proven=True)
        status, items = self.collect([self.event(package="cmd/proxy")])
        self.assertEqual(status, 0)
        self.assertEqual(items["contract"][0]["outcome"], "pass")

    def test_playwright_evidence_keeps_its_existing_shape(self):
        source = self.root / "test/ui/example.spec.ts"
        source.parent.mkdir(parents=True)
        source.write_text("test('fields @cov:ui-contract', async () => {});\n")
        status, items = self.collect(playwright={"suites": [{"title": "example.spec.ts", "specs": [{
            "title": "fields @cov:ui-contract", "tests": [{"status": "expected", "results": [{"duration": 50}]}]
        }]}]})
        self.assertEqual(status, 0)
        row, = items["ui-contract"]
        self.assertEqual((row["test"], row["package"], row["outcome"], row["seconds"]),
                         ("fields @cov:ui-contract", "example.spec.ts", "pass", 0.05))



def load_script(name):
    """A conformance script imported by path, with its directory on sys.path as when it runs as a script."""
    path = REPO / "conformance/scripts" / name
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location(name[:-3] + "_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(path.parent))


class MatrixAndCensusReportsTest(unittest.TestCase):
    def test_census_and_matrix_link_outputs_are_staleness_checked(self):
        targets = [target for _, target, _ in load_gate().REPORTS]
        for target in ("conformance/reports/compatibility-matrix.md", "conformance/registry/generated/matrix-link.json",
                       "conformance/reports/census.md", "conformance/registry/generated/census.json"):
            self.assertIn(target, targets)


class VictoriaLogsSurfaceReportsTest(unittest.TestCase):
    def test_surface_usage_and_links_are_checked_before_gaps(self):
        reports = load_gate().REPORTS
        order = [target for _, target, _ in reports]
        self.assertIn(("sync_vl_surface.py", "conformance/registry/generated/vl/logsql-surface.json", "--offline"), reports)
        self.assertLess(order.index("conformance/registry/generated/vl/logsql-surface.json"),
                        order.index("conformance/registry/generated/ast-links.json"))
        self.assertLess(order.index("conformance/registry/generated/ast-links.json"),
                        order.index("conformance/reports/gaps.md"))


class MatrixLiveProofTest(unittest.TestCase):
    """The live-proof line states what was recorded against what the registry defines, and who runs it."""

    def test_counts_blocked_cases_against_the_cases_defined(self):
        module = load_script("matrix_link.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "generated").mkdir()
            (root / "cases/semantics").mkdir(parents=True)
            for name in ("a", "b", "c"):
                (root / f"cases/semantics/{name}.yaml").write_text(f"id: semantics/{name}\n")
            (root / "generated/live-evidence.json").write_text(json.dumps({"recorded": "2026-09-22", "cases": {
                "semantics/a": {"verdict": "blocked"}, "semantics/b": {"verdict": "holds"}}}))
            self.assertEqual(module.live_counts(str(root)), (2, 1, 3))

    def test_names_the_workflows_that_run_it(self):
        module = load_script("matrix_link.py")
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "a.yaml").write_text("run: python3 conformance/scripts/live_proof.py --loki x\n")
            Path(directory, "b.yml").write_text("run: echo\n")
            self.assertEqual(module.live_proof_workflows(directory), ["a.yaml"])
            self.assertEqual(module.live_proof_workflows(str(Path(directory, "missing"))), [])


class MatrixLinkCheckTest(unittest.TestCase):
    """matrix_link.py --check is a gate: a pinned version the matrix does not run, or a missing score test, fails."""

    def check(self, matrix):
        module = load_script("matrix_link.py")
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            path.write_text(json.dumps(matrix))
            module.MATRIX = str(path)
            module.ROOT = str(Path(directory) / "registry")
            (Path(module.ROOT) / "generated").mkdir(parents=True)
            os.chdir(directory)
            old_argv, sys.argv = sys.argv, ["matrix_link.py", "--check"]
            try:
                return module.main()
            finally:
                sys.argv = old_argv
                os.chdir(cwd)

    def test_pinned_version_must_be_in_matrix_versions(self):
        ok = {"tracks": {}, "stack": {"loki": {"pinned_version": "3.7.9", "matrix_versions": ["3.7.8", "3.7.9"]}}}
        self.assertEqual(0, self.check(ok))
        bad = {"tracks": {}, "stack": {"loki": {"pinned_version": "3.7.9", "matrix_versions": ["3.7.8"]}}}
        self.assertEqual(1, self.check(bad))

    def test_a_track_score_test_must_exist(self):
        bad = {"tracks": {"loki": {"workflow": "w", "score_test": "TestNoSuchScoreTestAnywhere"}}, "stack": {}}
        self.assertEqual(1, self.check(bad))


class CensusDiffTest(unittest.TestCase):
    def diff(self, old, new):
        module = load_script("census.py")
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, doc in (("old.json", old), ("new.json", new)):
                paths.append(str(Path(directory) / name))
                Path(paths[-1]).write_text(json.dumps(doc))
            return module.diff(*paths)

    def test_a_cell_that_disappears_fails(self):
        self.assertEqual(1, self.diff({"cases": ["a | compare:exact", "b | compare:exact"]}, {"cases": ["a | compare:exact"]}))

    def test_a_cell_that_moves_under_another_id_is_listed_and_still_fails_until_mapped(self):
        self.assertEqual(1, self.diff({"shapes": ["s/A"]}, {"shapes": ["s/B"]}))

    def test_added_cells_pass(self):
        self.assertEqual(0, self.diff({"cases": ["a | compare:exact"]},
                                      {"cases": ["a | compare:exact", "b | compare:exact"]}))

    def test_a_growing_count_is_information_not_a_failure(self):
        self.assertEqual(0, self.diff({"counts": {"claiming_tests": {"go": 264}}, "claim_cells": ["t_test.go | x"]},
                                      {"counts": {"claiming_tests": {"go": 265}}, "claim_cells": ["t_test.go | x", "u_test.go | y"]}))

    def test_a_claim_dropped_from_a_file_that_keeps_others_fails(self):
        self.assertEqual(1, self.diff({"claim_cells": ["t_test.go | x", "t_test.go | y"]}, {"claim_cells": ["t_test.go | x"]}))


if __name__ == "__main__":
    unittest.main()
