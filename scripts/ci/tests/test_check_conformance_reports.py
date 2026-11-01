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


if __name__ == "__main__":
    unittest.main()
