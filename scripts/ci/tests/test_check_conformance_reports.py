"""The conformance gate diffs every generated report with what its generator renders now. The README.md and docs page
section between the quality-summary markers is one of them."""
import importlib.util
import json
import os
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


if __name__ == "__main__":
    unittest.main()
