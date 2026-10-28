"""The conformance gate diffs every generated report with what its generator renders now. The README.md and docs page
section between the quality-summary markers is one of them."""
import importlib.util
import os
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


if __name__ == "__main__":
    unittest.main()
