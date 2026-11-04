"""Unit tests for conformance/scripts/roadmap.py: consumers, cases and tests of each item."""
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "conformance" / "scripts"))
import roadmap  # noqa: E402

BEHAVIOUR = """id: b
title: A behaviour
cases:
  - semantics/written
  - semantics/named-only
perf:
  max_ratio: 2.0
"""
TRANSLATION = """id: t
title: A translation
consumers: [explore, drilldown]
deltas:
  - id: d1
    cases: [semantics/written, 'semantics/other']
  - id: d2
    cases: [semantics/named-only]
"""


class RoadmapTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        (self.root / "behaviours").mkdir()
        (self.root / "translations").mkdir()
        (self.root / "cases/semantics").mkdir(parents=True)
        (self.root / "behaviours/b.yaml").write_text(BEHAVIOUR)
        (self.root / "translations/t.yaml").write_text(TRANSLATION)
        (self.root / "cases/semantics/written.yaml").write_text("id: semantics/written\n")
        self.saved, roadmap.ROOT = roadmap.ROOT, str(self.root)

    def tearDown(self):
        roadmap.ROOT = self.saved

    def test_block_and_inline_lists_are_read(self):
        behaviour = roadmap.collect("behaviours", "behaviours")[0]
        translation = roadmap.collect("translations", "translations")[0]
        self.assertEqual(behaviour["cases"], ["semantics/written", "semantics/named-only"])
        self.assertEqual(behaviour["consumers"], [])
        self.assertEqual(translation["consumers"], ["explore", "drilldown"])
        self.assertEqual(translation["weight"], 6)
        self.assertEqual(translation["cases"], ["semantics/written", "semantics/other", "semantics/named-only"])

    def test_tests_wired_to_an_items_cases_count_for_the_item(self):
        translation = roadmap.collect("translations", "translations")[0]
        wiring = {"item_to_tests": {"t": ["a_test.go"], "semantics/written": ["a_test.go", "b_test.go"],
                                    "semantics/other": ["c.spec.ts"]}}
        self.assertEqual(roadmap.item_tests(wiring, translation), {"a_test.go", "b_test.go", "c.spec.ts"})

    def test_an_item_without_a_state_file_is_unknown(self):
        self.assertEqual(roadmap.collect("behaviours", "behaviours")[0]["state"], "unknown")
        self.assertTrue(roadmap.case_written("semantics/written"))
        self.assertFalse(roadmap.case_written("semantics/named-only"))


if __name__ == "__main__":
    unittest.main()
