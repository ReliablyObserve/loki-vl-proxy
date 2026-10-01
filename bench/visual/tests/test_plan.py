"""Unit tests for bench/visual/plan.py (python3 -m unittest discover -s bench/visual/tests)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import plan  # noqa: E402

SPEC = {
    "ranges": {"15m": 900, "1h": 3600, "6h": 21600, "live": 300},
    "ci_ranges": ["15m", "1h", "6h"],
    "core_range": "1h",
    "pages": [
        {"id": "core-logs", "kind": "explore", "core": True, "covers": ["loki_api_v1_query_range"]},
        {"id": "json-volume", "kind": "explore", "covers": ["parser-json", "loki_api_v1_query_range"]},
        {"id": "labels", "kind": "label-browser", "covers": ["loki_api_v1_labels"]},
        {"id": "tail", "kind": "tail", "covers": ["loki_api_v1_tail"]},
    ],
}
ITEMS = {
    "loki_api_v1_query_range": ({"internal/proxy/proxy.go"}, set()),
    "loki_api_v1_labels": ({"internal/proxy/label_handlers.go"}, set()),
    "loki_api_v1_tail": ({"internal/proxy/tail.go"}, set()),
    "parser-json": ({"internal/proxy/ordered_json_metric.go"}, set()),
}


def run(files, **kw):
    return plan.plan(files, spec=SPEC, items=ITEMS, **kw)


class PlanTest(unittest.TestCase):
    def test_docs_ci_tests_run_nothing(self):
        out = run(["docs/testing.md", ".github/workflows/ci.yaml", "internal/proxy/x_test.go", "CHANGELOG.md"])
        self.assertFalse(out["run"])
        self.assertEqual(out["entries"], {})

    def test_tooling_change_runs_the_core_set_only(self):
        out = run(["bench/visual/compare.py"])
        self.assertTrue(out["run"])
        self.assertEqual({k: v["ranges"] for k, v in out["entries"].items()}, {"core-logs": ["1h"]})
        self.assertEqual(out["captures"], 1)

    def test_code_selects_the_entries_that_cover_it(self):
        out = run(["internal/proxy/ordered_json_metric.go"])
        self.assertEqual(out["entries"]["json-volume"]["ranges"], ["15m", "1h", "6h"])
        self.assertEqual(out["entries"]["core-logs"]["ranges"], ["1h"])  # core stays at one range
        self.assertNotIn("labels", out["entries"])
        self.assertEqual(out["entries"]["json-volume"]["why"], ["internal/proxy/ordered_json_metric.go"])

    def test_endpoint_handler_selects_its_entries_but_query_range_does_not_select_all(self):
        self.assertIn("labels", run(["internal/proxy/label_handlers.go"])["entries"])
        out = run(["internal/proxy/proxy.go"])  # the query_range handler: every page reads it
        self.assertEqual(list(out["entries"]), ["core-logs"])

    def test_tail_entry_is_one_live_capture(self):
        out = run(["internal/proxy/tail.go"])
        self.assertEqual(out["entries"]["tail"]["ranges"], ["live"])
        self.assertEqual(out["captures"], 2)  # core 1h + the live pass

    def test_core_set_is_never_trimmed_and_budget_narrows_then_drops(self):
        out = run(["internal/proxy/ordered_json_metric.go", "internal/proxy/label_handlers.go"], max_captures=4)
        self.assertEqual(out["entries"]["core-logs"]["ranges"], ["1h"])
        self.assertEqual(set(out["trimmed"]), {"json-volume", "labels"})
        self.assertEqual(out["dropped"], [])
        out = run(["internal/proxy/ordered_json_metric.go", "internal/proxy/label_handlers.go"], max_captures=2)
        self.assertEqual(out["dropped"], ["labels"])
        self.assertIn("core-logs", out["entries"])

    def test_check_resolves_covers(self):
        self.assertEqual(plan.check(SPEC, ITEMS), [])
        bad = {**SPEC, "pages": SPEC["pages"] + [{"id": "x", "kind": "explore", "covers": ["nope"]},
                                                   {"id": "y", "kind": "explore", "covers": ["loki_api_v1_query_range"]},
                                                   {"id": "z", "kind": "explore"}]}
        problems = "\n".join(plan.check(bad, ITEMS))
        self.assertIn("x covers unknown registry id nope", problems)
        self.assertIn("y covers no registry item that names code", problems)
        self.assertIn("z has no covers", problems)

    def test_repository_spec_is_consistent(self):
        self.assertEqual(plan.check(), [])


if __name__ == "__main__":
    unittest.main()
