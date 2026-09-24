"""Unit tests for bench/ab/selection.py (python3 -m unittest discover -s bench/ab/tests)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import selection  # noqa: E402

SPEC = {
    "ranges": {"1h": [3600, 5]},
    "sets": {
        "control": {"ranges": ["1h"], "shapes": [
            {"name": "logs plain", "query": "{a=\"b\"}", "covers": ["loki_api_v1_query_range"], "smoke": True},
            {"name": "json volume", "query": "x", "covers": ["parser-json", "loki_api_v1_query_range"]},
            {"name": "rate", "query": "y", "covers": ["range_function-rate"]},
        ]},
        "feature": {"ranges": ["1h"], "shapes": [
            {"name": "A, with comma", "query": "z", "covers": ["semantics/case-a", "loki_api_v1_query_range"]},
        ]},
    },
}
ITEMS = {
    "loki_api_v1_query_range": ({"internal/proxy/proxy.go"}, set()),
    "parser-json": ({"internal/proxy/ordered_json_metric.go", "internal/translator/translator.go"}, set()),
    "range_function-rate": ({"internal/proxy/metric_agg.go"}, set()),
    # A case names no code; it proves a behaviour that does.
    "semantics/case-a": (set(), {"behaviour-b", "loki_api_v1_query_range"}),
    "behaviour-b": ({"internal/proxy/ordered_json_metric.go"}, set()),
}


def pick(files):
    return selection.select(files, spec=SPEC, items=ITEMS)


class SelectTest(unittest.TestCase):
    def test_docs_ci_tests_chart_select_nothing(self):
        result = pick(["docs/a.md", ".github/workflows/ci.yaml", "internal/proxy/x_test.go",
                       "charts/loki-vl-proxy/values.yaml", "conformance/registry/cases/x.yaml", "CHANGELOG.md",
                       "Dockerfile", "bench/ab/results/2026-09-24-x.json"])
        self.assertFalse(result["run"])
        self.assertEqual(result["sets"], {})
        self.assertEqual(len(result["ignored"]), 8)

    def test_code_file_selects_shapes_through_registry_and_one_hop(self):
        result = pick(["internal/proxy/ordered_json_metric.go"])
        self.assertTrue(result["run"])
        self.assertEqual(result["sets"]["feature"], ["A, with comma"])  # via case -> behaviour
        self.assertIn("json volume", result["sets"]["control"])
        self.assertIn("logs plain", result["sets"]["control"])  # smoke always rides along
        self.assertNotIn("rate", result["sets"]["control"])
        self.assertIn("internal/proxy/ordered_json_metric.go", result["why"]["feature/A, with comma"])

    def test_endpoint_handler_selects_only_smoke(self):
        result = pick(["internal/proxy/proxy.go"])
        self.assertEqual(result["sets"], {"control": ["logs plain"]})
        self.assertIn("internal/proxy/proxy.go (endpoint handler)", result["why"]["control/logs plain"])

    def test_unmapped_code_file_runs_smoke(self):
        result = pick(["internal/cache/cache.go"])
        self.assertEqual(result["sets"], {"control": ["logs plain"]})

    def test_runtime_files_select_whole_control_set(self):
        for path in ("cmd/proxy/main.go", "go.mod", "go.sum"):
            result = pick([path])
            self.assertEqual(result["sets"], {"control": ["logs plain", "json volume", "rate"]}, path)

    def test_harness_change_runs_smoke(self):
        for path in ("bench/ab/perf_matrix.py", "test/e2e-compat/log-generator.py", ".github/workflows/perf-ab.yaml"):
            self.assertEqual(pick([path])["sets"], {"control": ["logs plain"]}, path)

    def test_bench_tests_are_not_harness(self):
        self.assertFalse(pick(["bench/ab/tests/test_selection.py"])["run"])

    def test_link_tokens_inline_and_block(self):
        text = "id: x\nproves:\n  - a-b\n  - c/d  # note\ntitle: t\ncovers:\n  endpoints: [e1, e2]\n  logql: []\nwhy: |\n  - not-a-link\n"
        self.assertEqual(selection.link_tokens(text), {"a-b", "c/d", "e1", "e2"})


class CheckTest(unittest.TestCase):
    def test_unreachable_non_control_shape_fails(self):
        spec = {"ranges": SPEC["ranges"], "sets": {
            "control": SPEC["sets"]["control"],
            "feature": {"ranges": ["1h"], "shapes": [{"name": "orphan", "query": "q", "covers": ["loki_api_v1_query_range"]}]},
        }}
        problems = selection.check(spec, ITEMS)
        self.assertEqual(len(problems), 1)
        self.assertIn("feature/orphan", problems[0])

    def test_smoke_required_and_only_in_control(self):
        spec = {"ranges": SPEC["ranges"], "sets": {
            "control": {"ranges": ["1h"], "shapes": [{"name": "c", "query": "q", "covers": ["parser-json"]}]},
            "feature": {"ranges": ["1h"], "shapes": [{"name": "f", "query": "q", "covers": ["parser-json"], "smoke": True}]},
        }}
        problems = selection.check(spec, ITEMS)
        self.assertEqual(len(problems), 2)

    def test_repository_shapes_are_reachable(self):
        self.assertEqual(selection.check(), [])

    def test_repository_json_metric_change_selects_json_sets(self):
        result = selection.select(["internal/proxy/ordered_json_metric.go"])
        self.assertIn("json-filter-pushdown", result["sets"])
        self.assertTrue(any(s.get("smoke") for s in selection.load_shapes()["sets"]["control"]["shapes"]))


if __name__ == "__main__":
    unittest.main()
