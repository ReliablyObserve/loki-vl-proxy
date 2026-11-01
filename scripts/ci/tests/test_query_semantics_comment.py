"""The temporary catalog comment uses only validated PR snapshots and CI logs."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import query_semantics_comment as comment


class CatalogCommentTests(unittest.TestCase):
    def test_bridge_comment_shows_declared_and_observed_counts(self):
        base = {"quality_inventory": {"query_semantics": {"state": "available", "cases": 67,
            "operations": 24, "depth_paths": 12, "exact_metric_points": 33,
            "performance_cases": 0, "performance_shared": 0}}}
        head = {"quality_inventory": {"query_semantics": {"state": "available", "cases": 80,
            "operations": 25, "depth_paths": 12, "exact_metric_points": 42,
            "performance_cases": 50, "performance_shared": 13}}}
        body = comment.render(base, head, {"passed": 80, "failed": 0, "skipped": 0},
                              "https://github.com/owner/repo/actions/runs/123")
        self.assertIn("| Quality cases | 67 | 80 |", body)
        self.assertIn("| Live query cases | 80 | 0 | 0 |", body)
        self.assertIn("13", body)
        self.assertIn("Pinned Loki CI run", body)
        base["quality_inventory"]["query_semantics"]["performance_cases"] = 50
        self.assertEqual(comment.render(base, head, {}, "—"), "")

    def test_pinned_result_needs_a_source_verified_log(self):
        class API:
            repository = "owner/repo"

            def pages(self, path, field=None, max_pages=10):
                if path.startswith("actions/workflows/"):
                    return [{"id": 123, "head_sha": "a" * 40, "status": "completed", "run_attempt": 1}]
                return [{"id": 456, "name": "loki-pinned", "status": "completed"}]

            def request(self, path, binary=False):
                return ("[command]/usr/bin/git log -1 --format=%H\n" + "a" * 40 +
                        "\n--- PASS: TestQuerySemanticsMatrix/json (0.01s)\n").encode()

        observed, url = comment.pinned_result(API(), "a" * 40, None)
        self.assertEqual(observed["passed"], 1)
        self.assertEqual(observed["failed"], 0)
        self.assertEqual(url, "https://github.com/owner/repo/actions/runs/123")
        observed, _ = comment.pinned_result(API(), "x" * 40, None)
        self.assertEqual(observed, {})


if __name__ == "__main__":
    unittest.main()
