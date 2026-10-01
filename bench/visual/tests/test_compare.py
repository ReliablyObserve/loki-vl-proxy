"""compare.py on synthetic captures: identical, different, empty, and Loki differences the base has too."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import compare  # noqa: E402


def capture(values, ui=None, settled=True, uid="ds1", fields=None, status=200, error=None):
    frame = {"schema": {"name": "", "fields": fields or [{"type": "time"}, {"type": "number", "labels": {"level": "info"}}]},
             "data": {"values": [list(range(len(values))), values]}}
    result = {"error": error} if error else {"frames": [frame]}
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": status,
           "request": {"from": "1", "to": "2", "requestId": uid + "-1", "queries": [
               {"refId": "A", "expr": "sum(x)", "queryType": "range", "datasource": {"type": "loki", "uid": uid}, "maxLines": 1000}]},
           "response": {"results": {"A": result}}}
    return {"settled": settled, "settle_ms": 1000, "records": [rec], "ui": ui or {"noData": 0, "banners": [], "panelErrors": 0}}


class CompareTest(unittest.TestCase):
    def run_compare(self, main, pr, loki=None):
        with tempfile.TemporaryDirectory() as out:
            d = os.path.join(out, "data", "page-a", "1h")
            os.makedirs(d)
            for name, cap in (("main", main), ("pr", pr), ("loki", loki)):
                if cap is not None:
                    with open(os.path.join(d, f"{name}.json"), "w", encoding="utf-8") as f:
                        json.dump(cap, f)
            proc = subprocess.run([sys.executable, os.path.join(HERE, "..", "compare.py"), out], capture_output=True, text=True)
            with open(os.path.join(out, "compare.json"), encoding="utf-8") as f:
                return proc.returncode, json.load(f)[0]

    def test_identical_counts_points_and_exits_zero(self):
        code, row = self.run_compare(capture([1, 2, 0]), capture([1, 2, 0]), capture([1, 2, 0]))
        self.assertEqual(code, 0)
        self.assertEqual((row["points_main"], row["points_pr"]), (2, 2))  # zero points are not data
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertEqual(row["loki_diffs"], [])

    def test_value_difference_is_reported_and_exits_one(self):
        code, row = self.run_compare(capture([1, 2]), capture([1, 3]), capture([1, 2]))
        self.assertEqual(code, 1)
        self.assertTrue(row["main_pr_diffs"])
        self.assertEqual(len(row["loki_new"]), 1)  # PR differs from Loki where the base does not

    def test_loki_difference_the_base_has_is_not_new(self):
        _, row = self.run_compare(capture([1, 2]), capture([1, 2]), capture([5, 5]))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertEqual(row["loki_main_n"], 1)
        self.assertEqual(row["loki_new"], [])

    def test_ui_state_is_carried_per_side(self):
        bad = {"noData": 1, "banners": ["Plugin unavailable"], "panelErrors": 1}
        _, row = self.run_compare(capture([1]), capture([1], ui=bad))
        self.assertEqual(row["ui_pr"], bad)
        self.assertEqual(row["ui_main"]["noData"], 0)

    def test_datasource_uid_and_request_id_do_not_split_requests(self):
        _, row = self.run_compare(capture([1, 2], uid="vp-main"), capture([1, 2], uid="vp-pr"))
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertEqual(row["main_vs_pr"], "1/1")

    def test_other_request_fields_make_a_different_request(self):
        a, b = capture([1, 2]), capture([1, 2])
        b["records"][0]["request"]["queries"][0]["maxLines"] = 5000
        _, row = self.run_compare(a, b)
        self.assertEqual(len(row["main_pr_diffs"]), 2)  # one request on each side only

    def test_frame_schema_difference_is_a_difference(self):
        other = [{"type": "time"}, {"type": "number", "labels": {"level": "info"}, "config": {"interval": 60000}}]
        _, row = self.run_compare(capture([1, 2]), capture([1, 2], fields=other))
        self.assertIn("schema", row["main_pr_diffs"][0])

    def test_duplicate_answers_pair_by_content_not_arrival_order(self):
        a, b = capture([1, 2]), capture([3, 4])
        a["records"] = a["records"] + b["records"]
        b2, a2 = capture([3, 4]), capture([1, 2])
        b2["records"] = b2["records"] + a2["records"]  # same two answers, other order
        _, row = self.run_compare(a, b2)
        self.assertEqual(row["main_pr_diffs"], [])

    def test_errors_are_listed_per_side_minus_the_allow_list(self):
        bad = capture([1], error="plugin unavailable")
        _, row = self.run_compare(bad, bad)
        self.assertEqual(row["errors_pr"], ["plugin unavailable"])
        self.assertEqual(row["main_pr_diffs"], [])  # equal on both sides, and still reported
        self.assertEqual(compare.errors({"k": [{("A", "error"): ("error", "plugin unavailable")}]}, ["unavailable"]), [])
        _, row = self.run_compare(capture([1]), capture([1], status=500))
        self.assertTrue(row["errors_pr"])

    def test_missing_loki_capture_within_its_window_is_flagged(self):
        _, row = self.run_compare(capture([1]), capture([1]))
        self.assertTrue(row["loki_missing"])
        _, row = self.run_compare(capture([1]), capture([1]), capture([1]))
        self.assertFalse(row["loki_missing"])

    def test_points(self):
        self.assertEqual(compare.points({"k": [{"a": ("metric", {1: 0, 2: 5}, "sig"), "b": ("other", "h", 7), "c": ("error", "x")}]}), 8)


class NondeterministicTest(unittest.TestCase):
    def test_only_patterns_resources_are_history_dependent(self):
        import compare
        self.assertTrue(compare.nondeterministic("resource /api/datasources/uid/*/resources/patterns?x: body differs"))
        self.assertFalse(compare.nondeterministic("query ('A', 'sum(...)'): value differs"))
        self.assertFalse(compare.nondeterministic("resource /api/datasources/uid/*/resources/labels: body differs"))


if __name__ == "__main__":
    unittest.main()
