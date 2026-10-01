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


def capture(values, ui=None, settled=True):
    frame = {"schema": {"name": "", "fields": [{"type": "time"}, {"type": "number", "labels": {"level": "info"}}]},
             "data": {"values": [list(range(len(values))), values]}}
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": 200,
           "request": {"from": "1", "to": "2", "queries": [{"refId": "A", "expr": "sum(x)", "queryType": "range"}]},
           "response": {"results": {"A": {"frames": [frame]}}}}
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

    def test_points(self):
        self.assertEqual(compare.points({"k": [{"a": ("metric", {1: 0, 2: 5}), "b": ("other", "h", 7), "c": ("error", "x")}]}), 8)


if __name__ == "__main__":
    unittest.main()
