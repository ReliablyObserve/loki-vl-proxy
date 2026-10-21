"""Unit tests for bench/ab/history.py."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import history  # noqa: E402


# The shapes.json fields last_commit reads: the 7d range is never part of a daily run.
SPEC = {"ranges": {"1h": [3600, 5], "24h": [86400, 60], "7d": [604800, 600]},
        "sets": {"control": {"ranges": ["1h", "24h", "7d"]}, "json-filter-pushdown": {"ranges": ["1h"]}}}


def summary(date, main_p50, valid=True, status="200", ranges=("1h",), runs=history.FULL_RUNS):
    return {"set": "control", "label": "daily-control", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": runs, "valid": valid, "restart_before": "0", "restart_after": "0",
            "rows": [{"shape": "logs | json", "range": r, "verdict": "same",
                      "p50": {"release": 0.5, "main": main_p50, "loki": 0.4},
                      "cold": {"release": 1.0, "main": main_p50 * 2, "loki": 0.9},
                      "status": {"release": "200", "main": status, "loki": "200"},
                      "parity_vs_reference": "same"} for r in ranges]}


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_append_is_compact_and_once_per_day(self):
        path, added = history.append(summary("2026-09-20", 0.5), self.dir, "abc")
        self.assertTrue(added)
        _, again = history.append(summary("2026-09-20", 0.9), self.dir, "abc")
        self.assertFalse(again)
        with open(path) as f:
            lines = f.read().splitlines()
        self.assertEqual(len(lines), 1)
        e = json.loads(lines[0])
        self.assertEqual(e["cols"], history.COLS)
        self.assertEqual(e["rows"][0][:5], ["logs | json", "1h", "200", 0.5, 1.0])
        self.assertEqual(e["commit"], "abc")

    def test_trend_day_and_week(self):
        for date, p50 in (("2026-09-10", 1.0), ("2026-09-16", 0.5), ("2026-09-17", 0.52)):
            history.append(summary(date, p50), self.dir)
        text = history.trend(self.dir)
        self.assertIn("## control", text)
        self.assertIn("previous 2026-09-16; week 2026-09-10", text)
        row = [line for line in text.splitlines() if line.startswith("| logs \\| json")][0]
        self.assertIn("⚪ +4%", row)   # day: within noise
        self.assertIn("🟢 -48%", row)  # week: faster
        self.assertIn("1.30×", row)    # 0.52 / 0.4

    def test_trend_skips_invalid_previous_and_handles_errors(self):
        history.append(summary("2026-09-16", 0.5, valid=False), self.dir)
        history.append(summary("2026-09-17", 0.5, status="502"), self.dir)
        text = history.trend(self.dir)
        self.assertIn("previous —", text)
        self.assertIn("**502**", text)

    def test_last_commit_is_the_one_every_set_last_measured(self):
        def day(date, commit, set_name="control", **kw):
            history.append(dict(summary(date, 0.5, **kw), set=set_name), self.dir, commit)

        self.assertEqual(history.last_commit(self.dir, SPEC), "")
        day("2026-09-20", "aaa", ranges=("1h", "24h"))
        day("2026-09-20", "aaa", "json-filter-pushdown")
        self.assertEqual(history.last_commit(self.dir, SPEC), "aaa")
        # A run of one set only (a manual dispatch) leaves the others measured on an older commit.
        day("2026-09-21", "bbb", ranges=("1h", "24h"))
        self.assertEqual(history.last_commit(self.dir, SPEC), "")
        day("2026-09-21", "bbb", "json-filter-pushdown")
        self.assertEqual(history.last_commit(self.dir, SPEC), "bbb")
        # Lines written before commits were recorded say nothing about the code measured.
        day("2026-09-22", "", ranges=("1h", "24h"))
        day("2026-09-22", "", "json-filter-pushdown")
        self.assertEqual(history.last_commit(self.dir, SPEC), "")

    def test_last_commit_ignores_sets_that_no_longer_exist(self):
        history.append(dict(summary("2026-09-20", 0.5, ranges=("1h", "24h")), set="control"), self.dir, "aaa")
        history.append(dict(summary("2026-09-20", 0.5), set="json-filter-pushdown"), self.dir, "aaa")
        # The file of a renamed set keeps its old commit forever.
        history.append(dict(summary("2026-09-01", 0.5), set="old-name"), self.dir, "zzz")
        self.assertEqual(history.last_commit(self.dir, SPEC), "aaa")

    def test_last_commit_needs_a_full_measurement(self):
        def day(date, commit, set_name, **kw):
            history.append(dict(summary(date, 0.5, **kw), set=set_name), self.dir, commit)

        day("2026-09-20", "aaa", "control", ranges=("1h", "24h"))
        day("2026-09-20", "aaa", "json-filter-pushdown")
        # Fewer runs, or a shorter --max-range, is not a measurement of the day.
        day("2026-09-21", "bbb", "control", ranges=("1h", "24h"), runs=3)
        day("2026-09-21", "bbb", "json-filter-pushdown")
        self.assertEqual(history.last_commit(self.dir, SPEC), "")
        day("2026-09-22", "ccc", "control", ranges=("1h",))
        day("2026-09-22", "ccc", "json-filter-pushdown")
        self.assertEqual(history.last_commit(self.dir, SPEC), "")
        # A set that was never fully measured keeps the run on.
        other = tempfile.mkdtemp()
        history.append(dict(summary("2026-09-20", 0.5), set="json-filter-pushdown"), other, "aaa")
        self.assertEqual(history.last_commit(other, SPEC), "")

    def test_spark(self):
        self.assertEqual(history.spark([1, 2, 3]), "▁▄█")
        self.assertEqual(history.spark([1]), "")

    def test_empty(self):
        self.assertIn("No history yet.", history.trend(self.dir))


if __name__ == "__main__":
    unittest.main()
