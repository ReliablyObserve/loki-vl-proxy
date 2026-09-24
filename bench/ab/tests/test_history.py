"""Unit tests for bench/ab/history.py."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import history  # noqa: E402


def summary(date, main_p50, valid=True, status="200"):
    return {"set": "control", "label": "daily-control", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": 7, "valid": valid, "restart_before": "0", "restart_after": "0",
            "rows": [{"shape": "logs | json", "range": "1h", "verdict": "same",
                      "p50": {"release": 0.5, "main": main_p50, "loki": 0.4},
                      "cold": {"release": 1.0, "main": main_p50 * 2, "loki": 0.9},
                      "status": {"release": "200", "main": status, "loki": "200"},
                      "parity_vs_reference": "same"}]}


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

    def test_spark(self):
        self.assertEqual(history.spark([1, 2, 3]), "▁▄█")
        self.assertEqual(history.spark([1]), "")

    def test_empty(self):
        self.assertIn("No history yet.", history.trend(self.dir))


if __name__ == "__main__":
    unittest.main()
