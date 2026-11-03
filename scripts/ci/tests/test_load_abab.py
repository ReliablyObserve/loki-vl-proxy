"""load_abab.py: base and head alternate on one machine and their medians land in the quality JSON."""
import json
import tempfile
import unittest
from pathlib import Path

from scripts.ci import load_abab

LOG = """=== RUN   TestLoad_HighConcurrency_MemoryStability
    perf_test.go:710:   Total requests: 100000
    perf_test.go:712:   Throughput: 487015 req/s
    perf_test.go:715:   Memory growth: 3.2 MB (delta: 1.0 MB, before: 5.0 MB, after: 8.2 MB)
--- PASS: TestLoad_HighConcurrency_MemoryStability (0.21s)
"""


class LoadAbabTests(unittest.TestCase):
    def test_a_run_is_parsed(self):
        self.assertEqual(load_abab.parse_run(LOG), (487015.0, 3.2))

    def test_a_skipped_or_missing_test_is_not_parsed(self):
        self.assertIsNone(load_abab.parse_run("testing: warning: no tests to run\nPASS\n"))
        self.assertIsNone(load_abab.parse_run("--- SKIP: TestLoad_HighConcurrency_MemoryStability (0.00s)\n"))

    def test_base_and_head_alternate_after_a_dropped_warm_up(self):
        order = []
        base, head = load_abab.alternate(
            lambda: order.append("B") or (1.0, 1.0), lambda: order.append("H") or (2.0, 2.0), rounds=3
        )
        self.assertEqual("".join(order), "BH" + "BHBHBH")
        self.assertEqual((len(base), len(head)), (3, 3))

    def test_medians_and_every_run_are_kept(self):
        entry = load_abab.summarize([(300.0, 1.0), (100.0, 5.0), (200.0, 3.0)])
        self.assertEqual(entry["high_concurrency_req_per_s"], 200.0)
        self.assertEqual(entry["high_concurrency_req_per_s_samples"], [300.0, 100.0, 200.0])
        self.assertEqual(entry["high_concurrency_memory_growth_mb"], 3.0)

    def test_merge_fills_the_load_of_a_full_run_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            full, skipped = Path(tmp) / "full.json", Path(tmp) / "skipped.json"
            full.write_text(json.dumps({"performance": {"mode": "full", "load": {"high_concurrency_req_per_s": 0}}}))
            skipped.write_text(json.dumps({"performance": {"mode": "skipped", "load": {"high_concurrency_req_per_s": 0}}}))
            entry = load_abab.summarize([(10.0, 1.0)] * 3)
            self.assertTrue(load_abab.merge(str(full), entry))
            self.assertFalse(load_abab.merge(str(skipped), entry))
            self.assertEqual(json.loads(full.read_text())["performance"]["load"]["high_concurrency_req_per_s"], 10.0)
            self.assertEqual(json.loads(skipped.read_text())["performance"]["load"]["high_concurrency_req_per_s"], 0)

    def test_fewer_than_three_rounds_is_refused(self):
        self.assertEqual(load_abab.main(["--base-dir", "x", "--head-dir", "y", "--base-json", "a", "--head-json", "b", "--rounds", "2"]), 2)


if __name__ == "__main__":
    unittest.main()
