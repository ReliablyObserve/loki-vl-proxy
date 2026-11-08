"""Unit tests for conformance/scripts/translation_overhead.py: the benchmark output it reads, the table it renders
and the committed README section, which must be what the committed results render to."""
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "conformance", "scripts"))
import translation_overhead as to  # noqa: E402

OUTPUT = """goos: darwin
goarch: arm64
pkg: github.com/ReliablyObserve/Loki-VL-proxy/internal/translator
cpu: Apple M5 Pro
BenchmarkTranslateLogQL_Simple-4   	  400000	      2442 ns/op	     933 B/op	      23 allocs/op
BenchmarkTranslateLogQL_Simple-4   	  400000	      2500 ns/op	     933 B/op	      23 allocs/op
BenchmarkTranslateLogQL_Simple-4   	  400000	      2400 ns/op	     933 B/op	      23 allocs/op
BenchmarkTranslate_Rate-4          	  200000	      5592 ns/op	   12100 B/op	      74 allocs/op
PASS
"""


class ParseRenderTest(unittest.TestCase):
    def test_medians_and_machine_are_read(self):
        rows, meta = to.parse(OUTPUT)
        self.assertEqual(rows["BenchmarkTranslateLogQL_Simple"], {"ns": 2442, "allocs": 23, "runs": 3})
        self.assertEqual(rows["BenchmarkTranslate_Rate"]["allocs"], 74)
        self.assertEqual(meta["cpu"], "Apple M5 Pro")
        self.assertEqual(meta["goarch"], "arm64")

    def test_the_table_states_the_run(self):
        results = {"date": "2026-11-08", "commit": "abcdef1234", "go": "go1.27.3", "cpu": "Apple M5 Pro",
                   "goarch": "arm64", "gomaxprocs": "4", "count": 5,
                   "rows": [{"benchmark": "BenchmarkTranslateLogQL_Simple", "ns": 2442, "allocs": 23},
                            {"benchmark": "BenchmarkTranslate_Rate", "ns": 5592, "allocs": 74}]}
        text = to.render(results)
        self.assertIn("Measured on 2026-11-08 at commit `abcdef1234` by the daily run", text)
        self.assertIn("**2.4 to 5.6 µs per query**", text)
        self.assertIn("under 0.01% of wall time", text)
        self.assertIn('| Selector `{app="nginx"}` | 2.4 µs | 23 |', text)
        self.assertIn('| `rate({app="nginx",env="production"}[5m])` | 5.6 µs | 74 |', text)
        self.assertIn("go1.27.3 on Apple M5 Pro (arm64)", text)

    def test_markers_are_required_once(self):
        with self.assertRaises(ValueError):
            to.replace_section("no markers", "x")
        text = f"a\n{to.START}\nold\n{to.END}\nb"
        self.assertEqual(to.replace_section(text, "new"), f"a\n{to.START}\n\nnew\n\n{to.END}\nb")


class CommittedSectionTest(unittest.TestCase):
    def test_the_readme_states_the_committed_results(self):
        path = os.path.join(REPO, to.RESULTS)
        results = None
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                results = json.load(f)
            self.assertEqual([r["benchmark"] for r in results["rows"]], [n for n, _ in to.ROWS])
        with open(os.path.join(REPO, "README.md"), encoding="utf-8") as f:
            readme = f.read()
        self.assertEqual(to.replace_section(readme, to.render(results)), readme)

    def test_no_results_render_a_not_measured_note(self):
        self.assertEqual(to.render(None), to.NOT_MEASURED)


if __name__ == "__main__":
    unittest.main()
