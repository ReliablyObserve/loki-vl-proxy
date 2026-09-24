"""Unit tests for bench/ab/comment.py."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import comment  # noqa: E402


def row(shape, verdict, base, pr, loki, parity="same", base_parity="same", status=("200", "200", "200"), rng="1h",
        **extra):
    r = {"shape": shape, "range": rng, "verdict": verdict,
         "p50": {"base": base, "pr": pr, "loki": loki},
         "cold": {"base": base * 2 if base else None, "pr": pr * 2 if pr else None, "loki": loki * 3},
         "status": dict(zip(("base", "pr", "loki"), status)),
         "parity_vs_reference": parity, "baseline_parity_vs_reference": base_parity}
    r.update(extra)
    return r


def summary(rows, valid=True, set_name="control"):
    return {"set": set_name, "baseline": "base", "candidate": "pr", "reference": "loki", "runs": 4, "valid": valid,
            "restart_before": "0", "restart_after": "0" if valid else "1", "rows": rows}


SELECTION = {"run": True, "changed": 3, "why": {"control/logs plain": ["control smoke"],
                                                  "control/json | volume": ["internal/proxy/x.go"]}}
META = {"base": "a" * 40, "head": "b" * 40, "runs": 4, "ranges": ["1h"], "noise": 0.3, "min_delta": 0.1,
        "confirm_runs": 7, "elapsed_s": 420}


class RenderTest(unittest.TestCase):
    def test_skipped(self):
        text, verdict = comment.render([], {"run": False, "changed": 2}, META)
        self.assertTrue(text.startswith(comment.MARKER))
        self.assertIn("skipped", text)
        self.assertEqual(verdict["exit"], 0)

    def test_no_change_passes_and_folds_rows(self):
        text, verdict = comment.render([summary([row("logs plain", "same", 0.2, 0.21, 0.1)])], SELECTION, META)
        self.assertEqual(verdict["exit"], 0)
        self.assertIn("⚪ Performance A/B: no change beyond noise", text)
        self.assertIn("Every shape is within noise", text)
        self.assertIn("<details><summary>1 unchanged", text)
        self.assertIn("| ⚪ | logs plain | 1h | 200 ms | 210 ms | ⚪ +5% | 100 ms | **2.10×** | ✅ same |", text)

    def test_faster_and_fixed_pass(self):
        rows = [row("json \\| volume", "faster", 2.0, 0.5, 1.0),
                row("fixed one", "fixed", None, 0.4, 0.5, status=("502", "200", "200"), parity="same", base_parity="n/a")]
        text, verdict = comment.render([summary(rows)], SELECTION, META)
        self.assertEqual(verdict["exit"], 0)
        self.assertIn("🟢 Performance A/B: faster", text)
        self.assertIn("🟢 -75%", text)
        self.assertIn("| ✅ | fixed one | 1h | **502** | 400 ms | ✅ fixed |", text)
        self.assertIn("0.50×", text)

    def test_broken_slower_and_new_difference_fail(self):
        for r in (row("b", "broken", 0.2, None, 0.1, status=("200", "502", "200")),
                  row("s", "slower", 0.2, 0.9, 0.1, confirmed=True),
                  row("d", "same", 0.2, 0.2, 0.1, parity="differs", base_parity="same")):
            text, verdict = comment.render([summary([r])], SELECTION, META)
            self.assertEqual(verdict["exit"], 1, r["shape"])
            self.assertIn("🔴 Performance A/B: regression", text)
        self.assertIn("¹ re-measured with 7 runs, on windows", comment.render([summary([row("s", "slower", 0.2, 0.9, 0.1,
                                                                                  confirmed=True)])], SELECTION, META)[0])

    def test_preexisting_difference_does_not_fail(self):
        text, verdict = comment.render([summary([row("d", "same", 0.2, 0.2, 0.1, parity="differs",
                                                     base_parity="differs")])], SELECTION, META)
        self.assertEqual(verdict["exit"], 0)
        self.assertIn("➖ differs (as base)", text)
        self.assertEqual(verdict["preexisting_differences"], 1)

    def test_invalid_run(self):
        text, verdict = comment.render([summary([row("x", "same", 0.2, 0.2, 0.1)], valid=False)], SELECTION, META)
        self.assertEqual(verdict["exit"], 2)
        self.assertIn("invalid run", text)

    def test_rows_sorted_problems_first_and_pipes_escaped(self):
        rows = [row("a|b", "faster", 1.0, 0.3, 0.5), row("z", "broken", 0.2, None, 0.1, status=("200", "502", "200"))]
        text, _ = comment.render([summary(rows)], SELECTION, META)
        self.assertLess(text.index("| ❌ | z"), text.index("| 🟢 | a\\|b"))

    def test_errored_run_keeps_the_marker_and_fails(self):
        text, verdict = comment.errored(SELECTION, META, "RuntimeError: proxy 'pr' exited with 2")
        self.assertTrue(text.startswith(comment.MARKER))
        self.assertIn("did not complete", text)
        self.assertEqual(verdict["exit"], 3)

    def test_selection_reasons_listed(self):
        text, _ = comment.render([summary([row("logs plain", "same", 0.2, 0.2, 0.1)])], SELECTION, META)
        self.assertIn("`internal/proxy/x.go` → 1: control/json \\| volume", text)
        self.assertIn("7.0 min", text)


if __name__ == "__main__":
    unittest.main()
