"""Unit tests for bench/ab/smoke.py (the daily report's perf smoke)."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import history  # noqa: E402
import smoke  # noqa: E402


def row(shape, rng, main_cold, loki_cold, main_p50=0.2, loki_p50=0.2, status="200", loki_status="200", parity="same",
        served=False):
    out = {"shape": shape, "range": rng, "verdict": "same",
           "p50": {"release": 0.2, "main": main_p50, "loki": loki_p50},
           "cold": {"release": 1.0, "main": main_cold, "loki": loki_cold},
           "status": {"release": "200", "main": status, "loki": loki_status},
           "parity_vs_reference": parity,
           # Loki's own counters: its cold run was computed, or (served) its results cache answered it.
           "cache": {"loki": {"cold": {"hits": 0}}}}
    if served:
        out["cold_cache_served"] = ["loki"]
    return out


def summary(date, *rows, set_name="control", profile="loki-defaults"):
    return {"set": set_name, "label": f"daily-smoke-{set_name}", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": 3, "valid": True, "profile": profile, "rows": list(rows)}


class PlanTest(unittest.TestCase):
    def test_main_shapes_at_one_and_six_hours(self):
        spec = {"sets": {
            "a": {"ranges": ["1h", "3h", "6h", "24h"], "shapes": [{"name": "a1"}, {"name": "a2"}, {"name": "a3"}]},
            "b": {"ranges": ["1h", "24h"], "shapes": [{"name": "b1"}]},
            "c": {"ranges": ["5m-200ms"], "shapes": [{"name": "c1"}]},
        }}
        self.assertEqual(smoke.plan(spec), {"a": (["a1", "a2"], ["1h", "6h"]), "b": (["b1"], ["1h"])})

    def test_the_repository_shapes_plan(self):
        spec = history.load_spec()
        plan = smoke.plan(spec)
        self.assertTrue(plan)
        for name, (shapes, ranges) in plan.items():
            self.assertLessEqual(len(shapes), smoke.SHAPES_PER_SET)
            self.assertTrue(set(ranges) <= set(smoke.RANGES), name)
        # The smoke history is a directory of its own below the weekly history.
        self.assertEqual(os.path.dirname(smoke.DIRECTORY), history.HISTORY)


class JudgeTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_a_row_above_twice_loki_is_red(self):
        v = smoke.judge([summary("2026-11-14", row("logs plain", "1h", 3.0, 1.0))], [], self.dir)
        self.assertTrue(v["red"])
        self.assertEqual([(r["mode"], r["shape"]) for r in v["over_2x"]], [("cold", "logs plain")])
        self.assertEqual(v["distribution"]["cold"]["above"], 1)
        self.assertEqual(v["distribution"]["warm"]["close"], 1)

    def test_within_twice_loki_and_cache_answers_are_not_red(self):
        v = smoke.judge([summary("2026-11-14", row("a", "1h", 1.5, 1.0), row("b", "1h", 9.0, 1.0, served=True),
                                 row("c", "6h", 0.5, 1.0, main_p50=0.1, loki_p50=0.5))], [], self.dir)
        self.assertFalse(v["red"])
        self.assertEqual(v["rows"], 3)
        cold = v["distribution"]["cold"]
        self.assertEqual((cold["between"], cold["below"], cold["rows"]), (1, 1, 2))  # b's cold is not comparable
        self.assertEqual(v["distribution"]["warm"]["below"], 1)

    def test_a_first_ask_is_held_to_its_bound_not_to_loki(self):
        import over2x
        dp2 = "DP2 patterns of an environment"
        bound = over2x.first_ask_bound(dp2, "1h")
        self.assertIsNotNone(bound)
        # 8x Loki on the first ask, inside the bound: not above 2x, not red; its warm run keeps the ratio.
        v = smoke.judge([summary("2026-11-14", row(dp2, "1h", bound / 2, bound / 16), set_name="drilldown-patterns")],
                        [], self.dir)
        self.assertFalse(v["red"])
        self.assertEqual(v["over_2x"], [])
        self.assertEqual([(r["shape"], r["state"]) for r in v["first_ask"]], [(dp2, "within")])
        self.assertNotIn("cold", v["distribution"])
        self.assertEqual(v["distribution"]["warm"]["rows"], 1)
        # Above the bound it is red, whatever Loki's time.
        v = smoke.judge([summary("2026-11-14", row(dp2, "1h", bound + 0.5, bound + 1.0), set_name="drilldown-patterns")],
                        [], self.dir)
        self.assertTrue(v["red"])
        self.assertEqual([r["state"] for r in v["first_ask_over"]], ["above_bound"])
        self.assertIn("above the bound", smoke.render(v, "2026-11-14", "abcdef0123"))
        # The same shape at a range that is no first ask keeps the Loki ratio on cold.
        v = smoke.judge([summary("2026-11-14", row(dp2, "6h", 3.0, 1.0), set_name="drilldown-patterns")], [], self.dir)
        self.assertEqual([(r["mode"], r["range"]) for r in v["over_2x"]], [("cold", "6h")])
        self.assertEqual(v["first_ask"], [])

    def test_loki_failures_and_proxy_errors_are_counted_not_judged(self):
        v = smoke.judge([summary("2026-11-14", row("a", "1h", 9.0, 1.0, loki_status="500"),
                                 row("b", "1h", 9.0, 1.0, status="502"))], [], self.dir)
        self.assertFalse(v["red"])
        self.assertEqual(v["loki_failed"], 2)
        self.assertEqual(v["comparable"], 0)

    def test_a_ratchet_row_with_another_result_is_lost_at_once(self):
        ratchet = [{"set": "control", "shape": "a", "range": "1h"}]
        v = smoke.judge([summary("2026-11-14", row("a", "1h", 0.5, 1.0, parity="differs"), row("z", "1h", 0.5, 1.0, parity="differs"))],
                        ratchet, self.dir)
        self.assertTrue(v["red"])
        self.assertEqual([(r["shape"], r["detail"]) for r in v["ratchet_lost"]], [("a", "result differs from Loki")])

    def test_a_slower_cold_loses_the_ratchet_only_on_two_smoke_days(self):
        ratchet = [{"set": "control", "shape": "a", "range": "1h"}]
        slow = row("a", "1h", 1.8, 1.0)
        first = smoke.judge([summary("2026-11-13", slow)], ratchet, self.dir)
        self.assertEqual(first["ratchet_lost"], [])
        history.append(summary("2026-11-13", slow), directory=self.dir, commit="c1")
        second = smoke.judge([summary("2026-11-14", slow)], ratchet, self.dir)
        self.assertEqual(len(second["ratchet_lost"]), 1)
        self.assertIn("held for two runs", second["ratchet_lost"][0]["detail"])
        # Another profile's line is not this profile's previous day.
        other = smoke.judge([summary("2026-11-14", slow, profile="aligned")], ratchet, self.dir)
        self.assertEqual(other["ratchet_lost"], [])

    def test_write_appends_the_history_and_the_report(self):
        out = tempfile.mkdtemp()
        ratchet_dir = tempfile.mkdtemp()
        with open(os.path.join(ratchet_dir, history.RATCHET), "w") as f:
            json.dump([{"set": "control", "shape": "a", "range": "1h"}], f)
        v = smoke.write([summary("2026-11-14", row("a", "1h", 3.0, 1.0))], out, "2026-11-14", "0123456789abcdef",
                        directory=self.dir, ratchet_dir=ratchet_dir)
        self.assertTrue(v["red"])
        self.assertEqual(v["ratchet_problem"], "")
        with open(os.path.join(out, "smoke.json")) as f:
            self.assertEqual(json.load(f)["commit"], "0123456789abcdef")
        with open(os.path.join(out, "smoke.md")) as f:
            md = f.read()
        self.assertIn("main `0123456789`", md)
        self.assertIn("#### Rows above 2x Loki: 1", md)
        self.assertIn("control / a / 1h / loki-defaults / cold: 3.000s against Loki 1.000s", md)
        lines = history.read(os.path.join(self.dir, history.history_name("control", "loki-defaults")))
        self.assertEqual([e["commit"] for e in lines], ["0123456789abcdef"])


class JudgeCommandTest(unittest.TestCase):
    def test_judge_writes_the_verdict_of_saved_summaries(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "summary-control.json")
        with open(path, "w") as f:
            json.dump(summary("2026-11-16", row("a", "1h", 3.0, 1.0), row("b", "1h", 1.0, 1.0)), f)
        out = os.path.join(d, "verdict.json")
        self.assertEqual(smoke.main(["judge", "--out", out, "--ratchet-dir", d, path]), 0)
        with open(out) as f:
            v = json.load(f)
        self.assertEqual((v["rows"], len(v["over_2x"]), v["red"]), (2, 1, True))


if __name__ == "__main__":
    unittest.main()
