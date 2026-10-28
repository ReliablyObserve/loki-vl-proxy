"""Unit tests for perf_matrix.py window planning, request headers and cache counters."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import perf_matrix as pm  # noqa: E402

END = 1_800_000_000


def shape(name, **extra):
    return {"name": name, "query": f"q {name}", **extra}


SPEC = {"sets": {
    "control": {"shapes": [shape("a"), shape("b", shift={"1h": 300, "6h": 3600}), shape("c", instant=True)]},
    "feature": {"shapes": [shape("a"), shape("d", shift=600)]},
    "third": {"shapes": [shape("a")]},
}}
SETS = [("control", []), ("feature", []), ("third", ["a"])]


def all_ends(plans, runs, confirm_runs, ranges, refresh_s=0, refresh_runs=0):
    """Every (set, shape, range, pass) -> the window ends the matrix would use."""
    out = {}
    for plan in plans:
        for sh in SPEC["sets"][plan["set"]]["shapes"]:
            for rng in (["instant"] if sh.get("instant") else ranges):
                out[(plan["set"], sh["name"], rng, "first")] = [
                    pm.run_end_for(plan["end"], sh, rng, i, runs) for i in range(runs)]
                out[(plan["set"], sh["name"], rng, "confirm")] = [
                    pm.run_end_for(plan["confirm_end"], sh, rng, i, confirm_runs) for i in range(confirm_runs)]
                if refresh_s:
                    out[(plan["set"], sh["name"], rng, "refresh")] = [
                        pm.run_end_for(plan["refresh_end"], sh, rng, i, refresh_runs, refresh_s)
                        for i in range(refresh_runs)]
    return out


class RunEndTest(unittest.TestCase):
    def test_shifted_runs_move_back_by_the_shapes_shift_per_range(self):
        sh = shape("x", shift={"1h": 300}, end_offset=1000)
        self.assertEqual([pm.run_end_for(END, sh, "1h", i, 3) for i in range(3)], [END - 1000, END - 1300, END - 1600])
        self.assertEqual(pm.run_end_for(END, sh, "6h", 2, 3), END - 1000 - 120)

    def test_refresh_runs_move_forward_and_end_at_end(self):
        ends = [pm.run_end_for(END, shape("x"), "1h", i, 4, refresh_s=30) for i in range(4)]
        self.assertEqual(ends, [END - 90, END - 60, END - 30, END])
        self.assertTrue(all(b - a == 30 for a, b in zip(ends, ends[1:])))

    def test_refresh_respects_end_offset(self):
        self.assertEqual(pm.run_end_for(END, shape("x", end_offset=500), "1h", 1, 2, refresh_s=60), END - 500)

    def test_refresh_interval_must_divide_every_step(self):
        self.assertIsNone(pm.refresh_problem(0, 1, [5, 600]))
        self.assertIsNone(pm.refresh_problem(600, 3, [5, 60, 600]))
        self.assertIn("multiple", pm.refresh_problem(30, 3, [5, 60]))
        self.assertIn("--runs >= 2", pm.refresh_problem(60, 1, [5]))


class HeaderTest(unittest.TestCase):
    def test_only_the_shapes_own_headers_are_sent_to_every_target(self):
        self.assertEqual(pm.request_headers(shape("x", headers={"X-A": "1"})), {"X-A": "1"})
        self.assertEqual(pm.request_headers(shape("x")), {})

    def test_no_cache_control_header_is_ever_added(self):
        self.assertNotIn("Cache-Control", pm.request_headers(shape("x")))
        self.assertFalse(hasattr(pm, "NO_CACHE"))

    def test_shape_headers_are_not_mutated(self):
        sh = shape("x", headers={"X-A": "1"})
        pm.request_headers(sh)["X-B"] = "2"
        self.assertEqual(sh["headers"], {"X-A": "1"})


class CacheStatsTest(unittest.TestCase):
    def body(self, cache):
        return json.dumps({"status": "success", "data": {"resultType": "matrix", "result": [],
                                                         "stats": {"cache": cache}}}).encode()

    def test_result_cache_counters_are_kept(self):
        stats = pm.loki_cache_stats(self.body({"result": {"entriesFound": 1, "entriesRequested": 2, "bytesReceived": 99,
                                                          "downloadTime": 5}, "chunk": {"entriesFound": 7}}))
        self.assertEqual(stats, {"result": {"entriesFound": 1, "entriesRequested": 2, "bytesReceived": 99}})

    def test_other_result_sub_caches_are_kept_under_their_name(self):
        stats = pm.loki_cache_stats(self.body({"result": {"entriesFound": 0}, "volumeResult": {"entriesFound": 3}}))
        self.assertEqual(stats["volumeResult"], {"entriesFound": 3})

    def test_absent_or_malformed_means_none(self):
        self.assertIsNone(pm.loki_cache_stats(b'{"data":{"result":[]}}'))
        self.assertIsNone(pm.loki_cache_stats(b"not json with a \"cache\" word"))
        self.assertIsNone(pm.loki_cache_stats(self.body({"chunk": {"entriesFound": 1}})))
        self.assertIsNone(pm.loki_cache_stats(json.dumps({"data": {"stats": {"cache": 5}}}).encode()))


class PlanTest(unittest.TestCase):
    def test_blocks_do_not_overlap_and_no_request_repeats_across_sets(self):
        plans = pm.plan_windows(SPEC, SETS, ["1h", "6h"], 4, 7, END, refresh_s=60, refresh_runs=5)
        ends = all_ends(plans, 4, 7, ["1h", "6h"], 60, 5)
        # Same shape name "a" is in all three sets: its windows must still be disjoint.
        seen = {}
        for key, values in ends.items():
            for e in values:
                other = seen.setdefault((key[1], key[2], e), key)
                self.assertEqual(other, key, f"{key} repeats a window of {other}")
        self.assertTrue(all(e <= END for values in ends.values() for e in values))

    def test_blocks_are_stacked_downwards_in_whole_minutes(self):
        plans = pm.plan_windows(SPEC, SETS, ["1h"], 4, 7, END)
        self.assertEqual([p["end"] for p in plans], [END - p["offset"] for p in plans])
        for a, b in zip(plans, plans[1:]):
            self.assertEqual(b["offset"], a["offset"] + a["span"])
        self.assertTrue(all(p["span"] % 60 == 0 for p in plans))

    def test_confirm_uses_the_shapes_shift_not_a_minute(self):
        plan = pm.plan_windows(SPEC, [("feature", [])], ["1h"], 4, 7, END)[0]
        self.assertEqual(plan["shift"], 600)
        self.assertEqual(plan["confirm_end"], END - 600 * 4)
        first = {pm.run_end_for(plan["end"], s, "1h", i, 4) for s in SPEC["sets"]["feature"]["shapes"] for i in range(4)}
        confirm = {pm.run_end_for(plan["confirm_end"], s, "1h", i, 7) for s in SPEC["sets"]["feature"]["shapes"]
                   for i in range(7)}
        self.assertFalse(first & confirm)

    def test_dict_shift_counts_only_the_selected_ranges(self):
        self.assertEqual(pm.plan_windows(SPEC, [("control", [])], ["1h"], 2, 2, END)[0]["shift"], 300)
        self.assertEqual(pm.plan_windows(SPEC, [("control", [])], ["1h", "6h"], 2, 2, END)[0]["shift"], 3600)

    def test_named_shapes_limit_the_shift(self):
        self.assertEqual(pm.plan_windows(SPEC, [("feature", ["a"])], ["1h"], 2, 2, END)[0]["shift"], 60)

    def test_refresh_block_is_the_top_of_its_block_and_the_passes_follow_below_it(self):
        # the refresh pass models a dashboard refresh at now, so it is the nearest to now; the first and the
        # confirm pass lie below it, inside the block
        plans = pm.plan_windows(SPEC, SETS, ["1h"], 4, 7, END, refresh_s=60, refresh_runs=5)
        p = plans[0]
        self.assertEqual(p["refresh_end"], END)
        self.assertLessEqual(p["end"], p["refresh_end"] - 4 * 60)          # below the refresh pass's first window
        self.assertLess(p["confirm_end"], p["end"] - p["shift"] * 3)
        self.assertGreaterEqual(p["confirm_end"] - p["shift"] * 6, END - p["span"])
        no_refresh = pm.plan_windows(SPEC, SETS, ["1h"], 4, 7, END)[0]
        self.assertIsNone(no_refresh["refresh_end"])
        self.assertEqual(no_refresh["end"], END)  # without a refresh pass the first pass is the top, as before
        # the block is as tall as before: the refresh pass takes the room it always took
        self.assertEqual(p["span"], pm.plan_windows(SPEC, SETS, ["1h"], 4, 7, END, refresh_s=60, refresh_runs=5)[0]["span"])

    def test_every_planned_end_is_a_whole_minute_even_for_odd_shifts(self):
        spec = {"sets": {"a": {"shapes": [shape("x", shift=317)]}, "b": {"shapes": [shape("y", shift=317)]}}}
        plans = pm.plan_windows(spec, [("a", []), ("b", [])], ["1h"], 4, 7, END, refresh_s=60, refresh_runs=5)
        for p in plans:
            self.assertEqual([p["end"] % 60, p["confirm_end"] % 60, p["refresh_end"] % 60], [0, 0, 0])
        # the floored ends are the ones really sent, and the blocks stay disjoint
        sent = []
        for p in plans:
            sh = spec["sets"][p["set"]]["shapes"][0]
            sent.append([pm.run_end_for(p["end"], sh, "1h", i, 4) for i in range(4)] +
                        [pm.run_end_for(p["confirm_end"], sh, "1h", i, 7) for i in range(7)] +
                        [pm.run_end_for(p["refresh_end"], sh, "1h", i, 5, 60) for i in range(5)])
        self.assertFalse(set(sent[0]) & set(sent[1]))
        self.assertLess(max(sent[1]), min(sent[0]))

    def test_deterministic(self):
        args = (SPEC, SETS, ["1h"], 4, 7, END, 60, 5)
        self.assertEqual(pm.plan_windows(*args), pm.plan_windows(*args))

    def test_no_sets_no_plans(self):
        self.assertEqual(pm.plan_windows(SPEC, [], ["1h"], 4, 7, END), [])



if __name__ == "__main__":
    unittest.main()
