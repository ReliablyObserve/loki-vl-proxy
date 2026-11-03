"""shards.py: every planned capture lands in exactly one shard, a page is never divided, the split is deterministic."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import plan as planner  # noqa: E402
import shards  # noqa: E402


def entry(ranges, core=False, kind="explore"):
    return {"ranges": ranges, "why": ["core set"] if core else ["x.go"], "core": core, "kind": kind}


def make(entries):
    return {"run": True, "core_range": "1h", "entries": entries, "captures": shards.capture_count(entries),
            "trimmed": [], "dropped": [], "fix_cases": {}, "fix_trimmed": []}


class ShardsTest(unittest.TestCase):
    def test_every_capture_of_the_full_spec_is_in_exactly_one_shard(self):
        # The baseline plan: every page of bench/visual/spec.json at its own ranges (136 pages, 278 captures).
        full = planner.plan_all()
        self.assertGreater(full["captures"], 200)
        for k in range(1, shards.MAX_SHARDS + 3):
            with self.subTest(shards=k):
                parts = shards.split(full, k)
                self.assertEqual(shards.verify(full, parts), [])
                self.assertEqual(sorted(c for p in parts for c in shards.planned(p)), sorted(shards.planned(full)))
                self.assertEqual(sum(p["captures"] for p in parts), full["captures"])

    def test_the_live_pages_share_one_shard_and_a_page_keeps_all_its_ranges(self):
        full = planner.plan_all()
        parts = shards.split(full, 6)
        live = [i for i, p in enumerate(parts) if any(e["ranges"] == ["live"] for e in p["entries"].values())]
        self.assertEqual(len(live), 1)
        for p in parts:
            for pid, e in p["entries"].items():
                self.assertEqual(e, full["entries"][pid])

    def test_the_split_is_deterministic_and_balanced(self):
        full = planner.plan_all()
        self.assertEqual(shards.split(full, 8), shards.split(full, 8))
        sizes = [p["captures"] for p in shards.split(full, 8)]
        self.assertLessEqual(max(sizes) - min(sizes), shards.LIVE_WEIGHT * 2)

    def test_shard_count_follows_the_captures(self):
        def count(n, **kw):
            return shards.shard_count(make({f"p{i}": entry(["1h"]) for i in range(n)}), **kw)
        self.assertEqual(count(9), 1)  # the core set alone
        self.assertEqual(count(12), 1)
        self.assertEqual(count(13), 2)
        self.assertEqual(count(36), 3)
        self.assertEqual(count(278), shards.MAX_SHARDS)
        self.assertEqual(count(278, max_shards=3), 3)
        self.assertEqual(shards.shard_count({"run": False, "entries": {}}), 0)
        self.assertEqual(shards.shard_count(make({})), 0)

    def test_no_empty_shard_is_asked_for(self):
        # three pages cannot fill eight shards
        plan = make({"a": entry(["15m", "1h", "6h"]), "b": entry(["15m", "1h", "6h"]), "c": entry(["15m", "1h", "6h"])})
        k = shards.shard_count(plan, per_shard=1)
        self.assertEqual(k, 3)
        self.assertTrue(all(p["entries"] for p in shards.split(plan, k)))

    def test_verify_names_a_dropped_a_doubled_and_a_foreign_capture(self):
        plan = make({"a": entry(["1h"]), "b": entry(["15m", "1h"])})
        good = shards.split(plan, 2)
        self.assertEqual(shards.verify(plan, good), [])
        dropped = [good[0], {**good[1], "entries": {}, "captures": 0}]
        self.assertTrue(any("is in no shard" in p for p in shards.verify(plan, dropped)))
        doubled = [good[0], {**good[0], "shard": {"index": 1, "of": 2}}]
        self.assertTrue(any("is in shard 0 and shard 1" in p for p in shards.verify(plan, doubled)))
        foreign = [{**good[0], "entries": {**good[0]["entries"], "z": entry(["1h"])}, "captures": good[0]["captures"] + 1}, good[1]]
        self.assertTrue(any("not in the plan" in p for p in shards.verify(plan, foreign)))
        divided = [{**good[0], "entries": {"b": entry(["15m"])}, "captures": 1}, {**good[1], "entries": {"b": entry(["1h"]), "a": entry(["1h"])}, "captures": 2}]
        self.assertTrue(shards.verify(plan, divided))

    def test_a_shard_plan_is_a_plan_ci_run_can_use(self):
        plan = make({"a": entry(["1h"], core=True), "t": entry(["live"], kind="tail")})
        for p in shards.split(plan, 2):
            self.assertTrue(p["run"])
            self.assertEqual(set(plan) - set(p), set())  # every key of the plan, so no consumer needs a special case
            json.dumps(p)


if __name__ == "__main__":
    unittest.main()
