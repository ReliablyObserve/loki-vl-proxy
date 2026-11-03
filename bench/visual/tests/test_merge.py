"""merge.py: the shards join into the run a single job would have produced, and a broken run is reported, not hidden."""
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import compare  # noqa: E402
import merge  # noqa: E402
import shards  # noqa: E402
import visual_comment as vc  # noqa: E402
from vio import dump_json, load_json  # noqa: E402

SEED = {"end": 1000, "hours": 7, "vl_lines": 5, "loki_lines": 4}


def row(page, rng, diffs=()):
    return dict(page=page, range=rng, requests=3, series=1, main_vs_pr="3/3", pr_vs_loki="3/3 identical", settled=True,
                main_pr_diffs=list(diffs), loki_diffs=[], loki_new=[], loki_compared=True, loki_main_n=0,
                points_main=4, points_pr=4, points_loki=4, ui_main={}, ui_pr={}, ui_loki={}, settled_pr=True,
                errors_pr=[], errors_main=[], errors_loki=[], loki_missing=False, main_pr_nondet=[], main_pr_config=[],
                loki_explained=[], loki_nondet=[], main_pr_bodies=[], loki_bodies=[], main_pr_qids=[], loki_qids=[])


def make_plan():
    names = ["a", "a-b", "dd-x", "dd", "explore-1", "explore-2", "lvl-1", "lvl-2", "tail-1"]
    entries = {n: {"ranges": ["live"] if n == "tail-1" else ["15m", "1h"], "why": ["core set"] if n == "a" else ["f.go"],
                   "core": n == "a", "kind": "tail" if n == "tail-1" else "explore"} for n in names}
    return {"run": True, "core_range": "1h", "entries": entries, "captures": shards.capture_count(entries), "trimmed": [],
            "dropped": [], "fix_cases": {}, "fix_trimmed": []}


def write_shards(root, plan, k, rows, pixels, meta_of=None, skip=(), error=None):
    for i, sp in enumerate(shards.split(plan, k)):
        if i in skip:
            continue
        d = os.path.join(root, f"{merge.PREFIX}{i}")
        os.makedirs(os.path.join(d, "montage"))
        pages = set(sp["entries"])
        dump_json(os.path.join(d, "plan.json"), sp)
        mine = [r for r in rows if r["page"] in pages]
        dump_json(os.path.join(d, "compare.json"), sorted(mine, key=merge.row_order))
        dump_json(os.path.join(d, "pixeldiff.json"), {f"{r['page']}-{r['range']}": 0.0 for r in mine})
        for r in mine:
            with open(os.path.join(d, "montage", f"{r['page']}-{r['range']}.png"), "wb") as f:
                f.write(b"png")
        meta = {"base": "b" * 40, "head": "h" * 40, "recaptured": [], "elapsed_s": 100 + i, "seed": dict(SEED)}
        meta.update((meta_of or {}).get(i, {}))
        dump_json(os.path.join(d, "meta.json"), meta)
        if error and i == error[0]:
            dump_json(os.path.join(d, "error.json"), {"error": error[1]})


class MergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.plan = make_plan()
        self.rows = [row(p, r, diffs=["query x: value differs"] if p == "dd" and r == "1h" else ())
                     for p, e in self.plan["entries"].items() for r in e["ranges"]]
        self.rows.sort(key=merge.row_order)

    def merged(self, k, **kw):
        sd, out = os.path.join(self.tmp, f"s{k}"), os.path.join(self.tmp, f"o{k}")
        write_shards(sd, self.plan, k, self.rows, {}, **kw)
        return out, merge.merge(self.plan, k, sd, out, "b" * 40, "h" * 40)

    def render(self, out, rows=None, meta=None):
        plan = load_json(os.path.join(out, "plan.json"))
        a = type("A", (), dict(mode="branch", repo="o/r", pr="1", run_url="", artifact_url="", expected_change=False, image_names=[]))()
        return vc.render(rows if rows is not None else load_json(os.path.join(out, "compare.json")),
                         load_json(os.path.join(out, "pixeldiff.json")), plan, meta if meta is not None else load_json(os.path.join(out, "meta.json")), a)

    def test_the_rows_come_back_in_the_order_compare_py_writes_them(self):
        for k in (1, 2, 3, 5):
            with self.subTest(shards=k):
                out, problems = self.merged(k)
                self.assertIsNone(problems)
                self.assertEqual(load_json(os.path.join(out, "compare.json")), self.rows)
                # "a-b/15m" sorts before "a/15m" (as the paths do) although ("a", ..) < ("a-b", ..) as pairs
                names = [(r["page"], r["range"]) for r in self.rows]
                self.assertLess(names.index(("a-b", "15m")), names.index(("a", "15m")))

    def test_the_comment_and_the_gate_are_those_of_one_job(self):
        single = os.path.join(self.tmp, "single")
        os.makedirs(single)
        dump_json(os.path.join(single, "compare.json"), self.rows)
        dump_json(os.path.join(single, "pixeldiff.json"), {f"{r['page']}-{r['range']}": 0.0 for r in self.rows})
        meta = {"base": "b" * 40, "head": "h" * 40, "recaptured": []}
        dump_json(os.path.join(single, "meta.json"), meta)
        dump_json(os.path.join(single, "plan.json"), self.plan)
        want = self.render(single)
        for k in (1, 2, 4, 6):
            with self.subTest(shards=k):
                out, problems = self.merged(k)
                self.assertIsNone(problems)
                self.assertEqual(self.render(out), want)
        self.assertEqual(want[1]["exit"], 1)  # the planted difference is in the verdict

    def test_compare_md_is_the_one_compare_py_renders(self):
        out, _ = self.merged(3)
        with open(os.path.join(out, "compare.md"), encoding="utf-8") as f:
            self.assertEqual(f.read(), "\n".join(compare.render_md(self.rows)) + "\n")

    def test_recaptured_flipped_and_late_pages_are_those_of_every_shard(self):
        sp = shards.split(self.plan, 2)
        p0, p1 = list(sp[0]["entries"])[0], list(sp[1]["entries"])[0]
        out, problems = self.merged(2, meta_of={0: {"recaptured": [f"{p0} 1h"], "flipped": [f"{p0} 1h"], "late_requests": []},
                                                1: {"recaptured": [f"{p1} 15m"], "flipped": [], "late_requests": [f"{p1} 15m"]}})
        self.assertIsNone(problems)
        meta = load_json(os.path.join(out, "meta.json"))
        self.assertEqual(sorted(meta["recaptured"]), sorted([f"{p0} 1h", f"{p1} 15m"]))
        self.assertEqual(meta["flipped"], [f"{p0} 1h"])
        self.assertEqual(meta["late_requests"], [f"{p1} 15m"])
        self.assertEqual([s["captures"] for s in meta["shards"]], [p["captures"] for p in sp])

    def test_a_shard_that_left_nothing_or_died_fails_the_run(self):
        out, problems = self.merged(3, skip=(1,))
        self.assertIn("shard 1 left no result", problems)
        self.assertTrue(os.path.exists(os.path.join(out, "error.json")))
        text, verdict = self.render_error(out)
        self.assertEqual(verdict["exit"], 3)
        out, problems = self.merged(2, error=(0, "RuntimeError: Loki did not converge"))
        self.assertTrue(any("shard 0: RuntimeError" in p for p in problems))

    def render_error(self, out):
        a = type("A", (), dict(mode="artifact", repo="o/r", pr="1", run_url="", artifact_url="", expected_change=False, image_names=[]))()
        return vc.errored(a, load_json(os.path.join(out, "meta.json")), load_json(os.path.join(out, "error.json"))["error"])

    def test_a_capture_missing_from_a_shard_is_a_missing_capture(self):
        sd, out = os.path.join(self.tmp, "s"), os.path.join(self.tmp, "o")
        write_shards(sd, self.plan, 2, [r for r in self.rows if (r["page"], r["range"]) != ("dd", "1h")], {})
        self.assertIsNone(merge.merge(self.plan, 2, sd, out))
        _, verdict = self.render(out)
        self.assertEqual(verdict["exit"], 3)
        self.assertIn("dd 1h", verdict["missing"])

    def test_shards_that_ran_other_pages_or_other_data_fail_the_run(self):
        sd, out = os.path.join(self.tmp, "s"), os.path.join(self.tmp, "o")
        write_shards(sd, self.plan, 2, self.rows, {})
        other = load_json(os.path.join(sd, "visual-shard-1", "plan.json"))
        other["entries"].popitem()
        dump_json(os.path.join(sd, "visual-shard-1", "plan.json"), other)
        self.assertTrue(any("other than its share" in p for p in merge.merge(self.plan, 2, sd, out)))
        shutil.rmtree(sd)
        write_shards(sd, self.plan, 2, self.rows, {}, meta_of={1: {"seed": dict(SEED, vl_lines=6)}})
        self.assertTrue(any("seeded different data" in p for p in merge.merge(self.plan, 2, sd, out)))
        shutil.rmtree(sd)
        write_shards(sd, self.plan, 2, self.rows, {}, meta_of={1: {"head": "c" * 40}})
        self.assertTrue(any("different head commits" in p for p in merge.merge(self.plan, 2, sd, out)))

    def reloaded(self, per_shard, cap=6):
        """Two shards, each of which found `per_shard` differing captures in its first pass and loaded them again
        (the second look matched: the final rows are clean). Returns (out, first-pass rows, final rows)."""
        plan = self.plan
        parts = shards.split(plan, 2)
        pages = [[p for p in list(sp["entries"]) if sp["entries"][p]["ranges"] != ["live"]][:per_shard] for sp in parts]
        differing = {(p, "1h") for ps in pages for p in ps}
        first = [row(r["page"], r["range"], diffs=["query x: value differs"] if (r["page"], r["range"]) in differing else ())
                 for r in self.rows]
        final = [row(r["page"], r["range"]) for r in self.rows]
        sd, out = os.path.join(self.tmp, "sd"), os.path.join(self.tmp, "od")
        meta_of = {}
        for i, ps in enumerate(pages):
            names = [f"{p} 1h" for p in ps]
            meta_of[i] = {"recapture_candidates": len(ps), "max_recapture": cap, "recaptured": names, "flipped": names, "late_requests": []}
        write_shards(sd, plan, 2, final, {}, meta_of=meta_of)
        for i, ps in enumerate(pages):
            fp = os.path.join(sd, f"{merge.PREFIX}{i}", "first-pass")
            os.makedirs(os.path.join(fp, "montage"))
            dump_json(os.path.join(fp, "compare.json"), [r for r in first if (r["page"], r["range"]) in {(p, "1h") for p in ps}])
            dump_json(os.path.join(fp, "pixeldiff.json"), {f"{p}-1h": 0.5 for p in ps})
            for p in ps:
                with open(os.path.join(fp, "montage", f"{p}-1h.png"), "wb") as f:
                    f.write(b"first")
        self.assertIsNone(merge.merge(plan, 2, sd, out, "b" * 40, "h" * 40))
        return out, first, final

    def test_within_the_cap_every_shard_reloaded_as_a_single_job_would(self):
        out, first, final = self.reloaded(per_shard=3)  # 6 in all: at the cap of 6
        self.assertEqual(load_json(os.path.join(out, "compare.json")), final)
        meta = load_json(os.path.join(out, "meta.json"))
        self.assertEqual(len(meta["recaptured"]), 6)
        self.assertEqual(len(meta["flipped"]), 6)
        self.assertFalse(meta["recapture_over_cap"])

    def test_over_the_cap_the_run_stays_as_a_single_job_leaves_it_first_pass_only(self):
        # Two shards of 4 differences each are within a shard's cap of 6 and over the run's: a single job
        # would have loaded nothing again, so the first pass stands and nothing is recaptured or flipped.
        out, first, _ = self.reloaded(per_shard=4)
        self.assertEqual(load_json(os.path.join(out, "compare.json")), first)
        meta = load_json(os.path.join(out, "meta.json"))
        self.assertEqual(meta["recaptured"], [])
        self.assertNotIn("flipped", meta)
        self.assertNotIn("late_requests", meta)
        self.assertTrue(meta["recapture_over_cap"])
        pixel = load_json(os.path.join(out, "pixeldiff.json"))
        n = sum(1 for r in first if r["main_pr_diffs"])
        self.assertGreater(n, 6)
        self.assertEqual(sum(1 for v in pixel.values() if v == 0.5), n)
        kept = [x for x in os.listdir(os.path.join(out, "montage"))
                if open(os.path.join(out, "montage", x), "rb").read() == b"first"]
        self.assertEqual(len(kept), n)
        # the gate sees the first pass: the planted differences are in the verdict, none excused by a flip
        _, verdict = self.render(out)
        self.assertEqual(verdict["exit"], 1)
        self.assertGreaterEqual(len(verdict["failures"]), 7)

    def test_the_montages_and_logs_of_every_shard_are_kept(self):
        out, _ = self.merged(3)
        names = sorted(os.listdir(os.path.join(out, "montage")))
        self.assertEqual(names, sorted(f"{r['page']}-{r['range']}.png" for r in self.rows))


if __name__ == "__main__":
    unittest.main()
