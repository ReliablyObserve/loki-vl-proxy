"""Unit tests for the comparison gate and the PR comment (bench/visual/comment.py)."""
import argparse
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import comment  # noqa: E402

ARGS = argparse.Namespace(mode="branch", repo="o/r", pr="7", run_url="https://example.test/run/1", artifact_url="")
META = {"base": "b" * 40, "head": "h" * 40}


def row(page="explore-a", rng="1h", **kw):
    base = dict(page=page, range=rng, requests=3, series=2, main_vs_pr="3/3", pr_vs_loki="3/3 identical", settled=True,
                main_pr_diffs=[], loki_diffs=[], loki_new=[], loki_compared=True, points_main=10, points_pr=10,
                ui_main={"noData": 0, "banners": [], "panelErrors": 0}, ui_pr={"noData": 0, "banners": [], "panelErrors": 0})
    base.update(kw)
    return base


def plan(*pairs, core=()):
    entries = {}
    for page, rng in pairs:
        e = entries.setdefault(page, {"ranges": [], "core": page in core,
                                      "why": ["core set"] if page in core else ["internal/x.go"], "kind": "explore"})
        e["ranges"].append(rng)
    return {"run": True, "core_range": "1h", "entries": entries, "captures": len(pairs), "trimmed": [], "dropped": []}


class GateTest(unittest.TestCase):
    def verdict(self, rows, px=None, p=None):
        p = p or plan(*[(r["page"], r["range"]) for r in rows])
        return comment.render(rows, px or {}, p, META, ARGS)

    def test_clean_run_passes(self):
        text, v = self.verdict([row()])
        self.assertEqual(v["exit"], 0)
        self.assertIn("Visual smoke: passed", text)
        self.assertTrue(text.startswith(comment.MARKER))

    def test_data_difference_fails(self):
        text, v = self.verdict([row(main_pr_diffs=["query x: value at 1: 1 vs 2"])])
        self.assertEqual(v["exit"], 1)
        self.assertIn("data differs base vs PR", v["failures"][0])
        self.assertIn("![explore-a-1h](https://raw.githubusercontent.com/o/r/pr-visuals/pr-7/explore-a-1h.png?v=hhhhhhhh)", text)

    def test_panel_empty_on_pr_only_fails_but_empty_on_both_does_not(self):
        self.assertEqual(self.verdict([row(points_pr=0)])[1]["exit"], 1)
        text, v = self.verdict([row(points_main=0, points_pr=0)])
        self.assertEqual(v["exit"], 0)
        self.assertIn("no data on either side", text)

    def test_ui_problems_only_count_when_the_base_lacks_them(self):
        err = {"noData": 0, "banners": ["Plugin unavailable"], "panelErrors": 1}
        self.assertEqual(self.verdict([row(ui_pr=err)])[1]["exit"], 1)
        self.assertEqual(self.verdict([row(ui_pr=err, ui_main=err)])[1]["exit"], 0)  # pre-existing on both
        self.assertEqual(self.verdict([row(ui_pr={"noData": 2, "banners": [], "panelErrors": 0})])[1]["exit"], 1)

    def test_loki_differences_inform_unless_new(self):
        text, v = self.verdict([row(loki_diffs=["query a: x"], loki_new=[])])
        self.assertEqual(v["exit"], 0)
        self.assertIn("1 difference(s), 1 on base too", text)
        self.assertEqual(self.verdict([row(loki_diffs=["q: y"], loki_new=["q: y"])])[1]["exit"], 1)

    def test_pixel_diff_only_warns(self):
        text, v = self.verdict([row()], {"explore-a-1h": 0.2})
        self.assertEqual(v["exit"], 0)
        self.assertEqual(v["warnings"], 1)
        self.assertIn("passed with warnings", text)
        self.assertIn("<details open>", text)  # not clean: its montage is embedded

    def test_missing_capture_is_incomplete(self):
        text, v = comment.render([row()], {}, plan(("explore-a", "1h"), ("explore-b", "1h")), META, ARGS)
        self.assertEqual(v["exit"], 3)
        self.assertIn("did not complete", text)

    def test_images_only_for_unclean_rows_and_core_block(self):
        rows = [row("core-a", "1h"), row("det-b", "15m"), row("det-b", "1h", main_pr_diffs=["d"])]
        p = plan(("core-a", "1h"), ("det-b", "15m"), ("det-b", "1h"), core=("core-a",))
        text, _ = comment.render(rows, {}, p, META, ARGS)
        self.assertEqual(text.count("![core-a-1h]"), 1)  # the collapsed core block
        self.assertEqual(text.count("![det-b-1h]"), 1)   # the failing row
        self.assertNotIn("![det-b-15m]", text)           # clean detailed row: linked, not embedded
        self.assertIn("pr-visuals/pr-7", text)

    def test_core_entry_at_other_ranges_is_detailed_and_live_pixels_do_not_warn(self):
        rows = [row("core-a", "1h"), row("core-a", "6h"), row("tail", "live")]
        p = plan(("core-a", "1h"), ("core-a", "6h"), ("tail", "live"), core=("core-a",))
        text, v = comment.render(rows, {"tail-live": 0.5}, p, META, ARGS)
        self.assertEqual(v["warnings"], 0)
        self.assertIn("| core-a | 1h | core |", text)
        self.assertIn("| core-a | 6h | detailed |", text)

    def test_fork_mode_is_text_only(self):
        a = argparse.Namespace(**{**vars(ARGS), "mode": "artifact", "artifact_url": "https://example.test/artifact/9"})
        text, v = comment.render([row(main_pr_diffs=["d"])], {}, plan(("explore-a", "1h")), META, a)
        self.assertNotIn("![", text)
        self.assertIn("[artifact](https://example.test/artifact/9)", text)
        self.assertEqual(v["exit"], 1)

    def test_skipped_and_errored(self):
        text, v = comment.skipped(ARGS, META)
        self.assertIn("skipped", text)
        self.assertEqual(v["exit"], 0)
        text, v = comment.errored(ARGS, META, "RuntimeError: boom")
        self.assertEqual(v["exit"], 3)
        self.assertIn("boom", text)

    def test_ui_problems(self):
        self.assertEqual(comment.ui_problems({}, {}), [])
        found = comment.ui_problems({"noData": 1}, {"noData": 3, "banners": ["Failed to load x"], "panelErrors": 1})
        self.assertEqual(len(found), 3)


if __name__ == "__main__":
    unittest.main()
