"""Unit tests for the comparison gate and the PR comment (bench/visual/visual_comment.py)."""
import argparse
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import visual_comment as comment  # noqa: E402

ARGS = argparse.Namespace(mode="branch", repo="o/r", pr="7", run_url="https://example.test/run/1", artifact_url="")
META = {"base": "b" * 40, "head": "a" * 40}


def row(page="explore-a", rng="1h", **kw):
    base = dict(page=page, range=rng, requests=3, series=2, main_vs_pr="3/3", pr_vs_loki="3/3 identical", settled=True,
                main_pr_diffs=[], loki_diffs=[], loki_new=[], loki_compared=True, loki_main_n=0, points_loki=10,
                settled_pr=True, errors_pr=[], errors_main=[], loki_missing=False, points_main=10, points_pr=10,
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

    def test_history_dependent_patterns_difference_warns_not_fails(self):
        nondet = ["resource /api/datasources/uid/*/resources/patterns?end=x: body: content differs (rows 6 vs 2)"]
        text, v = self.verdict([row(main_pr_nondet=nondet)])
        self.assertEqual(v["exit"], 0)
        self.assertIn("history-dependent", text)

    def test_real_difference_next_to_patterns_still_fails(self):
        nondet = ["resource /api/datasources/uid/*/resources/patterns?end=x: body: content differs (rows 6 vs 2)"]
        _, v = self.verdict([row(main_pr_nondet=nondet, main_pr_diffs=["query A: value differs"], points_loki=0)])
        self.assertEqual(v["exit"], 1)

    def test_no_loki_range_passes_when_the_same_differences_were_proven_at_a_shorter_range(self):
        d1 = "query ('sum by (x) (count_over_time({a=\"b\"} [1m]))', 'range', '1790864400000', '1790868000000', 'abc123def4567890'): value differs"
        d6 = "query ('sum by (x) (count_over_time({a=\"b\"} [1m]))', 'range', '1790846400000', '1790868000000', '0123456789abcdef'): value differs"
        rows = [row(rng="1h", main_pr_diffs=[d1], loki_main_n=1, loki_diffs=[]),
                row(rng="6h", main_pr_diffs=[d6], loki_compared=False, points_loki=0)]
        text, v = self.verdict(rows)
        self.assertEqual(v["exit"], 0)
        self.assertIn("shorter range", text)

    def test_no_loki_range_with_an_unproven_difference_still_fails(self):
        d1 = "query ('sum by (x) (count_over_time({a=\"b\"} [1m]))', 'range', '1790864400000', '1790868000000', 'aa'): value differs"
        other = "resource /api/datasources/uid/*/resources/labels?end=2026-10-01T18%3A00%3A00Z: body: content differs (rows 3 vs 4)"
        rows = [row(rng="1h", main_pr_diffs=[d1], loki_main_n=1, loki_diffs=[]),
                row(rng="6h", main_pr_diffs=[d1, other], loki_compared=False, points_loki=0)]
        _, v = self.verdict(rows)
        self.assertEqual(v["exit"], 1)

    def test_a_regression_at_a_shorter_range_proves_nothing(self):
        d = "query ('{a=\"b\"}', 'range', '1790864400000', '1790868000000', 'aa'): content differs"
        rows = [row(rng="1h", main_pr_diffs=[d], loki_main_n=0, loki_diffs=["x"]),
                row(rng="6h", main_pr_diffs=[d], loki_compared=False, points_loki=0)]
        _, v = self.verdict(rows)
        self.assertEqual(v["exit"], 1)

    def test_configuration_difference_warns_not_fails(self):
        cfg = ["resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0)"]
        text, v = self.verdict([row(main_pr_config=cfg)])
        self.assertEqual(v["exit"], 0)
        self.assertIn("deployment configuration", text)

    def test_clean_run_passes(self):
        text, v = self.verdict([row()])
        self.assertEqual(v["exit"], 0)
        self.assertIn("Visual smoke: passed", text)
        self.assertTrue(text.startswith(comment.MARKER))

    def test_difference_with_no_loki_data_fails_unless_labelled(self):
        d = row(main_pr_diffs=["query x: value at 1: 1 vs 2"], loki_compared=False)
        text, v = self.verdict([d])
        self.assertEqual(v["exit"], 1)
        self.assertIn("no Loki data for this range", v["failures"][0])
        self.assertIn("![explore-a-1h](https://raw.githubusercontent.com/o/r/pr-visuals/pr-7/explore-a-1h.png?v=aaaaaaaa)", text)
        a = argparse.Namespace(**{**vars(ARGS), "expected_change": True})
        text, v = comment.render([d], {}, plan(("explore-a", "1h")), META, a)
        self.assertEqual(v["exit"], 0)
        self.assertEqual(v["expected"], 1)
        self.assertIn("expected change", text)
        self.assertIn("passed with warnings", text)

    def test_pr_closer_to_loki_is_an_improvement(self):
        d = row(main_pr_diffs=["q: base wrong"], loki_main_n=2, loki_diffs=[], points_loki=5)
        text, v = self.verdict([d])
        self.assertEqual(v["exit"], 0)
        self.assertEqual(v["improved"], 1)
        self.assertIn("improved (closer to Loki)", text)

    def test_base_query_error_the_pr_fixes_is_an_improvement(self):
        # The base rejects the query (400, the query row shows the parse error), the PR answers like Loki.
        d = row(main_pr_diffs=["q: base 400"], loki_main_n=1, errors_main=["400"],
                ui_main={"noData": 0, "banners": [], "panelErrors": 1})
        text, v = self.verdict([d])
        self.assertEqual(v["exit"], 0)
        self.assertEqual(v["improved"], 1)
        self.assertNotIn("panel error (", text)

    def test_pr_removing_a_base_difference_and_adding_none_is_an_improvement(self):
        # Base differs from Loki on the query and index/stats; the PR fixes the query, index/stats stays as on base.
        same = "resource index/stats: body differs"
        d = row(main_pr_diffs=["q: base wrong"], loki_main_n=2, loki_diffs=[same], loki_new=[], points_loki=5)
        text, v = self.verdict([d])
        self.assertEqual(v["exit"], 0)
        self.assertEqual(v["improved"], 1)
        # a new difference from Loki keeps it failing, even with fewer in total
        d = row(main_pr_diffs=["q: changed"], loki_main_n=3, loki_diffs=["q: new"], loki_new=["q: new"], points_loki=5)
        _, v = self.verdict([d])
        self.assertEqual(v["exit"], 1)
        # as many differences as the base is not an improvement
        d = row(main_pr_diffs=["q: changed"], loki_main_n=1, loki_diffs=[same], loki_new=[], points_loki=5)
        _, v = self.verdict([d])
        self.assertEqual(v["exit"], 1)

    def test_no_data_that_loki_shows_too_is_not_a_new_empty_panel(self):
        # The base showed a parse error with data around it; the PR and Loki both answer "No data".
        ui = {"noData": 1, "banners": [], "panelErrors": 0}
        d = row(main_pr_diffs=["q: base 400"], loki_main_n=1, loki_diffs=[], points_loki=5, points_pr=5,
                ui_pr=ui, ui_loki=ui)
        text, v = self.verdict([d])
        self.assertEqual(v["exit"], 0)
        self.assertNotIn("new empty panel", text)
        # Loki has data where the PR is empty: still a new empty panel
        d = row(main_pr_diffs=["q: changed"], loki_main_n=1, loki_diffs=[], points_loki=5, points_pr=5,
                ui_pr=ui, ui_loki={"noData": 0, "banners": [], "panelErrors": 0})
        text, v = self.verdict([d])
        self.assertEqual(v["exit"], 1)
        self.assertIn("new empty panel", text)

    def test_pr_diverging_from_loki_where_base_matched_fails_even_when_labelled(self):
        d = row(main_pr_diffs=["q: pr wrong"], loki_main_n=0, loki_diffs=["q: differs"], loki_new=["q: differs"], points_loki=5)
        a = argparse.Namespace(**{**vars(ARGS), "expected_change": True})
        _, v = comment.render([d], {}, plan(("explore-a", "1h")), META, a)
        self.assertEqual(v["exit"], 1)
        self.assertIn("regressed vs Loki", v["failures"][0])

    def test_neither_matching_loki_fails_unless_labelled(self):
        d = row(main_pr_diffs=["q: d"], loki_main_n=1, loki_diffs=["q: e"], loki_new=[], points_loki=5)
        _, v = self.verdict([d])
        self.assertEqual(v["exit"], 1)
        self.assertIn("neither build matches Loki", v["failures"][0])
        a = argparse.Namespace(**{**vars(ARGS), "expected_change": True})
        self.assertEqual(comment.render([d], {}, plan(("explore-a", "1h")), META, a)[1]["exit"], 0)

    def test_classify_requires_loki_points(self):
        self.assertEqual(comment.classify(row(points_loki=0)), "no-loki")
        self.assertEqual(comment.classify(row(loki_compared=False, points_loki=3)), "no-loki")

    def test_error_answers_on_the_pr_side_fail_even_when_the_base_has_them(self):
        d = row(errors_pr=["plugin unavailable"], errors_main=["plugin unavailable"])
        _, v = self.verdict([d])
        self.assertEqual(v["exit"], 1)
        self.assertIn("error answer", v["failures"][0])

    def test_pr_side_that_never_settled_fails(self):
        self.assertEqual(self.verdict([row(settled=False, settled_pr=False)])[1]["exit"], 1)
        text, v = self.verdict([row(settled=False, settled_pr=True)])  # only the base or Loki side: a warning
        self.assertEqual(v["exit"], 0)
        self.assertIn("a side did not settle", text)

    def test_difference_that_flipped_on_recapture_fails(self):
        meta = {**META, "flipped": ["explore-a 1h"]}
        text, v = comment.render([row()], {}, plan(("explore-a", "1h")), meta, ARGS)
        self.assertEqual(v["exit"], 1)
        self.assertIn("non-deterministic", text)

    def test_missing_loki_capture_warns_explicitly(self):
        text, v = self.verdict([row(loki_compared=False, loki_missing=True)])
        self.assertEqual(v["exit"], 0)
        self.assertIn("Loki was not captured", text)
        self.assertIn("not captured", text.split("| explore-a")[1])

    def test_pr_derived_text_cannot_inject_markup(self):
        evil = "x [click](http://evil.test) <img src=x onerror=1> @team |\n# h `c` *b* ![i](u)"
        d = row(page="explore-a", main_pr_diffs=[evil], loki_compared=False,
                ui_pr={"noData": 0, "banners": [evil], "panelErrors": 0})
        text, _ = self.verdict([d])
        for bad in ("](http://evil.test)", "<img", "@team", "![i]"):
            self.assertNotIn(bad, text.replace("\\" + bad, ""), bad)
        self.assertEqual(re.findall(r"(?<!\\)[<\[@]", text.split("| explore-a")[1].split("Gate:")[0]), [])  # table: every one escaped
        self.assertEqual(comment.esc("a|b\nc"), "a\\|b c")
        self.assertEqual(comment.esc_html('<a href="x">&'), "&lt;a href=&quot;x&quot;&gt;&amp;")

    def test_unsafe_names_get_no_image(self):
        self.assertEqual(comment.image(ARGS, "../x", META), "")

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

    def test_loki_differences_the_base_has_too_only_inform(self):
        text, v = self.verdict([row(loki_diffs=["query a: x"], loki_main_n=1, loki_new=[])])
        self.assertEqual(v["exit"], 0)
        self.assertIn("1 difference(s), 1 on base too", text)

    def test_explained_and_history_dependent_loki_differences_are_noted_not_counted(self):
        text, v = self.verdict([row(loki_explained=["resource x: by design -- explained: y"], loki_nondet=["resource patterns: z"])])
        self.assertEqual(v["exit"], 0)
        self.assertIn("identical (1 explained, 1 history-dependent)", text)

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

    def test_ui_problems_name_the_error_behind_a_banner(self):
        details = "Details\nTypeError: Cannot read properties of undefined (reading 'defaults')\n    at a (x.js:2:1)"
        found = comment.ui_problems({}, {"banners": ["Plugin failed to load"], "details": details})
        self.assertEqual(found, ["error banner only on the PR: Plugin failed to load "
                                 "(TypeError: Cannot read properties of undefined (reading 'defaults'))"])
        self.assertEqual(comment.ui_problems({}, {"banners": ["Plugin failed to load"]}),
                         ["error banner only on the PR: Plugin failed to load"])


if __name__ == "__main__":
    unittest.main()
