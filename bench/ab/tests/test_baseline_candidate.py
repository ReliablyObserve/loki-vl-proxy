"""Unit tests for bench/ab/baseline_candidate.py and selection.select_all: the baseline run's selection and
the files it leaves for a person to accept (python3 -m unittest discover -s bench/ab/tests)."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import baseline_candidate as bc  # noqa: E402
import over2x  # noqa: E402
import pr_smoke  # noqa: E402
import selection  # noqa: E402
from test_over2x import listed, row, summary  # noqa: E402

SPEC = {
    "ranges": {"1h": [3600, 5]},
    "sets": {
        "control": {"ranges": ["1h"], "shapes": [{"name": "a"}, {"name": "B, with comma"}]},
        "empty": {"ranges": ["1h"], "shapes": []},
        "feature": {"ranges": ["1h"], "shapes": [{"name": "c"}]},
    },
}


class SelectAllTest(unittest.TestCase):
    def test_every_shape_of_every_set_in_order(self):
        got = selection.select_all(SPEC)
        self.assertTrue(got["run"])
        self.assertTrue(got["baseline"])
        self.assertEqual(got["sets"], {"control": ["a", "B, with comma"], "feature": ["c"]})

    def test_every_shape_has_a_reason(self):
        got = selection.select_all(SPEC)
        self.assertEqual(sorted(got["why"]), ["control/B, with comma", "control/a", "feature/c"])
        self.assertTrue(all(v == [selection.BASELINE_WHY] for v in got["why"].values()))

    def test_the_real_spec_selects_all_of_it(self):
        spec = selection.load_shapes()
        got = selection.select_all()
        self.assertEqual(sum(len(v) for v in got["sets"].values()), sum(len(v["shapes"]) for v in spec["sets"].values()))
        self.assertEqual(set(got["sets"]), {k for k, v in spec["sets"].items() if v["shapes"]})

    def test_it_is_not_the_changed_files_selection(self):
        # A docs-only change selects nothing; the baseline run is never empty.
        self.assertFalse(selection.select(["docs/a.md"], spec=SPEC, items={})["run"])
        self.assertTrue(selection.select_all(SPEC)["run"])


def slow(shape="slow", rng="1h"):  # proxy 5x Loki on warm and cold: above 2x
    return row(shape, rng, pr=0.5, loki=0.1, pr_cold=0.5, loki_cold=0.1)


def fast(shape="fast", rng="1h"):  # proxy well below Loki
    return row(shape, rng, pr=0.05, loki=0.5, pr_cold=0.05, loki_cold=0.5)


class CandidateTest(unittest.TestCase):
    def test_over_2x_rows_are_the_rows_this_run_measured_above(self):
        rows, added, removed = bc.candidate_over_2x([summary(slow(), fast())], {})
        self.assertEqual(sorted((r["shape"], r["mode"]) for r in rows), [("slow", "cold"), ("slow", "warm")])
        self.assertEqual(len(added), 2)
        self.assertEqual(removed, [])

    def test_a_committed_row_of_an_unmeasured_range_is_kept(self):
        committed = {**listed("old", "24h", "warm", 4.0), **listed("old", "1h", "warm", 4.0)}
        rows, added, removed = bc.candidate_over_2x([summary(fast("old", "1h"))], committed)
        keys = {over2x.key(r) for r in rows}
        self.assertIn(("control", "old", "24h", "warm"), keys)
        self.assertNotIn(("control", "old", "1h", "warm"), keys)
        self.assertEqual(removed, [("control", "old", "1h", "warm")])
        self.assertEqual(added, [])

    def test_a_row_still_above_stays_and_is_not_added_again(self):
        committed = listed("slow", "1h", "warm", 6.0)
        rows, added, removed = bc.candidate_over_2x([summary(slow())], committed)
        self.assertNotIn(("control", "slow", "1h", "warm"), added)
        self.assertIn(("control", "slow", "1h", "cold"), added)
        self.assertEqual(removed, [])

    def test_the_distribution_counts_comparable_rows_only(self):
        loki_down = row("down", status=("200", "200", "500"))
        day = bc.acceptance_day([summary(slow(), fast(), loki_down)], "2026-10-28", "abcdef0123456789")
        self.assertEqual(day["date"], "2026-10-28")
        self.assertEqual(day["commits"], ["abcdef0123"])
        self.assertEqual(day["warm"]["rows"], 2)
        self.assertEqual((day["warm"]["below"], day["warm"]["above"]), (1, 1))
        self.assertEqual(day["warm"]["share_below"], 50.0)
        self.assertEqual(set(day["warm"]), {"rows", "share_below", "below", "close", "between", "above"})

    def test_write_produces_the_files_and_commits_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = bc.write(tmp, [summary(slow(), fast())], {}, {"date": "2026-10-27", "warm": {"share_below": 40.0}},
                            "2026-10-28", "c0ffee", ["the `baseline` label is set"], 600)
            self.assertEqual(sorted(os.listdir(tmp)), ["acceptance-day.json", "candidate.md", "meta.json", "over-2x-loki.json"])
            parsed, problem = over2x.read_baseline(os.path.join(tmp, "over-2x-loki.json"))
            self.assertEqual(problem, "")
            self.assertEqual(len(parsed), 2)
            with open(os.path.join(tmp, "acceptance-day.json")) as f:
                days = json.load(f)["days"]
            self.assertEqual(len(days), 1)
            self.assertIn("New baseline candidate", text)
            self.assertIn("Nothing was committed", text)
            self.assertIn("40.0% (2026-10-27)", text)
            self.assertIn("the `baseline` label is set", text)
            self.assertIn("next daily run", text)  # acceptance.json is rebuilt from the history, not replaced
            self.assertIn("not the same ones", text)

    def test_an_invalid_run_writes_no_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = bc.write(tmp, [summary(slow(), valid=False)], {}, {})
            self.assertEqual(os.listdir(tmp), ["candidate.md"])
            self.assertIn("Not written", text)

    def test_no_summaries_write_no_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIn("Not written", bc.write(tmp, [], {}, {}))
            self.assertEqual(os.listdir(tmp), ["candidate.md"])

    def test_the_committed_baseline_file_is_never_the_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            bc.write(tmp, [summary(slow())], {}, {})
            self.assertNotEqual(os.path.abspath(tmp), os.path.abspath(os.path.dirname(over2x.BASELINE)))


class PrSmokeTest(unittest.TestCase):
    """pr_smoke.main with the stack mocked out: the flag writes the candidate and adds the section."""

    def run_main(self, tmp, *extra):
        sel = os.path.join(tmp, "sel.json")
        with open(sel, "w") as f:
            json.dump(selection.select_all(SPEC), f)
        out = os.path.join(tmp, "out")
        argv = ["pr_smoke.py", "--base", "HEAD", "--selection", sel, "--out", out, *extra]
        with unittest.mock.patch.object(sys, "argv", argv), unittest.mock.patch.object(pr_smoke, "measure", return_value=[summary(slow(), fast())]), \
                contextlib.redirect_stdout(io.StringIO()):
            code = pr_smoke.main()
        with open(os.path.join(out, "comment.md")) as f:
            return code, out, f.read()

    def test_the_flag_writes_the_candidate_and_ends_the_comment_with_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            cand = os.path.join(tmp, "cand")
            _, _, text = self.run_main(tmp, "--baseline-candidate", cand)
            self.assertIn("New baseline candidate", text)
            self.assertTrue(text.rstrip().endswith("</details>"))
            self.assertEqual(sorted(os.listdir(cand)), ["acceptance-day.json", "candidate.md", "meta.json", "over-2x-loki.json"])

    def test_without_the_flag_the_comment_has_no_candidate_and_nothing_is_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, out, text = self.run_main(tmp)
            self.assertNotIn("New baseline candidate", text)
            self.assertFalse(os.path.exists(os.path.join(out, "baseline-candidate")))

    def test_the_committed_baseline_is_untouched(self):
        with open(over2x.BASELINE) as f:
            before = f.read()
        with tempfile.TemporaryDirectory() as tmp:
            self.run_main(tmp, "--baseline-candidate", os.path.join(tmp, "cand"))
        with open(over2x.BASELINE) as f:
            self.assertEqual(f.read(), before)


class FitTest(unittest.TestCase):
    def test_a_short_comment_is_untouched(self):
        self.assertEqual(bc.fit("a <details>x</details> b", 100), "a <details>x</details> b")

    def test_the_largest_blocks_go_first_and_the_text_fits(self):
        big, small = "<details>" + "x" * 5000 + "</details>", "<details>" + "y" * 200 + "</details>"
        out = bc.fit("head " + small + " mid " + big + " tail", 1000)
        self.assertLessEqual(len(out), 1000)
        self.assertIn("y" * 200, out)
        self.assertNotIn("x" * 100, out)
        self.assertIn("omitted to fit", out)

    def test_the_candidate_section_is_never_cut(self):
        section = "<details open><summary>New baseline candidate</summary>" + "z" * 3000 + "</details>"
        out = bc.fit("<details>" + "x" * 5000 + "</details>" + section, 4000)
        self.assertIn("z" * 3000, out)


if __name__ == "__main__":
    unittest.main()
