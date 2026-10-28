"""Unit tests for the generated "Quality and performance vs Loki" section (conformance/scripts/perf_evidence.py,
--quality-summary): the marker replacement, the figures it renders from given inputs, the Loki profile choice, the Loki
version read from the compose pin, and the staleness check on the committed README.md and docs page."""
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "conformance", "scripts"))
import perf_evidence as pe  # noqa: E402

START, END = pe.SUMMARY_START, pe.SUMMARY_END
TARGETS = ("README.md", "docs/real-window-compatibility-gaps.md")


def workload(pairs, comparable, faster, par, slower, cold=(0.1, 0.2), warm=(0.03, 0.04)):
    return {"pairs": pairs, "comparable": comparable, "cold": {"faster": faster, "par": par, "slower": slower},
            "cand_cold": [cold[0]], "ref_cold": [cold[1]], "cand_warm": [warm[0]], "ref_warm": [warm[1]]}


def inputs(**changes):
    q = {
        "loki": "3.7.9", "date": "2026-10-28", "profile": "aligned", "commits": ["507915a5d2aaaa"], "sets": 25,
        "ranges": ["1h", "24h"],
        "summary": {"pairs": 100, "comparable": 80, "results": {"same": 70, "differs": 25, "not compared": 5},
                    "cold": {"faster": 40, "par": 30, "slower": 10},
                    "workloads": {pe.WORKLOADS[0]: workload(10, 2, 0, 1, 1),
                                  pe.WORKLOADS[1]: workload(20, 20, 10, 6, 4),
                                  pe.WORKLOADS[2]: workload(20, 10, 1, 5, 4),
                                  pe.WORKLOADS[3]: workload(50, 48, 29, 19, 0)},
                    "metadata": {"Labels (label browser)": {"pairs": 4, "served": 4, "cand_warm": [0.01],
                                                            "ref_warm": [0.03]}}},
        "distribution": {"cold": {"rows": 80, "below": 40, "close": 20, "between": 10, "above": 10, "share_below": 50.0},
                         "warm": {"rows": 90, "below": 9, "close": 71, "between": 5, "above": 5, "share_below": 10.0}},
        "targets": {"share_below": "most queries below Loki (above 50%)"},
        "over": {"rows": [
            {"mode": "cold", "shape": "slow one", "range": "24h", "proxy_s": 9.0, "loki_s": 1.0},
            {"mode": "cold", "shape": "slow two", "range": "6h", "proxy_s": 3.0, "loki_s": 1.0},
            {"mode": "warm", "shape": "repeat one", "range": "1h", "proxy_s": 2.0, "loki_s": 0.5}]},
        "blocks": {"a": {"locked": True}, "b": {"locked": False}},
        "stats": {"endpoints": 33, "implemented": 23, "with_differential_tests": 18, "proven": 11,
                  "execution": {"native_vl": 6}},
        "states": {"proven": 6, "partial": 12, "gap": 3},
        "differences": [
            {"id": "x/open-one", "title": "`| keep` drops a label (open)", "status": "open", "impact": "explore-visible"},
            {"id": "x/open-two", "title": "An ungrouped sum is empty (open)", "status": "open", "impact": "explore-visible"},
            {"id": "x/doc", "title": "kept", "status": "documented", "impact": "api-only"},
            {"id": "x/fixed", "title": "fixed one (fixed)", "status": "fixed", "impact": "explore-visible"}],
        "claims": ["a zero divisor is NaN"], "ranked_signature": True,
    }
    q.update(changes)
    return q


class MarkerTest(unittest.TestCase):
    def test_text_outside_the_markers_is_untouched_and_the_replacement_is_idempotent(self):
        text = f"head\n\n## Section\n\n{START}\n\nold body\n\n{END}\n\ntail\n"
        once = pe.replace_section(text, "new body")
        self.assertEqual(once, f"head\n\n## Section\n\n{START}\n\nnew body\n\n{END}\n\ntail\n")
        self.assertEqual(pe.replace_section(once, "new body"), once)

    def test_missing_duplicate_or_reversed_markers_are_refused(self):
        for text in ("no markers", f"{START}\nonly start", f"only end\n{END}", f"{START}\n{END}\n{START}\n{END}",
                     f"{END}\n{START}"):
            with self.assertRaises(ValueError, msg=text):
                pe.replace_section(text, "body")

    def test_writing_a_file_without_markers_fails_and_leaves_it_alone(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "page.md")
            with open(path, "w") as f:
                f.write("# no markers\n")
            self.assertEqual(pe.write_quality_summary(path), 1)
            with open(path) as f:
                self.assertEqual(f.read(), "# no markers\n")


class VersionTest(unittest.TestCase):
    def test_the_loki_version_is_the_default_of_loki_image(self):
        self.assertEqual(pe.pinned_loki("image: ${LOKI_IMAGE:-grafana/loki:3.7.9}\n"), "3.7.9")

    def test_a_compose_file_without_the_pin_is_an_error(self):
        with self.assertRaises(ValueError):
            pe.pinned_loki("image: grafana/loki:3.7.9\n")


class RenderTest(unittest.TestCase):
    def test_states_the_position_with_dates_version_and_counts(self):
        text = pe.render_quality_summary(inputs())
        for want in ("Loki v3.7.9", "measured on 2026-10-28 at commit `507915a5d2`", "25 query-shape sets over 1h to 24h",
                     "**40 of 80** comparable pairs are faster (50%)", "**30 of 80** pairs are within noise (38%)",
                     "**10 of 80** pairs are slower (12%)", "**70 of 100** pairs return the same result as Loki (70%)",
                     "**25 of 100** pairs return a different result", "**50.0%** (40 of 80 rows)",
                     "**3 shape, range and mode rows are on the above-2× list**",
                     "1 of 2 covered registry items are locked", "| Behaviour and feature items | 6 proven, 12 partial, 3 gaps |",
                     "**3**: 2 open, 1 documented, 0 owner-kept, 0 upstream",
                     "A zero divisor is NaN.", pe.SCOPE_SENTENCE):
            self.assertIn(want, text)

    def test_says_which_profile_is_shown_and_that_the_stack_differs_from_loki_defaults(self):
        text = pe.render_quality_summary(inputs())
        self.assertIn("Loki profile shown: `aligned`", text)
        self.assertIn("differ from Loki's own defaults (align_queries_with_step off", text)
        shown = pe.render_quality_summary(inputs(profile="loki-defaults"))
        self.assertIn("Loki profile shown: `loki-defaults`", shown)
        self.assertIn("The `aligned` profile", shown)

    def test_the_top_open_differences_are_ranked_and_exclude_fixed_rows(self):
        text = pe.render_quality_summary(inputs())
        self.assertIn("The 2 open differences with the most impact (all explore-visible): "
                      "`x/open-one` (`| keep` drops a label), `x/open-two` (an ungrouped sum is empty)", text)
        self.assertNotIn("x/fixed", text)
        self.assertNotIn("x/doc`", text.split("most impact")[1])

    def test_largest_gaps_come_from_the_above_2x_list_by_seconds_behind(self):
        text = pe.render_quality_summary(inputs())
        self.assertIn("Largest gaps on that list, first request: `slow one` over 24h, **9.00 s** against **1.00 s**; "
                      "`slow two` over 6h", text)
        self.assertIn("Largest gaps on that list, repeat: `repeat one` over 1h", text)

    def test_no_history_wording(self):
        text = pe.render_quality_summary(inputs()).lower()
        for word in ("fixed since", "changelog", "pull request", "#8"):
            self.assertNotIn(word, text)

    def test_links_are_relative_to_where_the_section_is_published(self):
        readme, docs = pe.render_quality_summary(inputs()), pe.render_quality_summary(inputs(), True)
        self.assertIn("(docs/performance.md#current-position-versus-loki)", readme)
        self.assertIn("(conformance/reports/parity-gaps.md)", readme)
        self.assertIn("(performance.md#current-position-versus-loki)", docs)
        self.assertIn("(https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/parity-gaps.md)", docs)
        self.assertIn("(compatibility-coverage.md)", docs)

    def test_the_ranked_answer_note_needs_its_test(self):
        self.assertIn("ranked points", pe.render_quality_summary(inputs()))
        self.assertNotIn("ranked points", pe.render_quality_summary(inputs(ranked_signature=False)))


class DailyRunsTest(unittest.TestCase):
    def setUp(self):
        self._results = pe.RESULTS
        self._tmp = tempfile.TemporaryDirectory()
        pe.RESULTS = self._tmp.name

    def tearDown(self):
        pe.RESULTS = self._results
        self._tmp.cleanup()

    def save(self, name, date, profile=None, valid=True, set_name="s"):
        summary = {"set": set_name, "date": date, "valid": valid, "label": name, "candidate": "main",
                   "reference": "loki", "baseline": "release",
                   "rows": [{"shape": "a", "range": "1h", "status": {}, "cold": {}, "p50": {}, "verdict": "same"}]}
        if profile:
            summary["profile"] = profile
        with open(os.path.join(self._tmp.name, name), "w") as f:
            json.dump(summary, f)

    def run_daily(self):
        return pe.daily_runs({"s": {"shapes": [{"name": "a"}]}}, [])

    def test_the_newest_valid_date_and_the_aligned_profile_are_chosen(self):
        self.save("daily-s.json", "2026-10-27")
        self.save("daily-t.json", "2026-10-28", valid=False, set_name="s")
        latest, date, profile = self.run_daily()
        self.assertEqual((date, profile, len(latest)), ("2026-10-27", "aligned", 1))

    def test_loki_defaults_is_shown_when_that_profile_ran_that_day(self):
        self.save("daily-s.json", "2026-10-28")
        self.save("daily-s.loki-defaults.json", "2026-10-28", profile="loki-defaults")
        latest, date, profile = self.run_daily()
        self.assertEqual((date, profile), ("2026-10-28", "loki-defaults"))
        self.assertEqual({s["profile"] for _, s, _ in latest.values()}, {"loki-defaults"})

    def test_no_valid_daily_summary_is_an_error(self):
        self.save("daily-s.json", "2026-10-28", valid=False)
        with self.assertRaises(ValueError):
            self.run_daily()


class CommittedSectionTest(unittest.TestCase):
    """The staleness check the conformance gate runs: the committed text is what the generator renders now."""

    def setUp(self):
        self._cwd = os.getcwd()
        os.chdir(REPO)

    def tearDown(self):
        os.chdir(self._cwd)

    def test_the_committed_sections_are_current(self):
        q = pe.quality_inputs()
        for target in TARGETS:
            with open(target, encoding="utf-8") as f:
                committed = f.read()
            body = pe.render_quality_summary(q, target.startswith("docs/"))
            self.assertEqual(pe.replace_section(committed, body), committed, f"{target} is stale")

    def test_a_changed_figure_is_detected_as_stale(self):
        q = pe.quality_inputs()
        with open("README.md", encoding="utf-8") as f:
            committed = f.read()
        q["summary"]["results"]["same"] += 1
        self.assertNotEqual(pe.replace_section(committed, pe.render_quality_summary(q)), committed)

    def test_the_pinned_loki_is_the_compose_default(self):
        with open(pe.COMPOSE) as f:
            self.assertEqual(pe.pinned_loki(f.read()), pe.quality_inputs()["loki"])

    def test_the_section_is_generated_in_both_files_with_one_pair_of_markers(self):
        for target in TARGETS:
            with open(target, encoding="utf-8") as f:
                text = f.read()
            self.assertEqual((text.count(START), text.count(END)), (1, 1), target)
            self.assertIn("## Quality and performance vs Loki (current)", text)


if __name__ == "__main__":
    unittest.main()
