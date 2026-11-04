"""Each full measurement is its own history entry, keyed by run: two runs of one date are both kept and judged, the
rules that count consecutive runs follow the order they were measured in, and the README section states one run
(bench/ab/history.py, bench/ab/daily.py, conformance/scripts/perf_evidence.py)."""
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(REPO, "conformance", "scripts"))
import history  # noqa: E402
import over2x  # noqa: E402
import perf_evidence as pe  # noqa: E402
from test_alerts import SPEC, row, summary, timed  # noqa: E402

DAY = "2026-11-03"
EARLY, LATE = "2026-11-03T03:20:00Z", "2026-11-03T14:05:00Z"
SLOW, FAST = (timed(p50=2.0, loki_p50=0.4, cold=2.0), timed(p50=0.5, loki_p50=0.4, cold=0.4))


class RunsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "control.jsonl")

    def run_at(self, at, commit, *rows, date=DAY, run="", name="control", profile=None, **extra):
        """One set's line of a run that started at `at`."""
        extra = {**({"profile": profile} if profile else {}), **extra}
        s = summary(date, *rows, name=name, measured_at=at, **({"run": run} if run else {}), **extra)
        return history.append(s, self.dir, commit)[1]

    def listed(self, *rows):
        over2x.write_baseline(list(rows), os.path.join(self.dir, "over-2x-loki.json"))

    def commits(self):
        return [(e["measured_at"], e["commit"]) for e in history.read(self.path)]

    # --- two runs on one date -----------------------------------------------------------------------------------
    def test_two_runs_on_one_date_are_both_kept_and_a_repeat_of_one_is_not_added_twice(self):
        self.assertTrue(self.run_at(EARLY, "aaa", row(), run="10.1"))
        self.assertTrue(self.run_at(LATE, "bbb", row(), run="11.1"))
        self.assertFalse(self.run_at(LATE, "bbb", row(), run="11.1"))      # the same append again
        self.assertTrue(self.run_at(LATE, "bbb", row(), run="11.2"))       # a re-run of a CI run: its own attempt
        self.assertEqual(self.commits(), [(EARLY, "aaa"), (LATE, "bbb"), (LATE, "bbb")])
        self.assertEqual([e["run"] for e in history.read(self.path)], ["10.1", "11.1", "11.2"])

    def test_the_order_is_the_order_measured_however_the_lines_were_merged(self):
        self.run_at(LATE, "bbb", row())
        self.run_at(EARLY, "aaa", row())   # merged after the later run
        self.assertEqual(self.commits(), [(EARLY, "aaa"), (LATE, "bbb")])
        self.assertEqual(history.last_commit(self.dir, {"ranges": SPEC["ranges"], "sets": {"control": SPEC["sets"]["control"]}}),
                         "bbb")

    def test_old_lines_without_a_stamp_load_sort_by_date_and_stand_for_their_run(self):
        old = history.entry(summary("2026-11-02", row()), "old")
        self.assertNotIn("measured_at", old)
        self.assertNotIn("run", old)
        with open(self.path, "w") as f:
            f.write(json.dumps(old) + "\n")
        self.run_at(EARLY, "aaa", row())
        self.assertEqual([history.stamp(e) for e in history.read(self.path)], ["2026-11-02", EARLY])
        self.assertFalse(history.append(summary("2026-11-02", row()), self.dir, "old")[1])   # the old rule's repeat
        self.assertTrue(history.append(summary("2026-11-02", row()), self.dir, "other")[1])  # another commit that date

    def test_a_run_that_started_late_in_the_day_keeps_its_date_for_display_only(self):
        self.run_at("2026-11-04T00:10:00Z", "bbb", row(), date="2026-11-03")
        self.run_at(EARLY, "aaa", row())
        self.assertEqual([history.stamp(e) for e in history.read(self.path)], [EARLY, "2026-11-04T00:10:00Z"])

    # --- both are judged -------------------------------------------------------------------------------------
    def test_a_second_run_of_the_date_is_judged_against_the_first(self):
        self.run_at(EARLY, "aaa", row())
        self.run_at(LATE, "bbb", row(parity="differs"))
        a = history.alerts(self.dir, spec=SPEC)
        self.assertEqual([(r["kind"], r["detail"], r["commits"]) for r in a["regressions"]],
                         [("result", "result vs Loki: same → differs", "aaa..bbb")])
        self.assertEqual(a["range"]["control"], "aaa..bbb")

    def test_the_trend_shows_the_newest_run_and_compares_days_by_date(self):
        self.run_at(EARLY, "aaa", row(p50=0.5))
        self.run_at(LATE, "bbb", row(p50=0.9))
        self.run_at("2026-11-04T03:20:00Z", "ccc", row(p50=0.9), date="2026-11-04")
        text = history.trend(self.dir)
        self.assertIn("Latest 2026-11-04T03:20:00Z (`ccc`", text)
        self.assertIn("previous 2026-11-03;", text)   # a calendar window: the newest run of an earlier date

    # --- consecutive-run rules across same-date runs -------------------------------------------------------
    def test_two_runs_of_one_date_add_and_remove_a_row_to_the_above_2x_list(self):
        self.listed()
        self.run_at(EARLY, "aaa", SLOW)
        self.assertEqual(len(history.alerts(self.dir, spec=SPEC)["over_2x_new"]), 2)   # cold and warm: pending
        self.assertEqual(history.alerts(self.dir, spec=SPEC)["over_2x_added"], [])
        self.run_at(LATE, "bbb", SLOW)   # a second fresh-stack sample of the day confirms it
        history.write_alerts(self.dir, spec=SPEC)
        listed, _ = over2x.read_baseline(os.path.join(self.dir, "over-2x-loki.json"))
        self.assertEqual({(k[4], r["since"], r["commit"]) for k, r in listed.items()},
                         {("cold", DAY, "aaa"), ("warm", DAY, "aaa")})   # counted from the first of the two runs
        self.run_at("2026-11-03T20:00:00Z", "ccc", FAST)
        self.assertEqual(len(history.write_alerts(self.dir, spec=SPEC)["over_2x_pending"]), 2)
        self.run_at("2026-11-03T23:00:00Z", "ddd", FAST)
        self.assertEqual(len(history.write_alerts(self.dir, spec=SPEC)["over_2x_removed"]), 2)
        self.assertEqual(over2x.read_baseline(os.path.join(self.dir, "over-2x-loki.json"))[0], {})

    def test_one_run_written_twice_is_one_sample_not_two(self):
        self.listed()
        self.run_at(EARLY, "aaa", SLOW, run="10.1")
        self.run_at(EARLY, "aaa", SLOW, run="10.1")   # the same append again (a retried step)
        a = history.alerts(self.dir, spec=SPEC)
        self.assertEqual((len(a["over_2x_new"]), a["over_2x_added"]), (2, []))

    def test_a_noisy_run_between_two_quiet_ones_of_one_date_is_never_listed(self):
        self.listed()
        for at, r in ((EARLY, FAST), ("2026-11-03T08:00:00Z", SLOW), ("2026-11-03T12:00:00Z", FAST)):
            self.run_at(at, "aaa", r)
            self.assertEqual(history.write_alerts(self.dir, spec=SPEC)["over_2x"], [])

    def test_the_profile_a_later_run_did_not_run_is_stale_though_its_line_has_the_same_date(self):
        listed = [{"profile": "aligned", "set": "control", "shape": "logs | json", "range": "1h", "mode": m, "ratio": 5.0,
                   "proxy_s": 2.0, "loki_s": 0.4, "since": "2026-11-01"} for m in ("cold", "warm")]
        self.listed(*listed)
        self.run_at(EARLY, "aaa", FAST, profile="aligned")
        self.run_at(EARLY, "aaa", FAST, profile="loki-defaults")
        self.assertEqual(history.alerts(self.dir, spec=SPEC)["over_2x_stale"], [])       # one run: both current
        self.run_at(LATE, "bbb", FAST, profile="loki-defaults")                            # the aligned one did not run
        a = history.alerts(self.dir, spec=SPEC)
        self.assertEqual([(r["profile"], r["last"], r["current"]) for r in a["over_2x_stale"]],
                         [("aligned", DAY, DAY)])
        self.assertEqual(a["over_2x_removed"], [])   # nothing judged in it

    def test_the_ratchet_loss_rule_counts_runs_not_dates(self):
        slow_cold = row(cold=2.0, loki_cold=0.9)
        self.run_at(EARLY, "aaa", row())
        history.write_alerts(self.dir, spec=SPEC)   # ratchets the row
        self.run_at(LATE, "bbb", slow_cold)
        self.assertEqual(history.alerts(self.dir, spec=SPEC)["ratchet_lost"], [])   # one slow cold run: one sample
        self.run_at("2026-11-03T20:00:00Z", "ccc", slow_cold)
        self.assertEqual(len(history.alerts(self.dir, spec=SPEC)["ratchet_lost"]), 1)

    def test_the_replay_goes_run_by_run(self):
        spec = {"ranges": SPEC["ranges"], "sets": {"control": SPEC["sets"]["control"]}}
        for at in (EARLY, LATE):
            self.run_at(at, "aaa", SLOW, profile="loki-defaults")
        rows, left, added, pending = history.replay_over_2x(self.dir, spec, [])
        self.assertEqual(sorted((r["mode"], r["added"]) for r in added), [("cold", DAY), ("warm", DAY)])
        self.assertEqual(len(rows), 2)

    # --- acceptance.json is one entry per run -----------------------------------------------------------------
    def test_acceptance_has_one_entry_per_run(self):
        spec = {"ranges": SPEC["ranges"], "sets": {"control": SPEC["sets"]["control"]}}
        old = history.entry(summary("2026-11-02", row()), "old")
        with open(self.path, "w") as f:
            f.write(json.dumps(old) + "\n")
        self.run_at(EARLY, "aaa", row(), run="10.1")
        self.run_at(LATE, "bbb", row(), run="11.1")
        days = history.acceptance_history(self.dir, spec)["days"]
        self.assertEqual([(d["date"], d.get("measured_at"), d["commits"]) for d in days],
                         [("2026-11-02", None, ["old"]), (DAY, EARLY, ["aaa"]), (DAY, LATE, ["bbb"])])


class ShownRunTest(unittest.TestCase):
    """The README section states one run: one commit, one stamp, whatever else the results directory holds."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.patch = pe.RESULTS
        pe.RESULTS = self.dir
        self.spec = {"control": {"shapes": [{"name": "logs | json"}, {"name": "other"}]}}

    def tearDown(self):
        pe.RESULTS = self.patch

    def save(self, name, at, commit, profile="loki-defaults", shape="logs | json", date=DAY, **extra):
        s = summary(date, row(shape=shape), profile=profile, commit=commit, **({"measured_at": at} if at else {}), **extra)
        with open(os.path.join(self.dir, f"daily-{name}.json"), "w") as f:
            json.dump(s, f)

    def test_summaries_of_another_run_of_the_same_date_are_left_out(self):
        self.save("a", EARLY, "aaa", shape="other")
        self.save("b", LATE, "bbb")
        latest, date, profile = pe.daily_runs(self.spec, [])
        self.assertEqual([(k, s["commit"]) for k, (_, s, _) in latest.items()], [(("control", "logs | json", "1h"), "bbb")])
        self.assertEqual((date, profile), (DAY, "loki-defaults"))

    def test_a_profile_of_the_run_is_chosen_from_that_run_only(self):
        self.save("a", EARLY, "aaa", profile="loki-defaults", shape="other")
        self.save("a.aligned", LATE, "bbb", profile="aligned")
        latest, _, profile = pe.daily_runs(self.spec, [])
        self.assertEqual(profile, "aligned")
        self.assertEqual({s["commit"] for _, s, _ in latest.values()}, {"bbb"})

    def test_results_saved_before_runs_were_stamped_are_chosen_by_their_date(self):
        self.save("a", None, "aaa", date="2026-11-02")
        self.save("b", None, "bbb", date="2026-11-03")
        latest, date, _ = pe.daily_runs(self.spec, [])
        self.assertEqual((date, {s["commit"] for _, s, _ in latest.values()}), (DAY, {"bbb"}))

    def test_a_stamped_run_is_newer_than_an_unstamped_summary_of_its_date(self):
        self.save("a", None, "aaa")
        self.save("b", EARLY, "bbb", shape="other")
        latest, _, _ = pe.daily_runs(self.spec, [])
        self.assertEqual({s["commit"] for _, s, _ in latest.values()}, {"bbb"})

    def test_the_other_profile_rows_are_those_of_the_run_shown_not_of_the_date(self):
        self.save("a", EARLY, "aaa", profile="aligned", shape="other")
        self.save("b", LATE, "bbb", profile="loki-defaults")
        latest, date, profile = pe.daily_runs(self.spec, [])
        runs = pe.profile_runs(self.spec, [], date, pe.run_key(next(iter(latest.values()))[1]))
        self.assertEqual(list(runs), ["loki-defaults"])   # the aligned summary is another run's
        self.assertEqual(list(pe.profile_runs(self.spec, [], date)), ["aligned", "loki-defaults"])   # the date alone mixes

    def test_a_profile_only_an_earlier_run_of_the_date_ran_is_not_reported_as_run_today(self):
        days = [{"date": DAY, "commits": ["aaa"], "profiles": {"aligned": {}, "loki-defaults": {}}},
                {"date": DAY, "commits": ["bbb"], "profiles": {"loki-defaults": {}}}]
        last = pe.profile_last(days, {"loki-defaults"}, DAY)
        self.assertEqual(last["loki-defaults"], DAY)
        self.assertNotEqual(last["aligned"], DAY)
        self.assertIn("earlier run", last["aligned"])
        text = pe.other_profile_sentence({"profile_last": last, "date": DAY}, {"aligned": "n", "loki-defaults": "n"}, ["aligned"])
        self.assertIn("did not run on 2026-11-03", text)
        # A run that did run it, or a date it ran on alone, reads as before.
        self.assertEqual(pe.profile_last(days, {"aligned", "loki-defaults"}, DAY)["aligned"], DAY)


if __name__ == "__main__":
    unittest.main()
