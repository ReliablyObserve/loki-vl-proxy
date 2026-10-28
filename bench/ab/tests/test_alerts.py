"""Unit tests for the alerts and the ratchet in bench/ab/history.py."""
import glob
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import history  # noqa: E402

COUNTERS = {"cold": {"result": {"entriesFound": 0, "bytesReceived": 0}, "statsResult": {"entriesFound": 2}}}
SHAPE, RANGE = "logs | json", "1h"
# Sets of the tests; a full measurement of one is 7 runs and its 1h range.
SPEC = {"ranges": {"1h": [3600, 5], "24h": [86400, 60]}, "sets": {n: {"ranges": ["1h"]} for n in ("control", "a", "b")}}


def alerts(directory, **kw):
    return history.alerts(directory, spec=SPEC, **kw)


def write(directory, **kw):
    return history.write_alerts(directory, spec=SPEC, **kw)


def row(p50=0.5, cold=0.4, loki_cold=0.9, status="200", parity="same", verdict="same", served=False, reused=False,
        counters=True, loki_status="200", shape=SHAPE):
    r = {"shape": shape, "range": RANGE, "verdict": verdict,
         "p50": {"release": 0.5, "main": p50, "loki": 0.4}, "cold": {"release": 1.0, "main": cold, "loki": loki_cold},
         "status": {"release": "200", "main": status, "loki": loki_status}, "parity_vs_reference": parity}
    if counters:
        r["cache"] = {"loki": dict(COUNTERS)}
    if served:
        r["cold_cache_served"] = ["loki"]
    if reused:
        r["loki_cold_reused_from"] = {"set": "other", "shape": "x"}
    return r


def summary(date, *rows, name="control", runs=7, **extra):
    return {"set": name, "label": "daily", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": runs, "valid": True, "restart_before": "0", "restart_after": "0",
            "rows": list(rows or [row()]), **extra}


class AlertsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.n = 0

    def day(self, *rows, name="control", commit=None, runs=7):
        self.n += 1
        history.append(summary(f"2026-10-{self.n:02d}", *rows, name=name, runs=runs), self.dir,
                       commit or f"c{self.n}" * 12, SPEC)

    def kinds(self, key, **kw):
        return [(a["kind"], a["detail"]) for a in alerts(self.dir, **kw)[key]]

    # Loki profiles: a file and a history per profile; the aligned one is the one every older line is in.
    def profile_day(self, profile, *rows, commit=None, **extra):
        self.n += 1
        history.append(summary(f"2026-10-{self.n:02d}", *rows, profile=profile, **extra), self.dir,
                       commit or f"c{self.n}" * 12, SPEC)

    def test_a_line_on_a_recreated_loki_is_flagged_and_enters_no_ratchet(self):
        self.profile_day("aligned", row(), loki_restarted=True)
        line = history.read(os.path.join(self.dir, "control.jsonl"))[0]
        self.assertTrue(line["loki_restarted"])
        a = alerts(self.dir)
        self.assertEqual(a["ratchet_added"], [])
        self.profile_day("loki-defaults", row(), commit="d" * 12)   # the seeded Loki's line does
        self.assertEqual(len(alerts(self.dir)["ratchet_added"]), 1)
        self.profile_day("aligned", row(parity="differs"), loki_restarted=True)   # a loss on the recreated Loki: not read
        write(self.dir)
        self.profile_day("aligned", row(parity="differs"), loki_restarted=True)
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])
        self.assertNotIn("loki_restarted", history.read(os.path.join(self.dir, "control.loki-defaults.jsonl"))[0])

    def test_each_profile_has_its_own_history_file_and_old_lines_are_aligned(self):
        self.profile_day("aligned", row())
        self.profile_day("loki-defaults", row())
        self.day(row())  # no profile: written before the profiles existed
        self.assertEqual(sorted(os.listdir(self.dir)), ["control.jsonl", "control.loki-defaults.jsonl"])
        self.assertEqual(len(history.read(os.path.join(self.dir, "control.jsonl"))), 2)
        self.assertEqual(history.profile_of(history.read(os.path.join(self.dir, "control.jsonl"))[-1]), "aligned")
        self.assertEqual(history.history_name("control", "loki-defaults"), "control.loki-defaults.jsonl")

    def test_alerts_never_compare_a_line_with_the_other_profile_s(self):
        self.profile_day("aligned", row(parity="same"))
        self.profile_day("loki-defaults", row(parity="differs"))   # another Loki: not a regression
        self.assertEqual(alerts(self.dir)["regressions"], [])
        self.profile_day("loki-defaults", row(parity="same"))
        self.assertEqual([a["detail"] for a in alerts(self.dir)["improvements"]], ["result vs Loki: differs → same"])

    def test_last_commit_is_the_newest_full_line_of_any_profile(self):
        self.profile_day("aligned", row(), commit="a" * 12)
        one = {"ranges": SPEC["ranges"], "sets": {"control": SPEC["sets"]["control"]}}
        self.assertEqual(history.last_commit(self.dir, one), "a" * 12)
        # the aligned profile stops running (no build supports its flag): its file stays behind
        self.profile_day("loki-defaults", row(), commit="b" * 12)
        self.assertEqual(history.last_commit(self.dir, one), "b" * 12)

    def test_a_line_of_a_build_that_was_not_like_for_like_enters_no_ratchet_and_no_list(self):
        self.profile_day("aligned", row(), not_like_for_like=["main"])
        a = alerts(self.dir)
        self.assertEqual((a["ratchet_added"], a["over_2x_new"]), ([], []))
        line = history.read(os.path.join(self.dir, "control.jsonl"))[0]
        self.assertEqual(line["rows"][0][line["cols"].index("like_for_like")], False)
        self.profile_day("loki-defaults", row(), commit="d" * 12)
        self.assertEqual(len(alerts(self.dir)["ratchet_added"]), 1)  # the same row, like-for-like

    def test_an_over_2x_row_is_listed_and_new_per_profile(self):
        self.profile_day("aligned", timed(p50=2.0, loki_p50=0.4, cold=2.0))
        self.profile_day("loki-defaults", timed(p50=2.0, loki_p50=0.4, cold=2.0))
        new = alerts(self.dir)["over_2x_new"]
        self.assertEqual({(r["profile"], r["mode"]) for r in new}, set())  # no baseline file: nothing to compare with
        with open(os.path.join(self.dir, "over-2x-loki.json"), "w") as f:
            json.dump({"rows": []}, f)
        new = alerts(self.dir)["over_2x_new"]
        self.assertEqual({r["profile"] for r in new}, {"aligned", "loki-defaults"})

    def test_the_trend_gives_each_profile_a_section_and_marks_a_cache_served_cold(self):
        self.profile_day("aligned", row(), not_like_for_like=["main"])
        self.profile_day("loki-defaults", row(served=True))
        text = history.trend(self.dir)
        self.assertIn("## control\n", text)
        self.assertIn("## control (loki-defaults)", text)
        self.assertIn("‡", text)
        self.assertIn("900 ms †", text)

    # regressions
    def test_result_regression_names_the_commit_range(self):
        self.day(row(parity="same"), commit="aaaaaaaaaaaa")
        self.day(row(parity="differs"), commit="bbbbbbbbbbbb")
        a = alerts(self.dir)
        self.assertEqual([(r["kind"], r["commits"]) for r in a["regressions"]], [("result", "aaaaaaaaaa..bbbbbbbbbb")])
        self.assertEqual(a["range"], {"control": "aaaaaaaaaa..bbbbbbbbbb"})
        self.assertEqual(a["improvements"], [])

    def test_tolerance_pass_is_still_same(self):
        self.day(row(parity="same series, sum ±0.24%"))
        self.day(row(parity="differs"))
        self.assertEqual(len(alerts(self.dir)["regressions"]), 1)
        self.day(row(parity="same series, sum ±0.04%"))
        self.assertEqual(self.kinds("improvements"), [("result", "result vs Loki: differs → same")])

    def test_unknown_parity_is_not_a_transition(self):
        self.day(row(parity="n/a"))
        self.day(row(parity="differs"))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_new_error_is_a_regression(self):
        self.day(row())
        self.day(row(status="502", parity="differs"))
        self.assertIn(("status", "status 200 → 502"), self.kinds("regressions"))
        self.day(row(status=None, parity="n/a"))   # no status at all is not "unknown": nothing answered
        self.day(row())
        self.day(row(status=None, parity="n/a"))
        self.assertIn(("status", "status 200 → none"), self.kinds("regressions"))

    def test_error_loki_gives_too_is_a_fix_towards_loki_not_a_regression(self):
        self.day(row())
        self.day(row(status="400", loki_status="400", parity="same"))
        self.assertEqual(alerts(self.dir)["regressions"], [])
        # The same error with a different result, or an error Loki does not give, is one.
        self.day(row())
        self.day(row(status="400", loki_status="400", parity="differs"))
        self.assertEqual(len(alerts(self.dir)["regressions"]), 1)

    def test_fails_like_loki(self):
        self.assertTrue(history.fails_like_loki({"status": "400", "loki_status": "400", "parity": "same"}))
        self.assertFalse(history.fails_like_loki({"status": "400", "loki_status": "400", "parity": "differs"}))
        self.assertFalse(history.fails_like_loki({"status": "400", "loki_status": "200", "parity": "same"}))

    def test_single_run_slowdown_is_not_a_regression(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.6))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_slowdown_that_held_two_runs_is_one_from_before_it_began(self):
        self.day(row(p50=0.2), commit="aaaaaaaaaaaa")
        self.day(row(p50=0.6), commit="bbbbbbbbbbbb")
        self.day(row(p50=0.62), commit="cccccccccccc")
        a = alerts(self.dir)["regressions"]
        self.assertEqual([(r["kind"], r["commits"]) for r in a], [("slower", "aaaaaaaaaa..cccccccccc")])
        self.assertIn("held for two runs", a[0]["detail"])
        # Reported once: the next steady day is not worse than the run before it.
        self.day(row(p50=0.61))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_slowdown_within_noise_is_not_one(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.22, verdict="slower"))
        self.day(row(p50=0.23, verdict="slower"))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_release_verdict_slowdown_is_a_regression_with_the_slowdown_itself(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.6, verdict="slower"))
        a = alerts(self.dir)["regressions"]
        self.assertEqual(len(a), 1)
        self.assertIn("slower than the release in the same run", a[0]["detail"])

    def test_release_verdict_alone_is_state_not_a_transition(self):
        self.day(row(p50=0.6, verdict="slower"))
        self.day(row(p50=0.61, verdict="slower"))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_slowdown_reported_by_the_verdict_is_not_repeated_a_run_later(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.6, verdict="slower"))
        self.day(row(p50=0.6, verdict="slower"))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    # improvements
    def test_improvements(self):
        self.day(row(parity="differs", p50=0.6), row(shape="b", status="502", p50=0.2), row(shape="c", p50=0.9))
        self.day(row(parity="same", p50=0.6), row(shape="b", status="200", p50=0.2), row(shape="c", p50=0.3))
        got = {(a["shape"], a["kind"]) for a in alerts(self.dir)["improvements"]}
        self.assertEqual(got, {(SHAPE, "result"), ("b", "status"), ("c", "faster")})
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_only_the_newest_line_against_the_previous_valid_one(self):
        self.day(row(parity="same"))
        s = summary("2026-10-02", row(parity="differs"))
        s["valid"] = False
        history.append(s, self.dir, "bad")
        self.n = 2
        self.day(row(parity="same"))
        self.assertEqual(alerts(self.dir)["regressions"], [])
        self.day(row(parity="differs"))
        self.assertEqual(len(alerts(self.dir)["regressions"]), 1)

    def test_row_new_in_the_newest_line_has_no_transition(self):
        self.day(row())
        self.day(row(), row(shape="new", parity="differs", status="502"))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_sets_limit_what_is_compared(self):
        self.day(row(parity="same"), name="a")
        self.day(row(parity="differs"), name="a")
        self.day(row(parity="same"), name="b")
        self.day(row(parity="differs"), name="b")
        self.assertEqual({r["set"] for r in alerts(self.dir)["regressions"]}, {"a", "b"})
        self.assertEqual({r["set"] for r in alerts(self.dir, sets=["b"])["regressions"]}, {"b"})
        self.assertEqual(alerts(self.dir, sets=[])["regressions"], [])

    # the column
    def test_loki_cold_computed(self):
        col = history.COLS.index("loki_cold_computed")
        for r, want in ((row(), True), (row(served=True), False), (row(counters=False), None),
                        (row(reused=True), True)):
            self.assertEqual(history.entry(summary("2026-10-01", r))["rows"][0][col], want)
        # Identical answers for moved windows: a cache answered, whatever the counters say.
        stale = row()
        stale["stale"] = ["loki"]
        self.assertFalse(history.entry(summary("2026-10-01", stale))["rows"][0][col])
        self.assertIn("loki_cold_computed", history.COLS)

    # ratchet
    def test_ratchet_adds_only_computed_loki_colds_no_slower_than_loki(self):
        self.day(row(), row(shape="served", served=True), row(shape="unknown", counters=False),
                 row(shape="slow", cold=1.0), row(shape="warm only", p50=0.01, cold=2.0, loki_cold=0.9),
                 row(shape="differs", parity="differs"), row(shape="err", status="502"))
        added = alerts(self.dir)["ratchet_added"]
        self.assertEqual([r["shape"] for r in added], [SHAPE])
        self.assertEqual(added[0], {"set": "control", "shape": SHAPE, "range": RANGE, "added": "2026-10-01",
                                    "commit": "c1" * 12})

    def test_lines_without_the_column_never_ratchet(self):
        old = history.entry(summary("2026-10-01", row()))
        drop = history.COLS.index("loki_cold_computed")
        old["cols"] = [c for i, c in enumerate(history.COLS) if i != drop]
        old["rows"] = [[v for i, v in enumerate(r) if i != drop] for r in old["rows"]]
        with open(os.path.join(self.dir, "control.jsonl"), "w") as f:
            f.write(json.dumps(old) + "\n")
        a = alerts(self.dir)
        self.assertEqual((a["ratchet"], a["regressions"]), ([], []))
        self.n = 1
        self.day(row(parity="differs"))   # old line followed by a new one still compares
        self.assertEqual(len(alerts(self.dir)["regressions"]), 1)
        self.assertIn("control", history.trend(self.dir))

    def test_ratchet_is_written_sorted_and_only_ever_added_to(self):
        self.day(row(shape="b"), row(shape="a"))
        write(self.dir)
        path = os.path.join(self.dir, "ratchet.json")
        with open(path) as f:
            first = f.read()
        self.assertEqual([r["shape"] for r in json.loads(first)], ["a", "b"])
        self.day(row(shape="b", parity="differs"), row(shape="a"), row(shape="c"))
        a = write(self.dir)
        self.assertEqual([r["shape"] for r in a["ratchet_added"]], ["c"])
        with open(path) as f:
            self.assertEqual([r["shape"] for r in json.load(f)], ["a", "b", "c"])
        # Idempotent: the same data again changes nothing.
        write(self.dir)
        with open(path) as f:
            self.assertEqual(len(json.load(f)), 3)
        self.assertEqual(first, first.rstrip("\n") + "\n")

    def test_ratchet_losses(self):
        self.day(*(row(shape=s) for s in ("r", "e", "c", "ok", "gone", "s", "u")))
        write(self.dir)
        self.day(row(shape="r", parity="differs"), row(shape="e", status="500", parity="n/a"),
                 row(shape="c", cold=1.5), row(shape="ok", cold=0.95),
                 row(shape="s", cold=5.0, served=True), row(shape="u", cold=5.0, counters=False))
        # Result and status are lost at once; a slower cold run is one sample.
        lost = alerts(self.dir)["ratchet_lost"]
        self.assertEqual([(a["shape"], a["detail"]) for a in lost],
                         [("e", "status 500"), ("r", "result differs from Loki")])
        self.assertTrue(all(a["commits"] == f"{'c1' * 5}..{'c2' * 5}" for a in lost))

    def test_cold_loss_needs_two_runs_and_is_reported_while_it_holds(self):
        self.day(row())
        write(self.dir)
        self.day(row(cold=1.5))
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])
        self.day(row(cold=1.4))
        lost = alerts(self.dir)["ratchet_lost"]
        self.assertEqual([(a["kind"], a["commits"]) for a in lost], [("ratchet", f"{'c1' * 5}..{'c3' * 5}")])
        self.assertIn("held for two runs", lost[0]["detail"])
        self.day(row(cold=1.4))
        self.assertEqual(len(alerts(self.dir)["ratchet_lost"]), 1)
        self.day(row(cold=0.5))
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])

    def test_cold_loss_ignores_a_loki_cold_the_cache_answered(self):
        self.day(row())
        write(self.dir)
        self.day(row(cold=1.5, served=True))
        self.day(row(cold=1.5, served=True))
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])
        self.day(row(cold=1.5))
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])

    def test_markdown_is_stable(self):
        self.day(row(p50=0.2), row(shape="b", parity="differs"), commit="aaaaaaaaaaaa")
        write(self.dir)
        self.day(row(p50=0.2, parity="differs"), row(shape="b", parity="same"), commit="bbbbbbbbbbbb")
        text = history.render_alerts(alerts(self.dir))
        self.assertEqual(text, "\n".join([
            "## Regressions", "",
            "- `control` logs | json · 1h: result vs Loki: same → differs (`aaaaaaaaaa..bbbbbbbbbb`)", "",
            "## Improvements", "",
            "- `control` b · 1h: result vs Loki: differs → same (`aaaaaaaaaa..bbbbbbbbbb`)", "",
            "## Ratchet", "", "Lost (1):", "",
            "- `control` logs | json · 1h: result differs from Loki (`aaaaaaaaaa..bbbbbbbbbb`)", "",
            "Held: 2 rows (1 added by this run); the list is `bench/ab/history/ratchet.json`.", "",
            "## Loki failed (informational)", "",
            "Rows where Loki did not answer 200: their results are not compared and they do not fail the run.", "",
            "none", ""]))
        self.assertEqual(text, history.render_alerts(alerts(self.dir)))

    def test_empty_sections_say_none(self):
        self.day(row())
        text = history.render_alerts(alerts(self.dir))
        self.assertIn("## Regressions\n\nnone\n\n## Improvements\n\nnone\n\n## Ratchet\n\nLost (0):\n\nnone\n", text)

    def test_cli_writes_the_files(self):
        # The CLI reads bench/ab/shapes.json; this set has the single range 1h.
        self.day(row(parity="same"), name="pattern-line-filter")
        self.day(row(parity="differs"), name="pattern-line-filter")
        self.day(row(parity="differs"), name="not-in-shapes-json")
        out = tempfile.mkdtemp()
        js, md, body = (os.path.join(out, n) for n in ("alerts.json", "alerts.md", "alerts-body.md"))
        res = subprocess.run([sys.executable, os.path.join(HERE, "..", "history.py"), "alerts", "--dir", self.dir,
                              "--json", js, "--markdown", md, "--body", body], capture_output=True, text=True, check=True)
        self.assertEqual(res.stdout, "")
        with open(js) as f:
            a = json.load(f)
        self.assertEqual([r["kind"] for r in a["regressions"]], ["result"])
        self.assertEqual({r["set"] for r in a["regressions"]}, {"pattern-line-filter"})
        for k in ("improvements", "ratchet_lost", "ratchet_added", "loki_failed", "range"):
            self.assertIn(k, a)
        for path in (md, body):
            with open(path) as f:
                self.assertTrue(f.read().startswith("## Regressions\n\n- `pattern-line-filter`"))
        self.assertTrue(os.path.exists(os.path.join(self.dir, "ratchet.json")))
        res = subprocess.run([sys.executable, os.path.join(HERE, "..", "history.py"), "alerts", "--dir", self.dir],
                             capture_output=True, text=True, check=True)
        self.assertIn("## Ratchet", res.stdout)

    # Loki failing says nothing about the proxy
    def test_loki_failure_is_not_a_result_transition(self):
        self.day(row(parity="same"))
        self.day(row(parity="differs", loki_status="0"))
        a = alerts(self.dir)
        self.assertEqual((a["regressions"], a["improvements"]), ([], []))
        self.assertEqual([(r["shape"], r["kind"]) for r in a["loki_failed"]], [(SHAPE, "loki")])
        self.assertIn("Loki status 0", a["loki_failed"][0]["detail"])
        # The proxy rejecting what Loki rejects is agreement, not a Loki failure.
        self.day(row(status="400", loki_status="400", parity="same"))
        self.assertEqual(alerts(self.dir)["loki_failed"], [])
        self.day(row(parity="same"))
        # Loki back: compared with a line it also answered on, not with the failed one.
        self.day(row(parity="same"))
        self.assertEqual((alerts(self.dir)["regressions"], alerts(self.dir)["improvements"]), ([], []))
        self.day(row(parity="differs"))
        self.assertEqual(len(alerts(self.dir)["regressions"]), 1)

    def test_loki_failure_does_not_lose_a_ratcheted_row(self):
        self.day(row())
        write(self.dir)
        self.day(row(parity="differs", loki_status="0"))
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])
        self.assertIn("Loki failed", history.render_alerts(alerts(self.dir)))

    # which lines count
    def test_cut_down_runs_are_neither_compared_nor_a_baseline(self):
        self.day(row(parity="same"))
        self.day(row(parity="differs"), runs=3)
        self.day(row(parity="same"))
        self.assertEqual(alerts(self.dir)["regressions"], [])
        self.day(row(parity="differs"), runs=3)   # the newest line is cut down: nothing to report
        a = alerts(self.dir)
        self.assertEqual((a["regressions"], a["range"]), ([], {}))

    def test_each_row_is_compared_with_the_newest_full_line_that_has_it(self):
        self.day(row(shape="a", parity="same"), row(shape="b", parity="same"), commit="aaaaaaaaaaaa")
        self.day(row(shape="a", parity="same"), commit="bbbbbbbbbbbb")
        self.day(row(shape="a", parity="same"), row(shape="b", parity="differs"), commit="cccccccccccc")
        got = [(r["shape"], r["commits"]) for r in alerts(self.dir)["regressions"]]
        self.assertEqual(got, [("b", "aaaaaaaaaa..cccccccccc")])

    def test_sets_shapes_json_does_not_define_are_ignored(self):
        self.day(row(parity="same"), name="gone")
        self.day(row(parity="differs"), name="gone")
        self.assertEqual(alerts(self.dir)["regressions"], [])

    def test_the_full_run_replaces_a_cut_down_one_of_the_same_day(self):
        path = os.path.join(self.dir, "control.jsonl")

        def lines():
            with open(path) as f:
                return [json.loads(line) for line in f]

        self.assertTrue(history.append(summary("2026-10-01", row(), runs=3), self.dir, "manual", SPEC)[1])
        self.assertTrue(history.append(summary("2026-10-01", row(p50=0.7), runs=7), self.dir, "scheduled", SPEC)[1])
        self.assertEqual([(e["runs"], e["commit"]) for e in lines()], [(7, "scheduled")])
        # A full line is never replaced, a cut-down one never replaces anything.
        self.assertFalse(history.append(summary("2026-10-01", row(), runs=7), self.dir, "again", SPEC)[1])
        self.assertFalse(history.append(summary("2026-10-01", row(), runs=3), self.dir, "manual", SPEC)[1])
        self.assertEqual([(e["runs"], e["commit"]) for e in lines()], [(7, "scheduled")])
        # Other days are untouched by a replacement.
        history.append(summary("2026-10-02", row(), runs=3), self.dir, "m2", SPEC)
        history.append(summary("2026-10-02", row(), runs=7), self.dir, "s2", SPEC)
        self.assertEqual([(e["date"], e["commit"]) for e in lines()], [("2026-10-01", "scheduled"), ("2026-10-02", "s2")])

    # the exemptions for an error Loki gives too
    def test_ratchet_row_whose_loki_gives_the_same_error_is_not_lost(self):
        self.day(row())
        write(self.dir)
        self.day(row(status="400", loki_status="400", parity="same"))
        self.assertEqual(alerts(self.dir)["ratchet_lost"], [])
        self.day(row(status="400", loki_status="200", parity="same"))
        self.assertEqual([a["detail"] for a in alerts(self.dir)["ratchet_lost"]], ["status 400"])

    def test_status_improvement_not_while_loki_still_gives_the_error(self):
        self.day(row(status="400", loki_status="400", parity="same"))
        self.day(row(status="200", loki_status="400", parity="differs"))
        self.assertEqual(alerts(self.dir)["improvements"], [])
        self.day(row(status="400", loki_status="400"))
        self.day(row(status="200", loki_status="200"))
        self.assertEqual(len(alerts(self.dir)["improvements"]), 1)

    # repeated losses and slowdowns
    def test_ratchet_loss_range_starts_where_the_row_was_still_good(self):
        self.day(row(), commit="aaaaaaaaaaaa")
        write(self.dir)
        self.day(row(parity="differs"), commit="bbbbbbbbbbbb")
        self.assertEqual([a["commits"] for a in alerts(self.dir)["ratchet_lost"]], ["aaaaaaaaaa..bbbbbbbbbb"])
        self.day(row(parity="differs"), commit="cccccccccccc")
        self.day(row(parity="differs"), commit="dddddddddddd")
        self.assertEqual([a["commits"] for a in alerts(self.dir)["ratchet_lost"]], ["aaaaaaaaaa..dddddddddd"])

    def test_held_cold_loss_range_starts_before_the_first_slow_run(self):
        self.day(row(), commit="aaaaaaaaaaaa")
        write(self.dir)
        self.day(row(cold=1.5), commit="bbbbbbbbbbbb")
        self.day(row(cold=1.5), commit="cccccccccccc")
        self.day(row(cold=1.5), commit="dddddddddddd")
        self.assertEqual([a["commits"] for a in alerts(self.dir)["ratchet_lost"]], ["aaaaaaaaaa..dddddddddd"])

    def test_ratchet_loss_range_falls_back_to_the_commit_the_row_entered_at(self):
        with open(os.path.join(self.dir, "ratchet.json"), "w") as f:
            json.dump([{"set": "control", "shape": SHAPE, "range": RANGE, "added": "2026-09-01",
                        "commit": "eeeeeeeeeeee"}], f)
        self.day(row(parity="differs"), commit="bbbbbbbbbbbb")
        self.day(row(parity="differs"), commit="cccccccccccc")
        self.assertEqual([a["commits"] for a in alerts(self.dir)["ratchet_lost"]], ["eeeeeeeeee..cccccccccc"])

    def test_release_verdict_slowdown_is_not_repeated_when_p50_jitters(self):
        self.day(row(p50=1.0, verdict="slower"))
        self.day(row(p50=0.9, verdict="slower"))
        self.day(row(p50=1.2, verdict="slower"))
        self.assertEqual(alerts(self.dir)["regressions"], [])

    # the ratchet file and the report
    def test_unreadable_ratchet_is_left_alone(self):
        path = os.path.join(self.dir, "ratchet.json")
        self.day(row())
        for bad in ("{not json", '{"a": 1}'):
            with open(path, "w") as f:
                f.write(bad)
            a = write(self.dir)
            self.assertIn("cannot be read", a["ratchet_problem"])
            self.assertEqual((a["ratchet_added"], a["ratchet_lost"]), ([], []))
            with open(path) as f:
                self.assertEqual(f.read(), bad)
            self.assertIn("cannot be read", history.render_alerts(a))

    def test_ratchet_entries_without_their_key_are_skipped(self):
        with open(os.path.join(self.dir, "ratchet.json"), "w") as f:
            json.dump([{"set": "control", "shape": SHAPE}, "x", {"set": "control", "shape": "b", "range": RANGE}], f)
        self.day(row())
        a = write(self.dir)
        self.assertEqual(a["ratchet_problem"], "")
        self.assertEqual(sorted(r["shape"] for r in a["ratchet"]), ["b", SHAPE])   # b held, SHAPE just added

    def test_ratchet_rows_not_measured_are_counted(self):
        self.day(row(shape="a"), row(shape="b"))
        write(self.dir)
        self.day(row(shape="a"))
        a = alerts(self.dir)
        self.assertEqual(a["ratchet_unmeasured"], 1)
        self.assertIn("1 not measured by this run", history.render_alerts(a))
        # A set the run did not write is not counted.
        self.assertEqual(alerts(self.dir, sets=["b"])["ratchet_unmeasured"], 0)

    def test_pull_request_body_is_capped(self):
        self.day(*(row(shape=f"s{i}", parity="same") for i in range(8)))
        self.day(*(row(shape=f"s{i}", parity="differs") for i in range(8)))
        a = alerts(self.dir)
        text = history.render_alerts(a, limit=3)
        self.assertEqual(text.count("\n- `control`"), 3)
        self.assertIn("… and 5 more, see the job summary", text)
        self.assertEqual(history.render_alerts(a).count("\n- `control`"), 8)
        # One shared budget: regressions first, so nothing is left for the rest.
        self.assertNotIn("more, see", history.render_alerts(a, limit=100))
        out = tempfile.mkdtemp()
        write(self.dir, json_out=os.path.join(out, "a.json"), markdown_out=os.path.join(out, "a.md"),
              body_out=os.path.join(out, "b.md"))
        with open(os.path.join(out, "b.md")) as f:
            self.assertLess(len(f.read()), 65536)


LOCK = {**history.over2x.registry_io.PERF_DEFAULTS, "locked": True, "locked_since": "2026-10-27"}
OPEN = dict(history.over2x.registry_io.PERF_DEFAULTS)
EXCEPTION = {"reason": "a VictoriaLogs scan Loki does not need", "follow_up": "a native pipe"}


def bounds(block, shapes=(SHAPE,), item="semantics/x"):
    return {"blocks": {item: block}, "covers": {("control", s): [item] for s in shapes}}


def timed(p50=0.4, loki_p50=0.4, cold=0.9, loki_cold=0.9, **kw):
    r = row(p50=p50, cold=cold, loki_cold=loki_cold, **kw)
    r["p50"]["loki"] = loki_p50
    return r


class AcceptanceAlertsTest(unittest.TestCase):
    """The daily run against the registry's perf: blocks and the distribution ratchet."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.n = 0

    def day(self, *rows, **extra):
        self.n += 1
        history.append(summary(f"2026-10-{self.n:02d}", *rows, **extra), self.dir, f"c{self.n}" * 12, SPEC)

    def run_alerts(self, block=LOCK, **kw):
        return alerts(self.dir, bounds=bounds(block, **kw))

    def locked(self, a):
        return [(r["kind"], r["detail"]) for r in a["regressions"] if r["kind"] == "locked"]

    def test_locked_warm_above_its_bound_is_a_regression_at_once(self):
        self.day(timed(p50=1.0))  # 2.5× Loki's warm 400 ms
        found = self.locked(self.run_alerts())
        self.assertEqual(len(found), 1)
        self.assertIn("warm 1.00 s against Loki's 400 ms, 2.50×, above its 2.00× bound in the aligned profile "
                      "(locked: semantics/x since 2026-10-27)", found[0][1])
        self.assertEqual(self.locked(self.run_alerts(OPEN)), [])  # unlocked: not a locked regression

    def test_locked_cold_needs_two_runs(self):
        self.day(timed(cold=2.0))  # 2.2× Loki's computed cold
        a = self.run_alerts()
        self.assertEqual(self.locked(a), [])
        self.assertEqual([(r["mode"], r["shape"]) for r in a["locked_pending"]], [("cold", SHAPE)])
        self.day(timed(cold=2.0))
        a = self.run_alerts()
        self.assertEqual(len(self.locked(a)), 1)
        self.assertIn("held for two runs", self.locked(a)[0][1])
        self.assertEqual(a["locked_pending"], [])

    def test_a_tighter_bound_and_its_modes(self):
        self.day(timed(p50=0.7))  # 1.75×
        self.assertEqual(self.locked(self.run_alerts()), [])
        self.assertEqual(len(self.locked(self.run_alerts({**LOCK, "max_ratio": 1.5}))), 1)
        self.assertEqual(self.locked(self.run_alerts({**LOCK, "max_ratio": 1.5, "modes": ["cold"]})), [])

    def test_noise_floor_loki_failures_and_cache_answered_colds_are_not_regressions(self):
        self.day(timed(p50=0.04, loki_p50=0.006))  # 6.7×, inside the 50 ms floor
        self.assertEqual(self.locked(self.run_alerts()), [])
        self.day(timed(p50=1.0, loki_status="504"))
        self.assertEqual(self.locked(self.run_alerts()), [])
        self.day(timed(cold=2.0, served=True))
        self.day(timed(cold=2.0, served=True))
        a = self.run_alerts()
        self.assertEqual((self.locked(a), a["locked_pending"]), ([], []))

    def test_lost_below_is_flagged_not_a_regression(self):
        self.day(timed(cold=0.4))   # below Loki's 900 ms
        self.day(timed(cold=1.3))   # 1.44×
        a = self.run_alerts()
        self.assertEqual([(r["mode"], r["shape"]) for r in a["lost_below"]], [("cold", SHAPE)])
        self.assertIn("below Loki on the previous run (400 ms against 900 ms)", a["lost_below"][0]["detail"])
        self.assertEqual(a["regressions"], [])

    def test_between_rows_with_and_without_an_exception(self):
        self.day(timed(cold=1.3))
        a = self.run_alerts()
        self.assertEqual([(r["mode"], r["exception"], r["locked"]) for r in a["between"]], [("cold", [], True)])
        text = history.render_alerts(a)
        self.assertIn("1–2× Loki without an effort exception (flagged for review: below Loki is the target) (1):", text)
        a = self.run_alerts({**LOCK, "effort_exception": EXCEPTION})
        self.assertEqual(a["between"][0]["exception"], ["semantics/x"])
        self.assertIn("1–2× Loki with an effort exception (accepted up to the bound) (1):", history.render_alerts(a))

    def test_ready_to_lock_needs_two_runs_within(self):
        self.day(timed())
        self.assertEqual(self.run_alerts(OPEN)["lockable"], [])
        self.day(timed())
        self.assertEqual(self.run_alerts(OPEN)["lockable"], ["semantics/x"])
        self.assertEqual(self.run_alerts(LOCK)["lockable"], [])  # already locked
        self.day(timed(p50=1.0))
        self.assertEqual(self.run_alerts(OPEN)["lockable"], [])

    def shapes(self, n, **kw):
        return [timed(shape=f"s{i}", **kw) for i in range(n)]

    def dist(self, a):
        return [(r["shape"], r["detail"]) for r in a["regressions"] if r["kind"] == "distribution"]

    def test_distribution_ratchet(self):
        names = [f"s{i}" for i in range(10)]
        self.day(*self.shapes(10, cold=0.4))      # every cold below Loki
        self.day(*self.shapes(10, cold=0.9))      # all close now: 10 rows, 100 points
        found = self.dist(alerts(self.dir, bounds=bounds(OPEN, names)))
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0][0], "aligned cold: share below Loki")
        self.assertIn("share below Loki 100.0% → 0.0% (10 → 0 of the 10 rows compared on both), against the previous "
                      "full run", found[0][1])

    def test_distribution_ratchet_noise_and_the_count_above_2x(self):
        names = [f"s{i}" for i in range(4)]
        self.day(*self.shapes(4, cold=0.4))
        self.day(*self.shapes(4, cold=0.9))       # 4 rows lost: within the 5-row margin
        self.assertEqual(self.dist(alerts(self.dir, bounds=bounds(OPEN, names))), [])
        names = [f"s{i}" for i in range(10)]
        self.day(*self.shapes(10))
        self.day(*self.shapes(10, p50=1.0))       # 10 warm rows above 2×
        self.assertEqual([s for s, _ in self.dist(alerts(self.dir, bounds=bounds(OPEN, names)))],
                         ["aligned warm: rows above 2× Loki"])

    def test_a_move_only_loki_made_is_not_a_regression(self):
        names = [f"s{i}" for i in range(10)]
        self.day(*self.shapes(10, p50=0.3, loki_p50=0.2))     # warm 1.5×
        self.day(*self.shapes(10, p50=0.31, loki_p50=0.1))    # 3.1×: Loki's cache got faster, the proxy held
        a = alerts(self.dir, bounds=bounds(OPEN, names))
        self.assertEqual(a["distribution"]["aligned"]["warm"]["above"], 10)  # reported as measured
        self.assertEqual(self.dist(a), [])                         # but not a ratchet regression
        self.day(*self.shapes(10, cold=0.4))
        self.day(*self.shapes(10, cold=0.42, loki_cold=0.2))  # below Loki → above it, by Loki alone
        self.assertEqual(alerts(self.dir, bounds=bounds(OPEN, names))["lost_below"], [])

    def test_distribution_ratchet_catches_a_slow_drift_over_a_week(self):
        names = [f"s{i}" for i in range(20)]
        for lost in range(9):  # one row a day leaves the band below Loki: noise day to day
            self.day(*[timed(shape=n, cold=0.9 if i < lost else 0.4) for i, n in enumerate(names)])
        a = alerts(self.dir, bounds=bounds(OPEN, names))
        self.assertEqual([r["detail"].split(", against ")[1] for r in a["regressions"] if r["kind"] == "distribution"],
                         ["the full run a week earlier"])

    def test_distribution_and_section_with_targets(self):
        self.day(timed(cold=0.4), timed(shape="t", cold=3.0, p50=1.0))
        a = self.run_alerts(shapes=(SHAPE, "t"))
        self.assertEqual(a["distribution"]["aligned"]["cold"], {"rows": 2, "below": 1, "close": 0, "between": 0, "above": 1,
                                                     "share_below": 50.0})
        text = history.render_alerts(a)
        self.assertIn("## Acceptance versus Loki", text)
        self.assertIn("| aligned | cold | 2 | 1 (50.0%) | 0 | 0 | 1 | ❌ | ❌ |", text)
        self.assertNotIn("## Acceptance versus Loki", history.render_alerts(alerts(self.dir)))  # no blocks: off

    def test_acceptance_history_is_written_per_day(self):
        self.day(timed(cold=0.4))
        self.day(timed(cold=3.0))
        history.write_acceptance(self.dir, spec=SPEC)
        with open(os.path.join(self.dir, history.ACCEPTANCE)) as f:
            days = json.load(f)["days"]
        self.assertEqual([(d["date"], d["profiles"]["aligned"]["cold"]["below"], d["profiles"]["aligned"]["cold"]["above"])
                          for d in days], [("2026-10-01", 1, 0), ("2026-10-02", 0, 1)])
        self.assertEqual(days[0]["profiles"]["aligned"]["cold"]["share_below"], 100.0)

    # Loki profiles: the registry's bound judges a locked row in each profile, and each profile has its own history,
    # distribution and ratchet.
    def test_a_locked_row_is_judged_at_its_bound_in_each_profile(self):
        self.day(timed(p50=1.0), profile="aligned")
        self.day(timed(p50=1.0), profile="loki-defaults")
        found = self.locked(self.run_alerts())
        self.assertEqual(len(found), 2)
        self.assertEqual(sorted("aligned" in d or "loki-defaults" in d for _, d in found), [True, True])
        self.assertEqual(sorted(("loki-defaults" in d, "aligned profile" in d) for _, d in found),
                         [(False, True), (True, False)])

    def test_a_locked_row_within_its_bound_in_one_profile_only_fails_in_the_other(self):
        self.day(timed(p50=0.4), profile="aligned")           # 1.0×
        self.day(timed(p50=1.0), profile="loki-defaults")     # 2.5×
        found = self.locked(self.run_alerts())
        self.assertEqual(len(found), 1)
        self.assertIn("in the loki-defaults profile", found[0][1])

    def test_each_profile_has_its_own_distribution_and_ratchet(self):
        self.day(timed(cold=0.4), timed(shape="t", cold=3.0, p50=1.0), profile="aligned")
        self.day(timed(cold=0.4), timed(shape="t", cold=0.4), profile="loki-defaults")
        a = self.run_alerts(shapes=(SHAPE, "t"))
        self.assertEqual(a["distribution"]["aligned"]["cold"]["above"], 1)
        self.assertEqual(a["distribution"]["loki-defaults"]["cold"]["above"], 0)
        text = history.render_alerts(a)
        self.assertIn("| aligned | cold | 2 | 1 (50.0%) | 0 | 0 | 1 |", text)
        self.assertIn("| loki-defaults | cold | 2 | 2 (100.0%) | 0 | 0 | 0 |", text)

    def test_the_distribution_ratchet_does_not_compare_across_profiles(self):
        names = [f"s{i}" for i in range(10)]
        self.day(*self.shapes(10, cold=0.4), profile="aligned")
        self.day(*self.shapes(10, cold=0.9), profile="loki-defaults")   # another Loki: a different line, not a drop
        self.assertEqual(self.dist(alerts(self.dir, bounds=bounds(OPEN, names))), [])
        self.day(*self.shapes(10, cold=0.9), profile="aligned")         # the drop is in the aligned profile itself
        self.assertEqual([s for s, _ in self.dist(alerts(self.dir, bounds=bounds(OPEN, names)))],
                         ["aligned cold: share below Loki"])

    def test_a_build_that_was_not_like_for_like_is_in_no_distribution_and_locks_nothing(self):
        self.day(timed(p50=1.0), profile="aligned", not_like_for_like=["main"])
        a = self.run_alerts()
        self.assertEqual((self.locked(a), a["locked_pending"], a["distribution"]), ([], [], {}))

    def test_acceptance_history_has_a_record_per_profile_and_day(self):
        self.day(timed(cold=0.4), profile="aligned")
        self.day(timed(cold=3.0), profile="loki-defaults")
        history.write_acceptance(self.dir, spec=SPEC)
        with open(os.path.join(self.dir, history.ACCEPTANCE)) as f:
            days = {d["date"]: d["profiles"] for d in json.load(f)["days"]}
        self.assertEqual(list(days["2026-10-01"]), ["aligned"])
        self.assertEqual(list(days["2026-10-02"]), ["loki-defaults"])
        self.assertEqual(days["2026-10-02"]["loki-defaults"]["cold"]["above"], 1)


class AcceptanceReplayTest(unittest.TestCase):
    """The history up to the seed day (append-only, so fixed) against the seed's locks: the seed day trips no lock
    and no distribution ratchet."""

    def test_seed_day_against_the_seed_locks(self):
        sys.path.insert(0, HERE)
        from test_over2x import SEED, seed_bounds  # noqa: E402
        tmp = tempfile.mkdtemp()
        for path in glob.glob(os.path.join(history.HISTORY, "*.jsonl")):
            with open(os.path.join(tmp, os.path.basename(path)), "w") as f:
                f.writelines(json.dumps(e) + "\n" for e in history.read(path) if e["date"] <= SEED)
        a = history.alerts(tmp, bounds=seed_bounds())
        self.assertTrue(a["acceptance_active"])
        self.assertEqual([r for r in a["regressions"] if r["kind"] in ("locked", "distribution")], [])
        self.assertEqual(a["locked_pending"], [])
        # The two rows of locked items between 1× and 2× (A explore volume, 6h and 24h cold) are listed, not failed.
        locked_between = sorted((r["shape"], r["range"]) for r in a["between"] if r["locked"])
        self.assertEqual(locked_between, [("A explore volume, 2 filters, drop", "24h"),
                                          ("A explore volume, 2 filters, drop", "6h")])

    def test_committed_acceptance_record_is_current(self):
        with open(os.path.join(history.HISTORY, history.ACCEPTANCE)) as f:
            self.assertEqual(json.load(f), json.loads(json.dumps(history.acceptance_history())))


if __name__ == "__main__":
    unittest.main()
