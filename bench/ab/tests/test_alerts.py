"""Unit tests for the alerts and the ratchet in bench/ab/history.py."""
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


def summary(date, *rows, name="control", runs=7):
    return {"set": name, "label": "daily", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": runs, "valid": True, "restart_before": "0", "restart_after": "0",
            "rows": list(rows or [row()])}


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
        self.assertEqual(history.COLS[-1], "loki_cold_computed")

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
        old["cols"], old["rows"] = history.COLS[:-1], [r[:-1] for r in old["rows"]]
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


if __name__ == "__main__":
    unittest.main()
