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


def summary(date, *rows, name="control"):
    return {"set": name, "label": "daily", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": 7, "valid": True, "restart_before": "0", "restart_after": "0",
            "rows": list(rows or [row()])}


class AlertsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.n = 0

    def day(self, *rows, name="control", commit=None):
        self.n += 1
        history.append(summary(f"2026-10-{self.n:02d}", *rows, name=name), self.dir, commit or f"c{self.n}" * 12)

    def kinds(self, key, **kw):
        return [(a["kind"], a["detail"]) for a in history.alerts(self.dir, **kw)[key]]

    # regressions
    def test_result_regression_names_the_commit_range(self):
        self.day(row(parity="same"), commit="aaaaaaaaaaaa")
        self.day(row(parity="differs"), commit="bbbbbbbbbbbb")
        a = history.alerts(self.dir)
        self.assertEqual([(r["kind"], r["commits"]) for r in a["regressions"]], [("result", "aaaaaaaaaa..bbbbbbbbbb")])
        self.assertEqual(a["range"], {"control": "aaaaaaaaaa..bbbbbbbbbb"})
        self.assertEqual(a["improvements"], [])

    def test_tolerance_pass_is_still_same(self):
        self.day(row(parity="same series, sum ±0.24%"))
        self.day(row(parity="differs"))
        self.assertEqual(len(history.alerts(self.dir)["regressions"]), 1)
        self.day(row(parity="same series, sum ±0.04%"))
        self.assertEqual(self.kinds("improvements"), [("result", "result vs Loki: differs → same")])

    def test_unknown_parity_is_not_a_transition(self):
        self.day(row(parity="n/a"))
        self.day(row(parity="differs"))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_new_error_is_a_regression_unless_loki_fails_the_same_way(self):
        self.day(row())
        self.day(row(status="502", parity="differs"))
        self.assertIn(("status", "status 200 → 502"), self.kinds("regressions"))
        self.day(row(status="400", loki_status="400", parity="same"))
        self.assertEqual(self.kinds("regressions"), [])

    def test_single_run_slowdown_is_not_a_regression(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.6))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_slowdown_that_held_two_runs_is_one_from_before_it_began(self):
        self.day(row(p50=0.2), commit="aaaaaaaaaaaa")
        self.day(row(p50=0.6), commit="bbbbbbbbbbbb")
        self.day(row(p50=0.62), commit="cccccccccccc")
        a = history.alerts(self.dir)["regressions"]
        self.assertEqual([(r["kind"], r["commits"]) for r in a], [("slower", "aaaaaaaaaa..cccccccccc")])
        self.assertIn("held for two runs", a[0]["detail"])
        # Reported once: the next steady day is not worse than the run before it.
        self.day(row(p50=0.61))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_slowdown_within_noise_is_not_one(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.22, verdict="slower"))
        self.day(row(p50=0.23, verdict="slower"))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_release_verdict_slowdown_is_a_regression_with_the_slowdown_itself(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.6, verdict="slower"))
        a = history.alerts(self.dir)["regressions"]
        self.assertEqual(len(a), 1)
        self.assertIn("slower than the release in the same run", a[0]["detail"])

    def test_release_verdict_alone_is_state_not_a_transition(self):
        self.day(row(p50=0.6, verdict="slower"))
        self.day(row(p50=0.61, verdict="slower"))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_slowdown_reported_by_the_verdict_is_not_repeated_a_run_later(self):
        self.day(row(p50=0.2))
        self.day(row(p50=0.6, verdict="slower"))
        self.day(row(p50=0.6, verdict="slower"))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    # improvements
    def test_improvements(self):
        self.day(row(parity="differs", p50=0.6), row(shape="b", status="502", p50=0.2), row(shape="c", p50=0.9))
        self.day(row(parity="same", p50=0.6), row(shape="b", status="200", p50=0.2), row(shape="c", p50=0.3))
        got = {(a["shape"], a["kind"]) for a in history.alerts(self.dir)["improvements"]}
        self.assertEqual(got, {(SHAPE, "result"), ("b", "status"), ("c", "faster")})
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_only_the_newest_line_against_the_previous_valid_one(self):
        self.day(row(parity="same"))
        s = summary("2026-10-02", row(parity="differs"))
        s["valid"] = False
        history.append(s, self.dir, "bad")
        self.n = 2
        self.day(row(parity="same"))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])
        self.day(row(parity="differs"))
        self.assertEqual(len(history.alerts(self.dir)["regressions"]), 1)

    def test_row_new_in_the_newest_line_has_no_transition(self):
        self.day(row())
        self.day(row(), row(shape="new", parity="differs", status="502"))
        self.assertEqual(history.alerts(self.dir)["regressions"], [])

    def test_sets_limit_what_is_compared(self):
        self.day(row(parity="same"), name="a")
        self.day(row(parity="differs"), name="a")
        self.day(row(parity="same"), name="b")
        self.day(row(parity="differs"), name="b")
        self.assertEqual({r["set"] for r in history.alerts(self.dir)["regressions"]}, {"a", "b"})
        self.assertEqual({r["set"] for r in history.alerts(self.dir, sets=["b"])["regressions"]}, {"b"})
        self.assertEqual(history.alerts(self.dir, sets=[])["regressions"], [])

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
        added = history.alerts(self.dir)["ratchet_added"]
        self.assertEqual([r["shape"] for r in added], [SHAPE])
        self.assertEqual(added[0], {"set": "control", "shape": SHAPE, "range": RANGE, "added": "2026-10-01",
                                    "commit": "c1" * 12})

    def test_lines_without_the_column_never_ratchet(self):
        old = history.entry(summary("2026-10-01", row()))
        old["cols"], old["rows"] = history.COLS[:-1], [r[:-1] for r in old["rows"]]
        with open(os.path.join(self.dir, "control.jsonl"), "w") as f:
            f.write(json.dumps(old) + "\n")
        a = history.alerts(self.dir)
        self.assertEqual((a["ratchet"], a["regressions"]), ([], []))
        self.n = 1
        self.day(row(parity="differs"))   # old line followed by a new one still compares
        self.assertEqual(len(history.alerts(self.dir)["regressions"]), 1)
        self.assertIn("control", history.trend(self.dir))

    def test_ratchet_is_written_sorted_and_only_ever_added_to(self):
        self.day(row(shape="b"), row(shape="a"))
        history.write_alerts(self.dir)
        path = os.path.join(self.dir, "ratchet.json")
        with open(path) as f:
            first = f.read()
        self.assertEqual([r["shape"] for r in json.loads(first)], ["a", "b"])
        self.day(row(shape="b", parity="differs"), row(shape="a"), row(shape="c"))
        a = history.write_alerts(self.dir)
        self.assertEqual([r["shape"] for r in a["ratchet_added"]], ["c"])
        with open(path) as f:
            self.assertEqual([r["shape"] for r in json.load(f)], ["a", "b", "c"])
        # Idempotent: the same data again changes nothing.
        history.write_alerts(self.dir)
        with open(path) as f:
            self.assertEqual(len(json.load(f)), 3)
        self.assertEqual(first, first.rstrip("\n") + "\n")

    def test_ratchet_losses(self):
        self.day(*(row(shape=s) for s in ("r", "e", "c", "ok", "gone", "s", "u")))
        history.write_alerts(self.dir)
        self.day(row(shape="r", parity="differs"), row(shape="e", status="500", parity="n/a"),
                 row(shape="c", cold=1.5), row(shape="ok", cold=0.95),
                 row(shape="s", cold=5.0, served=True), row(shape="u", cold=5.0, counters=False))
        # Result and status are lost at once; a slower cold run is one sample.
        lost = history.alerts(self.dir)["ratchet_lost"]
        self.assertEqual([(a["shape"], a["detail"]) for a in lost],
                         [("e", "status 500"), ("r", "result differs from Loki")])
        self.assertTrue(all(a["commits"] == f"{'c1' * 5}..{'c2' * 5}" for a in lost))

    def test_cold_loss_needs_two_runs_and_is_reported_while_it_holds(self):
        self.day(row())
        history.write_alerts(self.dir)
        self.day(row(cold=1.5))
        self.assertEqual(history.alerts(self.dir)["ratchet_lost"], [])
        self.day(row(cold=1.4))
        lost = history.alerts(self.dir)["ratchet_lost"]
        self.assertEqual([(a["kind"], a["commits"]) for a in lost], [("ratchet", f"{'c1' * 5}..{'c3' * 5}")])
        self.assertIn("held for two runs", lost[0]["detail"])
        self.day(row(cold=1.4))
        self.assertEqual(len(history.alerts(self.dir)["ratchet_lost"]), 1)
        self.day(row(cold=0.5))
        self.assertEqual(history.alerts(self.dir)["ratchet_lost"], [])

    def test_cold_loss_ignores_a_loki_cold_the_cache_answered(self):
        self.day(row())
        history.write_alerts(self.dir)
        self.day(row(cold=1.5, served=True))
        self.day(row(cold=1.5, served=True))
        self.assertEqual(history.alerts(self.dir)["ratchet_lost"], [])
        self.day(row(cold=1.5))
        self.assertEqual(history.alerts(self.dir)["ratchet_lost"], [])

    def test_markdown_is_stable(self):
        self.day(row(p50=0.2), row(shape="b", parity="differs"), commit="aaaaaaaaaaaa")
        history.write_alerts(self.dir)
        self.day(row(p50=0.2, parity="differs"), row(shape="b", parity="same"), commit="bbbbbbbbbbbb")
        text = history.render_alerts(history.alerts(self.dir))
        self.assertEqual(text, "\n".join([
            "## Regressions", "",
            "- `control` logs | json · 1h: result vs Loki: same → differs (`aaaaaaaaaa..bbbbbbbbbb`)", "",
            "## Improvements", "",
            "- `control` b · 1h: result vs Loki: differs → same (`aaaaaaaaaa..bbbbbbbbbb`)", "",
            "## Ratchet", "", "Lost (1):", "",
            "- `control` logs | json · 1h: result differs from Loki (`aaaaaaaaaa..bbbbbbbbbb`)", "",
            "Held: 2 rows (1 added by this run); the list is `bench/ab/history/ratchet.json`.", ""]))
        self.assertEqual(text, history.render_alerts(history.alerts(self.dir)))

    def test_empty_sections_say_none(self):
        self.day(row())
        text = history.render_alerts(history.alerts(self.dir))
        self.assertIn("## Regressions\n\nnone\n\n## Improvements\n\nnone\n\n## Ratchet\n\nLost (0):\n\nnone\n", text)

    def test_cli_writes_the_files(self):
        self.day(row(parity="same"))
        self.day(row(parity="differs"))
        out = tempfile.mkdtemp()
        js, md = os.path.join(out, "alerts.json"), os.path.join(out, "alerts.md")
        res = subprocess.run([sys.executable, os.path.join(HERE, "..", "history.py"), "alerts", "--dir", self.dir,
                              "--json", js, "--markdown", md], capture_output=True, text=True, check=True)
        self.assertEqual(res.stdout, "")
        with open(js) as f:
            a = json.load(f)
        self.assertEqual([r["kind"] for r in a["regressions"]], ["result"])
        for k in ("improvements", "ratchet_lost", "ratchet_added", "range"):
            self.assertIn(k, a)
        with open(md) as f:
            self.assertTrue(f.read().startswith("## Regressions\n\n- `control`"))
        self.assertTrue(os.path.exists(os.path.join(self.dir, "ratchet.json")))
        res = subprocess.run([sys.executable, os.path.join(HERE, "..", "history.py"), "alerts", "--dir", self.dir],
                             capture_output=True, text=True, check=True)
        self.assertIn("## Ratchet", res.stdout)


if __name__ == "__main__":
    unittest.main()
