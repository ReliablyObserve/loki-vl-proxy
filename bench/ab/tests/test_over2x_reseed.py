"""The above-2× list after the aligned profile stopped running: the replay of the committed daily history.

tests/fixtures/over-2x-replay.json holds the lines of the daily history (bench/ab/history/<set>.jsonl and
<set>.loki-defaults.jsonl, copied unchanged, reduced to the rows named below) and the rows of
over-2x-loki.json as listed on origin/main f2998a86 (profile aligned, proxy_side unaligned) for six
shapes, one per way a row behaved over 2026-10-29 .. 2026-11-02 in the loki-defaults profile:

  passes_twice_leaves             within 2× on all five runs
  above_throughout_stays          above 2× on all five runs
  within_once_pending             above, then within on the newest run only
  left_then_above_twice_relisted  within on the first two runs, above on the last three
  interrupted_never_two_passes    within, above, within, above, above
  left_then_above_once_not_relisted  within on the first four runs, above on the newest only
"""
import json
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import history  # noqa: E402
import over2x  # noqa: E402

FIXTURE = os.path.join(HERE, "fixtures", "over-2x-replay.json")
DEFAULTS = "loki-defaults"
# The fixture keeps every mode of a shape: three more rows of those shapes were above 2× on two consecutive runs
# and were never listed (the rule adds them, as it would have listed them at the time).
EXTRA = (("json-detected-level", "DLV1 drilldown logs volume, json", "1h", "cold"),
         ("extracted-pushdown", "X1 json filter level_extracted, by level", "24h", "cold"),
         ("level-filter", "LF4 logs, detected_level regexp with unknown", "1h", "warm"))


def load():
    with open(FIXTURE) as f:
        return json.load(f)


def spec_of(fx):
    ranges = {}
    for lines in fx["lines"].values():
        for e in lines:
            for r in e["rows"]:
                ranges.setdefault(e["set"], set()).add(r[e["cols"].index("range")])
    return {"ranges": {"1h": [3600, 5], "6h": [21600, 30], "24h": [86400, 60]},
            "sets": {s: {"ranges": sorted(r)} for s, r in ranges.items()}}


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.fx = load()
        self.spec = spec_of(self.fx)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def directory(self, until="9999", listed=True):
        """The history up to a date (inclusive) in a scratch directory, with the legacy list."""
        out = tempfile.mkdtemp(dir=self.tmp)
        for name, lines in self.fx["lines"].items():
            with open(os.path.join(out, name), "w") as f:
                f.writelines(json.dumps(e, separators=(",", ":")) + "\n" for e in lines if e["date"] <= until)
        if listed:
            over2x.write_baseline(self.fx["listed"], os.path.join(out, over2x.BASELINE_NAME))
        return out

    def key(self, name, profile=DEFAULTS):
        s, shape, rng, mode = self.fx["rows"][name]
        return (profile, s, shape, rng, mode)

    def keys(self, rows):
        return {over2x.key(r) for r in rows}

    def test_the_fixture_is_the_committed_history_not_a_synthetic_one(self):
        # The aligned files end on 2026-10-28 (the profile stopped), the loki-defaults ones run to 2026-11-02.
        for name, lines in self.fx["lines"].items():
            self.assertEqual(lines[-1]["date"], "2026-11-02" if DEFAULTS in name else "2026-10-28", name)
        self.assertEqual({r["proxy_side"] for r in self.fx["listed"]}, {"unaligned"})
        self.assertEqual({r["profile"] for r in self.fx["listed"]}, {"aligned"})

    def test_a_stopped_profiles_listed_rows_are_reported_unjudged_and_nothing_leaves(self):
        # The daily alerts on the frozen aligned lines: nothing leaves, and the list says it judged nothing.
        a = history.alerts(self.directory(), spec=self.spec, bounds=history.NO_BOUNDS)
        self.assertEqual(a["over_2x_removed"], [])
        self.assertTrue(self.keys(self.fx["listed"]) <= self.keys(a["over_2x"]))  # every legacy row is still there
        # the profile that ran is judged, and only it adds rows
        self.assertEqual({r["profile"] for r in a["over_2x_added"]}, {DEFAULTS})
        self.assertEqual({r["set"] for r in a["over_2x_unjudged"]}, set(self.spec["sets"]))
        self.assertEqual({(r["profile"], r["last"], r["current"]) for r in a["over_2x_stale"]},
                         {("aligned", "2026-10-28", "2026-11-02")})
        self.assertIn("Not judged: 6 listed aligned rows", history.render_alerts(a))

    def test_a_row_leaves_on_the_run_that_is_its_second_pass_and_not_before(self):
        moved = [{**r, "profile": DEFAULTS} for r in self.fx["listed"]]
        for r in moved:
            r.pop("proxy_side")
        gone = {}
        for until, expect in (("2026-10-29", set()), ("2026-10-30", {"passes_twice_leaves", "left_then_above_twice_relisted",
                                                                       "left_then_above_once_not_relisted"})):
            rows, left, _, _ = history.replay_over_2x(self.directory(until), self.spec, moved)
            gone[until] = {n for n in self.fx["rows"] if self.key(n) in self.keys(left)}
            self.assertEqual(gone[until], expect, until)
        self.assertEqual({r["left"] for r in left}, {"2026-10-30"})

    def test_reseed_moves_the_rows_and_applies_the_rule_to_the_replayed_history(self):
        res = history.reseed_over_2x(self.directory(), self.spec)
        after = {over2x.key(r): r for r in res["rows"]}
        stays = ("above_throughout_stays", "within_once_pending", "left_then_above_twice_relisted",
                 "interrupted_never_two_passes")
        extra = {(DEFAULTS, *k) for k in EXTRA}
        self.assertEqual(set(after), {self.key(n) for n in stays} | extra)
        self.assertEqual({k[0] for k in after}, {DEFAULTS})
        self.assertNotIn("proxy_side", json.dumps(res["rows"]))
        # the original date is kept for a row that never left; a row that left and came back counts from its streak
        self.assertEqual(after[self.key("above_throughout_stays")]["since"], "2026-10-27")
        self.assertEqual(after[self.key("within_once_pending")]["since"], "2026-10-27")
        self.assertEqual(after[self.key("interrupted_never_two_passes")]["since"], "2026-10-27")
        self.assertEqual(after[self.key("left_then_above_twice_relisted")]["since"], "2026-10-31")
        self.assertEqual({over2x.key(r): r["left"] for r in res["left"]},
                         {self.key(n): "2026-10-30" for n in ("passes_twice_leaves", "left_then_above_twice_relisted",
                                                              "left_then_above_once_not_relisted")})
        self.assertEqual(self.keys(res["reseeded"]), {self.key("left_then_above_twice_relisted")})
        self.assertEqual(self.keys(res["seeded"]), extra)
        self.assertEqual(self.keys(res["warnings"]), {self.key("left_then_above_once_not_relisted")})
        self.assertEqual(history.count_table(res["before"]), {("aligned", "cold"): 2, ("aligned", "warm"): 4})
        self.assertEqual(history.count_table(res["after"]), {(DEFAULTS, "cold"): 3, (DEFAULTS, "warm"): 4})

    def test_a_row_above_2x_on_the_newest_run_only_is_a_warning_never_listed(self):
        res = history.reseed_over_2x(self.directory(), self.spec)
        k = self.key("left_then_above_once_not_relisted")
        self.assertNotIn(k, {over2x.key(r) for r in res["rows"]})
        # and the daily rule does not add it either: it is reported as new
        out = self.directory()
        over2x.write_baseline(res["rows"], os.path.join(out, over2x.BASELINE_NAME))
        a = history.alerts(out, spec=self.spec, bounds=history.NO_BOUNDS)
        self.assertIn(k, {(r["profile"], r["set"], r["shape"], r["range"], r["mode"]) for r in a["over_2x_new"]})
        self.assertNotIn(k, {over2x.key(r) for r in a["over_2x"]})

    def replay_empty(self, until="9999"):
        """The daily rule from an empty list over the fixture's loki-defaults history."""
        return history.replay_over_2x(self.directory(until, listed=False), self.spec, [])

    def test_the_rule_adds_a_row_after_two_consecutive_runs_above_2x(self):
        rows, left, added, pending = self.replay_empty()
        got = {over2x.key(r): r for r in rows}
        self.assertEqual(left, [])
        # above on all five runs: added on the second (10-30), counted from the first
        self.assertEqual(got[self.key("above_throughout_stays")]["since"], "2026-10-29")
        day = {over2x.key(r): r["added"] for r in added}
        self.assertEqual(day[self.key("above_throughout_stays")], "2026-10-30")
        # within, within, above, above, above: added on 11-01, counted from 10-31
        self.assertEqual(day[self.key("left_then_above_twice_relisted")], "2026-11-01")
        self.assertEqual(got[self.key("left_then_above_twice_relisted")]["since"], "2026-10-31")
        self.assertEqual(got[self.key("left_then_above_twice_relisted")]["profile"], DEFAULTS)
        self.assertNotIn(self.key("passes_twice_leaves"), got)

    def test_one_run_above_2x_stays_a_warning_and_is_not_added(self):
        rows, _, added, pending = self.replay_empty()
        spike = self.key("left_then_above_once_not_relisted")  # within on four runs, above on the newest only
        self.assertNotIn(spike, {over2x.key(r) for r in rows})
        self.assertIn(spike, {(r["profile"], r["set"], r["shape"], r["range"], r["mode"]) for r in pending})
        self.assertIn("added if the next run agrees", [r["detail"] for r in pending if r["shape"] == spike[2]][0])
        # above on 10-30, within on 10-31, above on 11-01 and 11-02: the first spike did not add it, the pair does
        broken = self.key("interrupted_never_two_passes")
        self.assertEqual({over2x.key(r): r["added"] for r in added}[broken], "2026-11-02")
        self.assertEqual({over2x.key(r): r["since"] for r in rows}[broken], "2026-11-01")
        # and a spike followed by a run within 2× is never added
        out = self.directory(listed=False)
        path = os.path.join(out, "json-filter-pushdown.loki-defaults.jsonl")
        line = history.read(path)[-2]  # the 11-01 line, within 2×
        with open(path, "a") as f:
            f.write(json.dumps({**line, "date": "2026-11-03"}, separators=(",", ":")) + "\n")
        rows, _, added, pending = history.replay_over_2x(out, self.spec, [])
        self.assertNotIn(spike, {over2x.key(r) for r in rows})
        self.assertNotIn(spike, {(r["profile"], r["set"], r["shape"], r["range"], r["mode"]) for r in pending})

    def test_a_row_is_added_then_removed_then_added_again_by_the_same_rule(self):
        cycle = self.key("within_once_pending")  # above on four runs, within on the newest
        rows, left, added, pending = self.replay_empty()
        self.assertEqual({over2x.key(r): r["added"] for r in added}[cycle], "2026-10-30")
        self.assertIn(cycle, {over2x.key(r) for r in rows})
        out = self.directory(listed=False)
        over2x.write_baseline(rows, os.path.join(out, over2x.BASELINE_NAME))
        path = os.path.join(out, "json-detected-level.loki-defaults.jsonl")
        within, above = history.read(path)[-1], history.read(path)[-2]
        days = [("2026-11-03", within), ("2026-11-04", above), ("2026-11-05", above)]
        events = []
        for date, line in days:
            with open(path, "a") as f:
                f.write(json.dumps({**line, "date": date}, separators=(",", ":")) + "\n")
            a = history.alerts(out, spec=self.spec, bounds=history.NO_BOUNDS)
            events.append((date, cycle in self.keys(a["over_2x_removed"]), cycle in self.keys(a["over_2x_added"]),
                           cycle in {over2x.key(r) for r in a["over_2x"]}))
            over2x.write_baseline(a["over_2x"], os.path.join(out, over2x.BASELINE_NAME))
        # 11-03: second pass within 2× -> removed; 11-04: above once -> a warning, not listed; 11-05: above twice -> added
        self.assertEqual(events, [("2026-11-03", True, False, False), ("2026-11-04", False, False, False),
                                  ("2026-11-05", False, True, True)])

    def test_the_list_after_the_reseed_is_stable_under_the_daily_rule(self):
        out = self.directory()
        res = history.reseed_over_2x(out, self.spec)
        over2x.write_baseline(res["rows"], os.path.join(out, over2x.BASELINE_NAME))
        a = history.alerts(out, spec=self.spec, bounds=history.NO_BOUNDS)
        self.assertEqual(self.keys(a["over_2x"]), self.keys(res["rows"]))
        self.assertEqual((a["over_2x_removed"], a["over_2x_unjudged"]), ([], []))
        self.assertEqual(a["over_2x_stale"] and {r["profile"] for r in a["over_2x_stale"]}, {"aligned"})
        self.assertEqual(self.keys(a["over_2x_pending"]), {self.key("within_once_pending")})
        # the next passing run removes the pending row, as the rule says
        line = json.loads(json.dumps(history.read(os.path.join(out, "json-detected-level.loki-defaults.jsonl"))[-1]))
        line["date"] = "2026-11-03"
        with open(os.path.join(out, "json-detected-level.loki-defaults.jsonl"), "a") as f:
            f.write(json.dumps(line, separators=(",", ":")) + "\n")
        b = history.alerts(out, spec=self.spec, bounds=history.NO_BOUNDS)
        self.assertEqual(self.keys(b["over_2x_removed"]), {self.key("within_once_pending")})

    def test_reseed_refuses_a_list_with_nothing_to_move(self):
        out = self.directory()
        res = history.reseed_over_2x(out, self.spec)
        over2x.write_baseline(res["rows"], os.path.join(out, over2x.BASELINE_NAME))
        with self.assertRaises(ValueError):
            history.reseed_over_2x(out, self.spec)

    def test_the_table_states_before_after_and_who_left(self):
        text = history.render_reseed(history.reseed_over_2x(self.directory(), self.spec))
        self.assertIn("| aligned | cold | 2 | 0 |", text)
        self.assertIn("| loki-defaults | warm | 0 | 4 |", text)
        self.assertIn("| total | | 6 | 7 |", text)
        self.assertIn("Left the list: 3 rows (2026-10-30: 3); 1 of them were above 2× on two consecutive runs again and are listed again; 3 loki-defaults rows", text)
        self.assertIn("| control | 0 | 0 | 1 | 0 |", text)
        self.assertIn("| extracted-pushdown | 0 | 1 | 1 | 1 |", text)
        self.assertIn("| control | topk rate by app | 24h | warm | 3.25× | 2026-10-30 | no |", text)
        self.assertIn("| extracted-pushdown | X1 json filter level_extracted, by level | 24h | warm | 2.63× | 2026-10-30 | yes |", text)
        self.assertIn("1 more rows are above 2× on the newest run only and stay pending add.", text)


if __name__ == "__main__":
    unittest.main()
