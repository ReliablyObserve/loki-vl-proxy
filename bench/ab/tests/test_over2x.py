"""Unit tests for bench/ab/over2x.py: the above-2× Loki definition, the PR gate, the daily list and the replays."""
import contextlib
import glob
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "conformance", "scripts"))
import comment  # noqa: E402
import history  # noqa: E402
import over2x  # noqa: E402
import perf_evidence  # noqa: E402
import pr_smoke  # noqa: E402

RESULTS = os.path.join(HERE, "..", "results")
NOISE, MIN_DELTA = 0.30, 0.10  # pr_smoke's defaults


def row(shape="s", rng="1h", pr=0.5, base=0.5, loki=0.1, pr_cold=None, base_cold=None, loki_cold=None,
        status=("200", "200", "200"), verdict="same", **extra):
    r = {"shape": shape, "range": rng, "verdict": verdict, "parity_vs_reference": "same",
         "baseline_parity_vs_reference": "same",
         "p50": {"base": base, "pr": pr, "loki": loki},
         "cold": {"base": base if base_cold is None else base_cold, "pr": pr if pr_cold is None else pr_cold,
                  "loki": loki if loki_cold is None else loki_cold},
         "status": dict(zip(("base", "pr", "loki"), status))}
    r.update(extra)
    return r


def summary(*rows, set_name="control", **extra):
    return {"set": set_name, "baseline": "base", "candidate": "pr", "reference": "loki", "runs": 4, "valid": True,
            "restart_before": "0", "restart_after": "0", "rows": list(rows), **extra}


def listed(shape="s", rng="1h", mode="warm", ratio=5.0, set_name="control"):
    return {(set_name, shape, rng, mode): {"set": set_name, "shape": shape, "range": rng, "mode": mode,
                                           "ratio": ratio}}


def kinds(s, baseline, mode="warm"):
    return [(f["kind"], f.get("note", "")) for f in over2x.gate(s, baseline, NOISE, MIN_DELTA) if f["mode"] == mode]


class DefinitionTest(unittest.TestCase):
    def test_ratio_and_absolute_floor(self):
        self.assertTrue(over2x.above(0.3, 0.1))
        self.assertFalse(over2x.above(0.2, 0.1))        # exactly 2×
        self.assertFalse(over2x.above(0.010, 0.003))    # 3.3× but 7 ms: noise
        self.assertFalse(over2x.above(0.149, 0.1))      # 1.49×
        self.assertFalse(over2x.above(0.09, 0.04))      # 2.25× but 50 ms: inside the floor
        self.assertTrue(over2x.above(0.0901, 0.04))
        self.assertTrue(over2x.above(0.2, 0.0))         # Loki answered in no time
        self.assertFalse(over2x.above(None, 0.1))

    def test_comparability(self):
        self.assertEqual(over2x.judge(0.5, 0.1, "200", "200", False), "above")
        self.assertEqual(over2x.judge(0.15, 0.1, "200", "200", False), "within")
        # A Loki error or timeout is not a proxy defect: recorded apart, never counted.
        self.assertEqual(over2x.judge(0.5, 30.0, "200", "504", False), "loki_failed")
        self.assertEqual(over2x.judge(0.5, 0.1, "200", "200/504", False), "loki_failed")
        self.assertEqual(over2x.judge(0.5, 0.1, "500", "200", False), "not_comparable")
        self.assertEqual(over2x.judge(0.5, 0.1, "200", "200", True), "not_comparable")  # Loki's cache answered

    def test_cache_answered_cold_drops_cold_only(self):
        r = row(pr=0.5, loki=0.1, cold_cache_served=["loki"])
        states = {m["mode"]: m["state"] for m in over2x.measure(summary(r))}
        self.assertEqual(states, {"cold": "not_comparable", "warm": "above"})
        stale = row(pr=0.5, loki=0.1, stale=["loki"])
        self.assertEqual({m["mode"]: m["state"] for m in over2x.measure(summary(stale))},
                         {"cold": "not_comparable", "warm": "above"})

    def test_modes_of_a_summary(self):
        cold_only = summary(row(pr=0.5, loki=0.1), timing="cold")
        self.assertEqual([m["mode"] for m in over2x.measure(cold_only)], ["cold"])
        refresh = row(pr=0.2, loki=0.1, refresh={"pr": 0.6, "loki": 0.1, "base": 0.6},
                      refresh_status={"pr": "200", "loki": "200", "base": "200"})
        got = {m["mode"]: (m["state"], m["ratio"]) for m in over2x.measure(summary(refresh))}
        self.assertEqual(got, {"cold": ("within", 2.0), "warm": ("within", 2.0), "refresh": ("above", 6.0)})

    def test_history_rows_judge_like_summaries(self):
        # The daily list is kept from history lines; they must give what the summary gives, on real data.
        n = 0
        for path in sorted(glob.glob(os.path.join(RESULTS, "daily-*.json"))):
            with open(path) as f:
                s = json.load(f)
            from_summary = {(m["shape"], m["range"], m["mode"]): m["state"] for m in over2x.measure(s)}
            for hrow in history.rows_of(history.entry(s)).values():
                for mode, pair in over2x.history_pairs(hrow).items():
                    self.assertEqual(over2x.judge(*pair), from_summary[(hrow["shape"], hrow["range"], mode)])
                    n += 1
        self.assertGreater(n, 0)


class GateTest(unittest.TestCase):
    def test_new_row(self):
        # base 0.15 s within 2× of Loki's 0.1 s; the PR takes 0.5 s, not listed.
        self.assertEqual(kinds(summary(row(pr=0.5, base=0.15)), {}), [("new", "")])

    def test_known_row_within_noise_passes(self):
        s = summary(row(pr=0.52, base=0.5))
        self.assertEqual(kinds(s, listed(ratio=5.0)), [("known", "as slow on base")])
        self.assertEqual(kinds(s, {}), [("known", "as slow on base")])  # listed or not

    def test_worsened_listed_row(self):
        # listed at 5×; the base is at 5× in this run, the PR at 9×: worse than both beyond noise.
        self.assertEqual(kinds(summary(row(pr=0.9, base=0.5)), listed(ratio=5.0)), [("worsened", "listed")])

    def test_listed_row_slower_than_base_but_within_its_listed_ratio_is_known(self):
        # The base is fine in this run (1.5×), the PR at 4×: slower than base, but not beyond the listed 5×.
        self.assertEqual(kinds(summary(row(pr=0.4, base=0.15)), listed(ratio=5.0)), [("known", "listed")])

    def test_listed_row_worse_than_its_ratio_but_not_than_base_is_known(self):
        # Both builds at 9× in this run (the PR smoke's data is not the daily's): the PR did not do it.
        self.assertEqual(kinds(summary(row(pr=0.92, base=0.9)), listed(ratio=5.0)), [("known", "as slow on base")])

    def test_improved_row_passes(self):
        s = summary(row(pr=0.3, base=0.9))
        self.assertEqual(kinds(s, listed(ratio=9.0)), [("known", "as slow on base")])
        fixed = summary(row(pr=0.15, base=0.9))
        self.assertEqual(kinds(fixed, listed(ratio=9.0)), [("cleared", "")])

    def test_base_also_above_in_the_same_run(self):
        # Not listed (the daily run never measured it above), but the base build is as slow here.
        self.assertEqual(kinds(summary(row(pr=0.55, base=0.5)), {}), [("known", "as slow on base")])
        # ... and a PR that makes it worse than the base beyond noise is flagged even though it is not listed.
        self.assertEqual(kinds(summary(row(pr=0.9, base=0.5)), {}), [("worsened", "above 2× on base too")])

    def test_base_just_under_the_floor_and_pr_within_noise_is_known(self):
        # Replayed 2026-10-14 (#760) D volume 1h warm: base 2.72× but 49.9 ms over Loki, PR 98 ms: not moved.
        self.assertEqual(kinds(summary(row(pr=0.098, base=0.0789, loki=0.029)), {}),
                         [("known", "within noise of base")])

    def test_base_not_comparable_makes_a_new_row(self):
        s = summary(row(pr=0.5, base=0.5, status=("502", "200", "200"), verdict="fixed"))
        self.assertEqual(kinds(s, {}), [("new", "")])

    def test_loki_failure_is_recorded_apart(self):
        s = summary(row(pr=0.5, base=0.15, loki=90.0, status=("200", "200", "504")))
        self.assertEqual(kinds(s, {}), [("loki_failed", "")])
        self.assertEqual(over2x.failing(over2x.gate(s, {}, NOISE, MIN_DELTA)), [])

    def test_cold_mode_uses_cold_runs(self):
        s = summary(row(pr=0.12, base=0.12, loki=0.1, pr_cold=1.0, base_cold=0.2, loki_cold=0.2))
        self.assertEqual(kinds(s, {}, "cold"), [("new", "")])
        self.assertEqual(kinds(s, {}, "warm"), [])

    def test_remeasure_lists_failing_cold_and_warm_shapes_only(self):
        r = row(shape="ref", pr=0.12, base=0.12, refresh={"pr": 0.9, "base": 0.1, "loki": 0.1},
                refresh_status={"pr": "200", "base": "200", "loki": "200"})
        s = summary(row(shape="a", pr=0.5, base=0.15), row(shape="b", pr=0.52, base=0.5), r)
        self.assertEqual(over2x.remeasure(s, {}, NOISE, MIN_DELTA), ["a"])

    def test_annotations(self):
        found = over2x.gate(summary(row(shape="50% | json", pr=0.5, base=0.15)), {}, NOISE, MIN_DELTA)
        lines = over2x.annotations(found)
        self.assertEqual(len(lines), 2)  # cold and warm
        self.assertTrue(all(line.startswith("::warning title=above 2x Loki::new: control / 50%25 | json") for line in lines))
        self.assertTrue(over2x.annotations(found, "fail")[0].startswith("::error "))
        many = [dict(found[0], shape=str(i)) for i in range(12)]
        self.assertEqual(over2x.annotations(many)[-1], "2 more above-2× Loki row(s) in the PR comment")


SELECTION = {"run": True, "changed": 1, "why": {}}
META = {"base": "a" * 40, "head": "b" * 40, "runs": 4, "ranges": ["1h"], "noise": 0.3, "min_delta": 0.1,
        "confirm_runs": 7}


class CommentTest(unittest.TestCase):
    rows = [row(shape="new one", pr=0.5, base=0.15), row(shape="known one", pr=0.52, base=0.5),
            row(shape="fine", pr=0.1, base=0.1)]

    def test_warn_mode_changes_nothing_but_the_text(self):
        plain_text, plain = comment.render([summary(*self.rows)], SELECTION, META)
        text, verdict = comment.render([summary(*self.rows)], SELECTION, dict(META, over_2x_loki="warn"), {})
        self.assertEqual({k: v for k, v in verdict.items() if k != "over_2x"}, plain)
        self.assertEqual((verdict["state"], verdict["exit"], verdict["failed"]), ("passed", 0, False))
        self.assertEqual({k: verdict["over_2x"][k] for k in ("mode", "new", "worsened", "known")},
                         {"mode": "warn", "new": 2, "worsened": 0, "known": 2})
        lines = text.splitlines()
        self.assertEqual(lines[0], comment.MARKER)
        self.assertEqual(lines[3], "#### ⚠️ Above 2× Loki: 2 new, 0 worsened")  # right under the title
        self.assertIn("Warning only", text)
        self.assertIn("| new | warm | new one | 1h | 500 ms | 100 ms | **5.00×** | 1.50× | — |", text)
        self.assertIn("2 shape×range above 2× Loki already", text)
        self.assertNotIn("Above 2× Loki", plain_text)

    def test_fail_mode_fails_the_check(self):
        text, verdict = comment.render([summary(*self.rows)], SELECTION, dict(META, over_2x_loki="fail"), {})
        self.assertEqual((verdict["state"], verdict["exit"], verdict["failed"]), ("failed", 1, True))
        self.assertIn("### 🔴 Performance A/B: above 2× Loki", text)
        self.assertIn("#### 🔴 Above 2× Loki: 2 new, 0 worsened", text)
        self.assertIn("This fails `perf-smoke`.", text)

    def test_fail_mode_passes_without_new_or_worsened_rows(self):
        text, verdict = comment.render([summary(*self.rows[1:])], SELECTION, dict(META, over_2x_loki="fail"), {})
        self.assertEqual((verdict["state"], verdict["exit"]), ("passed", 0))
        self.assertNotIn("####", text)

    def test_known_rows_are_capped(self):
        rows = [row(shape=f"k{i:02d}", pr=0.52, base=0.5) for i in range(30)]  # 60 known: cold and warm
        text, verdict = comment.render([summary(*rows)], SELECTION, META, {})
        self.assertEqual(verdict["over_2x"]["known"], 60)
        self.assertIn(f"… and {60 - comment.KNOWN_CAP} more", text)

    def test_other_failures_keep_their_title(self):
        slow = row(shape="slow", pr=0.5, base=0.15, verdict="slower")
        _, verdict = comment.render([summary(slow)], SELECTION, dict(META, over_2x_loki="warn"), {})
        self.assertEqual((verdict["state"], verdict["exit"]), ("failed", 1))


class ConfirmTest(unittest.TestCase):
    def test_a_new_above_2x_row_is_remeasured_and_replaced(self):
        first = summary(row(shape="s", pr=0.5, base=0.15), row(shape="t", pr=0.1, base=0.1))
        redo = summary(row(shape="s", pr=0.16, base=0.15))
        args = type("Args", (), dict(noise=NOISE, min_delta=MIN_DELTA, confirm_runs=7))()
        with mock.patch.object(pr_smoke, "run_matrix", return_value=redo) as run, mock.patch.object(pr_smoke.stack, "log"):
            out = pr_smoke.confirm(first, args, {"confirm_end": 1}, None, "/out", [("base", "u")], {})
        self.assertEqual(run.call_args.args[1], ["s"])
        self.assertTrue(out["rows"][0]["confirmed"])
        self.assertEqual(over2x.failing(over2x.gate(out, {}, NOISE, MIN_DELTA)), [])  # the re-run settles it

    def test_a_cold_finding_survives_a_rerun_whose_loki_cold_the_cache_answered(self):
        # 5 of the 5 confirmed rows of 2026-10-22-json-error-split-1h.json had a cache-served Loki cold.
        first = summary(row(shape="s", pr=0.12, base=0.12, pr_cold=1.0, base_cold=0.2, loki_cold=0.2))
        redo = summary(row(shape="s", pr=0.12, base=0.12, pr_cold=1.0, base_cold=0.2, loki_cold=0.01,
                           cold_cache_served=["loki"]))
        args = type("Args", (), dict(noise=NOISE, min_delta=MIN_DELTA, confirm_runs=7))()
        with mock.patch.object(pr_smoke, "run_matrix", return_value=redo), mock.patch.object(pr_smoke.stack, "log"):
            out = pr_smoke.confirm(first, args, {"confirm_end": 1}, None, "/out", [("base", "u")], {})
        r = out["rows"][0]
        self.assertEqual((r["cold"]["loki"], r.get("cold_cache_served"), r["cold_from_first_pass"]), (0.2, None, True))
        self.assertEqual([(f["kind"], f["mode"], f["confirmed"]) for f in over2x.failing(over2x.gate(out, {}, NOISE, MIN_DELTA))],
                         [("new", "cold", True)])

    def test_without_a_baseline_nothing_extra_is_remeasured(self):
        first = summary(row(shape="s", pr=0.5, base=0.15))
        args = type("Args", (), dict(noise=NOISE, min_delta=MIN_DELTA, confirm_runs=7))()
        with mock.patch.object(pr_smoke, "run_matrix") as run:
            pr_smoke.confirm(first, args, {"confirm_end": 1}, None, "/out", [("base", "u")])
        run.assert_not_called()


class MainTest(unittest.TestCase):
    """pr_smoke.main: the list comes from the base commit, reaches the comment, and the mode decides the exit."""

    def run_main(self, mode):
        out = tempfile.mkdtemp()
        sel = os.path.join(out, "sel.json")
        with open(sel, "w") as f:
            json.dump({"run": True, "sets": {"control": ["s"]}, "why": {}}, f)
        result = summary(row(shape="s", pr=0.5, base=0.15))
        argv = ["pr_smoke.py", "--base", "HEAD", "--selection", sel, "--out", out, "--over-2x-loki", mode]
        stdout = io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch.object(pr_smoke, "measure", return_value=[result]) as m, \
                mock.patch.object(pr_smoke.over2x, "baseline_at", return_value={}) as at, \
                contextlib.redirect_stdout(stdout):
            code = pr_smoke.main()
        base = pr_smoke.git("rev-parse", "HEAD")
        at.assert_called_once_with(base)
        self.assertEqual(m.call_args.args[-1], {})  # the list reaches measure (and so the re-measure)
        with open(os.path.join(out, "verdict.json")) as f:
            return code, stdout.getvalue(), json.load(f)

    def test_warn_prints_annotations_and_keeps_the_exit(self):
        code, text, verdict = self.run_main("warn")
        self.assertEqual((code, verdict["state"]), (0, "passed"))
        self.assertIn("::warning title=above 2x Loki::new: control / s / 1h / warm", text)
        self.assertIn("#### ⚠️ Above 2× Loki: 2 new, 0 worsened", text)

    def test_fail_fails(self):
        code, text, verdict = self.run_main("fail")
        self.assertEqual((code, verdict["state"]), (1, "failed"))
        self.assertIn("::error title=above 2x Loki::new: control / s / 1h / cold", text)

    def test_default_mode_is_warn(self):
        self.assertEqual(over2x.DEFAULT_GATE, "warn")


class BaselineFileTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, over2x.BASELINE_NAME)

    def test_missing_is_empty_and_garbage_is_a_problem(self):
        self.assertEqual(over2x.read_baseline(self.path), ({}, ""))
        with open(self.path, "w") as f:
            f.write("[1, 2]")
        rows, problem = over2x.read_baseline(self.path)
        self.assertEqual(rows, {})
        self.assertIn("cannot be read", problem)

    def test_seed_writes_sorted_rows(self):
        s = summary(row(shape="b", pr=0.5, base=0.5), row(shape="a", pr=0.5, base=0.5, cold_cache_served=["loki"]),
                    row(shape="c", pr=0.1, base=0.1), date="2026-10-24", commit="c" * 40)
        over2x.write_baseline(over2x.seed([s]), self.path)
        rows, _ = over2x.read_baseline(self.path)
        self.assertEqual(list(rows), [("control", "a", "1h", "warm"), ("control", "b", "1h", "cold"),
                                      ("control", "b", "1h", "warm")])
        self.assertEqual(rows[("control", "b", "1h", "warm")]["since"], "2026-10-24")
        self.assertEqual(over2x.count_line(list(rows.values())),
                         "above-2× baseline: 3 rows (cold 1, warm 2, refresh 0); the gate becomes blocking at 0")

    def test_the_committed_list_reads(self):
        # pr_smoke reads it from the base of every pull request: a list that cannot be read would stop them all.
        rows, problem = over2x.read_baseline(over2x.BASELINE)
        self.assertEqual(problem, "")
        self.assertTrue(all(r["mode"] in over2x.MODES and r["ratio"] > over2x.RATIO for r in rows.values()))

    def test_baseline_at_reads_the_committed_file(self):
        repo = self.dir
        git = ["git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"]
        subprocess.run(git[:3] + ["init", "-q"], check=True)
        os.makedirs(os.path.join(repo, "bench/ab/history"))
        over2x.write_baseline([dict(listed()[("control", "s", "1h", "warm")])],
                              os.path.join(repo, over2x.BASELINE_REL))
        subprocess.run(git + ["add", "."], check=True)
        subprocess.run(git + ["commit", "-q", "-m", "x"], check=True)
        self.assertEqual(list(over2x.baseline_at("HEAD", repo)), [("control", "s", "1h", "warm")])
        self.assertEqual(over2x.baseline_at("HEAD~5", repo), {})  # no such ref or file: empty


SPEC = {"ranges": {"1h": [3600, 5], "24h": [86400, 60]}, "sets": {"control": {"ranges": ["1h"]}}}


def daily_row(p50=0.5, loki=0.1, cold=0.5, loki_cold=0.1, loki_status="200", shape="s"):
    return {"shape": shape, "range": "1h", "verdict": "same", "parity_vs_reference": "same",
            "p50": {"release": p50, "main": p50, "loki": loki}, "cold": {"release": cold, "main": cold, "loki": loki_cold},
            "status": {"release": "200", "main": "200", "loki": loki_status},
            "cache": {"loki": {"cold": {"result": {"entriesFound": 0}}}}}


class DailyListTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.n = 0
        self.path = os.path.join(self.dir, over2x.BASELINE_NAME)
        over2x.write_baseline([{"set": "control", "shape": "s", "range": "1h", "mode": "warm", "ratio": 5.0,
                                "proxy_s": 0.5, "loki_s": 0.1, "since": "2026-10-01", "commit": "c"}], self.path)

    def day(self, *rows, runs=7):
        self.n += 1
        s = {"set": "control", "label": "daily", "date": f"2026-10-{self.n:02d}", "baseline": "release",
             "candidate": "main", "reference": "loki", "runs": runs, "valid": True, "rows": list(rows)}
        history.append(s, self.dir, f"c{self.n}" * 12, SPEC)
        return history.write_alerts(self.dir, spec=SPEC)

    def held(self):
        return [over2x.key(r) for r in over2x.read_baseline(self.path)[0].values()]

    def test_removal_needs_two_consecutive_runs_within_2x(self):
        a = self.day(daily_row(p50=0.15, cold=0.15))
        self.assertEqual([r["shape"] for r in a["over_2x_pending"]], ["s"])
        self.assertEqual(self.held(), [("control", "s", "1h", "warm")])
        a = self.day(daily_row(p50=0.5, cold=0.15))   # above again: starts over
        self.assertEqual((a["over_2x_pending"], a["over_2x_removed"]), ([], []))
        self.day(daily_row(p50=0.15, cold=0.15))
        a = self.day(daily_row(p50=0.15, cold=0.15))
        self.assertEqual([r["mode"] for r in a["over_2x_removed"]], ["warm"])
        self.assertIn("150 ms against Loki's 100 ms, 1.50×, and within 2× on the previous run too",
                      history.render_alerts(a))
        self.assertEqual(self.held(), [])

    def test_no_automatic_additions(self):
        a = self.day(daily_row(), daily_row(shape="t"))
        self.assertEqual([(r["shape"], r["mode"]) for r in a["over_2x_new"]],
                         [("s", "cold"), ("t", "cold"), ("t", "warm")])
        self.assertEqual(self.held(), [("control", "s", "1h", "warm")])
        body = history.render_alerts(a, limit=history.PR_LIMIT)
        self.assertTrue(body.startswith("## ⚠️ Above 2× Loki\n\n**above-2× baseline: 1 rows (cold 0, warm 1, refresh 0); "
                                        "the gate becomes blocking at 0.**"))
        self.assertIn("- `control` t · 1h · warm: 500 ms against Loki's 100 ms, 5.00×", body)
        self.assertEqual((a["regressions"], a["ratchet_lost"]), ([], []))  # warnings, never a red run

    def test_lower_ratio_tightens_and_higher_does_not(self):
        self.day(daily_row(p50=0.3))
        self.assertEqual(over2x.read_baseline(self.path)[0][("control", "s", "1h", "warm")]["ratio"], 3.0)
        self.day(daily_row(p50=0.9))
        self.assertEqual(over2x.read_baseline(self.path)[0][("control", "s", "1h", "warm")]["ratio"], 3.0)

    def test_loki_failure_and_cut_down_runs_do_not_count_as_within(self):
        self.day(daily_row(p50=0.15, loki_status="504"))
        a = self.day(daily_row(p50=0.15))
        self.assertEqual((len(a["over_2x_pending"]), a["over_2x_removed"]), (1, []))  # the 504 run did not count
        self.day(daily_row(p50=0.15), runs=3)  # a cut-down manual run is not compared
        self.assertEqual(self.held(), [("control", "s", "1h", "warm")])

    def test_inside_the_floor_counts_as_within(self):
        self.day(daily_row(p50=0.04, loki=0.006))
        a = self.day(daily_row(p50=0.04, loki=0.006))
        self.assertEqual(a["over_2x_removed"][0]["detail"],
                         "40 ms against Loki's 6 ms, 6.67× (inside the 50 ms floor), and within 2× on the previous run too")

    def test_no_list_no_section(self):
        os.remove(self.path)
        a = self.day(daily_row())
        self.assertEqual(a["over_2x_new"], [])
        self.assertNotIn("Above 2×", history.render_alerts(a))
        self.assertFalse(os.path.exists(self.path))


class PerfEvidenceTest(unittest.TestCase):
    def test_section(self):
        over = {"rule": over2x.RULE, "rows": [{"set": "control", "shape": "a | b", "range": "1h", "mode": "cold",
                                               "proxy_s": 0.5, "loki_s": 0.1, "ratio": 5.0, "since": "2026-10-24"}]}
        text = "\n".join(perf_evidence.over_2x(over))
        self.assertIn("## Above 2× Loki (must fix)", text)
        self.assertIn("1 rows: cold 1, warm 0, refresh 0.", text)
        self.assertIn("| control | a \\| b | 1h | cold | 0.50s | 0.10s | 5.00× | 2026-10-24 |", text)
        self.assertEqual(perf_evidence.over_2x(None), [])


class ReplayTest(unittest.TestCase):
    """Saved pr_smoke summaries (bench/ab/results) through the gate."""

    def gate(self, name, swap=False):
        with open(os.path.join(RESULTS, name)) as f:
            s = json.load(f)
        if swap:  # what reverting the change would have shown
            s["baseline"], s["candidate"] = s["candidate"], s["baseline"]
        return over2x.gate(s, {}, NOISE, MIN_DELTA)

    def test_merged_prs_add_nothing_above_2x(self):
        for name in ("2026-10-22-json-error-split-1h.json", "2026-10-22-json-error-split-control-1h.json",
                     "2026-10-17-unwrapwithout-control-1h.json", "2026-10-14-detlevel-d-capped-1h.json",
                     "2026-10-11-unwrapsum-1h.json"):
            self.assertEqual(over2x.failing(self.gate(name)), [], name)

    def test_reverting_758_would_be_flagged(self):
        # #758 took | json | __error__="" metrics to VictoriaLogs stats: J1..J5 warm went from ~30-50× Loki to within 2×.
        bad = over2x.failing(self.gate("2026-10-22-json-error-split-1h.json", swap=True))
        self.assertEqual(sorted({f["shape"].split()[0] for f in bad}), ["J1", "J2", "J3", "J4", "J5"])
        self.assertTrue(all(f["kind"] == "new" and f["mode"] == "warm" for f in bad))

    def test_long_runs_where_loki_failed_are_not_counted(self):
        found = self.gate("2026-10-22-json-error-split-long.json")
        self.assertTrue(found)
        self.assertEqual({f["kind"] for f in found}, {"loki_failed"})


if __name__ == "__main__":
    unittest.main()
