"""Unit tests for pr_smoke.py: per-set windows, the confirm pass and the refresh pass."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import perf_matrix  # noqa: E402
import pr_smoke  # noqa: E402

END = 1_800_000_000


def read_json(path):
    with open(path) as f:
        return json.load(f)


def args(**over):
    base = dict(ranges=["1h"], runs=4, confirm_runs=7, refresh_s=0, refresh_runs=5, noise=0.3, min_delta=0.1,
                timeout=90)
    base.update(over)
    return type("Args", (), base)()


def selection(*sets):
    with open(os.path.join(pr_smoke.HERE, "shapes.json")) as f:
        spec = json.load(f)
    return {"run": True, "sets": {name: [s["name"] for s in spec["sets"][name]["shapes"]] for name in sets}}


class WindowPlanTest(unittest.TestCase):
    def test_every_set_gets_its_own_block_and_the_seed_covers_all(self):
        sel = selection("control", "json-filter-pushdown", "metric-latency-gap")
        plans, seed_s = pr_smoke.window_plan(args(), sel, END)
        self.assertEqual([p["set"] for p in plans], list(sel["sets"]))
        self.assertEqual(len({p["end"] for p in plans}), 3)
        last = plans[-1]
        self.assertEqual(seed_s, 3600 + last["offset"] + last["span"] + 900)
        # the deepest window any pass can start from is inside the seeded data
        deepest = last["confirm_end"] - last["shift"] * 7 - 3600
        self.assertGreaterEqual(deepest, END - seed_s)

    def test_one_set_with_default_shift_costs_a_minute_over_the_old_window(self):
        plans, seed_s = pr_smoke.window_plan(args(), selection("control"), END)
        self.assertEqual(plans[0]["shift"], 60)
        self.assertEqual(seed_s, 3600 + 60 * (4 + 7) + 60 + 900)

    def test_a_shared_query_does_not_grow_the_seed(self):
        plans, seed_s = pr_smoke.window_plan(args(), selection("json-filter-pushdown", "control"), END)
        self.assertNotIn("gap", plans[0])
        self.assertEqual(plans[1]["offset"], plans[0]["span"])
        self.assertEqual(seed_s, 3600 + plans[0]["span"] + plans[1]["span"] + 900)

    def test_refresh_adds_to_the_seed_only_when_asked(self):
        sel = selection("control")
        plain = pr_smoke.window_plan(args(), sel, END)
        with_refresh = pr_smoke.window_plan(args(refresh_s=60), sel, END)
        self.assertGreater(with_refresh[1], plain[1])
        self.assertIsNone(plain[0][0]["refresh_end"])
        self.assertIsNotNone(with_refresh[0][0]["refresh_end"])

    def test_no_selected_set_still_plans(self):
        self.assertEqual(pr_smoke.window_plan(args(), {"run": True, "sets": {}}, END)[0], [])

    def test_the_real_shape_sets_never_repeat_a_window_across_sets(self):
        with open(os.path.join(pr_smoke.HERE, "shapes.json")) as f:
            spec = json.load(f)
        sel = selection(*spec["sets"])
        plans, _ = pr_smoke.window_plan(args(refresh_s=60), sel, END)
        seen = {}
        for plan in plans:
            for sh in spec["sets"][plan["set"]]["shapes"]:
                if sh.get("instant"):
                    rngs = ["instant"]
                else:
                    rngs = ["1h"]
                for rng in rngs:
                    ends = ([perf_matrix.run_end_for(plan["end"], sh, rng, i, 4) for i in range(4)] +
                            [perf_matrix.run_end_for(plan["confirm_end"], sh, rng, i, 7) for i in range(7)] +
                            [perf_matrix.run_end_for(plan["refresh_end"], sh, rng, i, 5, 60) for i in range(5)])
                    for e in ends:
                        key = (sh.get("query"), sh.get("path"), json.dumps(sh.get("params", {}), sort_keys=True), rng, e)
                        # Shapes of one set may share a URL on purpose (a refresh twin of a plain shape);
                        # a URL must never be sent by two different sets.
                        self.assertEqual(seen.setdefault(key, plan["set"]), plan["set"],
                                         f"{plan['set']}/{sh['name']} repeats a request of {seen[key]}")


class ArgsTest(unittest.TestCase):
    def run_main(self, *extra):
        with mock.patch.object(sys, "argv", ["pr_smoke.py", "--out", "/nonexistent-out", *extra]):
            return pr_smoke.main()

    def test_bad_refresh_interval_fails_before_the_stack_starts(self):
        with mock.patch.object(pr_smoke.stack, "stack_from") as stack_from, self.assertRaises(SystemExit) as raised, \
                contextlib.redirect_stderr(io.StringIO()):
            self.run_main("--ranges", "24h", "--refresh-s", "30")
        self.assertEqual(raised.exception.code, 2)
        stack_from.assert_not_called()

    def test_options_do_not_collide_with_the_stack_options(self):
        with mock.patch.object(sys, "argv", ["pr_smoke.py", "--help"]), self.assertRaises(SystemExit) as raised, \
                mock.patch("sys.stdout"):
            pr_smoke.main()
        self.assertEqual(raised.exception.code, 0)


class ConfirmTest(unittest.TestCase):
    def test_confirm_uses_the_planned_end_and_keeps_the_refresh_columns(self):
        moved = {"set": "control", "valid": True, "rows": [
            {"shape": "s", "range": "1h", "verdict": "slower", "refresh": {"pr": 0.2}, "refresh_status": {"pr": "200"}},
            {"shape": "t", "range": "1h", "verdict": "same"}]}
        redo = {"set": "control", "valid": True, "restart_after": "0", "rows": [
            {"shape": "s", "range": "1h", "verdict": "same", "p50": {}}]}
        plan = {"confirm_end": END - 999}
        with mock.patch.object(pr_smoke, "run_matrix", return_value=redo) as run, \
                mock.patch.object(pr_smoke.stack, "log"):
            out = pr_smoke.confirm(moved, args(), plan, None, "/out", [("base", "u")])
        self.assertEqual(run.call_args.args[4], END - 999)
        self.assertEqual(run.call_args.args[7], 7)
        row = out["rows"][0]
        self.assertTrue(row["confirmed"])
        self.assertEqual(row["verdict"], "same")
        self.assertEqual(row["refresh"], {"pr": 0.2})
        self.assertEqual(row["refresh_status"], {"pr": "200"})

    def test_confirm_does_not_ask_loki_again_about_what_timed_out_in_the_first_pass(self):
        moved = {"set": "control", "valid": True, "rows": [{"shape": "s", "range": "1h", "verdict": "slower"}]}
        redo = {"set": "control", "valid": True, "restart_after": "0", "rows": [
            {"shape": "s", "range": "1h", "verdict": "same", "p50": {}}]}
        with mock.patch.object(pr_smoke, "run_matrix", return_value=redo) as run, \
                mock.patch.object(pr_smoke.stack, "log"):
            pr_smoke.confirm(moved, args(), {"confirm_end": END}, None, "/out", [("base", "u")])
        self.assertEqual(run.call_args.kwargs["extra"], ("--known-timeouts", "/out/raw-control.json"))


FIRST = ("json-filter-pushdown", "D volume, no filters")  # the same query as the control set's row below
REPEAT = ("control", "json volume no filters")


def cold_row(shape, loki_cold, served=False, status="200", rng="1h", **extra):
    row = {"shape": shape, "range": rng, "verdict": "same", "cold": {"base": 0.5, "pr": 0.5, "loki": loki_cold},
           "p50": {"base": 0.2, "pr": 0.2, "loki": 0.05}, "status": {"base": "200", "pr": "200", "loki": status},
           "parity_vs_reference": "same"}
    if served:
        row["cold_cache_served"] = ["loki"]
    row.update(extra)
    return row


def sums(first_row, repeat_row):
    return [{"set": FIRST[0], "reference": "loki", "rows": [first_row]},
            {"set": REPEAT[0], "reference": "loki", "rows": [repeat_row]}]


class ReuseFirstColdTest(unittest.TestCase):
    def test_a_cache_served_repeat_takes_the_first_sets_computed_cold(self):
        first, repeat = pr_smoke.reuse_first_cold(sums(cold_row(FIRST[1], 0.27), cold_row(REPEAT[1], 0.003, served=True)))
        self.assertEqual(first["rows"][0]["cold"]["loki"], 0.27)
        row = repeat["rows"][0]
        self.assertEqual(row["cold"]["loki"], 0.27)
        self.assertNotIn("cold_cache_served", row)
        self.assertEqual(row["loki_cold_reused_from"], {"set": FIRST[0], "shape": FIRST[1]})
        self.assertNotIn("loki_cold_reused_from", first["rows"][0])

    def test_other_targets_stay_marked_and_every_other_field_is_untouched(self):
        repeat = cold_row(REPEAT[1], 0.003, served=True, refresh={"loki": 0.04}, confirmed=True)
        repeat["cold_cache_served"] = ["base", "loki"]
        before = json.loads(json.dumps(repeat))
        row = pr_smoke.reuse_first_cold(sums(cold_row(FIRST[1], 0.27), repeat))[1]["rows"][0]
        self.assertEqual(row["cold_cache_served"], ["base"])
        for key in ("p50", "status", "verdict", "parity_vs_reference", "refresh", "confirmed"):
            self.assertEqual(row[key], before[key], key)
        self.assertEqual(row["cold"]["base"], before["cold"]["base"])

    def test_not_reused_when_the_first_occurrence_was_cache_served_or_not_200(self):
        for first in (cold_row(FIRST[1], 0.003, served=True), cold_row(FIRST[1], 0.01, status="502")):
            repeat = cold_row(REPEAT[1], 0.003, served=True)
            row = pr_smoke.reuse_first_cold(sums(first, repeat))[1]["rows"][0]
            self.assertEqual(row["cold"]["loki"], 0.003)
            self.assertEqual(row["cold_cache_served"], ["loki"])
            self.assertNotIn("loki_cold_reused_from", row)

    def test_a_repeat_whose_cold_was_computed_is_left_alone(self):
        row = pr_smoke.reuse_first_cold(sums(cold_row(FIRST[1], 0.27), cold_row(REPEAT[1], 0.31)))[1]["rows"][0]
        self.assertEqual(row["cold"]["loki"], 0.31)
        self.assertNotIn("loki_cold_reused_from", row)

    def test_a_different_range_is_a_different_extent(self):
        row = pr_smoke.reuse_first_cold(sums(cold_row(FIRST[1], 0.27), cold_row(REPEAT[1], 0.003, served=True, rng="3h")))[1]["rows"][0]
        self.assertEqual(row["cold"]["loki"], 0.003)
        self.assertEqual(row["cold_cache_served"], ["loki"])

    def test_a_different_query_is_not_reused(self):
        row = pr_smoke.reuse_first_cold(sums(cold_row(FIRST[1], 0.27), cold_row("logs plain", 0.003, served=True)))[1]["rows"][0]
        self.assertEqual(row["cold"]["loki"], 0.003)

    def test_the_first_occurrence_stays_the_reference_for_a_third_row(self):
        rows = sums(cold_row(FIRST[1], 0.27), cold_row(REPEAT[1], 0.003, served=True))
        rows.append({"set": "pattern-line-filter", "reference": "loki", "rows": []})
        out = pr_smoke.reuse_first_cold(rows)
        self.assertEqual(out[1]["rows"][0]["cold"]["loki"], 0.27)


class MeasureTest(unittest.TestCase):
    def test_every_summary_is_marked_as_run_on_its_own_windows(self):
        st = mock.Mock(loki="http://loki")
        proxy = mock.Mock(url="http://p", alive=mock.Mock(return_value=True))
        a = args(end=END, same_build=True, head=None, proxy_port=1, service="s", keep_stack=False)
        with mock.patch.object(pr_smoke.stack, "stack_from", return_value=st), \
                mock.patch.object(pr_smoke.stack, "build_and_seed", return_value={"pr": "/t"}), \
                mock.patch.object(pr_smoke.stack, "start_proxies",
                                  side_effect=lambda specs, out, st, svc, proxies: proxies.extend([proxy, proxy])), \
                mock.patch.object(pr_smoke.stack, "teardown"), \
                mock.patch.object(pr_smoke, "run_matrix",
                                  side_effect=lambda name, *a, **kw: {"set": name, "valid": True, "rows": []}), \
                tempfile.TemporaryDirectory() as tmp:
            out = pr_smoke.measure(a, selection("json-filter-pushdown", "control"), tmp, "base")
            saved = [read_json(os.path.join(tmp, f"summary-{s['set']}.json")) for s in out]
        self.assertEqual([s["set"] for s in out], ["json-filter-pushdown", "control"])
        self.assertTrue(all(s["isolated_windows"] is True for s in out))
        self.assertEqual(saved, out)  # the saved summaries carry the marker too


    def test_summary_files_are_written_after_the_reuse_step(self):
        st = mock.Mock(loki="http://loki")
        proxy = mock.Mock(url="http://p", alive=mock.Mock(return_value=True))
        a = args(end=END, same_build=True, head=None, proxy_port=1, service="s", keep_stack=False)
        rows = {FIRST[0]: cold_row(FIRST[1], 0.27), REPEAT[0]: cold_row(REPEAT[1], 0.003, served=True)}
        with mock.patch.object(pr_smoke.stack, "stack_from", return_value=st), \
                mock.patch.object(pr_smoke.stack, "build_and_seed", return_value={"pr": "/t"}), \
                mock.patch.object(pr_smoke.stack, "start_proxies",
                                  side_effect=lambda specs, out, st, svc, proxies: proxies.extend([proxy, proxy])), \
                mock.patch.object(pr_smoke.stack, "teardown"), \
                mock.patch.object(pr_smoke, "run_matrix",
                                  side_effect=lambda name, *a, **kw: {"set": name, "valid": True, "reference": "loki",
                                                                      "rows": [rows[name]]}), \
                tempfile.TemporaryDirectory() as tmp:
            out = pr_smoke.measure(a, selection(FIRST[0], REPEAT[0]), tmp, "base")
            with open(os.path.join(tmp, f"summary-{REPEAT[0]}.json")) as f:
                saved = json.load(f)
        self.assertEqual(saved["rows"][0]["cold"]["loki"], 0.27)
        self.assertEqual(saved["rows"][0]["loki_cold_reused_from"], {"set": FIRST[0], "shape": FIRST[1]})
        self.assertNotIn("cold_cache_served", saved["rows"][0])
        self.assertTrue(saved["isolated_windows"])
        self.assertEqual(saved, out[1])


class CommandTest(unittest.TestCase):
    def test_refresh_pass_is_a_second_perf_matrix_call_with_its_own_end(self):
        st = mock.Mock(vl_container="vl", vl="http://vl")
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)

        summary = {"set": "control", "rows": []}
        with mock.patch.object(pr_smoke.subprocess, "run", side_effect=fake_run), \
                mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(summary))):
            pr_smoke.run_matrix("control", ["s"], [("base", "u1"), ("pr", "u2"), ("loki", "u3")], args(), END, st,
                                "/out", 4, refresh=(END - 5000, 60, 5))
        matrix, refresh, report = calls
        self.assertNotIn("--refresh-s", matrix)
        self.assertEqual(matrix[matrix.index("--end") + 1], str(END))
        self.assertEqual(refresh[refresh.index("--refresh-s") + 1], "60")
        self.assertEqual(refresh[refresh.index("--end") + 1], str(END - 5000))
        self.assertEqual(refresh[refresh.index("--runs") + 1], "5")
        self.assertTrue(refresh[refresh.index("--out") + 1].endswith("raw-control-refresh.json"))
        self.assertEqual(report[report.index("--refresh") + 1], refresh[refresh.index("--out") + 1])
        # the refresh pass reads the first pass's timeouts from its raw file
        self.assertEqual(refresh[refresh.index("--known-timeouts") + 1], matrix[matrix.index("--out") + 1])
        self.assertNotIn("--known-timeouts", matrix)

    def test_default_run_makes_no_refresh_call(self):
        st = mock.Mock(vl_container="vl", vl="http://vl")
        calls = []
        with mock.patch.object(pr_smoke.subprocess, "run", side_effect=lambda cmd, **kw: calls.append(cmd)), \
                mock.patch("builtins.open", mock.mock_open(read_data=json.dumps({"set": "c", "rows": []}))):
            pr_smoke.run_matrix("control", None, [("base", "u1"), ("pr", "u2"), ("loki", "u3")], args(), END, st, "/out", 4)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("--refresh", calls[1])


if __name__ == "__main__":
    unittest.main()
