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
    base = dict(ranges=["1h"], runs=4, confirm_runs=7, refresh_s=0, refresh_runs=5, noise=0.3, min_delta=0.1, end_lead=0,
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


class FakeProxy:
    """A started proxy build: its name, url and align state; stopping it is recorded."""

    def __init__(self, name, align="off", url=None):
        self.name, self.align, self.url, self.stopped = name, align, url or f"http://{name}", False
        self.log_path = f"/dev/null/{name}"

    def alive(self):
        return not self.stopped

    def stop(self):
        self.stopped = True


class MeasureTest(unittest.TestCase):
    def test_every_summary_is_marked_as_run_on_its_own_windows(self):
        st = mock.Mock(loki="http://loki")
        a = args(end=END, same_build=True, head=None, proxy_port=1, service="s", keep_stack=False)
        with mock.patch.object(pr_smoke.stack, "stack_from", return_value=st), \
                mock.patch.object(pr_smoke.stack, "build_and_seed", return_value={"pr": "/t"}), \
                mock.patch.object(pr_smoke.stack, "start_proxies",
                                  side_effect=lambda specs, out, st, svc, proxies, **kw: proxies.extend([FakeProxy("base"), FakeProxy("pr")])), \
                mock.patch.object(pr_smoke.stack, "probe_align_support", return_value=False), \
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
        a = args(end=END, same_build=True, head=None, proxy_port=1, service="s", keep_stack=False)
        rows = {FIRST[0]: cold_row(FIRST[1], 0.27), REPEAT[0]: cold_row(REPEAT[1], 0.003, served=True)}
        with mock.patch.object(pr_smoke.stack, "stack_from", return_value=st), \
                mock.patch.object(pr_smoke.stack, "build_and_seed", return_value={"pr": "/t"}), \
                mock.patch.object(pr_smoke.stack, "start_proxies",
                                  side_effect=lambda specs, out, st, svc, proxies, **kw: proxies.extend([FakeProxy("base"), FakeProxy("pr")])), \
                mock.patch.object(pr_smoke.stack, "probe_align_support", return_value=False), \
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


def spec_names(*sets):
    with open(os.path.join(pr_smoke.HERE, "shapes.json")) as f:
        spec = json.load(f)
    return spec, {"run": True, "sets": {n: [s["name"] for s in spec["sets"][n]["shapes"]] for n in sets}}


class WindowAgeTest(unittest.TestCase):
    def raw(self, rows):
        path = os.path.join(self.tmp, "raw.json")
        with open(path, "w") as f:
            json.dump({"rows": rows}, f)
        return path

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_ages_are_request_time_minus_window_end_over_the_sent_requests_only(self):
        path = self.raw([{"at": 1000, "end": 940}, {"at": 1100, "end": 760}, {"end": 1}, {"at": 5, "end": None}])
        self.assertEqual(pr_smoke.window_ages(path), (60, 340))

    def test_a_window_that_ends_after_its_request_is_a_negative_age(self):
        self.assertEqual(pr_smoke.window_ages(self.raw([{"at": 1000, "end": 1060}, {"at": 1000, "end": 900}])), (-60, 100))

    def test_no_timed_request_no_ages(self):
        self.assertIsNone(pr_smoke.window_ages(self.raw([{"end": 5}])))

    def test_the_ages_of_every_pass_of_every_set_go_into_the_profiles_meta(self):
        out = tempfile.mkdtemp()
        for suffix, rows in (("", [{"at": 100, "end": 40}]), ("-refresh", [{"at": 100, "end": 90}]),
                             ("-confirm", [{"at": 100, "end": 300}])):
            with open(pr_smoke.raw_path(out, "control", "loki-defaults", suffix), "w") as f:
                json.dump({"rows": rows}, f)
        entry = {}
        pr_smoke.note_ages(entry, out, "control", "loki-defaults")
        self.assertEqual(entry["window_age_s"], [-200, 60])
        pr_smoke.note_ages(entry, out, "missing", "loki-defaults")   # a set with no files changes nothing
        self.assertEqual(entry["window_age_s"], [-200, 60])


class PlanProfilesTest(unittest.TestCase):
    def test_range_metric_shapes_are_the_ones_the_flag_changes(self):
        self.assertTrue(pr_smoke.range_metric({"query": "sum(rate({a=\"b\"}[1m]))"}))
        self.assertFalse(pr_smoke.range_metric({"query": "x", "instant": True}))
        self.assertFalse(pr_smoke.range_metric({"query": "{a=\"b\"}", "logs": True}))
        self.assertFalse(pr_smoke.range_metric({"path": "/loki/api/v1/labels"}))

    def test_auto_runs_defaults_for_everything_and_aligned_for_range_metric_shapes_when_a_build_supports_the_flag(self):
        spec, sel = spec_names("control")
        runs, skipped = pr_smoke.plan_profiles("auto", sel, spec, True)
        self.assertEqual(skipped, {})
        # window order is execution order: loki-defaults runs on the Loki the seed wrote to, aligned on a recreated one
        self.assertEqual([r[0] for r in runs], ["loki-defaults", "aligned"])
        self.assertEqual(pr_smoke.execution_order(runs), ["loki-defaults", "aligned"])
        defaults, aligned = runs
        by_name = {s["name"]: s for s in spec["sets"]["control"]["shapes"]}
        self.assertEqual(defaults[2], sel["sets"]["control"])
        self.assertTrue(aligned[2] and all(pr_smoke.range_metric(by_name[n]) for n in aligned[2]))
        self.assertLess(len(aligned[2]), len(defaults[2]))

    def test_auto_skips_aligned_when_no_build_supports_the_flag_and_says_why(self):
        spec, sel = spec_names("control")
        runs, skipped = pr_smoke.plan_profiles("auto", sel, spec, False)
        self.assertEqual({r[0] for r in runs}, {"loki-defaults"})
        self.assertIn("not like-for-like", skipped["aligned"])

    def test_explicit_modes_run_what_they_name_even_unsupported(self):
        spec, sel = spec_names("control")
        both, _ = pr_smoke.plan_profiles("both", sel, spec, False)
        self.assertEqual([(r[0], len(r[2])) for r in both],
                         [("loki-defaults", len(sel["sets"]["control"])), ("aligned", len(sel["sets"]["control"]))])
        self.assertEqual([r[0] for r in pr_smoke.plan_profiles("aligned", sel, spec, False)[0]], ["aligned"])
        self.assertEqual([r[0] for r in pr_smoke.plan_profiles("loki-defaults", sel, spec, True)[0]], ["loki-defaults"])

    def test_a_set_with_no_range_metric_shape_has_no_aligned_run(self):
        spec = {"sets": {"meta": {"shapes": [{"name": "labels", "path": "/loki/api/v1/labels"}]}}}
        runs, skipped = pr_smoke.plan_profiles("auto", {"sets": {"meta": ["labels"]}}, spec, True)
        self.assertEqual([r[0] for r in runs], ["loki-defaults"])
        self.assertIn("aligned", skipped)

    def test_source_support_reads_main_go_at_a_ref(self):
        self.assertFalse(pr_smoke.source_supports_align("no-such-ref"))
        with tempfile.TemporaryDirectory() as repo:
            def git(*a):
                subprocess = pr_smoke.subprocess
                subprocess.run(["git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@e.x",
                                "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *a], check=True, capture_output=True)
            git("init", "-q")
            os.makedirs(os.path.join(repo, "cmd/proxy"))
            path = os.path.join(repo, "cmd/proxy/main.go")
            for text, tag in (("x", "old"), ('fs.Bool("align-queries-with-step", false, "")', "new")):
                with open(path, "w") as f:
                    f.write(text)
                git("add", ".")
                git("commit", "-q", "-m", tag)
                git("tag", tag)
            self.assertFalse(pr_smoke.source_supports_align("old", repo))
            self.assertTrue(pr_smoke.source_supports_align("new", repo))
            self.assertTrue(pr_smoke.source_supports_align(None, repo))  # the working tree


class ProfileWindowsTest(unittest.TestCase):
    def test_every_profile_run_gets_its_own_block_and_the_seed_covers_them_all(self):
        spec, sel = spec_names("control")
        runs, _ = pr_smoke.plan_profiles("both", sel, spec, True)
        plans, seed_s = pr_smoke.window_plan(args(), sel, END, runs)
        self.assertEqual([p["profile"] for p in plans], ["loki-defaults", "aligned"])
        self.assertEqual(len({p["end"] for p in plans}), 2)
        self.assertEqual(plans[1]["offset"], plans[0]["span"])
        self.assertEqual(seed_s, 3600 + plans[1]["offset"] + plans[1]["span"] + 900)
        # a second profile costs the first block of windows in data, not a second seed
        _, alone = pr_smoke.window_plan(args(), sel, END, runs[:1])
        self.assertEqual(seed_s - alone, plans[0]["span"])

    def test_without_runs_the_plan_is_the_old_one_with_no_profile(self):
        plans, _ = pr_smoke.window_plan(args(), selection("control"), END)
        self.assertEqual(plans[0]["profile"], "")


class RunMatrixProfileTest(unittest.TestCase):
    def test_profile_and_unaligned_builds_go_to_the_matrix_the_files_and_the_summary(self):
        st = mock.Mock(vl_container="vl", vl="http://vl")
        calls = []
        with mock.patch.object(pr_smoke.subprocess, "run", side_effect=lambda cmd, **kw: calls.append(cmd)), \
                mock.patch("builtins.open", mock.mock_open(read_data=json.dumps({"set": "control", "rows": []}))):
            pr_smoke.run_matrix("control", ["s"], [("base", "u1"), ("pr", "u2"), ("loki", "u3")], args(), END, st, "/out",
                                4, profile="aligned", unlike=["base"])
        matrix, report = calls
        self.assertEqual(matrix[matrix.index("--profile") + 1], "aligned")
        self.assertEqual(matrix[matrix.index("--not-like-for-like") + 1], "base")
        self.assertTrue(matrix[matrix.index("--out") + 1].endswith("raw-control-aligned.json"))
        self.assertTrue(report[report.index("--save") + 1].endswith("summary-control-aligned.json"))

    def test_no_profile_leaves_the_old_names_and_flags(self):
        st = mock.Mock(vl_container="vl", vl="http://vl")
        calls = []
        with mock.patch.object(pr_smoke.subprocess, "run", side_effect=lambda cmd, **kw: calls.append(cmd)), \
                mock.patch("builtins.open", mock.mock_open(read_data=json.dumps({"set": "c", "rows": []}))):
            pr_smoke.run_matrix("control", None, [("base", "u1"), ("pr", "u2"), ("loki", "u3")], args(), END, st, "/out", 4)
        self.assertNotIn("--profile", calls[0])
        self.assertNotIn("--not-like-for-like", calls[0])


class ConfirmColdTest(unittest.TestCase):
    def moved(self, **first):
        row = {"shape": "s", "range": "1h", "verdict": "slower", "cold": {"base": 0.5, "pr": 0.9, "loki": 0.2},
               "p50": {}, **first}
        return {"set": "control", "valid": True, "profile": "aligned", "not_like_for_like": ["base"], "reference": "loki",
                "rows": [row]}

    def redo(self, **row):
        base = {"shape": "s", "range": "1h", "verdict": "same", "cold": {"base": 0.05, "pr": 0.06, "loki": 0.003},
                "p50": {}}
        return {"set": "control", "valid": True, "restart_after": "0", "rows": [{**base, **row}]}

    def confirm(self, first, again):
        with mock.patch.object(pr_smoke, "run_matrix", return_value=again) as run, mock.patch.object(pr_smoke.stack, "log"):
            out = pr_smoke.confirm(first, args(), {"confirm_end": END}, None, "/out", [("base", "u")])
        return out["rows"][0], run

    def test_a_cache_served_confirm_cold_is_replaced_by_the_first_pass_for_every_target(self):
        row, _ = self.confirm(self.moved(), self.redo(cold_cache_served=["loki"]))
        self.assertEqual(row["cold"], {"base": 0.5, "pr": 0.9, "loki": 0.2})
        self.assertNotIn("cold_cache_served", row)
        self.assertTrue(row["cold_from_first_pass"])
        self.assertTrue(row["confirmed"])
        self.assertEqual(row["verdict"], "same")  # only the cold columns come from the first pass

    def test_a_first_pass_cold_that_was_served_keeps_its_mark_when_the_confirm_was_served_too(self):
        row, _ = self.confirm(self.moved(cold_cache_served=["loki"]), self.redo(cold_cache_served=["loki"]))
        self.assertEqual(row["cold"]["loki"], 0.2)
        self.assertEqual(row["cold_cache_served"], ["loki"])

    def test_a_computed_confirm_cold_stands(self):
        row, _ = self.confirm(self.moved(), self.redo())
        self.assertEqual(row["cold"]["loki"], 0.003)
        self.assertNotIn("cold_from_first_pass", row)

    def test_the_rerun_keeps_the_profile_the_unaligned_builds_and_the_first_pass_timeouts(self):
        _, run = self.confirm(self.moved(), self.redo())
        self.assertEqual(run.call_args.kwargs["profile"], "aligned")
        self.assertEqual(run.call_args.kwargs["unlike"], ["base"])
        self.assertEqual(run.call_args.kwargs["extra"], ("--known-timeouts", "/out/raw-control-aligned.json"))


class MeasureProfilesTest(unittest.TestCase):
    """measure() with both profiles: Loki moves between them, every profile starts its own proxies and stops them."""

    def run_measure(self, supported, mode="both", sets=("control",)):
        return self.run_measure_with(supported, mode, sets)

    def run_measure_with(self, supported, mode="both", sets=("control",), **argv):
        events = []
        st = mock.Mock(loki="http://loki")
        st.set_profile.side_effect = lambda profile, *a, **kw: events.append(("loki", profile)) or {"p": profile}
        a = args(end=argv.get("end", END), end_lead=argv.get("end_lead", 0), same_build=False, head=None,
                 proxy_port=1, service="s", keep_stack=False, profiles=mode)
        made = []

        def start(specs, out, stack_, svc, proxies, profile="", supported=None):
            for name, _, _, port in specs:
                p = FakeProxy(name, "on" if profile == "aligned" and supported[name] else
                              "unsupported" if profile == "aligned" else "off")
                p.profile = profile
                proxies.append(p)
                made.append(p)
            events.append(("proxies", profile))

        def matrix(name, shapes, targets, args_, end, st_, out, runs, **kw):
            events.append(("matrix", kw.get("profile"), name, tuple(kw.get("unlike") or ())))
            return {"set": name, "valid": True, "reference": "loki", "profile": kw.get("profile"),
                    "not_like_for_like": list(kw.get("unlike") or ()), "rows": []}

        meta = {}
        with mock.patch.object(pr_smoke.stack, "stack_from", return_value=st), \
                mock.patch.object(pr_smoke.stack, "build_and_seed", return_value={"pr": "/t", "base": "/t"}), \
                mock.patch.object(pr_smoke.stack, "start_proxies", side_effect=start), \
                mock.patch.object(pr_smoke.stack, "probe_align_support", side_effect=lambda b: supported[os.path.basename(b)[len("proxy-"):]]), \
                mock.patch.object(pr_smoke, "source_supports_align", return_value=any(supported.values())), \
                mock.patch.object(pr_smoke.stack, "teardown"), mock.patch.object(pr_smoke, "run_matrix", side_effect=matrix), \
                tempfile.TemporaryDirectory() as tmp:
            out = pr_smoke.measure(a, selection(*sets), tmp, "base", meta)
        return out, events, made, meta

    def test_defaults_then_aligned_with_the_loki_moved_between_and_the_proxies_restarted(self):
        out, events, made, meta = self.run_measure({"base": False, "pr": True})
        self.assertEqual([e for e in events if e[0] != "matrix"],
                         [("loki", "loki-defaults"), ("proxies", "loki-defaults"), ("loki", "aligned"), ("proxies", "aligned")])
        self.assertEqual([(e[1], e[3]) for e in events if e[0] == "matrix"],
                         [("loki-defaults", ()), ("aligned", ("base",))])  # the base build is not like-for-like in aligned
        self.assertTrue(all(p.stopped for p in made))  # each profile's processes are stopped when it is done
        self.assertEqual([s["profile"] for s in out], ["loki-defaults", "aligned"])
        # only the second profile ran on a Loki recreated on the seeded data
        self.assertEqual([s["loki_restarted"] for s in out], [False, True])
        self.assertEqual(meta["align_flag_supported"], {"base": False, "pr": True})
        self.assertEqual(meta["profiles"]["aligned"]["not_like_for_like"], ["base"])
        self.assertEqual(meta["profiles"]["aligned"]["proxy_align"], {"base": "unsupported", "pr": "on"})
        self.assertEqual(meta["profiles"]["loki-defaults"]["not_like_for_like"], [])
        self.assertEqual(set(meta["phases_s"]) >= {"stack_up", "seed_and_build", "aligned:matrix", "loki-defaults:matrix"}, True)

    def test_auto_without_a_build_that_supports_the_flag_runs_defaults_only_and_records_why(self):
        out, events, made, meta = self.run_measure({"base": False, "pr": False}, mode="auto")
        self.assertEqual([e[:2] for e in events if e[0] == "matrix"], [("matrix", "loki-defaults")])
        self.assertFalse(meta["profiles"]["aligned"]["ran"])
        self.assertIn("not like-for-like", meta["profiles"]["aligned"]["why_not"])
        self.assertTrue(meta["profiles"]["loki-defaults"]["ran"])

    def test_reuse_of_a_repeated_loki_cold_stays_within_a_profile(self):
        rows = {"aligned": cold_row(FIRST[1], 0.27), "loki-defaults": cold_row(REPEAT[1], 0.003, served=True)}
        out = pr_smoke.reuse_per_profile([
            {"set": FIRST[0], "reference": "loki", "profile": "aligned", "rows": [rows["aligned"]]},
            {"set": REPEAT[0], "reference": "loki", "profile": "loki-defaults", "rows": [rows["loki-defaults"]]}])
        # the other profile's Loki computed the first; this profile's Loki has not: nothing is borrowed
        self.assertEqual(out[1]["rows"][0]["cold"]["loki"], 0.003)
        self.assertEqual(out[1]["rows"][0]["cold_cache_served"], ["loki"])


class DailyProfilesTest(unittest.TestCase):
    """daily.main() with both profiles: the order, the per-profile summaries and the files they are written to."""

    def run_daily(self, supported, *extra):
        import daily
        events, written = [], []
        st = mock.Mock(loki="http://loki")
        st.set_profile.side_effect = lambda profile, *a, **kw: events.append(("loki", profile)) or {"p": profile}

        def start(specs, out, st_, svc, proxies, profile="", supported=None):
            for name, _, _, _ in specs:
                proxies.append(FakeProxy(name, "on" if profile == "aligned" and supported[name] else
                                         "unsupported" if profile == "aligned" else "off"))
            events.append(("proxies", profile))

        def matrix(name, shapes, targets, args_, end, st_, out, runs, **kw):
            events.append(("matrix", kw["profile"], name, len(shapes), tuple(kw["unlike"])))
            return {"set": name, "profile": kw["profile"], "valid": True, "date": "2026-10-28", "label": "daily",
                    "candidate": "main", "baseline": "release", "reference": "loki", "runs": 3, "rows": []}

        argv = ["daily.py", "--sets", "control,pattern-line-filter", "--max-range", "1h", "--runs", "3",
                "--baseline-ref", "v1.0.0", *extra]
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(sys, "argv", [*argv, "--out", tmp]), \
                mock.patch.object(daily.stack, "stack_from", return_value=st), \
                mock.patch.object(daily.stack, "build_and_seed", return_value={"main": "/t", "release": "/t"}), \
                mock.patch.object(daily.stack, "start_proxies", side_effect=start), \
                mock.patch.object(daily.stack, "probe_align_support",
                                  side_effect=lambda b: supported[os.path.basename(b)[len("proxy-"):]]), \
                mock.patch.object(daily.stack, "teardown"), mock.patch.object(daily.stack, "log"), \
                mock.patch.object(daily.pr_smoke, "source_supports_align", return_value=any(supported.values())), \
                mock.patch.object(daily.pr_smoke, "git", return_value="a" * 40), \
                mock.patch.object(daily.pr_smoke, "run_matrix", side_effect=matrix):
            code = daily.main()
            meta = read_json(os.path.join(tmp, "meta.json")) if os.path.exists(os.path.join(tmp, "meta.json")) else {}
        return code, events, meta

    def test_both_profiles_run_in_order_when_a_build_supports_the_flag(self):
        code, events, meta = self.run_daily({"release": False, "main": True}, "--no-write")
        self.assertEqual(code, 0)
        self.assertEqual([e[:2] for e in events if e[0] != "matrix"],
                         [("loki", "loki-defaults"), ("proxies", "loki-defaults"), ("loki", "aligned"), ("proxies", "aligned")])
        matrices = [e for e in events if e[0] == "matrix"]
        self.assertEqual(sorted((e[1], e[2]) for e in matrices),
                         [("aligned", "control"), ("aligned", "pattern-line-filter"),
                          ("loki-defaults", "control"), ("loki-defaults", "pattern-line-filter")])
        self.assertEqual([e[4] for e in matrices if e[1] == "aligned"], [("release",)] * 2)  # unaligned build marked
        self.assertEqual([e[4] for e in matrices if e[1] == "loki-defaults"], [()] * 2)
        aligned_control = next(e for e in matrices if e[1] == "aligned" and e[2] == "control")
        defaults_control = next(e for e in matrices if e[1] == "loki-defaults" and e[2] == "control")
        self.assertLess(aligned_control[3], defaults_control[3])   # aligned: range-metric shapes only
        self.assertTrue(meta["profiles"]["aligned"]["ran"] and meta["profiles"]["loki-defaults"]["ran"])

    def test_without_a_build_that_supports_the_flag_only_loki_defaults_runs(self):
        code, events, meta = self.run_daily({"release": False, "main": False}, "--no-write")
        self.assertEqual({e[1] for e in events if e[0] == "matrix"}, {"loki-defaults"})
        self.assertFalse(meta["profiles"]["aligned"]["ran"])
        self.assertIn("not like-for-like", meta["profiles"]["aligned"]["why_not"])

    def test_the_summaries_of_the_second_profile_go_to_their_own_files(self):
        import daily
        with tempfile.TemporaryDirectory() as results, tempfile.TemporaryDirectory() as hist:
            with mock.patch.object(daily, "RESULTS", results), mock.patch.object(daily.history, "HISTORY", hist), \
                    mock.patch.object(daily.history, "append", return_value=("x", True)) as append, \
                    mock.patch.object(daily.history, "trend", return_value="t"), \
                    mock.patch.object(daily.history, "write_alerts", side_effect=RuntimeError("skip")), \
                    mock.patch.object(daily.subprocess, "run"):
                code, _, _ = self.run_daily({"release": False, "main": True})
            self.assertEqual(code, 0)
            self.assertEqual(sorted(os.listdir(results)),
                             ["daily-control.json", "daily-control.loki-defaults.json",
                              "daily-pattern-line-filter.json", "daily-pattern-line-filter.loki-defaults.json"])
            self.assertEqual(sorted({c.args[0]["profile"] for c in append.call_args_list}),
                             ["aligned", "loki-defaults"])


if __name__ == "__main__":
    unittest.main()
