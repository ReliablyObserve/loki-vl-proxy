"""Unit tests for bench/ab/cold.py: the warm-up, the cold re-measure and the decision on its median.

What they pin:
  - one slow first-pass cold sample (the other samples equal) is outvoted and the row no longer fails;
  - a cold regression (slower in every sample, or in most) still fails, locked rows and plain above-2× rows alike;
  - when a sample cannot be taken, the first-pass cold and its finding stand;
  - gate()/judge()/the bounds are the ones the rest of the suite tests: nothing here moves a threshold;
  - the two real runs that failed on one cold sample (fixtures/cold-outlier-cases.json) fail the unchanged gate
    from their raw rows, and the new flow sends exactly those rows to the cold stage.
"""
import contextlib
import io
import json
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import cold  # noqa: E402
import over2x  # noqa: E402
import perf_matrix  # noqa: E402
import pr_smoke  # noqa: E402
import report  # noqa: E402
import comment  # noqa: E402
from test_over2x import META, MIN_DELTA, NOISE, NO_BOUNDS, SELECTION, row, summary  # noqa: E402

FIXTURE = os.path.join(HERE, "fixtures", "cold-outlier-cases.json")
LOCK = {"locked": True, "max_ratio": 2.0, "modes": ["cold", "warm"]}


def failing(s, baseline=None, bnds=None):
    return over2x.failing(over2x.gate(s, baseline or {}, NOISE, MIN_DELTA, bnds))


def cold_findings(s, baseline=None, bnds=None):
    return [(f["kind"], f.get("note", "")) for f in failing(s, baseline, bnds) if f["mode"] == "cold"]


def sampled(first_row, base=(), pr=(), first=None):
    """first_row after cold.decide with the given new samples of each build."""
    first = first or dict(first_row["cold"])
    cold.decide(first_row, first, {"base": list(base), "pr": list(pr)}, "base", "pr")
    return first_row


class MedianTest(unittest.TestCase):
    def test_median(self):
        self.assertEqual(cold.median([3, 1, 2]), 2)
        self.assertAlmostEqual(cold.median([5.3, 3.8, 3.9, 3.8]), 3.85)  # even count: the mean of the middle two
        self.assertEqual(cold.median([2.0]), 2.0)


class DecisionTest(unittest.TestCase):
    """The case-2 numbers: base 3.79 s, PR 5.32 s on the first pass, Loki 0.60 s; the base is above 2× too."""

    def outlier(self):
        return row(shape="X1", pr=0.16, base=0.15, loki=0.09, pr_cold=5.32, base_cold=3.79, loki_cold=0.6)

    def test_the_unchanged_gate_fails_the_single_sample(self):
        self.assertEqual(cold_findings(summary(self.outlier())), [("worsened", "above 2× on base too")])

    def test_a_single_slow_sample_among_equal_ones_does_not_fail(self):
        r = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[3.75, 3.80, 3.72])
        self.assertEqual(r["cold"], {"base": 3.795, "pr": 3.775, "loki": 0.6})  # medians of 4; Loki's cold is kept
        self.assertEqual(r["cold_samples"], {"base": [3.79, 3.80, 3.71, 3.85], "pr": [5.32, 3.75, 3.80, 3.72]})
        self.assertEqual(cold_findings(summary(r)), [])  # known, as slow on the base too, is not a failure

    def test_a_cold_regression_in_every_sample_still_fails(self):
        r = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[5.20, 5.41, 5.30])
        self.assertEqual(cold_findings(summary(r)), [("worsened", "above 2× on base too")])

    def test_a_cold_regression_from_below_2x_still_fails_as_new(self):
        r = sampled(row(shape="n", pr=0.2, base=0.2, loki=0.1, pr_cold=0.5, base_cold=0.15, loki_cold=0.1),
                    base=[0.15, 0.16, 0.15], pr=[0.5, 0.52, 0.49])
        self.assertEqual(cold_findings(summary(r)), [("new", "")])

    def test_slow_in_two_of_three_new_samples_still_fails(self):
        r = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[5.2, 5.3, 3.75])
        self.assertEqual(cold_findings(summary(r)), [("worsened", "above 2× on base too")])

    def test_a_listed_row_keeps_its_listed_semantics(self):
        listed = {("aligned", "control", "X1", "1h", "cold"): {"profile": "aligned", "set": "control", "shape": "X1",
                                                               "range": "1h", "mode": "cold", "ratio": 6.0}}
        clean = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[3.75, 3.80, 3.72])
        self.assertEqual(cold_findings(summary(clean), listed), [])
        bad = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[7.9, 8.1, 8.0])
        self.assertEqual(cold_findings(summary(bad), listed), [("worsened", "listed")])

    def test_a_locked_row_is_judged_at_its_lock_before_and_after(self):
        covers = {("control", "X1"): ["lk"]}
        b = {"blocks": {"lk": LOCK}, "covers": covers}
        one = failing(summary(self.outlier()), None, b)
        self.assertEqual([(f["kind"], f.get("locked"), f.get("bound")) for f in one if f["mode"] == "cold"],
                         [("worsened", True, 2.0)])
        gone = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[3.75, 3.80, 3.72])
        self.assertEqual(over2x.locked_failing(over2x.gate(summary(gone), {}, NOISE, MIN_DELTA, b)), [])
        real = sampled(self.outlier(), base=[3.80, 3.71, 3.85], pr=[5.2, 5.4, 5.3])
        self.assertEqual([f["locked"] for f in over2x.locked_failing(over2x.gate(summary(real), {}, NOISE, MIN_DELTA, b))
                          if f["mode"] == "cold"], [True])

    def test_warm_findings_are_untouched(self):
        r = row(shape="W", pr=0.5, base=0.15, loki=0.1, pr_cold=5.0, base_cold=3.8, loki_cold=0.6)
        before = [(f["mode"], f["kind"]) for f in failing(summary(r))]
        sampled(r, base=[3.8, 3.8, 3.8], pr=[3.8, 3.8, 3.8])
        after = [(f["mode"], f["kind"]) for f in failing(summary(r))]
        self.assertIn(("warm", "new"), before)
        self.assertEqual(after, [("warm", "new")])  # the warm p50 never was a cold cell

    def test_a_missing_sample_leaves_the_row_and_its_finding(self):
        for resampled in (None, {}, {"base": [3.8, 3.8, 3.8], "pr": []}):
            r = self.outlier()
            self.assertFalse(cold.decide(r, dict(r["cold"]), resampled, "base", "pr"))
            self.assertNotIn("cold_samples", r)
            self.assertEqual(r["cold"]["pr"], 5.32)
            self.assertEqual(cold_findings(summary(r)), [("worsened", "above 2× on base too")])

    def test_a_cache_served_loki_cold_stays_out_of_every_ratio_with_or_without_the_re_measure(self):
        # Loki's cache answered the cold run: no ratio exists for it, as before, so there is no finding either way.
        r = row(shape="s", pr=0.12, base=0.12, pr_cold=1.0, base_cold=0.2, loki_cold=0.2, cold_cache_served=["loki"])
        sampled(r, base=[0.2, 0.2, 0.2], pr=[1.0, 1.0, 1.0])
        self.assertEqual(cold_findings(summary(r)), [])  # not comparable, exactly as without the re-measure
        self.assertEqual(r["cold_cache_served"], ["loki"])


class FlaggedTest(unittest.TestCase):
    def test_only_new_and_worsened_cold_findings_are_flagged(self):
        s = summary(row(shape="outlier", pr=0.16, base=0.15, loki=0.09, pr_cold=5.32, base_cold=3.79, loki_cold=0.6),
                    row(shape="warm only", pr=0.5, base=0.15, loki=0.1, pr_cold=0.1, base_cold=0.1),
                    row(shape="clean", pr=0.1, base=0.1, loki=0.1),
                    row(shape="known", rng="3h", pr=0.52, base=0.5, loki=0.1, pr_cold=0.52, base_cold=0.5, loki_cold=0.1))
        findings = over2x.gate(s, {}, NOISE, MIN_DELTA)
        self.assertEqual(cold.flagged(findings), [("outlier", "1h")])

    def test_a_clean_summary_flags_nothing(self):
        s = summary(row(shape="a", pr=0.1, base=0.1, loki=0.1))
        self.assertEqual(cold.flagged(over2x.gate(s, {}, NOISE, MIN_DELTA)), [])


class FakeProxy:
    """Records what the re-measure does to a build: flushes, quiet waits and the requests it receives."""

    def __init__(self, name, log):
        self.name, self.url, self.log, self.log_path = name, f"http://{name}", log, f"/dev/null/{name}"

    def flush_target(self):
        return f"http://{self.name}/admin/cache/flush", "tok"

    def wait_quiet(self, *a, **kw):
        self.log.append(("quiet", self.name))
        return True


SPEC = {"ranges": {"1h": [3600, 5]},
        "sets": {"control": {"shapes": [{"name": "X1", "query": 'sum(count_over_time({a="b"}[$__auto]))'},
                                        {"name": "H", "query": "x", "end_offset": 3600}]}}}


class ResampleTest(unittest.TestCase):
    def run_resample(self, times, shape="X1", flush_fails=False, status=200):
        log = []
        proxies = {"base": FakeProxy("base", log), "pr": FakeProxy("pr", log)}
        seconds = iter(times)

        def fake_request(url, headers, tenant, timeout):
            log.append(("request", url.split("/loki")[0].split("//")[1], url))
            return status, b"", next(seconds), 0

        def fake_flush(url, token=""):
            if flush_fails:
                raise SystemExit("cache flush: refused")
            log.append(("flush", url.split("/")[2], token))

        with mock.patch.object(perf_matrix, "request", fake_request), mock.patch.object(perf_matrix, "flush_cache", fake_flush):
            got = cold.resample(proxies, SPEC["sets"]["control"]["shapes"][0 if shape == "X1" else 1], "1h",
                                SPEC["ranges"], 1_800_000_000, 3, "base", "pr")
        return got, log

    def test_three_flushed_samples_alternate_the_build_asked_first(self):
        got, log = self.run_resample([3.8, 3.7, 3.9, 3.8, 3.7, 3.8])
        self.assertEqual(got, {"base": [3.8, 3.8, 3.7], "pr": [3.7, 3.9, 3.8]})
        asked = [e[1] for e in log if e[0] == "request"]
        self.assertEqual(asked, ["base", "pr", "pr", "base", "base", "pr"])  # ABAB, as samples
        # every request is preceded by its build's flush and then by both builds' logs going quiet
        for i, e in enumerate(log):
            if e[0] == "request":
                self.assertEqual(log[i - 3][:2], ("flush", e[1]))
                self.assertEqual({log[i - 2], log[i - 1]}, {("quiet", "base"), ("quiet", "pr")})
        self.assertTrue(all(e[2] == "tok" for e in log if e[0] == "flush"))

    def test_the_samples_use_distinct_windows(self):
        _, log = self.run_resample([1] * 6)
        ends = [dict(p.split("=") for p in e[2].split("?")[1].split("&"))["end"] for e in log if e[0] == "request"]
        self.assertEqual(sorted(set(ends)), sorted({str(1_800_000_000 - 60 * i) for i in range(3)}))

    def test_a_refused_flush_or_an_error_answer_takes_no_sample(self):
        self.assertIsNone(self.run_resample([1] * 6, flush_fails=True)[0])
        self.assertIsNone(self.run_resample([1] * 6, status=502)[0])

    def test_a_shape_on_a_moved_window_is_not_re_measured(self):
        self.assertIsNone(self.run_resample([1] * 6, shape="H")[0])

    def test_recold_replaces_the_cold_and_records_what_it_did(self):
        s = summary(row(shape="X1", pr=0.16, base=0.15, loki=0.09, pr_cold=5.32, base_cold=3.79, loki_cold=0.6),
                    set_name="control", profile="loki-defaults")
        first = {("X1", "1h"): dict(s["rows"][0]["cold"])}
        log = []
        proxies = {"base": FakeProxy("base", log), "pr": FakeProxy("pr", log)}
        samples = {"base": iter([3.8, 3.7, 3.9]), "pr": iter([3.75, 3.8, 3.7])}

        def fake_request(url, headers, tenant, timeout):
            return 200, b"", next(samples[url.split("//")[1].split("/")[0]]), 0

        with mock.patch.object(perf_matrix, "request", fake_request), mock.patch.object(perf_matrix, "flush_cache"):
            records = cold.recold(s, first, [("X1", "1h")], proxies, SPEC, 1_800_000_000, 3, log=lambda *_: None)
        self.assertEqual([(r["shape"], r["applied"]) for r in records], [("X1", True)])
        self.assertEqual(s["rows"][0]["cold"]["pr"], 3.775)
        self.assertEqual(failing(s), [])
        self.assertEqual(log[-2:], [("quiet", "base"), ("quiet", "pr")])  # the flushes' refreshes end before the next set


class NoSampleTest(unittest.TestCase):
    """A flagged row whose re-measure cannot run keeps the first pass's cold, not the confirm pass's cache answer."""

    def flagged_after_confirm(self):
        first = summary(row(shape="X1", pr=0.16, base=0.15, loki=0.09, pr_cold=5.32, base_cold=3.79, loki_cold=0.6),
                        set_name="control", profile="loki-defaults")
        first_cold = {("X1", "1h"): dict(first["rows"][0]["cold"])}
        # the confirm pass: proxies answered run 0 from their window cache, Loki computed its own
        redo = summary(row(shape="X1", pr=0.16, base=0.15, loki=0.09, pr_cold=0.022, base_cold=0.029, loki_cold=0.092),
                       set_name="control", profile="loki-defaults")
        a = types.SimpleNamespace(noise=NOISE, min_delta=MIN_DELTA, confirm_runs=7)
        with mock.patch.object(pr_smoke, "run_matrix", return_value=redo), mock.patch.object(pr_smoke.stack, "log"):
            out = pr_smoke.confirm(first, a, {"confirm_end": 1}, None, "/out", [("base", "u")], {})
        self.assertEqual(failing(out), [])  # the cache answer clears the finding: the hole this closes
        return out, first_cold

    def recold(self, out, first_cold, **kw):
        log = []
        proxies = {"base": FakeProxy("base", log), "pr": FakeProxy("pr", log)}
        with mock.patch.object(perf_matrix, "request", lambda *a, **k: (200, b"", 3.8, 0)):
            return cold.recold(out, first_cold, [("X1", "1h")], proxies, SPEC, 1_800_000_000, 3, log=lambda *_: None, **kw)

    def test_a_refused_flush_restores_the_first_pass_cold_and_the_finding(self):
        out, first_cold = self.flagged_after_confirm()
        with mock.patch.object(perf_matrix, "flush_cache", side_effect=SystemExit("cache flush: refused")):
            records = self.recold(out, first_cold)
        self.assertEqual([r["applied"] for r in records], [False])
        self.assertEqual(out["rows"][0]["cold"], {"base": 3.79, "pr": 5.32, "loki": 0.6})
        self.assertEqual(cold_findings(out), [("worsened", "above 2× on base too")])

    def test_a_used_up_budget_restores_the_first_pass_cold_and_the_finding(self):
        out, first_cold = self.flagged_after_confirm()
        with mock.patch.object(perf_matrix, "flush_cache"):
            records = self.recold(out, first_cold, deadline=0)  # the deadline is long past
        self.assertEqual([(r["applied"], "budget" in r["why"]) for r in records], [(False, True)])
        self.assertEqual(cold_findings(out), [("worsened", "above 2× on base too")])

    def test_a_row_with_samples_is_not_restored(self):
        out, first_cold = self.flagged_after_confirm()
        with mock.patch.object(perf_matrix, "flush_cache"):
            records = self.recold(out, first_cold)
        self.assertEqual([r["applied"] for r in records], [True])
        self.assertEqual(out["rows"][0]["cold"]["pr"], 3.8)


class QuietTest(unittest.TestCase):
    def test_the_builds_are_waited_for_at_the_same_time(self):
        class Slow(FakeProxy):
            def wait_quiet(self, *a, **kw):
                time.sleep(0.3)
                return True

        proxies = {"base": Slow("base", []), "pr": Slow("pr", [])}
        t0 = time.time()
        self.assertTrue(cold.quiet_all(proxies))
        self.assertLess(time.time() - t0, 0.55)  # not the 0.6 s of two waits in a row


class SpareRegionTest(unittest.TestCase):
    def test_the_spare_region_is_inside_the_seeded_data_and_below_every_block(self):
        end, a = 1_800_000_000, types.SimpleNamespace(ranges=["1h", "24h"], runs=4, confirm_runs=7, refresh_s=0,
                                                       refresh_runs=5)
        sel = {"run": True, "sets": {"control": ["json volume no filters"], "json-filter-pushdown": None}}
        plans, seed_s = pr_smoke.window_plan(a, sel, end)
        blocks = pr_smoke.blocks_span(plans)
        spare = cold.spare_region(end, blocks, pr_smoke.max_range_secs(a))
        deepest_block = min(p["confirm_end"] - p["shift"] * a.confirm_runs for p in plans)
        self.assertLess(spare["cold_end"], deepest_block)
        self.assertLess(spare["warm_end"], spare["cold_end"])
        range_secs = pr_smoke.max_range_secs(a)
        lowest_cold = spare["cold_end"] - cold.SAMPLE_SHIFT * (cold.SPARE_WINDOWS - 1) - range_secs
        lowest_warm = spare["warm_end"] - cold.SAMPLE_SHIFT * (cold.WARM_ROUNDS - 1) - cold.WARM_SPAN
        self.assertGreaterEqual(lowest_cold, end - seed_s)  # seeded: no sample reads an empty window
        self.assertGreaterEqual(lowest_warm, end - seed_s)
        self.assertEqual(spare["cold_end"] % 60, 0)


class ProxyHelpersTest(unittest.TestCase):
    """stack.Proxy.flush_target and wait_quiet, on a real Proxy object with no process."""

    def proxy(self, args, log_dir):
        return cold.stack.Proxy("pr", "/bin/true", args, {}, 23192, log_dir)

    def test_the_flush_goes_to_the_main_listener_when_the_stack_file_sets_an_admin_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = self.proxy(["-listen=127.0.0.1:23192", "-server.admin-auth-token=e2e-test-token"], tmp)
            self.assertEqual(p.flush_target(), ("http://127.0.0.1:23192/admin/cache/flush", "e2e-test-token"))
            p = self.proxy(["-listen=127.0.0.1:23192", "-admin-listen=127.0.0.1:23193"], tmp)
            self.assertEqual(p.flush_target(), ("http://127.0.0.1:23193/admin/cache/flush", ""))
            p = self.proxy(["-server.admin-auth-token= "], tmp)  # a blank token is no token
            self.assertEqual(p.flush_target()[0], "http://127.0.0.1:23193/admin/cache/flush")

    def test_wait_quiet_waits_out_a_growing_log_and_gives_up_at_the_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = self.proxy([], tmp)
            with open(p.log_path, "w") as f:
                f.write("x")
            self.assertTrue(p.wait_quiet(quiet=0.3, cap=5, poll=0.05))
            stop = threading.Event()

            def writer():
                with open(p.log_path, "a") as f:
                    while not stop.is_set():
                        f.write("x\n")
                        f.flush()
                        time.sleep(0.05)

            t = threading.Thread(target=writer)
            t.start()
            try:
                self.assertFalse(p.wait_quiet(quiet=0.3, cap=0.8, poll=0.05))  # never quiet: returns at the cap
            finally:
                stop.set()
                t.join()
            self.assertTrue(p.wait_quiet(quiet=0.3, cap=5, poll=0.05))


class CommentTest(unittest.TestCase):
    def test_a_re_measured_cold_is_marked_and_its_samples_listed(self):
        r = row(shape="X1", pr=0.16, base=0.15, loki=0.09, pr_cold=5.32, base_cold=3.79, loki_cold=0.6)
        sampled(r, base=[3.80, 3.71, 3.85], pr=[3.75, 3.80, 3.72])
        text, verdict = comment.render([summary(r)], SELECTION, dict(META, over_2x_loki="fail"), {})
        self.assertRegex(text, r"3\.\d\d s ᵐ")
        self.assertIn("ᵐ median of the first pass's cold and 3 re-measured colds", text)
        self.assertIn("pr 5.32/3.75/3.80/3.72", text)
        self.assertEqual(verdict["exit"], 0)

    def test_a_row_that_was_not_re_measured_has_no_mark(self):
        text, _ = comment.render([summary(row(shape="X1"))], SELECTION, META, {})
        self.assertNotIn("ᵐ", text)


class FirstPassOrderTest(unittest.TestCase):
    """perf_matrix: the builds alternate who is asked first, and a cold request waits for the other build's log."""

    TARGETS = [["base", "http://base"], ["pr", "http://pr"], ["loki", "http://loki"]]

    def test_the_builds_swap_places_on_odd_pairs_and_loki_keeps_its_place(self):
        names = lambda k: [t[0] for t in perf_matrix.target_order(self.TARGETS, k)]  # noqa: E731
        self.assertEqual([names(k) for k in range(4)],
                         [["base", "pr", "loki"], ["pr", "base", "loki"], ["base", "pr", "loki"], ["pr", "base", "loki"]])
        self.assertEqual([t[0] for t in perf_matrix.target_order([["loki", "u"], ["a", "u"], ["b", "u"]], 1)],
                         ["loki", "b", "a"])
        self.assertEqual(perf_matrix.target_order([["only", "u"]], 1), [["only", "u"]])

    def test_the_wait_is_for_the_other_builds_logs_and_off_at_cap_zero(self):
        calls = []
        logs = {"base": "/l/base", "pr": "/l/pr"}
        with mock.patch.object(perf_matrix.stack, "logs_quiet", lambda paths, quiet, cap: calls.append((paths, quiet, cap))):
            perf_matrix.quiet_before(self.TARGETS, "pr", logs, 1.0, 4.0)
            perf_matrix.quiet_before(self.TARGETS, "base", logs, 1.0, 0)
            perf_matrix.quiet_before(self.TARGETS, "pr", {}, 1.0, 4.0)
        self.assertEqual(calls, [(["/l/base"], 1.0, 4.0)])  # not its own log, not when off, not without logs

    def run_main(self, argv, shapes):
        events = []
        spec = {"ranges": {"1h": [3600, 5]}, "sets": {"s": {"shapes": shapes}}}
        with tempfile.TemporaryDirectory() as tmp:
            shapes_file, out = os.path.join(tmp, "shapes.json"), os.path.join(tmp, "raw.json")
            with open(shapes_file, "w") as f:
                json.dump(spec, f)

            def fake_request(url, headers, tenant, timeout):
                events.append(("request", url.split("//")[1].split("/")[0]))
                return 200, b'{"data": {"result": []}}', 0.01, 0

            sampler = mock.Mock(samples=[], failed=0)
            sampler.between.return_value = (None, None)
            with mock.patch.object(sys, "argv", ["perf_matrix.py", "--set", "s", "--shapes-file", shapes_file, "--out", out,
                                                 "--end", "1800000000", "--ranges", "1h", *argv]), \
                    mock.patch.object(perf_matrix.subprocess, "check_output", return_value="0"), \
                    mock.patch.object(perf_matrix, "CPUSampler", return_value=sampler), \
                    mock.patch.object(perf_matrix, "request", fake_request), \
                    mock.patch.object(perf_matrix.stack, "logs_quiet",
                                      lambda paths, quiet, cap: events.append(("quiet", tuple(paths), cap)) or True), \
                    contextlib.redirect_stdout(io.StringIO()):
                perf_matrix.main()
            with open(out) as f:
                return events, json.load(f)

    def test_first_pass_alternates_the_first_build_per_shape_and_waits_before_each_cold_request(self):
        shapes = [{"name": "one", "query": "q"}, {"name": "two", "query": "q"}]
        events, raw = self.run_main(["--runs", "2", "--target", "base=http://base", "--target", "pr=http://pr",
                                     "--target", "loki=http://loki", "--quiet-log", "base=/l/base", "--quiet-log", "pr=/l/pr",
                                     "--quiet-cap-s", "3"], shapes)
        self.assertEqual(events, [
            # shape one, run 0: base first (waits for pr's log), then pr (waits for base's), then loki (no wait)
            ("quiet", ("/l/pr",), 3.0), ("request", "base"), ("quiet", ("/l/base",), 3.0), ("request", "pr"),
            ("request", "loki"),
            # shape one, run 1 is warm: no wait
            ("request", "base"), ("request", "pr"), ("request", "loki"),
            # shape two swaps the builds
            ("quiet", ("/l/base",), 3.0), ("request", "pr"), ("quiet", ("/l/pr",), 3.0), ("request", "base"),
            ("request", "loki"),
            ("request", "pr"), ("request", "base"), ("request", "loki")])
        self.assertIn("quiet_wait_s", raw)
        self.assertEqual({r["target"] for r in raw["rows"]}, {"base", "pr", "loki"})

    def test_without_logs_nothing_is_waited_for_and_the_order_still_alternates(self):
        events, _ = self.run_main(["--runs", "1", "--target", "base=http://base", "--target", "pr=http://pr"],
                                  [{"name": "one", "query": "q"}, {"name": "two", "query": "q"}])
        self.assertEqual(events, [("request", "base"), ("request", "pr"), ("request", "pr"), ("request", "base")])

    def test_the_command_line_carries_the_logs_and_the_cap(self):
        st = mock.Mock(vl_container="vl", vl="http://vl")
        a = types.SimpleNamespace(ranges=["1h"], timeout=90, quiet_logs={"base": "/o/b.log", "pr": "/o/p.log"}, quiet_cap_s=4.0)
        cmd = pr_smoke.matrix_cmd("s", None, [("base", "u1"), ("pr", "u2")], a, 1, st, 4, None, "/o/raw.json")
        self.assertEqual([cmd[i + 1] for i, c in enumerate(cmd) if c == "--quiet-log"], ["base=/o/b.log", "pr=/o/p.log"])
        self.assertEqual(cmd[cmd.index("--quiet-cap-s") + 1], "4.0")
        bare = pr_smoke.matrix_cmd("s", None, [("base", "u1")], types.SimpleNamespace(ranges=["1h"], timeout=90), 1, st, 4,
                                   None, "/o/raw.json")
        self.assertNotIn("--quiet-log", bare)

    def test_the_added_time_is_added_to_meta(self):
        with tempfile.TemporaryDirectory() as tmp:
            for suffix, secs in (("", 12.5), ("-confirm", 3.0)):
                with open(pr_smoke.raw_path(tmp, "s", "loki-defaults", suffix), "w") as f:
                    json.dump({"quiet_wait_s": secs}, f)
            meta = {}
            pr_smoke.note_quiet(meta, tmp, "s", "loki-defaults")
            pr_smoke.note_quiet(meta, tmp, "other", "loki-defaults")  # no raw file: nothing added
        self.assertEqual(meta["quiet_wait_s"], 15.5)

    def test_logs_quiet_waits_for_every_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, b = os.path.join(tmp, "a"), os.path.join(tmp, "b")
            for p in (a, b):
                open(p, "w").close()
            self.assertTrue(cold.stack.logs_quiet([a, b], quiet=0.2, cap=3, poll=0.05))
            stop = threading.Event()

            def writer():
                with open(b, "a") as f:
                    while not stop.is_set():
                        f.write("x\n")
                        f.flush()
                        time.sleep(0.05)

            t = threading.Thread(target=writer)
            t.start()
            try:
                self.assertFalse(cold.stack.logs_quiet([a, b], quiet=0.2, cap=0.6, poll=0.05))  # b keeps growing
                self.assertTrue(cold.stack.logs_quiet([a], quiet=0.2, cap=3, poll=0.05))
            finally:
                stop.set()
                t.join()


class WarmUpTest(unittest.TestCase):
    def proxy(self, log, name="base"):
        p = FakeProxy(name, log)
        return p

    def test_it_stops_when_a_round_is_not_slower_and_waits_for_quiet(self):
        log = []
        def fake_request(url, headers, tenant, timeout):
            log.append(url)
            return 200, b"", 0.1, 0  # four requests of 0.1 s a round

        with mock.patch.object(perf_matrix, "request", fake_request):
            out = cold.warm_up({"base": self.proxy(log)}, 1_800_000_000, rounds=3)
        self.assertEqual(out["base"]["rounds_s"], [0.4, 0.4])  # 4 x 0.1 s; round 2 is not slower than round 1
        self.assertTrue(out["base"]["quiet"])
        self.assertEqual(len([u for u in log if isinstance(u, str)]), 8)

    def test_a_round_that_got_slower_goes_on_up_to_the_cap(self):
        seconds = iter([0.1] * 4 + [0.5] * 4 + [1.5] * 4)
        with mock.patch.object(perf_matrix, "request", lambda *a, **kw: (200, b"", next(seconds), 0)):
            out = cold.warm_up({"pr": self.proxy([], "pr")}, 1_800_000_000, rounds=3)
        self.assertEqual(out["pr"]["rounds_s"], [0.4, 2.0, 6.0])

    def test_it_never_fails_the_run_on_an_error_answer(self):
        with mock.patch.object(perf_matrix, "request", lambda *a, **kw: (0, b"refused", 0.01, 0)):
            out = cold.warm_up({"base": self.proxy([])}, 1_800_000_000, rounds=2)
        self.assertEqual(len(out["base"]["rounds_s"]), 2)

    def test_the_requests_ask_for_a_window_no_shape_uses(self):
        urls = []
        with mock.patch.object(perf_matrix, "request", lambda url, *a, **kw: (urls.append(url), (200, b"", 0.1, 0))[1]):
            cold.warm_up({"base": self.proxy([])}, 1_800_000_000, rounds=1)
        self.assertEqual([u.split("?")[0].split("//base")[1] for u in urls],
                         ["/loki/api/v1/labels", "/loki/api/v1/label/env/values", "/loki/api/v1/query_range",
                          "/loki/api/v1/query_range"])


class MeasureTest(unittest.TestCase):
    """pr_smoke.measure sends a row to the cold stage only when its first-pass cold is a failing finding."""

    def measure(self, summaries, drop=(), **over):
        a = types.SimpleNamespace(ranges=["1h"], runs=4, confirm_runs=7, refresh_s=0, refresh_runs=5, noise=0.3,
                                  min_delta=0.1, timeout=90, end=1_800_000_000, same_build=True, head=None, proxy_port=1,
                                  service="s", keep_stack=False, cold_samples=3, warm_rounds=0)
        for k, v in over.items():
            setattr(a, k, v)
        for k in drop:
            a.__dict__.pop(k, None)
        st = mock.Mock(loki="http://loki")
        recorded = {}

        def start(specs, out, st_, svc, plist, **kw):
            plist.extend([FakeProxy("base", recorded.setdefault("log", [])), FakeProxy("pr", recorded["log"])])
            for p in plist:
                p.alive = lambda: True
                p.stop = lambda: None
                p.align = "off"

        replies = iter(summaries)
        with mock.patch.object(pr_smoke.stack, "stack_from", return_value=st), \
                mock.patch.object(pr_smoke.stack, "build_and_seed", return_value={"pr": "/t"}), \
                mock.patch.object(pr_smoke.stack, "start_proxies", side_effect=start), \
                mock.patch.object(pr_smoke.stack, "probe_align_support", return_value=False), \
                mock.patch.object(pr_smoke.stack, "teardown"), mock.patch.object(pr_smoke.stack, "log"), \
                mock.patch.object(pr_smoke, "run_matrix", side_effect=lambda *x, **kw: json.loads(json.dumps(next(replies)))), \
                mock.patch.object(cold, "recold", wraps=cold.recold) as recold, \
                mock.patch.object(perf_matrix, "request", lambda *x, **kw: (200, b"", 3.8, 0)), \
                mock.patch.object(perf_matrix, "flush_cache"), tempfile.TemporaryDirectory() as tmp:
            out = pr_smoke.measure(a, {"run": True, "sets": {"json-filter-pushdown": ["A explore volume, 2 filters, drop"]}},
                                   tmp, "base", baseline={}, bounds=NO_BOUNDS)
        return out, recold

    def shape_summary(self, **kw):
        return summary(row(shape="A explore volume, 2 filters, drop", **kw), set_name="json-filter-pushdown",
                       reference="loki")

    def test_a_failing_cold_is_re_measured_and_a_clean_summary_is_not(self):
        outlier = self.shape_summary(pr=0.1, base=0.1, loki=0.09, pr_cold=5.0, base_cold=3.8, loki_cold=0.6)
        out, recold = self.measure([outlier, outlier])  # first pass, then the confirm pass if it asks for one
        recold.assert_called_once()
        self.assertEqual(recold.call_args.args[2], [("A explore volume, 2 filters, drop", "1h")])
        r = out[0]["rows"][0]
        self.assertEqual(r["cold_samples"]["pr"], [5.0, 3.8, 3.8, 3.8])
        self.assertEqual(over2x.failing(over2x.gate(out[0], {}, NOISE, MIN_DELTA)), [])
        clean = self.shape_summary(pr=0.1, base=0.1, loki=0.09)
        _, recold = self.measure([clean])
        recold.assert_not_called()

    def test_a_caller_with_its_own_args_gets_neither_the_warm_up_nor_the_re_measure(self):
        outlier = self.shape_summary(pr=0.1, base=0.1, loki=0.09, pr_cold=5.0, base_cold=3.8, loki_cold=0.6)
        with mock.patch.object(cold, "warm_up") as warm_up:
            out, recold = self.measure([outlier, outlier], drop=("cold_samples", "warm_rounds", "cold_budget_s"))
        warm_up.assert_not_called()
        recold.assert_not_called()
        self.assertNotIn("cold_samples", out[0]["rows"][0])

    def test_no_baseline_or_no_samples_means_the_first_pass_stands(self):
        outlier = self.shape_summary(pr=0.1, base=0.1, loki=0.09, pr_cold=5.0, base_cold=3.8, loki_cold=0.6)
        _, recold = self.measure([outlier, outlier], cold_samples=0)
        recold.assert_not_called()


class ReplayTest(unittest.TestCase):
    """The two real runs that failed on one cold sample, from their first-pass raw rows."""

    @classmethod
    def setUpClass(cls):
        with open(FIXTURE) as f:
            cls.cases = json.load(f)["cases"]

    def summaries(self, case):
        out = []
        for profile, raw in case["raw"].items():
            with tempfile.TemporaryDirectory() as tmp:
                path, save = os.path.join(tmp, "raw.json"), os.path.join(tmp, "summary.json")
                with open(path, "w") as f:  # the fixture keeps no CPU samples
                    json.dump(dict(raw, rows=[dict(r, vl_cpu_max=None, vl_cpu_avg=None) for r in raw["rows"]]), f)
                ns = types.SimpleNamespace(raw=path, baseline="base", candidate="pr", reference="loki", label="replay",
                                           save=save, tolerance=0.01, refresh="", cold=False, noise=0.30, min_delta=0.10)
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    report.summarize(ns)
                with open(save) as f:
                    s = json.load(f)
            s["proxy_align"] = {"base": "unsupported" if profile == "aligned" else "off", "pr": "on"}
            out.append(s)
        return out

    def gate(self, summaries, baseline, bnds):
        found = []
        for s in summaries:
            found += over2x.gate(s, baseline, NOISE, MIN_DELTA, bnds, summaries)
        return found

    def locked(self, case):
        with open(os.path.join(pr_smoke.HERE, "shapes.json")) as f:
            spec = json.load(f)
        covers = over2x.shape_covers(spec)
        ids = covers[(case["set"], case["shape"])]
        return over2x.make_bounds({i: LOCK for i in ids}, spec) if case["name"] == "case-1" else NO_BOUNDS

    def baseline(self):
        base, problem = over2x.read_baseline()
        self.assertFalse(problem)
        return base

    def test_the_unchanged_gate_fails_both_cases_on_the_first_pass_cold(self):
        for case in self.cases:
            found = self.gate(self.summaries(case), self.baseline(), self.locked(case))
            self.assertEqual({(f["profile"], f["mode"], f["kind"]) for f in over2x.failing(found)
                              if f["shape"] == case["shape"] and f["mode"] == "cold"} >= {("loki-defaults", "cold", "worsened")},
                             True, case["name"])

    def test_the_new_flow_sends_exactly_those_rows_to_the_cold_stage(self):
        for case in self.cases:
            summaries = self.summaries(case)
            first = summaries[0]  # loki-defaults runs first
            pairs = cold.flagged(over2x.gate(first, self.baseline(), NOISE, MIN_DELTA, self.locked(case), []))
            self.assertEqual(pairs, [(case["shape"], case["range"])], case["name"])

    def test_the_confirm_pass_cold_is_a_cache_answer_for_the_proxies(self):
        # Why confirm() cannot clear a cold finding: its run 0 is as fast as a warm run (case 2: 0.02 s against 3.8 s).
        for case in self.cases:
            rows = case["confirm"].get("loki-defaults")
            if not rows:
                continue
            run0 = {r["target"]: r["seconds"] for r in rows if r["run"] == 0}
            first = {r["target"]: r["seconds"] for r in case["raw"]["loki-defaults"]["rows"] if r["run"] == 0}
            self.assertLess(max(run0["base"], run0["pr"]) * 10, min(first["base"], first["pr"]), case["name"])

    def test_real_cold_samples_of_the_same_builds_outvote_the_outlier_in_case_2(self):
        # The artifacts hold one cold sample per build per Loki profile. The aligned profile's run 0 is a second, real
        # cold sample of the same query on the same builds (fresh proxy processes); with it the middle of the samples
        # is within noise of the base. This is the decision on real numbers; the re-measure itself needs a stack.
        case = self.cases[1]
        defaults, aligned = (self.summaries(case)[i]["rows"][0]["cold"] for i in (0, 1))
        row0 = self.summaries(case)[0]["rows"][0]
        cold.decide(row0, dict(defaults), {"base": [aligned["base"]], "pr": [aligned["pr"]]}, "base", "pr")
        self.assertEqual(row0["cold_samples"]["pr"], [5.323, 3.738])
        self.assertAlmostEqual(row0["cold"]["pr"], 4.53, places=2)
        self.assertAlmostEqual(row0["cold"]["base"], 3.77, places=2)
        self.assertFalse(over2x.slower(row0["cold"]["base"], row0["cold"]["pr"], NOISE, MIN_DELTA))


if __name__ == "__main__":
    unittest.main()
