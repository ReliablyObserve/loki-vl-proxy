"""Unit tests for bench/ab/report.py summarize verdicts."""
import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import report  # noqa: E402
import comment  # noqa: E402


def raw_rows(statuses, signatures):
    rows = []
    for run in range(3):
        for target in ("main", "branch", "loki"):
            rows.append({"shape": "s", "range": "1h", "run": run, "target": target, "seconds": 0.1,
                         "status": statuses[target], "signature": signatures[target], "vl_cpu_max": 0,
                         "end": 1000 + run})
    return {"set": "x", "rows": rows}


def summarize(raw, refresh=None, whole=False):
    with tempfile.TemporaryDirectory() as d:
        path, refresh_path = os.path.join(d, "raw.json"), os.path.join(d, "refresh.json")
        with open(path, "w") as f:
            json.dump(raw, f)
        if refresh:
            with open(refresh_path, "w") as f:
                json.dump(refresh, f)
        args = argparse.Namespace(raw=path, baseline="main", candidate="branch", reference="loki", label="t",
                                  save=d, tolerance=0.01, cold=False, noise=0.25, min_delta=0.05,
                                  refresh=refresh_path if refresh else "")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            report.summarize(args)
        with open(os.path.join(d, [n for n in os.listdir(d) if n not in ("raw.json", "refresh.json")][0])) as f:
            out = json.load(f)
        return out if whole else out["rows"][0]


def timed(seconds, cache=None, targets=("main", "branch", "loki"), runs=3, **extra):
    """Raw rows where run 0 takes seconds[target][0] and later runs seconds[target][1]."""
    rows = []
    for run in range(runs):
        for target in targets:
            row = {"shape": "s", "range": "1h", "run": run, "target": target, "status": 200, "signature": "x",
                   "seconds": seconds[target][min(run, 1)], "vl_cpu_max": 0, "end": 1000 + run}
            if cache and target in cache and run in cache[target]:
                row["cache"] = cache[target][run]
            rows.append(row)
    return {"set": "x", "runs": runs, "rows": rows, **extra}


class PointCountParityTest(unittest.TestCase):
    def test_equal_sum_cannot_hide_missing_points(self):
        for series in ("1", "ranked"):
            with self.subTest(series=series):
                self.assertEqual(report.parity(
                    f"series={series} points=2 sum=4",
                    f"series={series} points=3 sum=4", 0.01), "differs")

    def test_equal_point_counts_keep_sum_tolerance(self):
        self.assertTrue(report.parity(
            "series=1 points=2 sum=4", "series=1 points=2 sum=4.01", 0.01).startswith("same"))

    def test_missing_points_are_a_regression_in_the_summary(self):
        complete = "series=1 points=3 sum=4"
        summary = summarize(raw_rows({"main": 200, "branch": 200, "loki": 200},
                                     {"main": complete, "branch": "series=1 points=2 sum=4", "loki": complete}),
                            whole=True)
        row = summary["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "differs")
        self.assertEqual(row["baseline_parity_vs_reference"], "same")
        self.assertEqual(row["parity_vs_baseline"], "differs")
        _, verdict = comment.render([summary], {}, {})
        self.assertEqual(verdict["exit"], 1)
        self.assertEqual(verdict["new_differences"], 1)


class VerdictTest(unittest.TestCase):
    def test_rejecting_like_the_reference_is_a_fix(self):
        err = "ERR parse error at line 1, col 64: syntax error: unexpected ."
        row = summarize(raw_rows({"main": 200, "branch": 400, "loki": 400},
                                 {"main": "streams lines=5", "branch": err, "loki": err}))
        self.assertEqual(row["verdict"], "fixed")

    def test_failing_differently_from_the_reference_is_broken(self):
        row = summarize(raw_rows({"main": 200, "branch": 502, "loki": 200},
                                 {"main": "streams lines=5", "branch": "ERR boom", "loki": "streams lines=5"}))
        self.assertEqual(row["verdict"], "broken")

    def test_failing_with_other_text_than_the_reference_is_broken(self):
        row = summarize(raw_rows({"main": 200, "branch": 400, "loki": 400},
                                 {"main": "streams lines=5", "branch": "ERR other", "loki": "ERR parse error"}))
        self.assertEqual(row["verdict"], "broken")


class CacheAndRefreshTest(unittest.TestCase):
    SECONDS = {"main": (0.5, 0.2), "branch": (0.5, 0.2), "loki": (0.3, 0.05)}

    def test_old_rows_record_nothing_about_cache(self):
        row = summarize(timed(self.SECONDS))
        for key in ("nocache", "cache", "cold_cache_served", "refresh"):
            self.assertNotIn(key, row)

    def test_cold_cache_hit_in_lokis_counters_marks_the_row(self):
        hit = {"result": {"entriesFound": 1, "entriesRequested": 1, "bytesReceived": 10}}
        miss = {"result": {"entriesFound": 0, "entriesRequested": 1, "bytesReceived": 0}}
        served = summarize(timed(self.SECONDS, cache={"loki": {0: hit, 1: hit, 2: hit}}))
        self.assertEqual(served["cold_cache_served"], ["loki"])
        self.assertEqual(served["cache"]["loki"]["warm"]["result"]["entriesFound"], 2)
        computed = summarize(timed(self.SECONDS, cache={"loki": {0: miss, 1: hit, 2: hit}}))
        self.assertNotIn("cold_cache_served", computed)  # warm hits are expected and do not invalidate the cold run
        self.assertEqual(computed["cache"]["loki"]["cold"], miss)

    def test_cache_served(self):
        self.assertFalse(report.cache_served(None))
        self.assertFalse(report.cache_served({"result": {"entriesFound": 0, "bytesReceived": 0}}))
        self.assertTrue(report.cache_served({"result": {"entriesFound": 1}}))
        self.assertTrue(report.cache_served({"volumeResult": {"bytesReceived": 5}}))
        self.assertTrue(report.cache_served({"statsResult": {"entriesFound": 1}, "result": {"entriesFound": 0},
                                             "seriesResult": {"entriesFound": 2}}))

    def test_a_stats_result_hit_alone_is_query_planning_not_a_cached_answer(self):
        stats = {"statsResult": {"entriesFound": 1, "entriesRequested": 1, "bytesReceived": 40},
                 "result": {"entriesFound": 0, "entriesRequested": 1, "bytesReceived": 0}}
        self.assertFalse(report.cache_served(stats))
        self.assertFalse(report.cache_served({"statsResult": {"entriesFound": 3, "bytesReceived": 9}}))
        row = summarize(timed(self.SECONDS, cache={"loki": {0: stats, 1: stats, 2: stats}}))
        self.assertNotIn("cold_cache_served", row)
        self.assertEqual(row["cache"]["loki"]["cold"], stats)  # still recorded

    def test_refresh_p50_per_target_skips_the_first_run(self):
        refresh = timed({"main": (9.0, 0.3), "branch": (9.0, 0.1), "loki": (9.0, 0.02)}, runs=4,
                        mode="refresh", refresh_s=60)
        whole = summarize(timed(self.SECONDS), refresh=refresh, whole=True)
        row = whole["rows"][0]
        self.assertEqual(row["refresh"], {"main": 0.3, "branch": 0.1, "loki": 0.02})
        self.assertEqual(row["refresh_status"]["loki"], "200")
        self.assertEqual((whole["refresh_s"], whole["refresh_runs"]), (60, 4))
        self.assertEqual(row["p50"]["main"], 0.2)  # warm is unchanged
        self.assertEqual(row["cold"]["main"], 0.5)

    def test_refresh_shows_in_the_markdown_next_to_warm(self):
        whole = summarize(timed(self.SECONDS), refresh=timed({"main": (1, 0.3), "branch": (1, 0.1), "loki": (1, 0.02)},
                                                              mode="refresh", refresh_s=60), whole=True)
        text = report.markdown(whole)
        self.assertIn("refresh main | refresh branch | refresh loki |", text)
        self.assertIn("| 0.30s | 0.10s | 0.02s |", text)
        self.assertNotIn("refresh main", report.markdown(summarize(timed(self.SECONDS), whole=True)))

    def test_an_invalid_refresh_run_invalidates_the_summary(self):
        whole = summarize(timed(self.SECONDS), refresh=timed(self.SECONDS, valid=False), whole=True)
        self.assertFalse(whole["valid"])


class ProfileTest(unittest.TestCase):
    SECONDS = {"main": (0.5, 0.2), "branch": (0.5, 0.9), "loki": (0.3, 0.05)}  # branch is 4.5x main warm

    def test_the_profile_and_the_unaligned_builds_are_carried_into_the_summary(self):
        whole = summarize(timed(self.SECONDS, profile="aligned", not_like_for_like=["main"]), whole=True)
        self.assertEqual((whole["profile"], whole["not_like_for_like"]), ("aligned", ["main"]))
        self.assertEqual(summarize(timed(self.SECONDS), whole=True)["profile"], None)
        self.assertEqual(summarize(timed(self.SECONDS), whole=True)["not_like_for_like"], [])

    def test_a_build_that_is_not_like_for_like_gets_no_timing_verdict_against_the_other(self):
        same_flags = summarize(timed(self.SECONDS, profile="loki-defaults", not_like_for_like=[]))
        self.assertEqual(same_flags["verdict"], "slower")
        for unlike in (["main"], ["branch"]):
            row = summarize(timed(self.SECONDS, profile="aligned", not_like_for_like=unlike))
            self.assertEqual(row["verdict"], "n/a", unlike)
            self.assertIn("not like-for-like", row["note"])

    def test_status_verdicts_survive(self):
        raw = timed(self.SECONDS, profile="aligned", not_like_for_like=["main"])
        for r in raw["rows"]:
            if r["target"] == "branch":
                r["status"] = 502
        self.assertEqual(summarize(raw)["verdict"], "broken")

    def test_the_markdown_marks_the_build_and_says_why(self):
        whole = summarize(timed(self.SECONDS, profile="aligned", not_like_for_like=["main"]), whole=True)
        text = report.markdown(whole)
        self.assertIn("Loki profile aligned", text)
        self.assertIn("0.20s† ‡", text)
        self.assertIn("‡ main: this build did not run the setting of the Loki profile (aligned)", text)
        self.assertNotIn("‡", report.markdown(summarize(timed(self.SECONDS), whole=True)))


if __name__ == "__main__":
    unittest.main()


def daily_rows(loki, main, release=None, cache=None, ranked_max=None, runs=7):
    """Raw rows shaped like a daily row: run 0 is the one Loki computed, later runs move the window back a
    minute each and Loki's results cache answers them whole (entriesFound == entriesRequested)."""
    cache = cache or {0: {"result": {"entriesFound": 0, "entriesRequested": 25}}}
    rows = []
    for run in range(runs):
        for target, sigs in (("release", release or main), ("main", main), ("loki", loki)):
            row = {"shape": "s", "range": "24h", "run": run, "target": target, "seconds": 0.1, "status": 200,
                   "signature": sigs[run], "vl_cpu_max": 0, "end": 1793810580 - 60 * run}
            if target == "loki":
                row["cache"] = cache.get(run, {"result": {"entriesFound": 25, "entriesRequested": 25}})
                if ranked_max is not None:
                    row["ranked_max_points"] = ranked_max
            rows.append(row)
    return {"set": "x", "runs": runs, "rows": rows}


def daily_summary(raw):
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "raw.json")
        with open(path, "w") as f:
            json.dump(raw, f)
        args = argparse.Namespace(raw=path, baseline="release", candidate="main", reference="loki", label="t",
                                  save=d, tolerance=0.01, cold=False, noise=0.25, min_delta=0.05, refresh="")
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            report.summarize(args)
        with open(os.path.join(d, [n for n in os.listdir(d) if n != "raw.json"][0])) as f:
            return json.load(f), out.getvalue()


class ComputedReferenceParityTest(unittest.TestCase):
    """Daily 2026-11-04 (main 69d07019): every run whose answer differed from Loki's was one Loki's results
    cache answered whole; the run Loki computed matched main."""

    def test_topk_more_points_than_k_per_step_on_cached_runs(self):
        # control "topk rate by app" 1h: 5 x 721 steps = 3605 points; Loki's cached runs held up to 3612.
        main = ["series=ranked points=3605 sum=9812.4"] + [f"series=ranked points=3605 sum={9810 - k}" for k in range(1, 7)]
        loki = [main[0], main[1]] + [f"series=ranked points={3605 + k} sum={9815 + k}" for k in (2, 3, 4, 5, 7)]
        cache = {0: {"result": {"entriesFound": 0, "entriesRequested": 2}}}
        for run in range(1, 7):
            cache[run] = {"result": {"entriesFound": 2, "entriesRequested": 2}}
        summary, text = daily_summary(daily_rows(loki, main, cache=cache, ranked_max=3605))
        row = summary["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "same")
        self.assertEqual(row["baseline_parity_vs_reference"], "same")
        self.assertEqual(row["reference_skipped"], {"cache": 6})
        self.assertIn("compared only with the loki runs loki computed", text)

    def test_ranked_reference_over_k_per_step_is_no_reference(self):
        # The same topk answer with no cache counters (an older Loki, an instant query): the points alone
        # rule the run out, it is not a difference.
        main = ["series=ranked points=10805 sum=495856"] * 7
        loki = main[:3] + ["series=ranked points=10806 sum=495753"] * 4
        raw = daily_rows(loki, main, ranked_max=10805)
        for r in raw["rows"]:
            r.pop("cache", None)
        row = daily_summary(raw)[0]["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "same")
        self.assertEqual(row["reference_skipped"], {"ranked": 4})

    def test_ranked_reference_within_k_per_step_still_differs(self):
        # Fewer points than k x steps is a legal ranked answer: a computed run that differs is a difference.
        main = ["series=ranked points=3605 sum=10"] * 7
        loki = ["series=ranked points=3600 sum=10"] * 7
        cache = {run: {"result": {"entriesFound": 0, "entriesRequested": 2}} for run in range(7)}
        row = daily_summary(daily_rows(loki, main, cache=cache, ranked_max=3605))[0]["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "differs")
        self.assertNotIn("reference_skipped", row)

    def test_one_point_fewer_on_cached_runs(self):
        # json-filter-pushdown B 24h: Loki's cached runs 3 and 5 lost one point; run 0 was computed.
        main = ["series=2 points=1939 sum=28389", "series=2 points=1940 sum=28393", "series=2 points=1941 sum=28383",
                "series=2 points=1941 sum=28385", "series=2 points=1941 sum=28376", "series=2 points=1941 sum=28380",
                "series=2 points=1941 sum=28379"]
        loki = list(main)
        loki[3], loki[5] = "series=2 points=1940 sum=28366", "series=2 points=1940 sum=28361"
        row = daily_summary(daily_rows(loki, main))[0]["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "same")
        self.assertEqual(row["reference_skipped"], {"cache": 6})

    def test_a_computed_run_that_differs_still_differs(self):
        main = ["series=2 points=1939 sum=28389"] + ["series=2 points=1941 sum=28383"] * 6
        loki = ["series=2 points=1940 sum=28400"] + main[1:]
        row = daily_summary(daily_rows(loki, main))[0]["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "differs")

    def test_partly_cached_runs_are_still_compared(self):
        # Only a request the cache answered whole is left out: one with entries left to compute counts.
        main = ["series=2 points=1939 sum=28389"] * 7
        loki = list(main)
        loki[2] = "series=2 points=1938 sum=28370"
        cache = {run: {"result": {"entriesFound": 24, "entriesRequested": 25}} for run in range(7)}
        cache[0] = {"result": {"entriesFound": 0, "entriesRequested": 25}}
        row = daily_summary(daily_rows(loki, main, cache=cache))[0]["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "differs")
        self.assertNotIn("reference_skipped", row)

    def test_index_stats_cache_is_not_an_answer(self):
        main = ["series=1 points=3 sum=4"] * 7
        loki = ["series=1 points=3 sum=4"] * 6 + ["series=1 points=2 sum=4"]
        cache = {run: {"statsResult": {"entriesFound": 1, "entriesRequested": 1}} for run in range(7)}
        row = daily_summary(daily_rows(loki, main, cache=cache))[0]["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "differs")

    def test_no_computed_run_is_not_compared(self):
        # Every Loki run answered whole from its cache (a query an earlier set already asked): no verdict,
        # neither same nor differs, and the summary says how many rows that left.
        main = ["series=1 points=3 sum=4"] * 7
        loki = ["series=1 points=2 sum=4"] * 7
        cache = {run: {"volumeResult": {"entriesFound": 3, "entriesRequested": 3}} for run in range(7)}
        summary, text = daily_summary(daily_rows(loki, main, cache=cache))
        row = summary["rows"][0]
        self.assertEqual(row["parity_vs_reference"], "n/a")
        self.assertEqual(row["baseline_parity_vs_reference"], "n/a")
        self.assertEqual(summary["result_differs_from_reference"], 0)
        self.assertIn("1 row(s) had no computed loki run", text)

    def test_timing_is_unchanged(self):
        main = ["series=1 points=3 sum=4"] * 7
        raw = daily_rows(main, main)
        before = daily_summary(raw)[0]["rows"][0]
        for r in raw["rows"]:
            r.pop("cache", None)
        after = daily_summary(raw)[0]["rows"][0]
        for k in ("p50", "p95", "cold", "verdict", "status"):
            self.assertEqual(before[k], after[k], k)


class RankedMaxPointsTest(unittest.TestCase):
    def test_k_per_step(self):
        import perf_matrix
        for query, secs, step, instant, want in (
                ('topk(5, sum by (app) (rate({env="production"}[5m])))', 3600, 5, False, 3605),
                ("topk(5, x)", 21600, 10, False, 10805),
                ("topk(5, x)", 86400, 60, False, 7205),
                ("  bottomk( 3 , x)", 3600, 5, False, 2163),
                ("topk(3, x)", 300, 0.2, False, 4503),
                ("topk(4, x)", 0, 0, True, 4),
                ("sum(topk(5, x))", 3600, 5, False, None),
                ('sum by (app) (rate({app="topk(5,"}[5m]))', 3600, 5, False, None),
                ("topk(5, x)", 3600, 0, False, None)):
            with self.subTest(query=query, secs=secs, step=step):
                self.assertEqual(perf_matrix.ranked_max_points(query, secs, step, instant), want)
