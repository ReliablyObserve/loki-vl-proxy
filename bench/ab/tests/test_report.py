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


if __name__ == "__main__":
    unittest.main()
