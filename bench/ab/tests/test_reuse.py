"""Unit tests for bench/ab/reuse.py and the reuse the daily smoke keeps."""
import http.server
import json
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import reuse  # noqa: E402
import smoke  # noqa: E402

METRICS = """# HELP x y
loki_vl_proxy_backend_duration_seconds_count{system="victorialogs",endpoint="/select/logsql/query",route="a,b"} 3
loki_vl_proxy_backend_duration_seconds_count{endpoint="/select/logsql/stats_query_range"} 2
loki_vl_proxy_backend_read_bytes_total{kind="query"} 100
loki_vl_proxy_cache_layer_requests_total{layer="loki",component="response",tier="l1_memory"} 4
loki_vl_proxy_cache_layer_hits_total{layer="loki",component="response",tier="l1_memory"} 3
process_cpu_seconds_total 1.5
loki_vl_proxy_translations_total 7
other_total 9
bad_value{a="1"} x
"""


class ReuseTest(unittest.TestCase):
    def test_parse_delta_ratio_median(self):
        s = reuse.parse(METRICS)
        self.assertEqual(s, {"vl_calls": 5.0, "vl_calls:/select/logsql/query": 3.0, "vl_calls:/select/logsql/stats_query_range": 2.0,
                             "vl_bytes": 100.0, "loki:l1_memory:requests": 4.0, "loki:l1_memory:hits": 3.0,
                             "cpu_seconds": 1.5, "translations": 7.0})
        d = reuse.delta({"vl_calls": 5, "loki:l1_memory:requests": 4}, {"vl_calls": 5, "loki:l1_memory:requests": 6, "loki:l1_memory:hits": 1})
        self.assertEqual(d, {"loki:l1_memory:requests": 2, "loki:l1_memory:hits": 1})
        self.assertEqual(reuse.hit_ratio(d), {"loki:l1_memory": 0.5})
        self.assertEqual(reuse.median([{"vl_calls": 1}, {"vl_calls": 3}, None, {"vl_calls": 0, "vl_bytes": 4}]), {"vl_calls": 1})
        self.assertIsNone(reuse.median([None]))

    def test_scrape(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200 if self.path == "/metrics" else 404)
                self.end_headers()
                self.wfile.write(METRICS.encode())

            def log_message(self, *args):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            self.assertEqual(reuse.scrape(f"http://127.0.0.1:{srv.server_port}/")["vl_calls"], 5.0)
        finally:
            srv.shutdown()
            srv.server_close()
        self.assertIsNone(reuse.scrape("http://127.0.0.1:1", timeout=1))


def summary(date, repeat_calls, hits=1.0, repeat_bytes=0, exact=None, exact_problem=None):
    row = {"shape": "s", "range": "1h", "verdict": "same", "p50": {"main": 0.2, "loki": 0.2, "release": 0.2},
           "cold": {"main": 0.5, "loki": 0.5, "release": 0.5}, "status": {"main": "200", "loki": "200", "release": "200"},
           "parity_vs_reference": "same",
           "reuse": {"main": {"cold": {"vl_calls": 2, "vl_bytes": 10},
                              "repeat": {"vl_calls": repeat_calls, "vl_bytes": repeat_bytes, "loki:l1_memory:requests": 2,
                                         "loki:l1_memory:hits": 2 * hits},
                              "refresh": {"vl_calls": 1}}}}
    if exact is not None:
        row["reuse"]["main"]["exact_fill"] = {"vl_calls": 2, "vl_bytes": 10}
        row["reuse"]["main"]["exact_repeat"] = {"vl_calls": exact, "vl_bytes": 7 * exact}
    if exact_problem:
        row["reuse"]["main"]["exact_repeat_problem"] = exact_problem
    return {"set": "control", "label": "daily-smoke-control", "date": date, "baseline": "release", "candidate": "main",
            "reference": "loki", "runs": 3, "valid": True, "profile": "loki-defaults", "rows": [row]}


class CallsTest(unittest.TestCase):
    def test_a_missing_header_is_not_zero_calls(self):
        self.assertEqual([reuse.calls_of(v) for v in ("0", "3", None, "", "-1", "x", "1.5")], [0, 3, None, None, None, None, None])

    def test_the_requests_own_count_replaces_the_counters(self):
        # The counters saw 5 calls around the request, 2 of them its own.
        got = reuse.with_request_calls({"vl_calls": 5.0, "vl_calls:/q": 3.0, "vl_bytes": 7.0}, 2)
        self.assertEqual(got, {"vl_calls": 2.0, "background_vl_calls": 3.0, "vl_calls:/q": 3.0, "vl_bytes": 7.0})
        # A cache hit: no call of its own, the counters moved only by background work.
        self.assertEqual(reuse.with_request_calls({"vl_calls": 1.0}, 0), {"background_vl_calls": 1.0})
        # Nothing moved at all.
        self.assertEqual(reuse.with_request_calls({"vl_bytes": 4.0}, 0), {"vl_bytes": 4.0})
        # A request that started more calls than the counters moved by (a late read) has no negative background.
        self.assertEqual(reuse.with_request_calls({"vl_calls": 1.0}, 2), {"vl_calls": 2.0})


class SmokeReuseTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_reuse_is_kept_and_a_drop_is_red(self):
        out = tempfile.mkdtemp()
        first = smoke.write([summary("2026-11-13", 0)], out, "2026-11-13", "c1", directory=self.dir, ratchet_dir=self.dir)
        self.assertFalse(first["red"])
        self.assertEqual(first["reuse"], {"rows": 1, "cold_vl_calls": 2, "repeat_vl_calls": 0, "repeat_without_vl_calls": 1,
                                          "repeat_vl_bytes": 0, "cold_vl_bytes": 10, "exact_rows": 0, "exact_repeat_vl_calls": 0,
                                          "exact_without_vl_calls": 0, "exact_problems": 0})
        with open(os.path.join(self.dir, smoke.REUSE_HISTORY)) as f:
            line = json.loads(f.readline())
        self.assertEqual(line["rows"]["control|s|1h|loki-defaults"]["refresh_vl_calls"], 1)
        # The next day the memory tier answers half: red. The moved-window repeat's calls are not judged (it asks for the
        # buckets it lacks); a call more with the same bytes is not red.
        second = smoke.judge([summary("2026-11-14", 1, hits=0.5)], [], self.dir)
        self.assertTrue(second["red"])
        self.assertEqual(len(second["reuse_lost"]), 1)
        self.assertIn("loki:l1_memory hit ratio 1.00 -> 0.50", second["reuse_lost"][0]["detail"])
        self.assertNotIn("calls", second["reuse_lost"][0]["detail"])
        self.assertFalse(smoke.judge([summary("2026-11-14", 3)], [], self.dir)["red"])
        with open(os.path.join(out, "smoke.md")) as f:
            self.assertIn("1 rows repeat without calling VictoriaLogs", f.read())
        # Equal reuse is not red; a row without reuse figures is not judged.
        self.assertFalse(smoke.judge([summary("2026-11-14", 0)], [], self.dir)["red"])
        bare = summary("2026-11-14", 0)
        del bare["rows"][0]["reuse"]
        self.assertEqual(smoke.judge([bare], [], self.dir)["reuse"]["rows"], 0)

    def test_the_moved_window_repeat_is_judged_by_bytes(self):
        smoke.write([summary("2026-11-13", 1, repeat_bytes=10000)], tempfile.mkdtemp(), "2026-11-13", "c1", directory=self.dir, ratchet_dir=self.dir)
        # Bytes within the noise (a bucket more) are not red; a repeat that reads far more is.
        self.assertFalse(smoke.judge([summary("2026-11-14", 1, repeat_bytes=14000)], [], self.dir)["red"])
        grown = smoke.judge([summary("2026-11-14", 1, repeat_bytes=40000)], [], self.dir)
        self.assertTrue(grown["red"])
        self.assertIn("moved-window repeat VictoriaLogs bytes 10000 -> 40000", grown["reuse_lost"][0]["detail"])
        # A small repeat that doubles stays under the floor.
        smoke.write([summary("2026-11-15", 1, repeat_bytes=100)], tempfile.mkdtemp(), "2026-11-15", "c2", directory=self.dir, ratchet_dir=self.dir)
        self.assertFalse(smoke.judge([summary("2026-11-16", 1, repeat_bytes=900)], [], self.dir)["red"])

    def test_the_exact_repeat_is_kept_apart_and_expected_to_call_nothing(self):
        out = tempfile.mkdtemp()
        first = smoke.write([summary("2026-11-13", 2, exact=0)], out, "2026-11-13", "c1", directory=self.dir, ratchet_dir=self.dir)
        self.assertFalse(first["red"])
        self.assertEqual((first["reuse"]["exact_rows"], first["reuse"]["exact_without_vl_calls"], first["reuse"]["exact_repeat_vl_calls"]), (1, 1, 0))
        self.assertEqual(first["reuse_rows"]["control|s|1h|loki-defaults"]["exact_fill_vl_calls"], 2)
        with open(os.path.join(out, "smoke.md")) as f:
            text = f.read()
        self.assertIn("An exact repeat (the same request asked again at a settled window) is the cache's to answer: 1 rows measured, "
                      "1 without a VictoriaLogs call, 0 calls in all.", text)
        self.assertIn("The two repeats are measured apart.", text)
        self.assertIn("#### Exact repeats that called VictoriaLogs: 0", text)
        # The next day the exact repeat calls VictoriaLogs: red, listed, with the calls.
        second = smoke.judge([summary("2026-11-14", 2, exact=3)], [], self.dir)
        self.assertTrue(second["red"])
        self.assertIn("exact repeat VictoriaLogs calls 0 -> 3", second["reuse_lost"][0]["detail"])
        self.assertEqual([(r["calls"], r["bytes"]) for r in second["exact_repeat_called"]], [(3, 21)])
        # A day without an exact repeat is not compared with one, and is not 0 calls either.
        none = smoke.judge([summary("2026-11-14", 2)], [], self.dir)
        self.assertFalse(none["red"])
        self.assertEqual(none["reuse"]["exact_rows"], 0)
        # An exact repeat that could not be measured is counted, never read as a clean repeat.
        bad = smoke.judge([summary("2026-11-14", 2, exact_problem="the repeat answered differently")], [], self.dir)
        self.assertEqual((bad["reuse"]["exact_rows"], bad["reuse"]["exact_problems"]), (0, 1))
        with tempfile.TemporaryDirectory() as d:
            smoke.write([summary("2026-11-14", 2, exact_problem="x")], d, "2026-11-14", "c3", directory=self.dir, ratchet_dir=self.dir)
            with open(os.path.join(d, "smoke.md")) as f:
                self.assertIn("No exact repeat was measured in this run.", f.read())

    def test_no_previous_day(self):
        self.assertEqual(smoke.previous_reuse(self.dir, "2026-11-14"), {})
        self.assertEqual(smoke.reuse_lost({"k": {"repeat_vl_calls": 3}}, {}), [])


if __name__ == "__main__":
    unittest.main()
