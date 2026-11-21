"""perf_matrix.py's loop: no sleep per request, CPU read from the sampler afterwards, one Loki timeout per shape and range.

The loop is driven by rows recorded in a real run (fixtures/raw-unwrap-sum-without-w8-w9.json: the
weekly run of 2026-10-27, where Loki timed out at 180 s on W8 at 6h and at 24h on all 7 runs). Every
request is answered from the recorded rows of its target, in order, with the recorded window checked.
"""
import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock as mock
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import perf_matrix  # noqa: E402
import report  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "raw-unwrap-sum-without-w8-w9.json")
W8 = "W8 sum without pod, count_over_time (production)"
W9 = "W9 label_replace control, instant"
URLS = {"release": "http://release", "main": "http://main", "loki": "http://loki"}


def recorded():
    with open(FIXTURE) as f:
        return json.load(f)


class FakeSampler:
    """Stands in for the docker-stats thread: records how the loop uses it."""
    instances = []

    def __init__(self, container):
        self.samples, self.failed, self.stop = [], 0, False
        self.drained, self.windows = [], []
        FakeSampler.instances.append(self)

    def start(self):
        pass

    def drain(self, until, timeout=10.0):
        self.drained.append(until)
        return True

    def between(self, t0, t1):
        self.windows.append((t0, t1))
        return 12.5, 6.25


class Replay:
    """Answers each request with the recorded row for the same target, query, range and window; keeps what was sent.

    The body carries the recorded signature and cache counters (signature() and loki_cache_stats() are stubbed to
    read them back). The recorded run sent Loki's later timeouts too, so a row is found by what the request says
    (a shortened run asks for fewer of them) instead of being taken in order.
    """

    def __init__(self, raw):
        with open(os.path.join(perf_matrix.HERE, "shapes.json")) as f:
            spec = json.load(f)
        query = {sh["name"]: sh["query"] for sh in spec["sets"][raw["set"]]["shapes"]}
        self.rows = {(r["target"], query[r["shape"]], r["range"], r["end"]): r for r in raw["rows"]}
        self.secs = {name: v[0] for name, v in spec["ranges"].items()}
        self.sent = []
        self.clock = 1000.0
        self.calls = None  # the proxy's X-Loki-VL-Backend-Calls of every answer; None: it sends none

    def request(self, url, headers, tenant, timeout):
        target = next(t for t, base in URLS.items() if url.startswith(base + "/"))
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        if "time" in q:
            rng, end = "instant", int(q["time"])
        else:
            end = int(q["end"])
            rng = next(n for n, secs in self.secs.items() if secs == end - int(q["start"]))
        row = self.rows[(target, q["query"], rng, end)]  # KeyError: the loop asked for a window nobody recorded
        self.sent.append((target, row["shape"], row["range"], row["run"]))
        self.clock += row["seconds"]
        body = json.dumps({"signature": row["signature"], "cache": row.get("cache")}).encode()
        return row["status"], body, row["seconds"], self.clock - row["seconds"]


def run_main(raw, extra=(), shapes=(W8, W9)):
    """perf_matrix.main() over the recorded rows; returns (written raw, replay, sleeps, sampler)."""
    replay = Replay(raw)
    sleeps = []
    FakeSampler.instances = []
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "raw.json")
        argv = ["perf_matrix.py", "--set", raw["set"], "--runs", str(raw["runs"]), "--ranges", "1h,6h,24h",
                "--end", str(raw["end"]), "--timeout", "180", "--long-runs", "release=3",
                "--out", out, *extra]
        for name in shapes:
            argv += ["--shape", name]
        for name, url in URLS.items():
            argv += ["--target", f"{name}={url}"]
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(perf_matrix, "request_full", lambda *a: (*replay.request(*a), replay.calls)), \
                mock.patch.object(perf_matrix, "CPUSampler", FakeSampler), \
                mock.patch.object(perf_matrix, "signature", lambda status, body, logs, ranked=False: json.loads(body)["signature"]), \
                mock.patch.object(perf_matrix, "loki_cache_stats", lambda body: json.loads(body)["cache"]), \
                mock.patch.object(perf_matrix.subprocess, "check_output", return_value="0\n"), \
                mock.patch.object(perf_matrix.urllib.request, "urlopen") as urlopen, \
                mock.patch.object(perf_matrix.time, "sleep", side_effect=sleeps.append), \
                contextlib.redirect_stdout(io.StringIO()):
            urlopen.return_value.__enter__.return_value.status = 200
            perf_matrix.main()
        with open(out) as f:
            written = json.load(f)
    return written, replay, sleeps, FakeSampler.instances[0]


def summarize(raw):
    with tempfile.TemporaryDirectory() as d:
        path, saved = os.path.join(d, "raw.json"), os.path.join(d, "summary.json")
        with open(path, "w") as f:
            json.dump(raw, f)
        args = argparse.Namespace(raw=path, baseline="release", candidate="main", reference="loki", label="t",
                                  save=saved, tolerance=0.01, cold=False, noise=0.25, min_delta=0.05, refresh="")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            report.summarize(args)
        with open(saved) as f:
            return json.load(f)


class RankedSignatureTest(unittest.TestCase):
    """Loki picks one of several tied series at each step, so a topk answer's series count is no evidence."""

    @staticmethod
    def body(*series):
        return json.dumps({"data": {"resultType": "matrix", "result": [
            {"metric": {"s": name}, "values": [[1, v], [2, v]]} for name, v in series]}}).encode()

    def test_a_ranked_answer_names_no_series_count(self):
        proxy = perf_matrix.signature(200, self.body(("a", "5"), ("b", "4")), False, True)
        loki = perf_matrix.signature(200, self.body(("a", "5"), ("b", "4"), ("c", "4")), False, True)
        self.assertEqual(proxy, "series=ranked points=4 sum=18")
        self.assertNotEqual(proxy, loki)  # a third series adds points: a real difference stays visible
        again = perf_matrix.signature(200, self.body(("a", "5"), ("c", "4")), False, True)
        self.assertEqual(proxy, again)  # a different tied pick with the same values is the same answer
        self.assertEqual(report.parity(proxy, again, 0.01), "same")

    def test_a_patterns_answer_is_its_lines_and_steps(self):
        # Loki and the proxy mine different patterns: the lines they count, per step, are what compares.
        body = json.dumps({"status": "success", "data": [
            {"pattern": "a <_>", "samples": [[10, 2], [20, 3]]}, {"pattern": "b", "level": "info", "samples": [[20, 1]]}]}).encode()
        self.assertEqual(perf_matrix.signature(200, body, False), "patterns lines=6 steps=2")
        other_text = json.dumps({"status": "success", "data": [{"pattern": "a b", "samples": [[10, 2], [20, 4]]}]}).encode()
        self.assertEqual(perf_matrix.signature(200, other_text, False), "patterns lines=6 steps=2")

    def test_other_answers_keep_the_series_count(self):
        self.assertEqual(perf_matrix.signature(200, self.body(("a", "5"), ("b", "4")), False), "series=2 points=4 sum=18")
        wrong = perf_matrix.signature(200, self.body(("a", "5"), ("b", "9")), False, True)
        self.assertEqual(report.parity(perf_matrix.signature(200, self.body(("a", "5"), ("b", "4")), False, True), wrong, 0.01), "differs")


class MetadataShapeTest(unittest.TestCase):
    def test_matrix_accepts_metadata_shape_without_query(self):
        with tempfile.TemporaryDirectory() as directory:
            shapes = os.path.join(directory, "shapes.json")
            out = os.path.join(directory, "raw.json")
            with open(shapes, "w") as handle:
                json.dump({"ranges": {"1h": [3600, 60]}, "sets": {"metadata-inventory": {
                    "ranges": ["1h"], "shapes": [{"name": "explore labels", "path": "/loki/api/v1/labels"}]
                }}}, handle)
            argv = ["perf_matrix.py", "--set", "metadata-inventory", "--shapes-file", shapes,
                    "--runs", "1", "--end", "1700003600", "--target", "pr=http://pr", "--out", out]
            FakeSampler.instances = []
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(perf_matrix, "request_full", return_value=(200, b'{"data":["app"]}', 0.01, 1000.0, None)), \
                    mock.patch.object(perf_matrix, "CPUSampler", FakeSampler), \
                    mock.patch.object(perf_matrix.subprocess, "check_output", return_value="0\n"), \
                    contextlib.redirect_stdout(io.StringIO()):
                perf_matrix.main()
            with open(out) as handle:
                rows = json.load(handle)["rows"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], 200)
            self.assertIn("names=1", rows[0]["signature"])


class ExactRepeatTest(unittest.TestCase):
    """--exact-repeat: the same request twice in a row at a settled window, the second one's cost kept apart."""

    def run_matrix(self, calls_of, extra=(), background=1):
        seen, state = {}, {"vl_calls": 0.0, "vl_bytes": 0.0}
        asked = []

        def request_full(url, headers, tenant, timeout):
            n = seen[url] = seen.get(url, 0) + 1
            calls = calls_of(url, n)
            asked.append((url, calls))
            if not url.startswith("http://loki"):
                # the counters also move by the proxy's own background calls
                state["vl_calls"] += (calls or 0) + background
                state["vl_bytes"] += 100.0 * (calls or 0)
            return 200, b'{"data":["app"]}', 0.01, 1000.0, calls

        with tempfile.TemporaryDirectory() as directory:
            shapes, out = os.path.join(directory, "shapes.json"), os.path.join(directory, "raw.json")
            with open(shapes, "w") as handle:
                json.dump({"ranges": {"1h": [3600, 60]}, "sets": {"meta": {"ranges": ["1h"], "shapes": [
                    {"name": "labels", "path": "/loki/api/v1/labels"}]}}}, handle)
            argv = ["perf_matrix.py", "--set", "meta", "--shapes-file", shapes, "--runs", "2", "--end", "1700003640",
                    "--target", "pr=http://pr", "--target", "loki=http://loki", "--proxy-metrics", "--out", out, *extra]
            FakeSampler.instances = []
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(perf_matrix, "request_full", request_full), \
                    mock.patch.object(perf_matrix.reuse, "scrape", lambda base, timeout=10: dict(state) if base == "http://pr" else None), \
                    mock.patch.object(perf_matrix, "CPUSampler", FakeSampler), \
                    mock.patch.object(perf_matrix.subprocess, "check_output", return_value="0\n"), \
                    contextlib.redirect_stdout(io.StringIO()):
                perf_matrix.main()
            with open(out) as handle:
                return json.load(handle), asked

    def test_the_end_is_settled_and_on_the_step(self):
        self.assertEqual(perf_matrix.exact_repeat_end(1700003640, {}, 60), 1700003640 - 660)
        self.assertEqual(perf_matrix.exact_repeat_end(1700003630, {}, 300) % 300, 0)
        self.assertLess(1700003630 - perf_matrix.exact_repeat_end(1700003630, {}, 300), 660 + 300)
        self.assertEqual(perf_matrix.exact_repeat_end(1700003640, {"end_offset": 600}, 60), 1700003640 - 600 - 660)
        self.assertEqual(perf_matrix.exact_repeat_end(1700003640, {}, 0), 1700003640 - 660)
        # Older than the proxy's -query-range-freshness (10 minutes).
        self.assertGreater(perf_matrix.EXACT_REPEAT_AGE, 600)

    def test_each_proxy_is_asked_twice_in_a_row_and_the_second_ask_is_kept(self):
        # A request answered again at the same URL starts no VictoriaLogs call; the first ask of a URL starts two.
        raw, asked = self.run_matrix(lambda url, n: 2 if n == 1 else 0, ["--exact-repeat"])
        self.assertEqual(raw["exact_repeat_age_s"], perf_matrix.EXACT_REPEAT_AGE)
        (e,) = raw["exact_repeats"]
        self.assertEqual((e["target"], e["shape"], e["range"], e["end"]), ("pr", "labels", "1h", 1700003640 - 660))
        self.assertNotIn("problem", e)
        self.assertEqual(e["fill"]["vl_calls_header"], 2)
        self.assertEqual(e["repeat"]["vl_calls_header"], 0)
        # The request's own calls, not the counters': the proxy's background call moved them by 1 around the repeat.
        self.assertEqual(e["repeat"]["reuse"], {"background_vl_calls": 1.0})
        self.assertEqual(e["fill"]["reuse"], {"vl_calls": 2.0, "vl_bytes": 200.0, "background_vl_calls": 1.0})
        # Loki is not asked; the two asks are adjacent and at the same URL.
        urls = [u for u, _ in asked if "660" not in u]
        exact = [u for u, _ in asked if f"end={1700003640 - 660}000000000" in u]
        self.assertEqual(len(exact), 2)
        self.assertEqual(exact[0], exact[1])
        self.assertFalse(any(u.startswith("http://loki") for u in exact))
        self.assertTrue(urls)
        i = [u for u, _ in asked].index(exact[0])
        self.assertEqual(asked[i + 1][0], exact[0])
        # The runs' rows carry the header's count too, and their reuse is the request's own.
        pr = [r for r in raw["rows"] if r["target"] == "pr"]
        self.assertEqual([r["vl_calls_header"] for r in pr], [2, 2])
        self.assertEqual(pr[0]["reuse"]["vl_calls"], 2.0)

    def test_without_the_flag_nothing_is_measured_apart(self):
        raw, asked = self.run_matrix(lambda url, n: 0, [])
        self.assertEqual(raw["exact_repeats"], [])
        self.assertIsNone(raw["exact_repeat_age_s"])
        self.assertEqual(len(asked), 4)  # two runs, two targets

    def test_a_proxy_without_the_header_is_counted_by_the_counters(self):
        raw, _ = self.run_matrix(lambda url, n: None, ["--exact-repeat"], background=2)
        (e,) = raw["exact_repeats"]
        self.assertIsNone(e["repeat"]["vl_calls_header"])
        self.assertEqual(e["repeat"]["reuse"], {"vl_calls": 2.0})  # the counters' delta: its background call is counted
        self.assertTrue(all("vl_calls_header" not in r for r in raw["rows"]))

    def test_a_pair_that_answered_differently_carries_a_problem_and_no_cost(self):
        raw, _ = self.run_matrix(lambda url, n: 0, ["--exact-repeat"])
        self.assertNotIn("problem", raw["exact_repeats"][0])
        answers = iter(f"answer {i}" for i in range(100))
        with mock.patch.object(perf_matrix, "signature", side_effect=lambda *a: next(answers)):
            raw, _ = self.run_matrix(lambda url, n: 0, ["--exact-repeat"])
        (e,) = raw["exact_repeats"]
        self.assertEqual(e["problem"], "the repeat answered differently")

    def test_the_summary_keeps_both_repeats_apart(self):
        raw, _ = self.run_matrix(lambda url, n: 2 if n == 1 else 0, ["--exact-repeat"])
        raw["targets"] = ["pr", "loki"]
        with tempfile.TemporaryDirectory() as d:
            path, saved = os.path.join(d, "raw.json"), os.path.join(d, "summary.json")
            with open(path, "w") as f:
                json.dump(raw, f)
            args = argparse.Namespace(raw=path, baseline="pr", candidate="pr", reference="loki", label="t", save=saved,
                                      tolerance=0.01, cold=False, noise=0.25, min_delta=0.05, refresh="")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                report.summarize(args)
            with open(saved) as f:
                summary = json.load(f)
        slot = summary["rows"][0]["reuse"]["pr"]
        self.assertEqual(slot["cold"]["vl_calls"], 2.0)
        self.assertEqual(slot["exact_repeat"], {"background_vl_calls": 1.0})
        self.assertEqual(slot["exact_fill"]["vl_calls"], 2.0)
        # A problem is carried, never a clean repeat.
        raw["exact_repeats"][0]["problem"] = "the repeat answered differently"
        with tempfile.TemporaryDirectory() as d:
            path, saved = os.path.join(d, "raw.json"), os.path.join(d, "summary.json")
            with open(path, "w") as f:
                json.dump(raw, f)
            args = argparse.Namespace(raw=path, baseline="pr", candidate="pr", reference="loki", label="t", save=saved,
                                      tolerance=0.01, cold=False, noise=0.25, min_delta=0.05, refresh="")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                report.summarize(args)
            with open(saved) as f:
                slot = json.load(f)["rows"][0]["reuse"]["pr"]
        self.assertNotIn("exact_repeat", slot)
        self.assertEqual(slot["exact_repeat_problem"], "the repeat answered differently")


class ProfileRecordTest(unittest.TestCase):
    def test_the_profile_and_the_unaligned_builds_go_into_the_raw_file(self):
        raw = recorded()
        written, _, _, _ = run_main(raw, ["--profile", "aligned", "--not-like-for-like", "release"])
        self.assertEqual((written["profile"], written["not_like_for_like"]), ("aligned", ["release"]))
        written, _, _, _ = run_main(raw)
        self.assertEqual((written["profile"], written["not_like_for_like"]), ("", []))


class NoSleepTest(unittest.TestCase):
    def test_runtime_answers_distinguish_empty_query_rejections_and_infrastructure(self):
        valid = perf_matrix.answer_proof
        self.assertEqual(valid(200, b'{"data":{"result":[]}}', {}), "answer")
        self.assertEqual(valid(200, b'{"data":[]}', {"path": "/loki/api/v1/labels"}), "answer")
        self.assertIsNone(valid(200, b'{}', {}))
        self.assertIsNone(valid(500, b'plugin.unavailable', {}))
        series_limit = (b"maximum number of series (500) reached for a single query; consider reducing query cardinality "
                        b"by adding more specific stream selectors, reducing the time range, or aggregating results with "
                        b"functions like sum(), count() or topk()")
        for body in (series_limit, b"only label matchers are supported", b"limit reached while evaluating the query",
                     b'strconv.Atoi: parsing "abc": invalid syntax'):
            self.assertTrue(valid(400, body, {}).startswith("rejection:"))
        self.assertIsNone(valid(500, b"too many outstanding requests", {}))
        self.assertIsNone(valid(403, b'permission denied', {}))
        self.assertNotEqual(valid(400, b'parse error: bad token', {}), valid(400, b'parse error: unexpected operator', {}))
        self.assertEqual(perf_matrix.query_lookback('sum(rate({a="b"}[1h5m] offset 30s))'), 3930)

    def test_runtime_selection_keeps_each_shapes_own_ranges(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "selection.json")
            with open(path, "w") as f:
                json.dump({"runtime_proof": {"cells": {f"unwrap-sum-without/{W8}": ["1h"],
                                                         f"unwrap-sum-without/{W9}": ["instant"]}}}, f)
            written, _, _, _ = run_main(recorded(), ["--range-plan", path])
        self.assertTrue(written["rows"])
        self.assertEqual({r["range"] for r in written["rows"] if r["shape"] == W8}, {"1h"})

    def test_no_request_is_followed_by_a_sleep(self):
        _, replay, sleeps, _ = run_main(recorded())
        self.assertGreater(len(replay.sent), 50)
        self.assertEqual(sleeps, [])

    def test_pause_is_opt_in(self):
        _, replay, sleeps, _ = run_main(recorded(), ["--pause", "0.05"])
        self.assertEqual(sleeps, [0.05] * len(replay.sent))

    def test_cpu_is_read_from_the_sampler_after_the_last_request(self):
        written, replay, _, sampler = run_main(recorded())
        measured = [r for r in written["rows"] if "skipped" not in r]
        self.assertEqual(len(sampler.windows), len(measured))
        self.assertEqual(len(sampler.drained), 1)
        # the sampler is given time to take a sample covering the last request's window
        self.assertEqual(sampler.drained[0], max(t1 for _, t1 in sampler.windows) + perf_matrix.SAMPLE_WINDOW)
        self.assertTrue(all((r["vl_cpu_max"], r["vl_cpu_avg"]) == (12.5, 6.25) for r in measured))

    def test_rows_keep_their_field_order(self):
        written, _, _, _ = run_main(recorded())
        first = recorded()["rows"][0]
        self.assertEqual(list(written["rows"][0]), [*first, "at", "response_bytes"])
        self.assertGreater(written["rows"][0]["response_bytes"], 0)
        self.assertTrue(all(isinstance(r["at"], int) for r in written["rows"] if "skipped" not in r))
        self.assertTrue(all("at" not in r for r in written["rows"] if "skipped" in r))


class DrainTest(unittest.TestCase):
    def test_returns_once_a_sample_covers_the_deadline(self):
        s = perf_matrix.CPUSampler("c")
        s.samples = [(100.0, 1.0), (205.0, 2.0)]
        self.assertTrue(s.drain(200.0, timeout=1))

    def test_gives_up_after_the_timeout(self):
        s = perf_matrix.CPUSampler("c")
        s.samples = [(100.0, 1.0)]
        self.assertFalse(s.drain(200.0, timeout=0.2))
        self.assertFalse(perf_matrix.CPUSampler("c").drain(1.0, timeout=0.2))

    def test_between_still_uses_a_window_around_the_request(self):
        s = perf_matrix.CPUSampler("c")
        s.samples = [(9.0, 1.0), (10.5, 5.0), (12.4, 7.0), (14.0, 99.0)]
        self.assertEqual(s.between(10.0, 11.0), (7.0, 13.0 / 3))
        self.assertEqual(s.between(50.0, 51.0), (None, None))


class LokiTimeoutTest(unittest.TestCase):
    def test_is_timeout_needs_status_zero_and_the_timeout_text(self):
        self.assertTrue(perf_matrix.is_timeout(0, b"timed out"))
        self.assertTrue(perf_matrix.is_timeout(0, b"<urlopen error timed out>"))
        self.assertFalse(perf_matrix.is_timeout(0, b"[Errno 61] Connection refused"))
        self.assertFalse(perf_matrix.is_timeout(504, b"timed out"))

    def test_loki_is_asked_once_per_timed_out_shape_and_range(self):
        written, replay, _, _ = run_main(recorded())
        loki = [s for s in replay.sent if s[0] == "loki" and s[1] == W8]
        self.assertEqual(sorted(loki), [("loki", W8, "1h", r) for r in range(7)]
                         + [("loki", W8, "24h", 0), ("loki", W8, "6h", 0)])
        # the proxies still run every run of every range
        for target, runs in (("release", 7 + 7 + 3), ("main", 21)):  # the weekly run caps the baseline's 24h runs at 3
            self.assertEqual(len([s for s in replay.sent if s[0] == target and s[1] == W8]), runs)
        self.assertEqual([t["range"] for t in written["timeouts"]], ["6h", "24h"])
        self.assertEqual({t["run"] for t in written["timeouts"]}, {0})
        self.assertEqual(written["timeouts"][0]["target"], "loki")

    def test_skipped_rows_repeat_the_timeout_and_are_marked(self):
        written, _, _, _ = run_main(recorded())
        rows = [r for r in written["rows"] if r["target"] == "loki" and r["shape"] == W8 and r["range"] == "6h"]
        self.assertEqual([r["run"] for r in rows], list(range(7)))
        self.assertEqual({(r["status"], r["seconds"], r["signature"]) for r in rows}, {(0, 180.002, "ERR timed out")})
        self.assertNotIn("skipped", rows[0])
        self.assertTrue(all(r["skipped"] == "loki_timeout" and r["vl_cpu_max"] is None for r in rows[1:]))
        self.assertEqual([r["end"] for r in rows], [r["end"] for r in recorded()["rows"]
                                                    if r["target"] == "loki" and r["shape"] == W8 and r["range"] == "6h"])

    def test_a_proxy_that_times_out_is_still_measured_every_run(self):
        raw = recorded()
        for r in raw["rows"]:
            if r["target"] == "main" and r["shape"] == W8 and r["range"] == "6h":
                r.update(status=0, signature="ERR timed out", seconds=90.0)
        written, replay, _, _ = run_main(raw)
        self.assertEqual(len([s for s in replay.sent if s[0] == "main" and s[2] == "6h"]), 7)
        self.assertEqual(written["timeouts"][0]["target"], "loki")
        self.assertEqual(len(written["timeouts"]), 2)

    def test_a_timeout_in_a_later_run_skips_only_the_runs_after_it(self):
        raw = recorded()
        for r in raw["rows"]:
            if r["target"] == "loki" and r["shape"] == W8 and r["range"] == "1h" and r["run"] >= 3:
                r.update(status=0, signature="ERR timed out", seconds=180.0)
        written, replay, _, _ = run_main(raw)
        sent = sorted(s[3] for s in replay.sent if s[0] == "loki" and s[1] == W8 and s[2] == "1h")
        self.assertEqual(sent, [0, 1, 2, 3])
        skipped = [r["run"] for r in written["rows"] if r.get("skipped") and r["range"] == "1h"]
        self.assertEqual(skipped, [4, 5, 6])

    def test_a_failure_that_is_not_a_timeout_is_asked_again(self):
        raw = recorded()
        for r in raw["rows"]:
            if r["target"] == "loki" and r["shape"] == W8 and r["range"] == "6h":
                r.update(status=0, signature="ERR [Errno 61] Connection refused")
        # a refused connection is not a timeout; the loop re-checks VictoriaLogs and goes on
        written, replay, _, _ = run_main(raw)
        self.assertEqual(len([s for s in replay.sent if s[0] == "loki" and s[2] == "6h"]), 7)
        self.assertEqual([t["range"] for t in written["timeouts"]], ["24h"])

    def test_known_timeouts_of_an_earlier_pass_are_not_asked_again(self):
        first, _, _, _ = run_main(recorded())
        raw = recorded()
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "raw-first.json")
            with open(path, "w") as f:
                json.dump(first, f)
            written, replay, sleeps, _ = run_main(raw, ["--known-timeouts", path])
        asked = [s for s in replay.sent if s[0] == "loki" and s[2] in ("6h", "24h")]
        self.assertEqual(asked, [])
        self.assertEqual(written["timeouts"], [])
        rows = [r for r in written["rows"] if r["target"] == "loki" and r["range"] == "24h"]
        self.assertEqual(len(rows), 7)
        self.assertTrue(all(r["skipped"] == "loki_timeout" and r["status"] == 0 for r in rows))
        self.assertEqual(sleeps, [])

    def test_no_known_timeouts_file_means_nothing_is_skipped(self):
        self.assertEqual(perf_matrix.read_known_timeouts(""), {})
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "old-raw.json")
            with open(path, "w") as f:
                json.dump({"rows": []}, f)  # a raw file from before the field existed
            self.assertEqual(perf_matrix.read_known_timeouts(path), {})


class ReplayedVerdictsTest(unittest.TestCase):
    """The summary of the shortened run says what the summary of the full recorded run says."""

    def test_summary_of_the_replayed_run_matches_the_recorded_one(self):
        full = summarize(recorded())
        written, _, _, _ = run_main(recorded())
        short = summarize(written)
        self.assertEqual(len(full["rows"]), len(short["rows"]))
        for key in ("verdicts", "result_differs_from_reference", "result_differs_preexisting", "valid", "stale_answers"):
            self.assertEqual(full[key], short[key], key)
        for a, b in zip(full["rows"], short["rows"]):
            self.assertEqual((a["shape"], a["range"]), (b["shape"], b["range"]))
            for key in a:
                if key in ("vl_cpu_max", "p50", "p95"):
                    continue  # CPU is re-sampled; Loki's timeout seconds are the first one's, not the median of 7
                self.assertEqual(a[key], b[key], (a["shape"], a["range"], key))
            for t in ("release", "main"):
                self.assertEqual(a["p50"][t], b["p50"][t])
                self.assertEqual(a["p95"][t], b["p95"][t])
            self.assertAlmostEqual(a["p50"]["loki"], b["p50"]["loki"], delta=0.2)
            self.assertAlmostEqual(a["p95"]["loki"], b["p95"]["loki"], delta=0.2)

    def test_the_timeouts_stay_loki_failures_in_the_over_2x_judgement(self):
        import over2x
        written, _, _, _ = run_main(recorded())
        rows = {(r["shape"], r["range"]): r for r in summarize(written)["rows"]}
        row = rows[(W8, "24h")]
        self.assertEqual(row["status"]["loki"], "0")
        self.assertEqual(over2x.judge(row["p50"]["main"], row["p50"]["loki"], row["status"]["main"],
                                      row["status"]["loki"], False), "loki_failed")


if __name__ == "__main__":
    unittest.main()
