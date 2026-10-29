"""ci_run.py helpers: the Loki metric wait and the first-pass difference list."""
import http.server
import json
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import ci_run  # noqa: E402


class FakeLoki(http.server.BaseHTTPRequestHandler):
    answers = []

    def do_GET(self):
        body = json.dumps({"data": {"result": self.answers.pop(0) if self.answers else []}}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


class CiRunTest(unittest.TestCase):
    def test_wait_loki_metric_waits_through_empty_answers(self):
        FakeLoki.answers = [[], [{"values": [[1, "0"]]}], [{"values": [[1, "7"]]}]]
        srv = http.server.HTTPServer(("127.0.0.1", 0), FakeLoki)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        orig = ci_run.time.sleep
        ci_run.time.sleep = lambda s: None  # the polling pause, not the logic, is slow
        try:
            self.assertIsNotNone(ci_run.wait_loki_metric(srv.server_address[1], 1000, deadline_s=30))
            self.assertEqual(FakeLoki.answers, [])  # it kept asking until non-zero data appeared
            FakeLoki.answers = []
            self.assertIsNone(ci_run.wait_loki_metric(srv.server_address[1], 1000, deadline_s=0))
        finally:
            ci_run.time.sleep = orig
            srv.shutdown()

    def test_differing_lists_the_captures_with_a_difference(self):
        with tempfile.TemporaryDirectory() as out:
            self.assertEqual(ci_run.differing(out), set())
            with open(os.path.join(out, "compare.json"), "w", encoding="utf-8") as f:
                json.dump([{"page": "a", "range": "1h", "main_pr_diffs": ["x"]}, {"page": "b", "range": "1h", "main_pr_diffs": []}], f)
            self.assertEqual(ci_run.differing(out), {"a 1h"})

    def test_a_delayed_request_that_matches_on_recapture_is_not_a_data_flip(self):
        first = [
            {"page": "late", "range": "1h", "main_pr_diffs": ["query x: request only on one side (left)"]},
            {"page": "value", "range": "1h", "main_pr_diffs": ["query x: value differs"]},
            {"page": "mixed", "range": "1h", "main_pr_diffs": ["query x: request only on one side (left)", "query y: value differs"]},
            {"page": "persistent", "range": "1h", "main_pr_diffs": ["query x: value differs"]},
        ]
        second = [{**r, "main_pr_diffs": r["main_pr_diffs"] if r["page"] == "persistent" else []} for r in first]
        self.assertEqual(ci_run.recapture_flips(first, second),
                         (["mixed 1h", "value 1h"], ["late 1h"]))

    def test_the_recapture_runs_before_the_live_tail_pass_and_one_compare_sees_both(self):
        # The live generator writes into the streams the static windows read, and Loki's index/stats is an estimate from
        # chunk metadata that moves while it writes: a recapture after it compares a different Loki answer.
        calls = []
        with tempfile.TemporaryDirectory() as out:
            plan_path = os.path.join(out, "in-plan.json")
            with open(plan_path, "w", encoding="utf-8") as f:
                json.dump({"run": True, "captures": 2, "entries": {"p": {"kind": "explore", "ranges": ["1h"]},
                                                                   "t": {"kind": "tail", "ranges": ["live"]}}}, f)
            with open(os.path.join(out, "state.json"), "w", encoding="utf-8") as f:
                json.dump({"end": 1, "ports": {"grafana": 1, "loki": 2}}, f)

            def step(name, result=None):
                def fn(*args, **kw):
                    calls.append(name)
                    return result
                return fn
            patches = {"git": lambda *a: "sha", "capture": step("capture"), "recapture": step("recapture", True),
                       "capture_live": step("capture_live", True),
                       "run": lambda cmd, **kw: calls.append("compare" if cmd[1].endswith("compare.py") else
                                                             "montage" if cmd[1].endswith("montage.py") else "stack")}
            saved = {k: getattr(ci_run, k) for k in patches}
            argv = sys.argv
            try:
                for k, fn in patches.items():
                    setattr(ci_run, k, fn)
                sys.argv = ["ci_run.py", "--base", "main", "--plan", plan_path, "--out", out]
                self.assertEqual(ci_run.main(), 0)
            finally:
                sys.argv = argv
                for k, fn in saved.items():
                    setattr(ci_run, k, fn)
        steps = [c for c in calls if c != "stack"]
        self.assertEqual(steps, ["capture", "compare", "recapture", "capture_live", "compare", "montage"])


if __name__ == "__main__":
    unittest.main()
