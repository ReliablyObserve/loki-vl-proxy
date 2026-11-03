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

    def test_the_seed_fingerprint_counts_both_backends_and_tolerates_a_dead_one(self):
        class Both(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = (b'{"c":"42"}\n' if "logsql" in self.path else
                        json.dumps({"data": {"result": [{"value": [1, "17"]}]}}).encode())
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), Both)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            port = srv.server_address[1]
            state = {"end": 7200, "hours": 7, "loki_hours": 1.5}
            self.assertEqual(ci_run.seed_fingerprint(state, port, port), {"end": 7200, "hours": 7, "vl_lines": 42, "loki_lines": 17})
            dead = ci_run.seed_fingerprint(state, 1, 1)
            self.assertEqual((dead["vl_lines"], dead["loki_lines"]), (None, None))
        finally:
            srv.shutdown()

    def test_the_first_pass_of_a_capture_loaded_again_is_kept_apart(self):
        from PIL import Image
        with tempfile.TemporaryDirectory() as out:
            for page in ("again", "once"):
                for name in ("main", "pr", "loki"):
                    d = os.path.join(out, "shots", page, "1h")
                    os.makedirs(d, exist_ok=True)
                    Image.new("RGB", (40, 30), (200, 10, 10) if name == "pr" else (10, 10, 200)).save(os.path.join(d, f"{name}.png"))
            with open(os.path.join(out, "compare.json"), "w", encoding="utf-8") as f:
                json.dump([{"page": "again", "range": "1h", "main_pr_diffs": ["x"]}, {"page": "once", "range": "1h", "main_pr_diffs": []}], f)
            ci_run.keep_first_pass(out, {"again": {"kind": "explore", "ranges": ["1h"]}})
            first = os.path.join(out, "first-pass")
            with open(os.path.join(first, "compare.json"), encoding="utf-8") as f:
                self.assertEqual([r["page"] for r in json.load(f)], ["again"])
            self.assertEqual(os.listdir(os.path.join(first, "montage")), ["again-1h.png"])
            with open(os.path.join(first, "pixeldiff.json"), encoding="utf-8") as f:
                self.assertEqual(list(json.load(f)), ["again-1h"])

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
