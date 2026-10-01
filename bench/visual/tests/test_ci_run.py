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


if __name__ == "__main__":
    unittest.main()
