import http.server
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import perf_matrix  # noqa: E402


class _Handler(http.server.BaseHTTPRequestHandler):
    calls = []
    status = 200

    def do_POST(self):
        _Handler.calls.append((self.path, self.headers.get("X-Admin-Token")))
        self.send_response(_Handler.status)
        self.end_headers()

    def log_message(self, *args):
        pass


class FlushCacheTest(unittest.TestCase):
    def setUp(self):
        _Handler.calls, _Handler.status = [], 200
        self.server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/admin/cache/flush"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_posts_with_token(self):
        perf_matrix.flush_cache(self.url, "secret")
        self.assertEqual(_Handler.calls, [("/admin/cache/flush", "secret")])

    def test_refusal_fails_the_run(self):
        _Handler.status = 401
        with self.assertRaises(SystemExit):
            perf_matrix.flush_cache(self.url)


if __name__ == "__main__":
    unittest.main()
