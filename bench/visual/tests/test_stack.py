"""The fixed visual fixture must give Loki and VictoriaLogs identical plain lines."""
import json
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import stack  # noqa: E402


class StackFixtureTest(unittest.TestCase):
    def test_unknown_level_fixture_is_mirrored_without_message_parsing(self):
        sent = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

        def send(req, timeout):
            sent.append((req.full_url, req.data, timeout))
            return Response()

        backend = type("Backend", (), {"loki": "http://loki", "vl": "http://vl"})()
        with patch.object(stack.urllib.request, "urlopen", side_effect=send):
            stack.seed_unknown_level_fixture(backend, 7200)

        self.assertEqual([url for url, _, _ in sent],
                         ["http://loki/loki/api/v1/push",
                          "http://vl/insert/loki/api/v1/push?disable_message_parsing=1"])
        self.assertEqual(sent[0][1], sent[1][1])
        stream = json.loads(sent[0][1])["streams"][0]
        self.assertNotIn("level", stream["stream"])
        self.assertEqual(len(stream["values"]), 119)
        self.assertTrue(all("level" not in line and "error" not in line and "warn" not in line
                            for _, line in stream["values"]))
        self.assertLess(int(stream["values"][-1][0]), 7200 * 1_000_000_000)

    def test_keep_metadata_fixture_uses_equivalent_loki_and_vl_fields(self):
        sent = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

        def send(req, timeout):
            sent.append((req.full_url, req.data, req.headers))
            return Response()

        backend = type("Backend", (), {"loki": "http://loki", "vl": "http://vl"})()
        with patch.object(stack.urllib.request, "urlopen", side_effect=send):
            stack.seed_keep_metadata_fixture(backend, 7200)

        self.assertEqual(len(sent), 2)
        self.assertEqual(sent[0][0], "http://loki/loki/api/v1/push")
        self.assertEqual(sent[1][0], "http://vl/insert/jsonline?_stream_fields=app")
        entry = json.loads(sent[0][1])["streams"][0]
        row = json.loads(sent[1][1])
        self.assertEqual(entry["stream"]["app"], row["app"])
        self.assertEqual(entry["values"][0][1], row["_msg"])
        self.assertEqual(entry["values"][0][2]["deployment_environment"],
                         row["deployment.environment"])
        self.assertEqual(entry["values"][0][0], str(6900 * 1_000_000_000 + 500_000_000))


if __name__ == "__main__":
    unittest.main()
