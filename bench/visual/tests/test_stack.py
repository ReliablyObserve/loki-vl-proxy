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


class PluginWaitTest(unittest.TestCase):
    def test_plugin_ids_drop_versions_and_urls(self):
        self.assertEqual(stack.plugin_ids("victoriametrics-logs-datasource@0.30.0, grafana-lokiexplore-app@2.0.4@https://x/y.zip"),
                         ["victoriametrics-logs-datasource", "grafana-lokiexplore-app"])
        self.assertEqual(stack.plugin_ids(""), [])

    def test_wait_plugins_polls_until_every_plugin_answers(self):
        answers = {"a": [OSError, OSError, None], "b": [None]}
        seen = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

        def get(url, timeout):
            pid = url.split("/api/plugins/")[1].split("/")[0]
            seen.append(pid)
            outcome = answers[pid].pop(0) if len(answers[pid]) > 1 else answers[pid][0]
            if outcome is OSError:
                raise OSError("404")
            return Response()

        with patch.object(stack.urllib.request, "urlopen", side_effect=get), patch.object(stack.time, "sleep", lambda s: None):
            _, missing = stack.wait_plugins("http://g", ["a", "b"], deadline_s=30)
        self.assertEqual(missing, [])
        self.assertEqual(seen.count("a"), 3)
        self.assertEqual(seen.count("b"), 1)  # answered once, not asked again

    def test_a_plugin_that_never_appears_is_reported_not_raised(self):
        with patch.object(stack.urllib.request, "urlopen", side_effect=OSError("404")), patch.object(stack.time, "sleep", lambda s: None):
            _, missing = stack.wait_plugins("http://g", ["a"], deadline_s=0)
        self.assertEqual(missing, ["a"])


class StartProxiesTest(unittest.TestCase):
    SPECS = [("main", "/bin/main", "/tree", 33200), ("pr", "/bin/pr", "/tree", 33202)]

    def run_with(self, outcomes):
        """start_proxies with ab.start_proxies replaced: `outcomes` per call is None (started) or an error."""
        calls, procs = [], []

        def fake(specs, out, st, service, proxies, supported=None):
            calls.append(specs[0][0])
            proxy = type("P", (), {"name": specs[0][0], "stopped": False, "stop": lambda self: setattr(self, "stopped", True)})()
            proxies.append(proxy)
            outcome = outcomes.pop(0)
            if outcome:
                raise RuntimeError(outcome)

        with patch.object(stack.ab, "probe_align_support", return_value=True), patch.object(stack.ab, "start_proxies", side_effect=fake), \
                patch.object(stack.time, "sleep", lambda s: None):
            error = None
            try:
                stack.start_proxies(self.SPECS, "/out", None, procs)
            except RuntimeError as e:
                error = e
        return calls, procs, error

    def test_a_proxy_that_lost_its_port_is_started_again(self):
        calls, procs, error = self.run_with([None, "proxy 'pr' exited with 1", None])
        self.assertIsNone(error)
        self.assertEqual(calls, ["main", "pr", "pr"])
        self.assertEqual([p.name for p in procs], ["main", "pr"])  # the dead attempt is not kept
        self.assertFalse(any(p.stopped for p in procs))

    def test_a_proxy_that_never_starts_fails_the_stack_and_stays_listed_for_teardown(self):
        calls, procs, error = self.run_with([None, "exit 1", "exit 1", "exit 1"])
        self.assertIsNotNone(error)
        self.assertEqual(calls, ["main", "pr", "pr", "pr"])
        self.assertEqual([p.name for p in procs], ["main", "pr"])  # main and the last pr: `down` stops both


if __name__ == "__main__":
    unittest.main()
