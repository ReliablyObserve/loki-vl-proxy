import copy
import unittest
from pathlib import Path

from scripts.ci.grafana_resource_guard import failures


class GrafanaResourceGuardTest(unittest.TestCase):
    def setUp(self):
        self.before = {"container": "same", "started_at": "original", "restarts": 0, "running": True, "events": {"oom_kill": 2}}
        self.after = copy.deepcopy(self.before)

    def test_prior_oom_is_not_a_new_failure(self):
        self.assertEqual(failures(self.before, self.after, ""), [])

    def test_child_oom_fails_while_container_remains_healthy(self):
        self.after["events"]["oom_kill"] += 1
        self.assertEqual(len(failures(self.before, self.after, "")), 1)

    def test_killed_plugin_fails_even_when_it_automatically_recovers(self):
        logs = 'logger=plugin.loki msg="plugin process exited" error="signal: killed"'
        self.assertEqual(len(failures(self.before, self.after, logs)), 1)

    def test_replacement_or_restart_cannot_reset_the_counters(self):
        for key, value in [("container", "replacement"), ("restarts", 1), ("started_at", "manual-restart"), ("running", False)]:
            with self.subTest(key=key):
                after = {**self.after, key: value}
                self.assertEqual(len(failures(self.before, after, "")), 1)

    def test_ci_checks_after_failed_tests_and_retains_resource_evidence(self):
        workflow = (Path(__file__).resolve().parents[3] / ".github/workflows/ci.yaml").read_text()
        ui = workflow.split("\n  e2e-ui:\n", 1)[1]
        self.assertIn("grafana_resource_guard.py before", ui)
        self.assertIn("always() && steps.grafana-resources.outcome == 'success'", ui)
        self.assertIn("grafana_resource_guard.py after", ui)
        self.assertIn("${{ runner.temp }}/grafana-resources", ui)
