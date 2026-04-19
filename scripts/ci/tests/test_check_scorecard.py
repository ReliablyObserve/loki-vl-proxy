import json
import tempfile
import unittest
from pathlib import Path

from scripts.ci import check_scorecard


class CheckScorecardTests(unittest.TestCase):
    def write_report(self, payload):
        with tempfile.NamedTemporaryFile("w", delete=False) as handle:
            handle.write(json.dumps(payload))
            return Path(handle.name)

    def test_parse_required_checks(self):
        parsed = check_scorecard.parse_required_checks(
            ["Dangerous-Workflow=10", "Token-Permissions=8"]
        )
        self.assertEqual(parsed["Dangerous-Workflow"], 10.0)
        self.assertEqual(parsed["Token-Permissions"], 8.0)

    def test_main_succeeds_when_thresholds_met(self):
        report = self.write_report(
            {
                "score": 7.2,
                "checks": [
                    {"name": "Dangerous-Workflow", "score": 10},
                    {"name": "Token-Permissions", "score": 8},
                ],
            }
        )
        rc = check_scorecard.main_for_test(
            report,
            min_overall=5.0,
            require_checks={"Dangerous-Workflow": 10.0, "Token-Permissions": 8.0},
        )
        self.assertEqual(rc, 0)

    def test_main_fails_when_check_is_below_threshold(self):
        report = self.write_report(
            {
                "score": 6.5,
                "checks": [
                    {"name": "Dangerous-Workflow", "score": 10},
                    {"name": "Token-Permissions", "score": 6},
                ],
            }
        )
        rc = check_scorecard.main_for_test(
            report,
            min_overall=5.0,
            require_checks={"Dangerous-Workflow": 10.0, "Token-Permissions": 8.0},
        )
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
