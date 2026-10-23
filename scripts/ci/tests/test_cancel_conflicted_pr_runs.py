import unittest

from scripts.ci import cancel_conflicted_pr_runs as ccr


class ConflictingTests(unittest.TestCase):
    def test_only_conflicting_pull_requests_are_selected(self):
        prs = [
            {"number": 1, "mergeable": "CONFLICTING"},
            {"number": 2, "mergeable": "MERGEABLE"},
            {"number": 3, "mergeable": "UNKNOWN"},
        ]
        self.assertEqual([pr["number"] for pr in ccr.conflicting(prs)], [1])


class RunsToCancelTests(unittest.TestCase):
    HEAD = "a" * 40

    def run_(self, **overrides):
        run = {"id": 1, "event": "pull_request", "head_sha": self.HEAD, "status": "in_progress"}
        run.update(overrides)
        return run

    def test_active_pull_request_runs_of_the_head_are_cancelled(self):
        for status in ("queued", "in_progress", "waiting", "pending", "requested"):
            with self.subTest(status=status):
                self.assertEqual(len(ccr.runs_to_cancel([self.run_(status=status)], self.HEAD)), 1)

    def test_finished_runs_are_left_alone(self):
        self.assertEqual(ccr.runs_to_cancel([self.run_(status="completed")], self.HEAD), [])

    def test_other_events_are_left_alone(self):
        for event in ("pull_request_target", "push", "schedule", "workflow_dispatch"):
            with self.subTest(event=event):
                self.assertEqual(ccr.runs_to_cancel([self.run_(event=event)], self.HEAD), [])

    def test_runs_of_another_commit_are_left_alone(self):
        self.assertEqual(ccr.runs_to_cancel([self.run_(head_sha="b" * 40)], self.HEAD), [])


if __name__ == "__main__":
    unittest.main()
