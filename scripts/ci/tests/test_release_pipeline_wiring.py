import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO / ".github" / "workflows"


def text(name):
    return (WORKFLOWS / name).read_text(encoding="utf-8")


class AutoReleaseWiring(unittest.TestCase):
    def test_every_push_to_main_runs_the_release_check(self):
        # A docs-only or CHANGELOG-only push must still release earlier shipped
        # changes whose own run was cancelled.
        self.assertNotIn("paths-ignore", text("auto-release.yaml"))

    def test_the_trigger_commit_message_no_longer_decides_anything(self):
        auto = text("auto-release.yaml")
        self.assertNotIn("HEAD_COMMIT_MSG", auto)
        self.assertNotIn("sync release metadata for v[0-9]", auto)

    def test_the_decision_notes_and_metadata_come_from_the_scripts_with_tests(self):
        auto = text("auto-release.yaml")
        self.assertIn("scripts/ci/release_plan.py plan", auto)
        self.assertEqual(auto.count("scripts/ci/changelog_release.py notes"), 2)  # validate and publish
        self.assertIn("scripts/ci/release_metadata.py prepare", auto)
        self.assertIn("scripts/ci/release_metadata.py refresh", auto)
        self.assertNotIn("materialize_unreleased", auto)
        self.assertNotIn("sync_release_metadata.sh", auto)

    def test_the_release_is_still_validated_tagged_and_published_from_one_commit(self):
        auto = text("auto-release.yaml")
        self.assertEqual(auto.count("ref: ${{ needs.prepare-release.outputs.release_sha }}"), 2)
        self.assertIn("git checkout --detach origin/main", auto)
        for step in ("cosign sign", "attest-build-provenance", "Create release tag", "Scan the pushed image with Trivy"):
            self.assertIn(step, auto)

    def test_the_metadata_branch_is_refreshed_on_every_push(self):
        auto = text("auto-release.yaml")
        job = auto[auto.index("  refresh-metadata-pr:") :]
        self.assertIn("release_metadata.py refresh", job)
        self.assertNotIn("needs:", job)
        self.assertIn("contents: read", job)

    def test_the_manual_release_uses_the_same_metadata_script(self):
        release = text("release.yaml")
        self.assertIn("scripts/ci/release_metadata.py prepare", release)
        self.assertNotIn("sync_release_metadata.sh", release)

    def test_scripts_the_workflows_call_exist(self):
        for name in ("auto-release.yaml", "release.yaml"):
            for script in set(re.findall(r"scripts/ci/[\w.]+", text(name))):
                self.assertTrue((REPO / script).exists(), f"{name} calls {script}")


class BotBranchWiring(unittest.TestCase):
    def test_the_daily_report_branch_is_never_force_pushed(self):
        # Force-pushing a rebuilt branch over its old tip counts every workflow
        # file main changed in between as the bot's change; a token without the
        # workflow scope is refused. The remote branch is reset to main through
        # the API first and the push is a fast-forward of the bot's files.
        perf = text("perf-daily.yaml")
        step = perf[perf.index("name: Open or update the daily report PR") :]
        step = step[: step.index("name: Arm auto-merge")]
        self.assertNotIn("--force", step.replace("-F force=true", ""))
        self.assertNotRegex(step, r"push [^\n]*(--force|-f )")
        self.assertIn("-X PATCH", step)
        self.assertIn("-X DELETE", step)  # a branch that cannot be moved is created again
        self.assertLess(step.index("set_branch\n"), step.index("push_branch\n"))
        self.assertIn("BASE=\"$(git rev-parse HEAD)\"", step)

    def test_release_workflows_never_force_push(self):
        for name in ("auto-release.yaml", "release.yaml", "perf-daily.yaml"):
            self.assertNotRegex(text(name), r"git[^\n]* push [^\n]*--force")

    def test_the_new_tests_run_in_ci(self):
        ci = text("ci.yaml")
        for module in ("test_changelog_release", "test_release_plan", "test_release_metadata",
                       "test_release_replay", "test_release_pipeline_wiring"):
            self.assertIn(f"scripts.ci.tests.{module}", ci)


if __name__ == "__main__":
    unittest.main()
