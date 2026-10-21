import json
import os
import shutil
import stat
import subprocess
import tempfile
import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOW = REPO / ".github" / "workflows" / "release-pr-validation.yaml"

# A stand-in for the gh CLI. It serves recorded GitHub API answers from FAKE_DIR:
# pr.json, files.json, commits.json and contents/<path>. A --jq filter is applied
# with jq, as the real gh does.
FAKE_GH = r"""#!/usr/bin/env python3
import os, subprocess, sys
d = os.environ["FAKE_DIR"]
args = sys.argv[1:]
route = next(a for a in args if a.startswith("repos/"))
if route.endswith("/files"):
    path = "files.json"
elif route.endswith("/commits"):
    path = "commits.json"
elif "/contents/" in route:
    path = "contents/" + route.split("/contents/", 1)[1].split("?", 1)[0]
else:
    path = "pr.json"
full = os.path.join(d, path)
if not os.path.exists(full):
    sys.stderr.write("gh: Not Found (HTTP 404)\n")
    sys.exit(1)
if "--jq" in args:
    sys.exit(subprocess.run(["jq", "-r", args[args.index("--jq") + 1], full]).returncode)
sys.stdout.write(open(full).read())
"""

CHART = 'apiVersion: v2\nversion: 1.2.3\nappVersion: "1.2.3"\n'
CHANGELOG = "# Changelog\n\n## [Unreleased]\n\n## [1.2.3] - 2026-10-21\n\n- thing\n"
OBSERVABILITY = 'example: {"service.version": "1.2.3"}\n'
META_FILES = ["CHANGELOG.md", "README.md", "docs/observability.md", "charts/loki-vl-proxy/Chart.yaml"]
PERF_FILES = [
    "bench/ab/results/daily-2026-10-21.json",
    "bench/ab/history/trend.md",
    "conformance/registry/generated/perf-evidence.json",
    "conformance/reports/performance.md",
    "conformance/reports/gaps.md",
]


def check_script():
    """The run: text of the scope-check step, read without a YAML parser."""
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index("      - name: Check the pull request scope")
    run = next(i for i in range(start, len(lines)) if lines[i] == "        run: |")
    body = []
    for line in lines[run + 1:]:
        if line.strip() and not line.startswith(" " * 10):
            break
        body.append(line[10:])
    return "\n".join(body)


def pr(head_ref, author="reliablyobserve-bot", head_repo="acme/repo", base_ref="main"):
    return {
        "head": {"ref": head_ref, "sha": "headsha", "repo": {"full_name": head_repo} if head_repo else None},
        "base": {"ref": base_ref},
        "user": {"login": author},
    }


def files(*names, renamed=None):
    out = [{"filename": n} for n in names]
    if renamed:
        out.append({"filename": renamed[0], "previous_filename": renamed[1]})
    return out


def commits(*verified):
    return [{"sha": f"c{i}", "commit": {"verification": {"verified": v}}} for i, v in enumerate(verified)]


@unittest.skipUnless(shutil.which("jq") and shutil.which("bash"), "needs jq and bash")
class ScopeCheckTests(unittest.TestCase):
    def run_check(self, pr_json, files_json=None, commits_json=None, contents=None, number="7"):
        script = check_script()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "bin").mkdir()
            gh = tmp / "bin" / "gh"
            gh.write_text(FAKE_GH)
            gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
            (tmp / "pr.json").write_text(json.dumps(pr_json))
            if files_json is not None:
                (tmp / "files.json").write_text(json.dumps(files_json))
            if commits_json is not None:
                (tmp / "commits.json").write_text(json.dumps(commits_json))
            for path, text in (contents or {}).items():
                target = tmp / "contents" / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text)
            env = {
                **os.environ,
                "PATH": f"{tmp / 'bin'}:{os.environ['PATH']}",
                "FAKE_DIR": str(tmp),
                "REPO": "acme/repo",
                "PR_NUMBER": number,
                "ALLOWED_AUTHORS": "reliablyobserve-bot szibis",
                "GH_TOKEN": "unused",
            }
            return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)

    @staticmethod
    def metadata_contents(**overrides):
        base = {
            "charts/loki-vl-proxy/Chart.yaml": CHART,
            "CHANGELOG.md": CHANGELOG,
            "docs/observability.md": OBSERVABILITY,
        }
        base.update(overrides)
        return base

    def metadata(self, names=META_FILES, signed=(True,), author="reliablyobserve-bot", contents=None, **kw):
        return self.run_check(
            pr("release/metadata-v1.2.3", author=author),
            files(*names, **kw),
            commits(*signed),
            contents if contents is not None else self.metadata_contents(),
        )

    def test_good_metadata_pull_request_passes(self):
        proc = self.metadata()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("in scope", proc.stdout)

    def test_owner_may_open_the_metadata_pull_request(self):
        self.assertEqual(self.metadata(author="szibis").returncode, 0)

    def test_metadata_pull_request_touching_internal_fails(self):
        proc = self.metadata(names=META_FILES + ["internal/proxy/handler.go"])
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("internal/proxy/handler.go", proc.stdout)

    def test_metadata_rename_out_of_another_directory_fails(self):
        proc = self.metadata(renamed=("CHANGELOG.md", "internal/secret.go"))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("internal/secret.go", proc.stdout)

    def test_unsigned_metadata_commit_fails(self):
        proc = self.metadata(signed=(True, False))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("verified signature", proc.stdout)

    def test_wrong_chart_version_fails(self):
        proc = self.metadata(contents=self.metadata_contents(**{
            "charts/loki-vl-proxy/Chart.yaml": 'version: 1.2.4\nappVersion: "1.2.3"\n'}))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("Chart.yaml version", proc.stdout)

    def test_missing_changelog_section_fails(self):
        proc = self.metadata(contents=self.metadata_contents(**{"CHANGELOG.md": "## [Unreleased]\n## [1.2.30] - x\n"}))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("CHANGELOG.md has no", proc.stdout)

    def test_dots_in_the_version_are_literal(self):
        proc = self.metadata(contents=self.metadata_contents(**{
            "charts/loki-vl-proxy/Chart.yaml": 'version: 1x2x3\nappVersion: "1x2x3"\n'}))
        self.assertNotEqual(proc.returncode, 0)

    def test_wrong_observability_version_fails(self):
        proc = self.metadata(contents=self.metadata_contents(**{
            "docs/observability.md": '{"service.version": "9.9.9"}\n'}))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("service.version", proc.stdout)

    def test_observability_without_a_service_version_example_passes(self):
        proc = self.metadata(contents=self.metadata_contents(**{
            "docs/observability.md": "# Observability\n\nNo version example here.\n"}))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_observability_example_with_a_stale_version_fails(self):
        proc = self.metadata(contents=self.metadata_contents(**{
            "docs/observability.md": 'example: {"service.version":"1.2.2"}\n'}))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("'1.2.2', not 1.2.3", proc.stdout)

    def test_observability_key_without_a_string_value_fails(self):
        proc = self.metadata(contents=self.metadata_contents(**{
            "docs/observability.md": 'the "service.version" attribute\n'}))
        self.assertNotEqual(proc.returncode, 0)

    def test_metadata_pull_request_that_changes_only_changelog_and_chart_passes(self):
        # The shape of the release bot's real pull request: no README or
        # observability change, one verified commit, observability has no example.
        proc = self.metadata(
            names=["CHANGELOG.md", "charts/loki-vl-proxy/Chart.yaml"],
            contents=self.metadata_contents(**{"docs/observability.md": "# Observability\n"}),
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_other_author_fails(self):
        proc = self.metadata(author="someone-else")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("someone-else", proc.stdout)

    def test_branch_with_a_bad_version_fails(self):
        proc = self.run_check(pr("release/metadata-v1.2"), files(*META_FILES), commits(True), self.metadata_contents())
        self.assertNotEqual(proc.returncode, 0)

    def test_good_daily_pull_request_passes_even_with_unsigned_commits(self):
        proc = self.run_check(pr("bench/daily-perf"), files(*PERF_FILES), commits(False))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_daily_pull_request_touching_cmd_fails(self):
        proc = self.run_check(pr("bench/daily-perf"), files(*PERF_FILES, "cmd/proxy/main.go"), commits(False))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("cmd/proxy/main.go", proc.stdout)

    def test_daily_pull_request_from_another_author_fails(self):
        proc = self.run_check(pr("bench/daily-perf", author="mallory"), files(*PERF_FILES), commits(False))
        self.assertNotEqual(proc.returncode, 0)

    def test_unrelated_pull_request_passes_without_reading_anything_else(self):
        # No files.json or commits.json exist: any further API read would fail.
        proc = self.run_check(pr("feature/x", author="anyone"))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("nothing to check", proc.stdout)

    def test_fork_pull_request_with_a_bot_branch_name_passes_at_once(self):
        proc = self.run_check(pr("release/metadata-v1.2.3", head_repo="fork/repo"))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_deleted_fork_passes_at_once(self):
        proc = self.run_check(pr("bench/daily-perf", head_repo=None))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_pull_request_to_another_base_passes_at_once(self):
        proc = self.run_check(pr("bench/daily-perf", base_ref="dev"))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_non_numeric_pull_request_number_is_refused(self):
        proc = self.run_check(pr("feature/x"), number="7; rm -rf /")
        self.assertNotEqual(proc.returncode, 0)


class WorkflowShapeTests(unittest.TestCase):
    """The guard runs from main with a token that can read the repository: keep
    it from ever checking out, running or exposing pull request content."""

    def setUp(self):
        self.text = WORKFLOW.read_text()

    def test_triggers(self):
        self.assertRegex(self.text, r"(?m)^  pull_request_target:\n    branches: \[main\]\n")
        self.assertRegex(self.text, r"(?m)^  workflow_dispatch:")

    def test_permissions_are_minimal(self):
        self.assertRegex(self.text, r"(?m)^permissions: \{\}$")
        self.assertRegex(self.text, r"(?m)^    permissions:\n      contents: read\n      pull-requests: read\n")

    def test_nothing_is_checked_out_or_run_from_the_pull_request(self):
        self.assertIsNone(re.search(r"(?m)^\s*(-\s+)?uses:", self.text), "no action, so no checkout")
        self.assertNotIn("github.event.pull_request.head", self.text)
        self.assertNotIn("github.head_ref", self.text)

    def test_no_secrets_and_no_expression_in_the_script(self):
        self.assertNotIn("secrets.", self.text)
        self.assertNotIn("${{", check_script())


if __name__ == "__main__":
    unittest.main()
