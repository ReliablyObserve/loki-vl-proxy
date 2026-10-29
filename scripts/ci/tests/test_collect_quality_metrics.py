"""collect_quality_metrics.sh: a saved test count and coverage replaces the test run, nothing else changes.

The script is run for real with `go` and `docker` replaced by stubs that answer like the tools do
(canned `go test -json` events, a canned coverage total, canned compatibility scores) and log every
call. The same snapshot must come out whether the numbers are computed or read back.
"""
import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "ci" / "collect_quality_metrics.sh"

# Two packages, five passing tests (one a subtest), a failing one, a skipped one and package-level events:
# the count is the number of pass events that carry a test name.
EVENTS = [
    {"Action": "run", "Package": "p/a", "Test": "TestOne"},
    {"Action": "pass", "Package": "p/a", "Test": "TestOne"},
    {"Action": "pass", "Package": "p/a", "Test": "TestTwo"},
    {"Action": "pass", "Package": "p/a", "Test": "TestTwo/sub"},
    {"Action": "pass", "Package": "p/b", "Test": "TestThree"},
    {"Action": "pass", "Package": "p/b", "Test": "TestFour"},
    {"Action": "fail", "Package": "p/b", "Test": "TestBroken"},
    {"Action": "skip", "Package": "p/b", "Test": "TestSkipped"},
    {"Action": "pass", "Package": "p/a"},
    {"Action": "fail", "Package": "p/b"},
]
EXPECTED_COUNT = 5
COVERAGE_TOTAL = "83.4%"

SCORE_LOG = """\
=== RUN   TestScore
PASS labels: ok
PASS labels: ok again
FAIL series: broken
Score: 3/4 (75.0%)
"""

GO_STUB = """\
#!/bin/bash
echo "go $*" >> "$STUB_LOG"
case "$*" in
  "test ./... "*"-json")
    for a in "$@"; do case "$a" in -coverprofile=*) printf 'mode: set\\n' > "${a#-coverprofile=}";; esac; done
    cat "$STUB_EVENTS"
    if [ "${STUB_GO_TEST_FAIL:-0}" = "1" ]; then
      echo "simulated go test failure" >&2
      exit 42
    fi
    ;;
  "tool cover -func="*)
    printf 'p/a/a.go:1:\\tf\\t100.0%%\\ntotal:\\t(statements)\\t%s\\n' "$STUB_COVERAGE"
    ;;
  "test -v -tags=e2e "*)
    cat "$STUB_SCORE"
    ;;
  *)
    echo "unexpected go call: $*" >&2
    exit 97
    ;;
esac
"""

DOCKER_STUB = """\
#!/bin/bash
echo "docker $*" >> "$STUB_LOG"
exit 0
"""

WAIT_STUB = """\
#!/bin/bash
exit 0
"""


def executable(path, text):
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class CollectQualityMetricsTest(unittest.TestCase):
    """Four runs of the real script (each takes a couple of seconds), shared by the tests."""

    @classmethod
    def setUpClass(cls):
        if shutil.which("jq") is None:
            raise unittest.SkipTest("jq is not installed")
        cls.dir = Path(tempfile.mkdtemp())
        cls.addClassCleanup(shutil.rmtree, cls.dir, True)
        bin_dir = cls.dir / "bin"
        bin_dir.mkdir()
        executable(bin_dir / "go", GO_STUB)
        executable(bin_dir / "docker", DOCKER_STUB)
        # The script runs from the checkout it measures: a root with the paths it touches.
        cls.root = cls.dir / "root"
        (cls.root / "test" / "e2e-compat").mkdir(parents=True)
        (cls.root / "scripts" / "ci").mkdir(parents=True)
        executable(cls.root / "scripts" / "ci" / "wait_e2e_stack.sh", WAIT_STUB)
        (cls.dir / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in EVENTS))
        (cls.dir / "score.txt").write_text(SCORE_LOG)
        cls.computed = cls.run_script("computed")
        numbers = cls.computed["result"]["tests"]
        # Extra fields in the saved file (as the workflow saves the whole "tests" object) are dropped.
        cls.read_back = cls.run_script("read-back", {**numbers, "saved_by": "an earlier run"})
        # What a collection that timed out leaves behind, and a file that is not one.
        cls.zeros = cls.run_script("zeros", {"count": 0, "coverage_pct": 0})
        cls.garbage = cls.run_script("garbage", "oops")

    @classmethod
    def run_script(cls, name, snapshot=None, extra_env=None):
        out, log = cls.dir / f"{name}.json", cls.dir / f"{name}.calls"
        log.write_text("")
        env = {
            **os.environ,
            "PATH": f"{cls.dir / 'bin'}{os.pathsep}{os.environ['PATH']}",
            "STUB_LOG": str(log),
            "STUB_EVENTS": str(cls.dir / "events.jsonl"),
            "STUB_SCORE": str(cls.dir / "score.txt"),
            "STUB_COVERAGE": COVERAGE_TOTAL,
            "QUALITY_SKIP_PERF": "1",
        }
        env.pop("QUALITY_TESTS_SNAPSHOT", None)
        env.update(extra_env or {})
        if snapshot is not None:
            path = cls.dir / f"{name}.snapshot"
            path.write_text(snapshot if isinstance(snapshot, str) else json.dumps(snapshot))
            env["QUALITY_TESTS_SNAPSHOT"] = str(path)
        done = subprocess.run(["bash", str(SCRIPT), str(out)], cwd=cls.root, env=env, capture_output=True, text=True,
                              timeout=120)
        if done.returncode:
            raise AssertionError(done.stderr)
        calls = log.read_text().splitlines()
        return {"result": json.loads(out.read_text()), "bytes": out.read_text(), "calls": calls,
                "stderr": done.stderr,
                "test_runs": [c for c in calls if c.startswith("go test ./...")]}

    def test_without_a_snapshot_the_tests_run_and_are_counted(self):
        self.assertEqual(len(self.computed["test_runs"]), 1)
        self.assertEqual(self.computed["result"]["tests"], {"count": EXPECTED_COUNT, "coverage_pct": 83.4})

    def test_a_snapshot_replaces_the_test_run_and_nothing_else_changes(self):
        self.assertEqual(self.read_back["test_runs"], [])
        self.assertEqual(self.read_back["result"], self.computed["result"])
        # byte for byte, not only equal as data
        self.assertEqual(self.read_back["bytes"], self.computed["bytes"])

    def test_the_compatibility_scores_are_still_measured_with_a_snapshot(self):
        scored = [c for c in self.read_back["calls"] if c.startswith("go test -v -tags=e2e")]
        self.assertEqual(len(scored), 3)
        loki = self.read_back["result"]["compatibility"]["loki"]
        self.assertEqual((loki["passed"], loki["total"]), (3, 4))
        self.assertEqual(loki["components"]["labels"], {"passed": 2, "total": 2, "pct": 100.0})

    def test_an_unusable_snapshot_is_ignored(self):
        # all-zero, empty, not JSON, coverage missing or not a number, and a missing file all fail the same
        # jq predicate; the two that matter in practice are run.
        for run in (self.zeros, self.garbage):
            self.assertEqual(len(run["test_runs"]), 1)
            self.assertEqual(run["result"], self.computed["result"])

    def test_failed_test_collection_preserves_events_and_reports_real_exit(self):
        diagnostics = self.dir / "failed-test-diagnostics"
        run = self.run_script("failed-tests", extra_env={
            "STUB_GO_TEST_FAIL": "1",
            "QUALITY_DIAGNOSTICS_DIR": str(diagnostics),
        })
        self.assertEqual(run["result"]["tests"], {"count": 0, "coverage_pct": 0})
        self.assertIn("tests_and_coverage failed (exit=42)", run["stderr"])
        self.assertEqual((diagnostics / "go-test-events.json").read_text(),
                         (self.dir / "events.jsonl").read_text())
        self.assertIn("simulated go test failure", (diagnostics / "tests_and_coverage.stderr").read_text())

    def test_failed_collection_renders_unavailable_not_regressed_coverage(self):
        base = self.dir / "render-base.json"
        head = self.dir / "render-head.json"
        output = self.dir / "render-failed-collection.md"
        base.write_text(self.computed["bytes"])
        failed = json.loads(self.computed["bytes"])
        failed["tests"] = {"count": 0, "coverage_pct": 0}
        head.write_text(json.dumps(failed))
        subprocess.run(["bash", str(REPO / "scripts" / "ci" / "render_pr_report.sh"),
                        str(base), str(head), str(output)], check=True, cwd=REPO)
        report = output.read_text()
        self.assertIn("| Test count | 5 | unavailable | n/a |", report)
        self.assertIn("| Coverage | 83.4% | unavailable | n/a |", report)
        self.assertIn("Test count and coverage could not be collected", report)


class WorkflowWiringTest(unittest.TestCase):
    TEXT = (REPO / ".github" / "workflows" / "pr-quality-report.yaml").read_text()

    def test_only_the_base_collection_reads_the_snapshot(self):
        head = self.TEXT.index("collect_quality_metrics.sh\" /tmp/pr-head-quality.json")
        base = self.TEXT.index("QUALITY_TESTS_SNAPSHOT=/tmp/pr-base-tests.json")
        self.assertEqual(self.TEXT.count("QUALITY_TESTS_SNAPSHOT"), 1)
        self.assertLess(head, base)

    def test_the_cache_key_names_the_base_commit_and_the_script(self):
        self.assertIn("github.event.pull_request.base.sha }}\n", self.TEXT)
        self.assertIn("hashFiles('scripts/ci/collect_quality_metrics.sh')", self.TEXT)

    def test_a_zero_collection_is_not_saved(self):
        self.assertIn("jq -e '.tests.count > 0' /tmp/pr-base-quality.json", self.TEXT)

    def test_failure_diagnostics_are_uploaded(self):
        self.assertIn("QUALITY_DIAGNOSTICS_DIR: /tmp/pr-quality-diagnostics/head", self.TEXT)
        self.assertIn("QUALITY_DIAGNOSTICS_DIR=/tmp/pr-quality-diagnostics/base", self.TEXT)
        self.assertIn("name: pr-quality-diagnostics", self.TEXT)


if __name__ == "__main__":
    unittest.main()
