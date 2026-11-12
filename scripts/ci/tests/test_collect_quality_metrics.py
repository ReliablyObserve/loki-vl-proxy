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
  "test ./internal/querycorpus -run ^TestBuildQueryCorpus$"*)
    echo "recordings: $(ls "$LVP_QUERY_CORPUS_DIR" | tr '\\n' ' ')" >> "$STUB_LOG"
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
        # A package timeout under the collection budget: Go's 10m default cut internal/proxy short.
        self.assertIn("-timeout=18m", self.computed["test_runs"][0].split())
        tests = self.computed["result"]["tests"]
        self.assertEqual((tests["count"], tests["coverage_pct"]), (EXPECTED_COUNT, 83.4))
        self.assertEqual(tests["outcomes"]["p/a\tTestTwo/sub"], "pass")
        self.assertEqual(tests["outcomes"]["p/b\tTestSkipped"], "skip")
        self.assertEqual(tests["outcomes"]["p/b\tTestBroken"], "fail")
        self.assertEqual(tests["package_coverage"], {})

    def test_a_snapshot_replaces_the_test_run_and_nothing_else_changes(self):
        self.assertEqual(self.read_back["test_runs"], [])
        self.assertEqual(self.read_back["result"], self.computed["result"])
        # byte for byte, not only equal as data
        self.assertEqual(self.read_back["bytes"], self.computed["bytes"])

    def test_large_named_case_inventory_does_not_use_shell_argument_space(self):
        events = self.dir / "large-events.jsonl"
        events.write_text("".join(json.dumps({"Action": "pass", "Package": "p/a", "Test": "Test" + "LongCase" * 12 + str(index)}) + "\n" for index in range(12000)))
        run = self.run_script("large-cases", extra_env={"STUB_EVENTS": str(events)})
        self.assertEqual(run["result"]["tests"]["count"], 12000)
        self.assertEqual(len(run["result"]["tests"]["outcomes"]), 12000)

    def test_a_perf_only_run_measures_nothing_but_performance(self):
        run = self.run_script("perf-only", extra_env={"QUALITY_SKIP_PERF": "0", "QUALITY_SKIP_LOAD": "1",
                                                     "QUALITY_PERF_ONLY": "1"})
        self.assertEqual(run["test_runs"], [])
        self.assertEqual([c for c in run["calls"] if c.startswith("go test -v -tags=e2e")], [])
        # No stack starts; the exit trap's `docker compose down` has nothing to stop.
        self.assertEqual([c for c in run["calls"] if c.startswith("docker") and " up " in c + " "], [])
        self.assertTrue(any("-bench" in c for c in run["calls"]), run["calls"])
        self.assertEqual(run["result"]["performance"]["mode"], "full")
        self.assertEqual(run["result"]["tests"], {"count": 0, "coverage_pct": 0})

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
        tests = run["result"]["tests"]
        self.assertEqual((tests["count"], tests["coverage_pct"]), (0, 0))
        self.assertEqual((tests["execution_state"], tests["test_exit_code"]), ("failed", 42))
        self.assertEqual(tests["outcomes"]["p/b\tTestBroken"], "fail")
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

    # --- the tests run by parallel jobs (QUALITY_TESTS_FROM) and the compatibility scores measured by another job ---

    PKG = "x/internal/proxy"
    QC = "x/internal/querycorpus"

    @classmethod
    def shard_dir(cls, name, shard, exit_code=0, events=None, assigned=None, listed=None, profile="mode: set\nf.go:1.1,2.2 1 1\n"):
        d = cls.dir / name / f"pr-quality-tests-head-{shard}"
        d.mkdir(parents=True)
        (d / "test-exit-code").write_text(f"{exit_code}\n")
        (d / "unit-results.json").write_text("".join(json.dumps(e) + "\n" for e in events or []))
        (d / "coverage.out").write_text(profile)
        if shard != "rest":
            (d / "assigned.txt").write_text("".join(f"{a}\n" for a in assigned or []))
            (d / "all-tests.txt").write_text("".join(f"{a}\n" for a in listed or []))
        return d

    @classmethod
    def sharded(cls, name, drop=None, exit_codes=None, skip_outcome=None):
        """Three shards holding every test of both split packages once; returns the run of the script over them."""
        listed = ["internal/proxy:TestA", "internal/proxy:TestB", "internal/querycorpus:TestQ"]
        parts = {"1": ["internal/proxy:TestA", "internal/querycorpus:TestQ"], "2": ["internal/proxy:TestB"]}
        for shard, tests in parts.items():
            events = [{"Action": "pass", "Package": cls.PKG if t.startswith("internal/proxy") else cls.QC, "Test": t.split(":")[1]}
                      for t in tests if t != skip_outcome]
            if shard == "1":
                events.append({"Action": "pass", "Package": cls.PKG, "Test": "TestA/sub"})
            if drop != shard:
                cls.shard_dir(name, shard, (exit_codes or {}).get(shard, 0), events, tests, listed)
        if drop != "rest":
            cls.shard_dir(name, "rest", (exit_codes or {}).get("rest", 0), [{"Action": "pass", "Package": "x/cmd", "Test": "TestRest"},
                                                                       {"Action": "fail", "Package": "x/cmd"}])
        return cls.run_script(name, extra_env={"QUALITY_TESTS_FROM": str(cls.dir / name), "QUALITY_TESTS_PREFIX": "pr-quality-tests-head-",
                                               "QUALITY_TESTS_SHARDS": "rest 1 2", "QUALITY_DIAGNOSTICS_DIR": str(cls.dir / f"{name}-diag")})

    def test_tests_run_by_parallel_jobs_are_merged_and_counted_without_running_go_test(self):
        run = self.sharded("shards-ok")
        self.assertEqual(run["test_runs"], [])
        tests = run["result"]["tests"]
        # TestA, TestA/sub, TestB, TestQ, TestRest
        self.assertEqual((tests["count"], tests["coverage_pct"], tests["execution_state"]), (5, 83.4, "complete"))
        self.assertEqual(tests["outcomes"]["x/internal/proxy\tTestA/sub"], "pass")
        self.assertEqual(tests["outcomes"]["x/cmd\tTestRest"], "pass")

    def test_a_failing_test_in_one_shard_is_reported_as_a_failed_collection_with_its_exit_status(self):
        run = self.sharded("shards-fail", exit_codes={"2": 1})
        tests = run["result"]["tests"]
        self.assertEqual((tests["count"], tests["coverage_pct"], tests["execution_state"], tests["test_exit_code"]), (0, 0, "failed", 1))
        self.assertIn("x/internal/proxy\tTestA", tests["outcomes"])
        self.assertIn("tests_and_coverage failed (exit=1)", run["stderr"])

    def test_a_shard_that_left_no_result_or_dropped_a_test_is_a_collection_that_failed_not_a_partial_count(self):
        for name, kw in (("shards-missing", {"drop": "2"}), ("shards-lost-test", {"skip_outcome": "internal/querycorpus:TestQ"})):
            with self.subTest(name):
                run = self.sharded(name, **kw)
                self.assertEqual(run["result"]["tests"], {"count": 0, "coverage_pct": 0})
                self.assertEqual(run["test_runs"], [])

    def test_a_shard_run_with_the_corpus_collects_the_recordings_of_every_shard(self):
        name = "shards-corpus"
        run_dir = self.dir / name
        self.sharded(name)  # the shards of a run without the corpus; add the recordings the corpus run would have left
        for shard in ("1", "2"):
            rec = run_dir / f"pr-quality-tests-head-{shard}" / "recordings"
            rec.mkdir()
            (rec / "100.jsonl").write_text("{}\n")
        run = self.run_script(name + "-run", extra_env={
            "QUALITY_TESTS_FROM": str(run_dir), "QUALITY_TESTS_PREFIX": "pr-quality-tests-head-", "QUALITY_TESTS_SHARDS": "rest 1 2",
            "QUALITY_QUERY_CORPUS_OUT": str(self.dir / "corpus.jsonl.gz")})
        self.assertEqual(run["test_runs"], [])
        self.assertIn("recordings: 1-100.jsonl 2-100.jsonl ", run["calls"])

    def test_compatibility_scores_measured_by_another_job_are_read_back(self):
        scores = {k: {"passed": 3, "total": 4, "pct": 75.0, "components": {}} for k in ("loki", "drilldown", "vl")}
        path = self.dir / "scores.json"
        path.write_text(json.dumps(scores))
        run = self.run_script("compat-from", extra_env={"QUALITY_COMPAT_FROM": str(path)})
        self.assertEqual(run["result"]["compatibility"], scores)
        self.assertEqual([c for c in run["calls"] if c.startswith("go test -v -tags=e2e") or " up " in c], [])
        path.write_text("oops")
        run = self.run_script("compat-from-garbage", extra_env={"QUALITY_COMPAT_FROM": str(path)})
        self.assertEqual(run["result"]["compatibility"]["loki"], {"passed": 0, "total": 0, "pct": 0})

    def test_compat_only_measures_the_scores_and_nothing_else(self):
        run = self.run_script("compat-only", extra_env={"QUALITY_COMPAT_ONLY": "1"})
        self.assertEqual(run["test_runs"], [])
        self.assertEqual(set(run["result"]), {"loki", "drilldown", "vl"})
        self.assertEqual((run["result"]["loki"]["passed"], run["result"]["loki"]["total"]), (3, 4))
        self.assertEqual(len([c for c in run["calls"] if c.startswith("go test -v -tags=e2e")]), 3)


class WorkflowWiringTest(unittest.TestCase):
    TEXT = (REPO / ".github" / "workflows" / "pr-quality-report.yaml").read_text()

    @classmethod
    def setUpClass(cls):
        from scripts.ci.change_scope import parse_jobs
        cls.jobs = parse_jobs(cls.TEXT)

    def body(self, name):
        return self.jobs[name]["body"]

    def test_the_snapshots_and_the_performance_run_in_parallel_and_meet_in_report(self):
        for name in ("snapshot-tests", "snapshot-compat", "perf"):
            with self.subTest(name):
                self.assertEqual(self.jobs[name]["needs"], ["scope", "plan"])
        # the snapshots wait for the jobs that measure their parts; the report waits for every one of them
        for name in ("head", "base"):
            with self.subTest(name):
                self.assertEqual(self.jobs[name]["needs"], ["scope", "plan", "snapshot-tests", "snapshot-compat"])
        self.assertEqual(self.jobs["report"]["needs"], ["scope", "plan", "snapshot-tests", "snapshot-compat", "head", "base", "perf"])
        self.assertIn("needs.plan.outputs.run_perf == 'true'", self.jobs["perf"]["if"])

    def test_report_fails_with_a_measuring_job_whose_snapshot_could_still_be_uploaded(self):
        report = self.body("report")
        for result in ("TESTS_RESULT: ${{ needs.snapshot-tests.result }}", "COMPAT_RESULT: ${{ needs.snapshot-compat.result }}"):
            self.assertIn(result, report)
        self.assertIn('"snapshot-tests=$TESTS_RESULT" "snapshot-compat=$COMPAT_RESULT"', report)

    def test_the_tests_run_in_shards_through_the_runner_of_the_ci_shards_with_the_flags_of_the_single_run(self):
        job = self.body("snapshot-tests")
        self.assertIn("matrix: ${{ fromJSON(needs.plan.outputs.tests_matrix) }}", job)
        self.assertIn("scripts/ci/go_test_unit.sh", job)
        # the flags of `go test ./... -count=1 -timeout=18m` with the coverage, and the race detector off
        for env in ('UNIT_RACE: "0"', 'UNIT_COUNT1: "1"', "UNIT_TIMEOUT: 18m", "UNIT_SHARDS: ${{ needs.plan.outputs.test_shard_count }}"):
            self.assertIn(env, job)
        # the corpus build tag, and the recordings, only on the PR's tree and when the plan wants the corpus
        self.assertIn("UNIT_TAGS: ${{ matrix.tree == 'head' && needs.plan.outputs.query_corpus == 'true' && 'corpuscapture' || '' }}", job)
        # the base commit's tests run in its own checkout, with these scripts
        self.assertIn("UNIT_ROOT=/tmp/loki-vl-proxy-base", job)
        # a failing test is recorded for the report, not a failed job; a missing artifact is
        self.assertIn('echo "$status" > shard-out/test-exit-code', job)
        self.assertIn("if-no-files-found: error", job)
        self.assertNotIn("docker", job)

    def test_the_plan_runs_the_base_tests_only_without_a_saved_count_and_keeps_the_shard_names_in_one_place(self):
        plan = self.body("plan")
        self.assertIn("lookup-only: true", plan)
        self.assertIn("hashFiles('scripts/ci/collect_quality_metrics.sh', 'scripts/ci/pr_evidence.py')", plan)
        self.assertIn("trees='[\"head\"]'", plan)
        self.assertEqual(self.TEXT.count("count=6"), 1)
        for env in ("QUALITY_TESTS_SHARDS: ${{ needs.plan.outputs.test_shards }}", "QUALITY_TESTS_PREFIX: pr-quality-tests-"):
            self.assertIn(env, self.body("head") + self.body("base"))

    def test_the_compatibility_scores_are_measured_in_jobs_of_their_own_and_only_they_need_the_stack(self):
        compat = self.body("snapshot-compat")
        self.assertIn('QUALITY_COMPAT_ONLY: "1"', compat)
        self.assertIn("docker/build-push-action", compat)
        self.assertIn("matrix.tree == 'base'", compat)
        for name in ("head", "base"):
            body = self.body(name)
            self.assertNotIn("docker/", body)
            self.assertIn("QUALITY_COMPAT_FROM: /tmp/%s-compat/pr-%s-compat.json" % (name, name), body)

    def test_only_the_base_collection_reads_the_snapshot(self):
        self.assertEqual(self.TEXT.count("QUALITY_TESTS_SNAPSHOT"), 1)
        self.assertIn("QUALITY_TESTS_SNAPSHOT=/tmp/pr-base-tests.json", self.body("base"))
        self.assertIn('collect_quality_metrics.sh" /tmp/pr-head-quality.json', self.body("head"))

    def test_the_cache_key_names_the_base_commit_and_the_script(self):
        self.assertIn("github.event.pull_request.base.sha }}\n", self.TEXT)
        self.assertIn("hashFiles('scripts/ci/collect_quality_metrics.sh', 'scripts/ci/pr_evidence.py')", self.body("base"))

    def test_a_zero_collection_is_not_saved(self):
        self.assertIn("jq -e '.tests.count > 0' /tmp/pr-base-quality.json", self.body("base"))

    def test_failure_diagnostics_are_uploaded(self):
        self.assertIn("QUALITY_DIAGNOSTICS_DIR: /tmp/pr-quality-diagnostics/head", self.body("head"))
        self.assertIn("QUALITY_DIAGNOSTICS_DIR=/tmp/pr-quality-diagnostics/base", self.body("base"))
        self.assertIn("name: pr-quality-diagnostics-head", self.body("head"))
        self.assertIn("name: pr-quality-diagnostics-base", self.body("base"))

    def test_the_snapshots_skip_the_performance_that_one_runner_measures_for_both_trees(self):
        self.assertIn('QUALITY_SKIP_PERF: "1"', self.body("head"))
        self.assertIn("QUALITY_SKIP_PERF=1", self.body("base"))
        perf = self.body("perf")
        self.assertEqual(self.TEXT.count('QUALITY_SKIP_LOAD: "1"'), 1)
        self.assertIn('QUALITY_SKIP_LOAD: "1"', perf)
        self.assertIn('QUALITY_PERF_ONLY: "1"', perf)
        head, base = perf.index("/tmp/pr-head-perf.json"), perf.index("/tmp/pr-base-perf.json")
        abab = perf.index("scripts/ci/load_abab.py")
        self.assertLess(max(head, base), abab)
        self.assertIn("--base-dir /tmp/loki-vl-proxy-base --head-dir \"$WORKSPACE\"", perf)
        self.assertNotIn("load_abab.py", self.body("head") + self.body("base") + self.body("report"))

    def test_report_takes_the_performance_from_the_perf_job(self):
        report = self.body("report")
        self.assertIn("name: pr-quality-perf", report)
        self.assertIn("if: needs.perf.result == 'success'", report)
        self.assertIn(".performance = $perf[0].performance", report)
        self.assertLess(report.index(".performance = $perf[0].performance"), report.index("pr_evidence.py --provenance"))
        self.assertLess(report.index("render_pr_report.sh"), report.index("check_quality_gate.py"))

    def test_the_report_artifact_keeps_its_name_and_files(self):
        # pr_evidence.py reads these three files from the pr-quality-report artifact.
        report = self.body("report")
        self.assertIn("name: pr-quality-report\n", report)
        for name in ("/tmp/pr-base-quality.json", "/tmp/pr-head-quality.json", "/tmp/quality-meta.json"):
            self.assertIn(name, report)

    def test_the_plan_is_compared_with_lokis_tree_and_report_requires_every_snapshot_job(self):
        head = self.body("head")
        self.assertIn("go test ./internal/queryplan -run '^TestPlanMatchesLokiOracle$'", head)
        self.assertIn('LVP_RUN_ORACLES: "1"', head)
        self.assertLess(head.index("TestPlanMatchesLokiOracle"), head.index("name: pr-quality-diagnostics-head"))
        report = self.body("report")
        self.assertIn("HEAD_RESULT: ${{ needs.head.result }}", report)
        self.assertIn("BASE_RESULT: ${{ needs.base.result }}", report)
        self.assertIn("success|skipped", report)
        self.assertLess(report.index("Require the snapshot jobs"), report.index("Download the head snapshot"))

    def test_a_query_plan_change_runs_the_oracles(self):
        # The query-plan resolver is compared with Loki's tree, and the lowered queries and the builders they share
        # with the translator (internal/translate) with both oracles, only in the query-corpus scope.
        self.assertIn("internal/(logql|logsql|translator|translate|proxy|querycorpus|queryplan|rulesmigrate)/", self.body("plan"))

    def test_a_change_to_the_load_measurement_runs_the_perf_smoke(self):
        self.assertIn("check_quality_gate\\.py|load_abab\\.py", self.TEXT)


if __name__ == "__main__":
    unittest.main()
