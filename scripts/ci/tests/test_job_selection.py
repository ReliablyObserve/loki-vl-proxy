"""Tests of the job selection of a forced run (the `ci-full` label, a push to main).

scripts/ci/change_scope.py maps the paths of a change to the jobs they can affect, so a
forced run of an infra-only change runs those jobs and not the whole product suite. The
mapping may only ever make a run lighter on purpose: one path that no entry maps keeps the
forced run of every lane. The tests below pin that, with the changes of the examples the
owner named (the visual harness, the A/B harness, a single ci.yaml job, docs), product and
unknown paths, mixed changes, and the real workflows.
"""

import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.ci import change_scope
from scripts.ci.change_scope import (
    ALL_CI,
    ALL_JOBS,
    CI_JOBS,
    CI_WORKFLOW,
    JOB_MAP,
    NEVER_NARROW,
    PERF_AB_JOBS,
    PRODUCT_OVERRIDES,
    VISUAL_JOBS,
    WORKFLOW_JOBS,
    ci_closure,
    ci_workflow_jobs,
    format_jobs,
    glob_match,
    mapped_jobs,
    parse_jobs,
    select_jobs,
    ships,
    workflow_blocks,
)

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "ci" / "change_scope.py"
CI_TEXT = (REPO / CI_WORKFLOW).read_text()


def tracked_files():
    try:
        out = subprocess.run(["git", "ls-files"], cwd=REPO, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    return [line for line in out.splitlines() if line]


TRACKED = tracked_files()


def ci(*names):
    return frozenset("ci/" + name for name in names)


def edited_ci(old, new, count=1):
    """ci.yaml with `old` replaced by `new`, as the pair read_pair returns."""
    assert CI_TEXT.count(old) >= 1, old
    return CI_TEXT, CI_TEXT.replace(old, new, count)


def pair_of(base, head):
    return lambda path: (base, head)


def job_edit(job, line="      - run: echo changed\n"):
    """ci.yaml where `job`'s block gained a step."""
    match = re.search(r"^  %s:\n" % re.escape(job), CI_TEXT, re.M)
    assert match, job
    following = re.search(r"^  [a-z0-9-]+:\n", CI_TEXT[match.end():], re.M)
    end = match.end() + following.start() if following else len(CI_TEXT)
    return CI_TEXT, CI_TEXT[:end].rstrip("\n") + "\n" + line + "\n" + CI_TEXT[end:]


def select_ci_edit(job, line="      - run: echo changed\n"):
    return select_jobs([CI_WORKFLOW], pair_of(*job_edit(job, line)))


class ExampleChanges(unittest.TestCase):
    """The infra-only examples: what a forced run (ci-full, push to main) runs."""

    def test_visual_harness_runs_visual_smoke_in_full_only(self):
        for path in ("bench/visual/plan.py", "bench/visual/ci_run.py", "bench/visual/tests/test_plan.py",
                     "bench/visual/capture.spec.ts", "bench/visual/requirements.txt",
                     "bench/visual/docker-compose.visual.yml", ".github/workflows/visual-smoke.yaml"):
            with self.subTest(path):
                self.assertEqual(select_jobs([path]), VISUAL_JOBS)
        self.assertEqual(VISUAL_JOBS, {"visual-smoke/visual-plan", "visual-smoke/visual-smoke",
                                       "visual-smoke/full"})

    def test_the_visual_spec_is_read_by_the_conformance_gate_so_it_runs_everything(self):
        self.assertIsNone(select_jobs(["bench/visual/spec.json"]))
        self.assertIsNone(select_jobs(["bench/visual/plan.py", "bench/visual/spec.json"]))

    def test_ab_harness_runs_perf_ab_in_full_only(self):
        for path in ("bench/ab/pr_smoke.py", "bench/ab/comment.py", "bench/ab/daily.py", "bench/ab/history.py",
                     "bench/ab/tests/test_comment.py", ".github/workflows/perf-ab.yaml"):
            with self.subTest(path):
                self.assertEqual(select_jobs([path]), PERF_AB_JOBS)

    def test_the_parts_of_the_ab_harness_other_gates_read_run_everything(self):
        for path in ("bench/ab/selection.py", "bench/ab/stack.py", "bench/ab/shapes.json",
                     "bench/ab/docker-compose.ab.yml"):
            with self.subTest(path):
                self.assertIsNone(select_jobs([path]))

    def test_daily_report_data_runs_nothing_beyond_the_always_on_checks(self):
        for path in ("bench/ab/history/trend.md", "bench/ab/results/daily-control.json",
                     "conformance/reports/performance.md", "conformance/registry/generated/perf-evidence.json"):
            with self.subTest(path):
                self.assertEqual(select_jobs([path]), frozenset())

    def test_docs_and_notes_run_nothing_beyond_the_always_on_checks(self):
        for path in ("docs/operations.md", "website/docusaurus.config.js", "README.md", "CHANGELOG.md",
                     "examples/loki-vl-proxy.yaml", "bench/README.md", ".github/dependabot.yml",
                     ".github/workflows/release.yaml", ".github/workflows/perf-daily.yaml"):
            with self.subTest(path):
                self.assertEqual(select_jobs([path]), frozenset())

    def test_a_single_ci_job_runs_that_job_and_its_aggregate(self):
        self.assertEqual(select_ci_edit("bench"), ci("bench"))
        self.assertEqual(select_ci_edit("fuzz-smoke-shard"), ci("fuzz-smoke-shard", "fuzz-smoke"))
        self.assertEqual(select_ci_edit("fuzz-smoke"), ci("fuzz-smoke-shard", "fuzz-smoke"))
        self.assertEqual(select_ci_edit("race-stress"), ci("race-stress-shard", "race-stress", "test-shard", "test"))
        self.assertEqual(select_ci_edit("tuple-contract"), ci("tuple-contract"))
        self.assertEqual(select_ci_edit("docker"), ci("docker"))
        self.assertEqual(select_ci_edit("stress-tests"), ci("stress-tests"))
        self.assertEqual(select_ci_edit("memory-leak-tests"), ci("memory-leak-tests"))

    def test_a_stack_job_runs_the_image_it_loads_and_not_its_siblings(self):
        self.assertEqual(select_ci_edit("e2e-ui"), ci("e2e-ui", "image"))
        self.assertEqual(select_ci_edit("e2e-fleet"), ci("e2e-fleet", "image"))
        self.assertEqual(select_ci_edit("tuple-smoke"), ci("tuple-smoke", "image"))
        self.assertEqual(select_ci_edit("e2e-compat-group"), ci("e2e-compat-group", "e2e-compat", "image"))
        self.assertEqual(select_ci_edit("e2e-compat"), ci("e2e-compat-group", "e2e-compat", "image"))

    def test_the_image_runs_every_job_that_loads_it(self):
        self.assertEqual(select_ci_edit("image"),
                         ci("image", "tuple-smoke", "e2e-compat-group", "e2e-compat", "e2e-fleet", "e2e-ui"))

    def test_the_unit_test_jobs_run_their_fan_in_and_the_gate_that_reads_their_results(self):
        self.assertEqual(select_ci_edit("test-shard"), ci("test-shard", "test", "conformance"))
        self.assertEqual(select_ci_edit("test"), ci("test-shard", "test", "conformance"))
        self.assertEqual(select_ci_edit("conformance"), ci("conformance", "test", "test-shard"))

    def test_lint_and_helm_always_run_so_an_edit_to_them_selects_nothing_more(self):
        self.assertEqual(select_ci_edit("lint"), frozenset())
        self.assertEqual(select_ci_edit("helm"), frozenset())

    def test_two_edited_jobs_run_both(self):
        base, head = job_edit("bench")
        _, head = (base, head.replace("  conformance:\n", "  conformance:\n    # edited\n    timeout-minutes: 30\n", 1))
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(base, head)), ci("bench", "conformance", "test", "test-shard"))

    def test_other_workflows_run_whole_and_in_full(self):
        expected = {
            ".github/workflows/compat-loki.yaml": {"compat-loki/loki-pinned"},
            ".github/workflows/compat-vl.yaml": {"compat-vl/vl-pinned"},
            ".github/workflows/compat-drilldown.yaml": {"compat-drilldown/drilldown-pinned-runtime",
                                                       "compat-drilldown/drilldown-grafana-pr-matrix"},
            ".github/workflows/e2e-pipeline.yml": {"e2e-pipeline/e2e-pipeline"},
            ".github/workflows/security-pr.yaml": {"security-pr/security-runtime"},
            ".github/workflows/pr-quality-report.yaml": {"pr-quality-report/report"},
        }
        for path, jobs in expected.items():
            with self.subTest(path):
                self.assertEqual(select_jobs([path]), frozenset(jobs))

    def test_scripts_run_the_jobs_that_use_them(self):
        self.assertEqual(select_jobs(["scripts/ci/fuzz_smoke.py"]), ci("fuzz-smoke-shard", "fuzz-smoke"))
        self.assertEqual(select_jobs(["scripts/ci/fuzz_smoke_targets.tsv"]), ci("fuzz-smoke-shard", "fuzz-smoke"))
        self.assertEqual(select_jobs(["scripts/ci/go_test_unit.sh"]), ci("test-shard", "test", "conformance"))
        self.assertEqual(select_jobs(["scripts/ci/race_sensitive_tests.sh"]), ci("conformance", "test", "test-shard"))
        self.assertEqual(select_jobs(["scripts/ci/pr_quality_report.py"]), frozenset({"pr-quality-report/report"}))
        self.assertEqual(select_jobs(["test/e2e-ui/package-lock.json"]), ci("e2e-ui", "image"))
        for path in ("scripts/ci/tests/test_change_scope.py", "scripts/ci/sync_release_metadata.sh",
                     "scripts/ci/check_changelog_pr.py", "scripts/ci/validate_compose.sh"):
            with self.subTest(path):
                self.assertEqual(select_jobs([path]), frozenset())

    def test_stack_configs_run_every_job_that_starts_a_stack(self):
        for path in ("test/e2e-compat/docker-compose.yml", "test/e2e-fleet/docker-compose.yml",
                     "test/e2e-compat/loki-local-config.yaml"):
            with self.subTest(path):
                got = select_jobs([path])
                self.assertEqual(got, change_scope.STACK_JOBS)
                self.assertIn("ci/image", got)
                self.assertIn("security-pr/security-runtime", got)
                self.assertNotIn("ci/test-shard", got)
                self.assertNotIn("ci/conformance", got)


class WhatKeepsTheFullRun(unittest.TestCase):
    """One path no entry maps: every lane, as before."""

    def test_shipped_and_product_paths(self):
        for path in ("internal/proxy/proxy.go", "internal/proxy/proxy_test.go", "internal/proxy/testdata/x.json",
                     "cmd/proxy/main.go", "pkg/x/y.go", "go.mod", "go.sum", "Dockerfile", ".dockerignore",
                     "charts/loki-vl-proxy/values.yaml", "dashboard/x.json", "alerting/x.yaml"):
            with self.subTest(path):
                self.assertIsNone(select_jobs([path]))

    def test_the_gating_machinery_and_shared_scripts(self):
        for path in ("scripts/ci/change_scope.py", ".github/workflows/change-scope.yaml",
                     "scripts/ci/pr_evidence.py", "scripts/ci/check_conformance.py", "scripts/ci/run_zap_scan.sh",
                     "scripts/ci/wait_e2e_stack.sh", "scripts/smoke-test.sh", "scripts/compat/check.sh",
                     "scripts/ci/collect_quality_metrics.sh"):
            with self.subTest(path):
                self.assertIsNone(select_jobs([path]))

    def test_unknown_and_untrusted_paths(self):
        for path in ("something/new/file.txt", "scripts/ci/brand_new_script.sh", ".github/workflows/new.yaml",
                     "test/e2e-compat/new_test.go", "conformance/registry/loki/endpoints/x.yaml", "",
                     "/etc/passwd", "../x", "docs/../internal/x.go", '"docs/caf\\303\\251.md"'):
            with self.subTest(path):
                self.assertIsNone(select_jobs([path]))

    def test_an_empty_change_cannot_be_narrowed(self):
        self.assertIsNone(select_jobs([]))
        self.assertIsNone(select_jobs(["", "  "]))

    def test_one_unmapped_path_among_mapped_ones(self):
        self.assertIsNone(select_jobs(["docs/a.md", "bench/visual/plan.py", "internal/proxy/proxy.go"]))
        self.assertIsNone(select_jobs(["docs/a.md", "scripts/ci/brand_new_script.sh"]))

    def test_mapped_paths_combine(self):
        self.assertEqual(select_jobs(["docs/a.md", "bench/visual/plan.py", "bench/ab/pr_smoke.py"]),
                         VISUAL_JOBS | PERF_AB_JOBS)
        self.assertEqual(select_jobs(["docs/a.md", "README.md"]), frozenset())

    def test_a_ci_workflow_edit_that_cannot_be_read_runs_every_ci_job(self):
        self.assertEqual(select_jobs([CI_WORKFLOW]), ALL_CI)
        self.assertEqual(select_jobs([CI_WORKFLOW], lambda path: None), ALL_CI)
        self.assertEqual(select_jobs([CI_WORKFLOW], lambda path: (CI_TEXT, None)), ALL_CI)

        def broken(path):
            raise OSError("no git")

        self.assertEqual(select_jobs([CI_WORKFLOW], broken), ALL_CI)

    def test_an_edit_outside_the_job_blocks_runs_every_ci_job(self):
        for old, new in (("permissions:\n  contents: read\n", "permissions:\n  contents: write\n"),
                         ("    branches: [main]\n    paths-ignore:", "    branches: [main, dev]\n    paths-ignore:")):
            with self.subTest(old):
                self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(*edited_ci(old, new))), ALL_CI)

    def test_an_edit_of_the_scope_job_runs_every_ci_job(self):
        base, head = job_edit("scope", "    timeout-minutes: 3\n")
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(base, head)), ALL_CI)

    def test_a_new_job_that_the_registry_does_not_know_runs_every_ci_job(self):
        base, head = CI_TEXT, CI_TEXT.rstrip("\n") + "\n\n  brand-new:\n    runs-on: ubuntu-latest\n    steps: []\n"
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(base, head)), ALL_CI)

    def test_a_comment_only_edit_selects_nothing(self):
        base, head = job_edit("bench", "      # a note\n")
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(base, head)), frozenset())
        base, head = CI_TEXT, "# a new header comment\n" + CI_TEXT
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(base, head)), frozenset())

    def test_an_inline_comment_edit_is_an_edit(self):
        base, head = edited_ci('go-version: "1.27.4"\n          cache: true\n\n      - name: Tuple contract gate',
                               'go-version: "1.27.4"\n          cache: true  # note\n\n      - name: Tuple contract gate')
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(base, head)), ci("tuple-contract"))

    def test_a_ci_workflow_that_is_identical_selects_nothing(self):
        self.assertEqual(select_jobs([CI_WORKFLOW], pair_of(CI_TEXT, CI_TEXT)), frozenset())


class MappingTable(unittest.TestCase):
    def test_every_mapped_job_is_a_known_id(self):
        for rule_id, _, jobs in JOB_MAP:
            self.assertLessEqual(jobs, ALL_JOBS, rule_id)
        for path, (stem, names, extra) in WORKFLOW_JOBS.items():
            for name in names + extra:
                self.assertIn(f"{stem}/{name}", ALL_JOBS, path)

    def test_rule_ids_are_unique(self):
        ids = [rule_id for rule_id, _, _ in JOB_MAP]
        self.assertEqual(len(ids), len(set(ids)))

    def test_no_glob_is_dead(self):
        dead = [f"{rule_id}: {glob}" for rule_id, globs, _ in JOB_MAP for glob in globs
                if not any(glob_match(glob, path) for path in TRACKED)]
        self.assertEqual(dead, [], "JOB_MAP globs that match no tracked file")
        for workflow in list(WORKFLOW_JOBS) + [CI_WORKFLOW]:
            self.assertIn(workflow, TRACKED)

    def test_the_workflows_with_no_job_are_the_rest_of_the_workflows(self):
        present = {Path(path).name for path in TRACKED if path.startswith(".github/workflows/")}
        mapped = {Path(path).name for path in WORKFLOW_JOBS} | {Path(CI_WORKFLOW).name, "change-scope.yaml"}
        self.assertEqual(present - mapped, set(change_scope.NO_JOB_WORKFLOWS))

    def test_nothing_that_ships_is_ever_mapped(self):
        for path in TRACKED:
            if ships(path):
                self.assertIsNone(mapped_jobs(path), path)

    def test_no_product_override_is_ever_mapped(self):
        for path in TRACKED:
            if any(glob_match(glob, path) for _, globs in PRODUCT_OVERRIDES for glob in globs):
                self.assertIsNone(mapped_jobs(path), path)

    def test_never_narrow_paths_stay_unmapped(self):
        for glob in NEVER_NARROW:
            for path in TRACKED:
                if glob_match(glob, path):
                    self.assertIsNone(mapped_jobs(path), path)

    def test_the_mapping_only_ever_touches_non_go_runtime_files(self):
        # A mapped path is not Go under the module roots, and not a test the unit jobs run.
        for path in TRACKED:
            if mapped_jobs(path) is not None and path != CI_WORKFLOW:
                self.assertFalse(path.startswith(("cmd/", "internal/", "pkg/", "charts/")), path)
                self.assertNotIn(path, ("go.mod", "go.sum", "Dockerfile"))

    def test_every_tracked_file_answers_without_error(self):
        for path in TRACKED:
            result = mapped_jobs(path)
            self.assertTrue(result is None or isinstance(result, frozenset), path)

    def test_the_jobs_that_read_a_file_are_mapped_to_it(self):
        """A file a selectable job names in its steps maps to that job, or to every lane.

        Reads the real workflows (comments left out) and checks each file a job's steps name.
        An indirect read (an import, a script that calls another) is not found this way; the
        shared scripts are unmapped, so they run everything.
        """
        ref = re.compile(r"(?<![\w./-])((?:scripts|bench|test|conformance|security|internal)/[A-Za-z0-9_./-]+)")
        selectable = {}
        for name in CI_JOBS:
            selectable[("ci.yaml", name)] = "ci/" + name
        for path, (stem, names, extra) in WORKFLOW_JOBS.items():
            for name in names:
                selectable[(Path(path).name, name)] = f"{stem}/{name}"
        checked = 0
        for (filename, name), job_id in selectable.items():
            jobs = parse_jobs((REPO / ".github" / "workflows" / filename).read_text())
            body = "\n".join(line for line in jobs[name]["body"].splitlines() if not line.lstrip().startswith("#"))
            for token in sorted(set(ref.findall(body))):
                token = token.rstrip(".,:)")
                if not (REPO / token).is_file():
                    continue
                checked += 1
                result = mapped_jobs(token)
                with self.subTest(job=job_id, file=token):
                    self.assertTrue(result is None or job_id in result,
                                    f"{token} is read by {job_id} but a change to it would not run it")
        self.assertGreater(checked, 15)


class ClosureTables(unittest.TestCase):
    def test_closure_adds_partners_and_what_a_job_needs_never_its_siblings(self):
        self.assertEqual(ci_closure({"e2e-ui"}), {"e2e-ui", "image"})
        self.assertEqual(ci_closure({"e2e-compat-group"}), {"e2e-compat-group", "e2e-compat", "image"})
        self.assertEqual(ci_closure({"conformance"}), {"conformance", "test", "test-shard"})
        self.assertEqual(ci_closure({"fuzz-smoke"}), {"fuzz-smoke", "fuzz-smoke-shard"})

    def test_an_edit_adds_the_consumers(self):
        self.assertEqual(ci_closure({"image"}, edit=True), {"image", "tuple-smoke", "e2e-compat-group", "e2e-compat",
                                                           "e2e-fleet", "e2e-ui"})
        self.assertEqual(ci_closure({"image"}, edit=False), {"image"})

    def test_closure_is_a_fixpoint_and_stays_inside_ci(self):
        for name in CI_JOBS:
            first = ci_closure({name})
            self.assertEqual(ci_closure(first, edit=False), first, name)
            self.assertLessEqual(first, set(CI_JOBS), name)

    def test_the_expansion_of_a_mapped_set_keeps_other_workflows_as_they_are(self):
        got = change_scope.expand({"visual-smoke/full", "ci/fuzz-smoke"})
        self.assertEqual(got, {"visual-smoke/full", "ci/fuzz-smoke", "ci/fuzz-smoke-shard"})


class WorkflowBlocks(unittest.TestCase):
    def test_blocks_split_the_header_from_the_jobs(self):
        header, jobs = workflow_blocks(CI_TEXT)
        self.assertTrue(any(line.startswith("name: CI") for line in header))
        self.assertEqual(set(jobs), set(CI_JOBS) | set(change_scope.ALWAYS_ON_CI))

    def test_comments_and_blank_lines_do_not_count(self):
        header, jobs = workflow_blocks("name: x\n\n# note\non: push\njobs:\n  # between\n  a:\n    # note\n    runs-on: x\n\n  b:\n    needs: [a]\n")
        self.assertEqual(header, ["name: x", "on: push"])
        self.assertEqual(jobs, {"a": ["    runs-on: x"], "b": ["    needs: [a]"]})

    def test_a_top_level_key_after_the_jobs_belongs_to_the_header(self):
        header, jobs = workflow_blocks("jobs:\n  a:\n    runs-on: x\nenv:\n  K: v\n")
        self.assertEqual(header, ["env:", "  K: v"])
        self.assertEqual(jobs, {"a": ["    runs-on: x"]})

    def test_ci_workflow_jobs_reports_the_registry_for_the_real_file(self):
        self.assertEqual(ci_workflow_jobs(pair_of(CI_TEXT, CI_TEXT)), frozenset())


class Cli(unittest.TestCase):
    def run_cli(self, *args, stdin=None, cwd=REPO):
        return subprocess.run([sys.executable, "-I", str(SCRIPT), *args], input=stdin, capture_output=True,
                              text=True, cwd=cwd)

    def lanes(self, files, *args):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", "-", "--github-output", str(out), *args,
                                  stdin="".join(path + "\n" for path in files))
            self.assertEqual(result.returncode, 0, result.stderr)
            return dict(line.split("=", 1) for line in out.read_text().splitlines())

    def test_format_jobs(self):
        self.assertEqual(format_jobs([]), ",")
        self.assertEqual(format_jobs(["b/x", "a/y"]), ",a/y,b/x,")

    def test_a_narrowed_run_reports_no_lane_and_the_jobs(self):
        got = self.lanes(["bench/visual/plan.py"], "--narrow", "--base-ref", "main", "--labels", "ci-full")
        self.assertEqual(got, {"product": "false", "stack": "false", "perf": "false", "baseline": "false",
                               "evidence": "false", "jobs": ",visual-smoke/full,visual-smoke/visual-plan,"
                                                            "visual-smoke/visual-smoke,"})

    def test_a_docs_only_narrowed_run_reports_an_empty_but_present_job_list(self):
        got = self.lanes(["docs/a.md"], "--narrow")
        self.assertEqual(got["jobs"], ",")
        self.assertEqual((got["product"], got["stack"], got["perf"]), ("false", "false", "false"))

    def test_ci_full_on_a_product_change_keeps_every_lane_and_the_baseline(self):
        got = self.lanes(["internal/proxy/proxy.go"], "--narrow", "--base-ref", "main", "--labels", "ci-full")
        self.assertEqual((got["product"], got["stack"], got["perf"], got["baseline"]), ("true",) * 4)
        self.assertNotIn("jobs", got)

    def test_ci_full_with_one_unmapped_path_keeps_every_lane_and_the_baseline(self):
        got = self.lanes(["docs/a.md", "scripts/ci/brand_new.sh"], "--narrow", "--base-ref", "main",
                         "--labels", "ci-full")
        self.assertEqual((got["product"], got["stack"], got["perf"], got["baseline"]), ("true",) * 4)
        self.assertNotIn("jobs", got)

    def test_a_push_has_no_baseline_and_runs_every_lane_for_a_product_path(self):
        got = self.lanes(["internal/proxy/proxy.go"], "--narrow")
        self.assertEqual((got["product"], got["stack"], got["perf"], got["baseline"]), ("true", "true", "true", "false"))

    def test_an_empty_push_diff_runs_every_lane(self):
        got = self.lanes([], "--narrow")
        self.assertEqual((got["product"], got["stack"], got["perf"]), ("true", "true", "true"))
        self.assertNotIn("jobs", got)

    def test_force_product_wins_over_narrow(self):
        got = self.lanes(["docs/a.md"], "--narrow", "--force-product")
        self.assertEqual((got["product"], got["stack"], got["perf"]), ("true", "true", "true"))
        self.assertNotIn("jobs", got)

    def test_without_narrow_the_output_is_the_one_it_always_was(self):
        got = self.lanes(["docs/a.md"])
        self.assertEqual(list(got), ["product", "stack", "perf", "baseline", "evidence"])

    def test_evidence_is_still_reported_for_a_narrowed_runtime_change(self):
        # A runtime path is never narrowed, so a narrowed run never has evidence=true.
        got = self.lanes(["docs/a.md", "bench/ab/pr_smoke.py"], "--narrow")
        self.assertEqual(got["evidence"], "false")

    def test_the_summary_names_the_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "summary.md"
            self.run_cli("--files-from", "-", "--narrow", "--summary", str(summary), stdin="bench/ab/pr_smoke.py\n")
            text = summary.read_text()
            self.assertIn("narrowed to the jobs the change can affect", text)
            self.assertIn("`perf-ab/perf-smoke`", text)

    def test_a_ci_workflow_edit_is_read_from_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workflow = root / CI_WORKFLOW
            workflow.parent.mkdir(parents=True)
            workflow.write_text(CI_TEXT)
            env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com", "GIT_COMMITTER_NAME": "t",
                   "GIT_COMMITTER_EMAIL": "t@example.com", "HOME": tmp, "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"}

            def git(*args):
                subprocess.run(["git", "-c", "commit.gpgsign=false", *args], cwd=root, check=True,
                               capture_output=True, env=env)

            git("init", "-q")
            git("add", "-A")
            git("commit", "-q", "-m", "base")
            workflow.write_text(job_edit("bench")[1])
            with tempfile.TemporaryDirectory() as out_dir:
                out = Path(out_dir) / "out"
                result = self.run_cli("--files-from", "-", "--narrow", "--git-base", "HEAD",
                                      "--github-output", str(out), stdin=CI_WORKFLOW + "\n", cwd=root)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("jobs=,ci/bench,\n", out.read_text())
            # no base revision to read: the whole of ci.yaml, not a guess
            with tempfile.TemporaryDirectory() as out_dir:
                out = Path(out_dir) / "out"
                self.run_cli("--files-from", "-", "--narrow", "--git-base", "no-such-revision",
                             "--github-output", str(out), stdin=CI_WORKFLOW + "\n", cwd=root)
                self.assertIn("jobs=" + format_jobs(ALL_CI) + "\n", out.read_text())


class RealChanges(unittest.TestCase):
    """Recorded changes: what they would run if forced."""

    def test_a_change_like_the_visual_shards_pr(self):
        # #1027: bench/visual/*, visual-smoke.yaml, ci.yaml's conformance job and a docs file.
        files = [".github/workflows/visual-smoke.yaml", "bench/visual/README.md", "bench/visual/ci_run.py",
                 "bench/visual/shards.py", "bench/visual/tests/test_shards.py"]
        self.assertEqual(select_jobs(files), VISUAL_JOBS)
        self.assertEqual(select_jobs(files + [CI_WORKFLOW], pair_of(*job_edit("conformance"))),
                         VISUAL_JOBS | ci("conformance", "test", "test-shard"))

    def test_a_product_pull_request_is_not_narrowed(self):
        self.assertIsNone(select_jobs(["internal/proxy/proxy.go", "internal/proxy/proxy_test.go", "CHANGELOG.md",
                                       "conformance/registry/loki/endpoints/x.yaml"]))


if __name__ == "__main__":
    unittest.main()
