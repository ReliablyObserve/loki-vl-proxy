"""Guards the wiring of the change-scope gate in the CI workflows.

A product job that skips is reported as a pass, so a mistake in the gate lets a
broken product pull request go green. These tests read the workflow text (stdlib
only; the parser understands the block layout the workflows use) and assert:

- lint and helm always run, so the infra checks they carry are never skipped;
- every job that reads `needs.scope` lists `scope` in `needs`, and in ci.yaml
  uses the gate of its lane (product, stack, or the written-out gate of a job
  that follows `test`) or, for the three aggregators, `if: always()`;
- every selectable job (scripts/ci/change_scope.py: the ids a narrowed forced run
  may select) also tests its own id in `needs.scope.outputs.jobs`, in the exact
  form `contains(format(',{0},', ...), ',<workflow>/<job>,')`, so `ci/test` never
  matches `ci/test-shard`; the registry in change_scope.py and ci.yaml list the
  same jobs, the same needs and the same aggregates;
- no job downstream of `test` can run when `test` failed, and only the jobs that
  follow `test` for ordering, with exactly the gate below, can run when `test` was
  skipped; the jobs that read what `test` or `image` produce keep the implicit
  success();
- the aggregators pass on skipped groups only when scope says `false` and the
  group was not selected;
- the baseline output reaches only the three heavy workflows, only through
  `needs.scope.outputs.baseline == 'true'` (or, for the two with a full run, a
  selected `<workflow>/full`), only for pull requests into main, and never
  commits anything.
"""

import re
import unittest
from pathlib import Path

from scripts.ci.change_scope import (
    ALL_CI,
    ALL_JOBS,
    ALWAYS_ON_CI,
    CI_CONSUMERS,
    CI_JOBS,
    CI_NEEDS_RUN,
    CI_PARTNERS,
    WORKFLOW_JOBS,
    parse_jobs,
    product_evidence,
)

REPO = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO / ".github" / "workflows"
CI = WORKFLOWS / "ci.yaml"


def sel(job_id):
    """How a workflow tests whether a narrowed run selected a job."""
    return "contains(format(',{0},', needs.scope.outputs.jobs), ',%s,')" % job_id


def lane_gate(lane, job_id):
    return "${{ !cancelled() && (needs.scope.outputs.%s != 'false' || %s) }}" % (lane, sel(job_id))


TEST_OK = "(needs.test.result == 'success' || needs.test.result == 'skipped')"
CANONICAL_GATE = "${{ !cancelled() && (needs.scope.outputs.product != 'false' || %s) }}"
STACK_GATE = "${{ !cancelled() && (needs.scope.outputs.stack != 'false' || %s) }}"
# A job that follows `test` only to run after it: a skipped `test` (nothing selected it) is fine, a
# failed one is not. The status function is explicit, so the job states what it accepts.
ORDERED_GATE = "${{ !cancelled() && " + TEST_OK + " && (needs.scope.outputs.product != 'false' || %s) }}"
# The same for a stack job: it also needs the image, which only runs for a stack path or a selected job.
STACK_AFTER_TEST_GATE = ("${{ !cancelled() && needs.image.result == 'success' && " + TEST_OK
                         + " && (needs.scope.outputs.stack != 'false' || %s) }}")
# A job that downloads what `test` produced: no status function, so it runs only after a green `test`.
NEEDS_RUN_GATE = "${{ needs.scope.outputs.product != 'false' || %s }}"

CI_GATES = {
    "test-shard": CANONICAL_GATE,
    "test": CANONICAL_GATE,
    "bench": CANONICAL_GATE,
    "image": STACK_GATE,
    "tuple-smoke": STACK_AFTER_TEST_GATE,
    "e2e-compat-group": STACK_AFTER_TEST_GATE,
    "e2e-fleet": STACK_AFTER_TEST_GATE,
    "e2e-ui": STACK_AFTER_TEST_GATE,
    "tuple-contract": ORDERED_GATE,
    "fuzz-smoke-shard": ORDERED_GATE,
    "race-stress-shard": ORDERED_GATE,
    "docker": ORDERED_GATE,
    "stress-tests": ORDERED_GATE,
    "memory-leak-tests": ORDERED_GATE,
    "conformance": NEEDS_RUN_GATE,
}
AGGREGATORS = {"e2e-compat": "e2e-compat-group", "race-stress": "race-stress-shard", "fuzz-smoke": "fuzz-smoke-shard"}
# The gate of every other workflow's selectable job: (file, job) -> (lane output, the ids it tests).
WORKFLOW_LANES = {
    ("compat-loki.yaml", "loki-pinned"): "stack",
    ("compat-vl.yaml", "vl-pinned"): "stack",
    ("compat-drilldown.yaml", "drilldown-pinned-runtime"): "stack",
    ("compat-drilldown.yaml", "drilldown-grafana-pr-matrix"): "stack",
    ("e2e-pipeline.yml", "e2e-pipeline"): "stack",
    ("security-pr.yaml", "security-runtime"): "stack",
}
LANES = ("product", "stack", "perf")
ALWAYS_ON = ("lint", "helm")
STATUS_FUNCTIONS = re.compile(r"\b(always|cancelled|failure)\(\)")


def reads_scope(job):
    return "needs.scope" in job["body"]


def reaches(jobs, start, targets):
    """True when `start` depends, directly or not, on any job in `targets`."""
    seen = set()
    stack = list(jobs[start]["needs"])
    while stack:
        name = stack.pop()
        if name in targets:
            return True
        if name in seen or name not in jobs:
            continue
        seen.add(name)
        stack.extend(jobs[name]["needs"])
    return False


def expected_gate(name):
    gate = CI_GATES[name]
    return gate % sel("ci/" + name) if "%s" in gate else gate


def ci_problems(text):
    """Return every violation of the scope wiring in ci.yaml's text."""
    jobs = parse_jobs(text)
    found = []

    scope = jobs.get("scope")
    if scope is None:
        return ["ci.yaml has no scope job"]
    if scope["uses"] != "./.github/workflows/change-scope.yaml":
        found.append("scope must call ./.github/workflows/change-scope.yaml")
    for permission in ("contents: read", "pull-requests: read"):
        if permission not in scope["body"]:
            found.append(f"scope job must grant {permission}")

    for name in ALWAYS_ON:
        if name not in jobs:
            found.append(f"{name} job is missing")
        elif jobs[name]["if"] is not None:
            found.append(f"{name} must always run, but has if: {jobs[name]['if']}")

    # change_scope.py and ci.yaml list the same jobs: a job missing from the registry would never be
    # selected (or, narrowed, would run on no change at all).
    registered = set(CI_JOBS) | set(ALWAYS_ON_CI)
    for name in jobs:
        if name not in registered:
            found.append(f"{name} is not in change_scope.py (CI_JOBS or ALWAYS_ON_CI)")
    for name in registered:
        if name not in jobs:
            found.append(f"{name} is in change_scope.py but not in ci.yaml")

    for name, job in jobs.items():
        if name == "scope":
            continue
        if reads_scope(job) and "scope" not in job["needs"]:
            found.append(f"{name} reads needs.scope but does not list scope in needs")
        if name in AGGREGATORS:
            shard = AGGREGATORS[name]
            if job["if"] != "always()":
                found.append(f"{name} must use if: always(), got {job['if']}")
            needles = ["needs.scope.outputs.product", '"skipped"', '= "false"',
                       "SELECTED: ${{ %s }}" % sel("ci/" + shard), '[ "$SELECTED" != "true" ]']
            if name == "e2e-compat":
                # Stack jobs run on a stack path, so a skipped group is a pass only
                # when the stack lane says false as well.
                needles += ["needs.scope.outputs.stack", '"$STACK" = "false"']
            for needle in needles:
                if needle not in job["body"]:
                    found.append(f"{name} must test the scope output: missing {needle}")
        elif name in CI_GATES:
            if "scope" not in job["needs"]:
                found.append(f"{name} must list scope in needs")
            if job["if"] != expected_gate(name):
                found.append(f"{name} must use its gate, got if: {job['if']}")
        elif name not in ALWAYS_ON:
            found.append(f"{name} has no gate in CI_GATES")

    for name in CI_JOBS:
        if name not in jobs or name in AGGREGATORS:
            continue
        if name not in CI_GATES:
            found.append(f"{name} is selectable but has no expected gate")

    # What a job downloads or follows is what the registry says it needs.
    for name, deps in CI_NEEDS_RUN.items():
        for dep in deps:
            if name in jobs and dep not in jobs[name]["needs"] and not reaches(jobs, name, {dep}):
                found.append(f"{name} must need {dep}: change_scope.py runs {dep} with it")
    for left, right in CI_PARTNERS:
        if right not in jobs or left not in jobs:
            continue
        if left not in jobs[right]["needs"]:
            found.append(f"{right} must need {left}: they are partners in change_scope.py")
    for producer, consumers in CI_CONSUMERS.items():
        for consumer in consumers:
            if consumer in jobs and not reaches(jobs, consumer, {producer}):
                found.append(f"{consumer} is a consumer of {producer} in change_scope.py but does not follow it")
    if "test" in jobs and "test-shard" not in jobs["test"]["needs"]:
        found.append("test must fan in test-shard")
    if "conformance" in jobs and not reaches(jobs, "conformance", {"test"}):
        found.append("conformance must need test: it reads the unit results of test")

    # A job downstream of `test` that is not one of the exact gates above must keep the implicit
    # success(): no status function in its condition.
    for name, job in jobs.items():
        if name in AGGREGATORS or name in CI_GATES or name == "scope":
            continue
        if reaches(jobs, name, {"test"}) and job["if"] and STATUS_FUNCTIONS.search(job["if"]):
            found.append(f"{name} is downstream of test but its if: bypasses success(): {job['if']}")
    for name in ("fuzz-smoke-shard", "race-stress-shard", "tuple-contract", "docker", "stress-tests", "memory-leak-tests",
                 "tuple-smoke", "e2e-compat-group", "e2e-fleet", "e2e-ui"):
        if name in jobs and not {"scope", "test"} <= set(jobs[name]["needs"]):
            found.append(f"{name} must need scope and test, got {sorted(jobs[name]['needs'])}")
    for name in ("tuple-smoke", "e2e-compat-group", "e2e-fleet", "e2e-ui"):
        if name in jobs and "image" not in jobs[name]["needs"]:
            found.append(f"{name} must need the image")
    return found


class ScopeWiringTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = CI.read_text()
        cls.jobs = parse_jobs(cls.text)

    def test_parser_finds_the_jobs(self):
        for name in ("scope", "test", "lint", "helm", "bench", "conformance", "e2e-compat", "race-stress"):
            self.assertIn(name, self.jobs)
        self.assertEqual(self.jobs["e2e-compat"]["needs"], ["scope", "e2e-compat-group"])
        self.assertEqual(self.jobs["docker"]["needs"], ["scope", "test"])

    def test_ci_scope_wiring(self):
        self.assertEqual(ci_problems(self.text), [])

    def test_lint_and_helm_have_no_condition(self):
        for name in ALWAYS_ON:
            self.assertIsNone(self.jobs[name]["if"], name)
            self.assertNotIn("scope", self.jobs[name]["needs"], name)

    def test_aggregators_read_the_scope_output(self):
        for name in AGGREGATORS:
            self.assertIn("needs.scope.outputs.product", self.jobs[name]["body"], name)
        self.assertIn("needs.scope.outputs.stack", self.jobs["e2e-compat"]["body"])

    def test_infra_checks_run_in_lint_not_in_test(self):
        moved = (
            "./scripts/ci/sync_observability_assets.sh --check",
            "go run ./cmd/configdoc -check",
            "scripts.ci.tests.test_change_scope",
            "scripts.ci.tests.test_scope_wiring",
            "-run '^TestVictoriaLogsSupportPolicy$'",
            "./scripts/ci/validate_compose.sh",
            # Their paths are in the perf, stack and infra lanes, which skip the
            # Go unit tests: the bench module holds the drift guard of the three
            # Loki configs, and bench/ab/tests the A/B harness.
            "Benchmark harness vet and tests (bench module)",
            "python3 -m unittest discover -s bench/ab/tests",
            # bench/visual is mapped to the visual-smoke workflow alone, so a narrowed run no longer
            # reaches the conformance job that used to carry its unit tests.
            "python3 -m unittest discover -s bench/visual/tests",
        )
        for command in moved:
            self.assertIn(command, self.jobs["lint"]["body"], command)
        for command in moved[:3] + moved[6:]:
            self.assertNotIn(command, self.jobs["test"]["body"], command)
        self.assertNotIn("python3 -m unittest discover -s bench/ab/tests", self.jobs["conformance"]["body"])
        self.assertNotIn("python3 -m unittest discover -s bench/visual/tests", self.jobs["conformance"]["body"])

    def test_every_selectable_job_has_its_exact_gate(self):
        for name in CI_GATES:
            with self.subTest(name):
                self.assertEqual(self.jobs[name]["if"], expected_gate(name))
                self.assertIn(sel("ci/" + name), self.jobs[name]["if"])
                if CI_GATES[name] == STACK_AFTER_TEST_GATE:
                    self.assertEqual(self.jobs[name]["needs"], ["scope", "test", "image"])
                if CI_GATES[name] == ORDERED_GATE:
                    self.assertEqual(self.jobs[name]["needs"], ["scope", "test"])
        self.assertEqual(self.jobs["image"]["needs"], ["scope"])

    def test_the_registry_and_ci_yaml_list_the_same_jobs(self):
        self.assertEqual(set(self.jobs), set(CI_JOBS) | set(ALWAYS_ON_CI))
        self.assertEqual(ALL_CI, {"ci/" + name for name in CI_JOBS})
        self.assertLessEqual(ALL_CI, ALL_JOBS)

    def test_a_job_that_follows_test_for_ordering_accepts_a_skipped_test_only(self):
        # Selecting fuzz-smoke must not run the unit tests; a failed or cancelled test still stops it.
        for name in ("tuple-contract", "fuzz-smoke-shard", "race-stress-shard", "docker", "stress-tests",
                     "memory-leak-tests", "tuple-smoke", "e2e-compat-group", "e2e-fleet", "e2e-ui"):
            with self.subTest(name):
                self.assertIn(TEST_OK, self.jobs[name]["if"])
                self.assertIn("!cancelled()", self.jobs[name]["if"])
                self.assertNotIn("needs.test.result != ", self.jobs[name]["if"])
        # The job that downloads test's artifacts keeps the implicit success().
        self.assertNotRegex(self.jobs["conformance"]["if"], STATUS_FUNCTIONS)

    def test_other_workflows_gate_on_their_lane_or_their_selection(self):
        for (filename, name), lane in WORKFLOW_LANES.items():
            with self.subTest(f"{filename}:{name}"):
                jobs = parse_jobs((WORKFLOWS / filename).read_text())
                condition = jobs[name]["if"] or ""
                stem = filename.rsplit(".", 1)[0]
                self.assertIn("scope", jobs[name]["needs"])
                self.assertIn(f"(needs.scope.outputs.{lane} != 'false' || {sel(stem + '/' + name)})", condition)
                for other in LANES:
                    if other != lane:
                        self.assertNotIn(f"needs.scope.outputs.{other}", condition)

    def test_every_selectable_job_of_another_workflow_tests_its_own_id(self):
        # WORKFLOW_JOBS says which jobs a change to a workflow file runs; each must read its own id.
        for filename, (stem, names, extra) in WORKFLOW_JOBS.items():
            jobs = parse_jobs((REPO / filename).read_text())
            for name in names:
                with self.subTest(f"{stem}/{name}"):
                    self.assertIn(name, jobs)
                    self.assertIn(sel(f"{stem}/{name}"), jobs[name]["if"] or "")
            for flag in extra:
                with self.subTest(f"{stem}/{flag}"):
                    self.assertIn(sel(f"{stem}/{flag}"), (REPO / filename).read_text())

    def test_visual_smoke_requires_product_evidence_or_a_selection(self):
        jobs = parse_jobs((WORKFLOWS / "visual-smoke.yaml").read_text())
        # The required check name is the fan-in job; the plan and the shards sit behind the same gate.
        for name in ("visual-plan", "visual-smoke"):
            with self.subTest(name):
                self.assertIn("scope", jobs[name]["needs"])
                self.assertIn(f"(needs.scope.outputs.evidence == 'true' || {sel('visual-smoke/' + name)})",
                              jobs[name]["if"])
        self.assertIn("plan.py --base", jobs["visual-plan"]["body"])
        self.assertEqual(jobs["visual-smoke"]["needs"], ["scope", "visual-plan", "visual-shard"])
        self.assertEqual(jobs["visual-shard"]["needs"], ["scope", "visual-plan"])
        # A shard runs only for a plan that has captures, and never when the plan itself failed.
        self.assertIn("needs.visual-plan.outputs.run == 'true'", jobs["visual-shard"]["if"])
        self.assertIn("needs.visual-plan.result == 'success'", jobs["visual-shard"]["if"])
        self.assertIn("Core set montages", jobs["visual-comment"]["body"])

    def test_visual_shards_are_one_stack_each_and_one_fan_in(self):
        jobs = parse_jobs((WORKFLOWS / "visual-smoke.yaml").read_text())
        shard, plan, fan_in = jobs["visual-shard"]["body"], jobs["visual-plan"]["body"], jobs["visual-smoke"]["body"]
        # every shard gets the same seed end, its own share of one plan, and uploads under its own name
        self.assertIn("SEED_END: ${{ needs.visual-plan.outputs.end }}", shard)
        self.assertIn('--end "$SEED_END"', shard)
        self.assertIn('shards.py --plan /tmp/visual-plan/plan.json --shard "$SHARD" --of "$SHARDS"', shard)
        self.assertIn("name: visual-shard-${{ matrix.shard }}", shard)
        self.assertIn("matrix: ${{ fromJSON(needs.visual-plan.outputs.matrix) }}", shard)
        self.assertIn("fail-fast: false", shard)
        self.assertIn("python3 bench/visual/shards.py --plan", plan)
        # the fan-in joins them once: merge, then the same renderer and gate as a single job
        self.assertIn('merge.py --plan /tmp/visual-plan/plan.json --of "$SHARDS"', fan_in)
        self.assertIn("pattern: visual-shard-*", fan_in)
        for artifact in ("visual-montage", "visual-quality-evidence", "visual-baseline-candidate"):
            self.assertIn(f"name: {artifact}", fan_in)
        self.assertNotIn("ci_run.py", fan_in)
        self.assertNotIn("playwright", fan_in)
        # a shard whose stack did not complete is red itself, so a re-run of failed jobs repeats it
        self.assertIn('if [ -f "$OUT/error.json" ]', shard)
        self.assertIn("Fail the shard when its stack did not complete", shard)
        # the jobs that run the pull request's code hold a read-only token
        for name in ("visual-plan", "visual-shard", "visual-smoke"):
            self.assertIn("contents: read", jobs[name]["body"])
            self.assertNotIn("contents: write", jobs[name]["body"])
            self.assertNotIn("pull-requests: write", jobs[name]["body"])

    def test_ab_smoke_requires_product_evidence_or_a_selection(self):
        jobs = parse_jobs((WORKFLOWS / "perf-ab.yaml").read_text())
        self.assertEqual(jobs["perf-smoke"]["if"],
                         "${{ !cancelled() && (needs.scope.outputs.evidence == 'true' || %s) }}"
                         % sel("perf-ab/perf-smoke"))

    def test_quality_report_requires_product_evidence_or_a_selection(self):
        job = parse_jobs((WORKFLOWS / "pr-quality-report.yaml").read_text())["report"]
        self.assertIn("scope", job["needs"])
        self.assertEqual(job["if"], "${{ !cancelled() && (needs.scope.outputs.evidence == 'true' || %s) }}"
                         % sel("pr-quality-report/report"))
        self.assertFalse(product_evidence(["scripts/ci/change_scope.py", ".github/workflows/change-scope.yaml"]))
        self.assertFalse(product_evidence(["docs/release.md", "CHANGELOG.md"]))
        self.assertTrue(product_evidence(["scripts/ci/change_scope.py", "internal/proxy/proxy.go"]))

    def test_scope_exposes_evidence_independently_of_forced_product(self):
        text = (WORKFLOWS / "change-scope.yaml").read_text()
        self.assertIn("value: ${{ jobs.scope.outputs.evidence }}", text)
        self.assertIn("evidence: ${{ steps.result.outputs.evidence }}", text)
        self.assertIn("jobs: ${{ steps.result.outputs.jobs }}", text)
        self.assertIn("CLASSIFIED_EVIDENCE: ${{ steps.classify.outputs.evidence }}", text)
        self.assertIn('if [ "$CLASSIFIED_EVIDENCE" = "false" ]; then evidence=false; fi', text)
        self.assertNotIn('if [ "$product" = "true" ]; then evidence=true; fi', text)

    def test_every_workflow_reading_scope_declares_it(self):
        for path in sorted(WORKFLOWS.glob("*.y*ml")):
            text = path.read_text()
            if "needs.scope" not in text:
                continue
            jobs = parse_jobs(text)
            self.assertIn("scope", jobs, f"{path.name} reads needs.scope without a scope job")
            self.assertEqual(jobs["scope"]["uses"], "./.github/workflows/change-scope.yaml", path.name)
            for name, job in jobs.items():
                if name != "scope" and reads_scope(job):
                    self.assertIn("scope", job["needs"], f"{path.name}: {name}")

    # --- the baseline run -------------------------------------------------------------------------

    HEAVY = ("perf-ab.yaml", "visual-smoke.yaml", "compat-vl.yaml")

    def test_only_the_heavy_workflows_read_the_baseline(self):
        readers = sorted(p.name for p in WORKFLOWS.glob("*.y*ml")
                         if "outputs.baseline" in p.read_text() and p.name != "change-scope.yaml")
        self.assertEqual(readers, sorted(self.HEAVY))
        self.assertNotIn("outputs.baseline", CI.read_text())  # the ordinary CI is the same for every size

    def test_heavy_workflows_only_run_for_pull_requests_into_main(self):
        # A stacked pull request (base is not main) starts none of them, so it never pays for a baseline.
        for name in self.HEAVY + ("ci.yaml",):
            with self.subTest(name):
                text = (WORKFLOWS / name).read_text()
                self.assertRegex(text, r"(?m)^  pull_request:\n    branches: \[main\]")

    def test_the_scope_workflow_fetches_the_history_a_push_needs(self):
        text = (WORKFLOWS / "change-scope.yaml").read_text()
        # 0 (the full history, for a push of several commits) is falsy, so it must be the last operand.
        self.assertIn("fetch-depth: ${{ github.event_name != 'push' && 2 || 0 }}", text)
        self.assertNotIn("&& 0 ||", text)
        for path in sorted(WORKFLOWS.glob("*.y*ml")):
            self.assertNotRegex(path.read_text(), r"&&\s+0\s+\|\|", path.name)
        # The change of a push is its commit against the previous tip of main, one that may be unknown.
        self.assertIn('base="$BEFORE"', text)
        self.assertIn('git cat-file -e "${base}^{commit}"', text)
        self.assertIn('--narrow', text)

    def test_the_scope_workflow_reports_a_baseline_that_fails_off(self):
        text = (WORKFLOWS / "change-scope.yaml").read_text()
        self.assertIn("baseline: ${{ steps.result.outputs.baseline }}", text)
        self.assertRegex(text, r"(?m)^          baseline=false$")  # the default before any evidence
        self.assertIn('if [ "$CLASSIFIED_BASELINE" = "true" ]; then baseline=true; fi', text)
        # The classifier gets the diff, the base branch and the labels.
        for needle in ("--numstat", '--base-ref "$BASE_REF"', '--labels "$LABELS"',
                       "BASE_REF: ${{ github.event.pull_request.base.ref }}"):
            self.assertIn(needle, text)
        # ci-full and baseline force the product lanes here; the classifier gives both the baseline.
        self.assertIn("ci-full", text)
        self.assertIn("if printf '%s\\n' \"$labels\" | grep -qx 'baseline'; then\n              mode=full", text)
        self.assertIn("elif printf '%s\\n' \"$labels\" | grep -qx 'ci-full'; then\n              mode=narrow", text)
        # The baseline can only come from a classifier that ran on a pull request diff.
        self.assertIn('[ "$DIFF" = "true" ]', text)

    def test_the_perf_workflow_runs_every_shape_on_a_baseline_and_keeps_its_budget_otherwise(self):
        text = (WORKFLOWS / "perf-ab.yaml").read_text()
        jobs = parse_jobs(text)
        plan, shard, job = jobs["perf-plan"], jobs["perf-smoke-shard"], jobs["perf-smoke"]
        full = sel("perf-ab/full")
        for body in (plan["body"], job["body"]):
            self.assertIn("BASELINE: ${{ needs.scope.outputs.baseline == 'true' || %s }}" % full, body)
        self.assertIn("timeout-minutes: ${{ (needs.scope.outputs.baseline == 'true' || %s) && 60 || 45 }}" % full,
                      shard["body"])
        self.assertIn('if [ "$BASELINE" = "true" ]; then\n            python3 bench/ab/selection.py --all', plan["body"])
        self.assertIn('selection.py --base "$BASE"', plan["body"])  # an ordinary pull request keeps the selection
        self.assertIn('--baseline-candidate "$OUT/baseline-candidate"', job["body"])
        self.assertIn("name: perf-baseline-candidate", job["body"])
        # A baseline on a non-runtime PR does not run A/B; a change to the A/B harness selects it, in full.
        for name in ("perf-plan", "perf-smoke"):
            self.assertEqual(jobs[name]["if"], "${{ !cancelled() && (needs.scope.outputs.evidence == 'true' || %s) }}"
                             % sel("perf-ab/perf-smoke"))

    def test_the_perf_smoke_shards_measure_and_the_fan_in_decides(self):
        jobs = parse_jobs((WORKFLOWS / "perf-ab.yaml").read_text())
        plan, shard, fan_in = jobs["perf-plan"]["body"], jobs["perf-smoke-shard"], jobs["perf-smoke"]
        self.assertIn("pr_smoke.py --split 4", plan)
        self.assertIn("shard: ${{ fromJSON(needs.perf-plan.outputs.shards) }}", shard["body"])
        self.assertEqual(shard["if"], "${{ !cancelled() && needs.perf-plan.result == 'success' && "
                                      "needs.perf-plan.outputs.run == 'true' }}")
        self.assertIn('--selection "/tmp/ab-plan/shard-${SHARD}.json"', shard["body"])
        self.assertIn("name: perf-ab-shard-${{ matrix.shard }}", shard["body"])
        self.assertIn("overwrite: true", shard["body"])
        # a shard that did not complete is red itself, so a re-run of failed jobs repeats it
        self.assertIn("Fail the shard when it did not complete", shard["body"])
        # the fan-in waits for every shard and names every planned one, so a missing shard is incomplete
        self.assertEqual(set(fan_in["needs"]), {"scope", "perf-plan", "perf-smoke-shard"})
        self.assertIn("pattern: perf-ab-shard-*", fan_in["body"])
        self.assertIn('pr_smoke.py --merge "${DIRS[@]}"', fan_in["body"])
        self.assertIn("Fail when the shapes could not be planned", fan_in["body"])
        # the evidence collector reads the merged outputs under the old artifact name
        self.assertIn("name: perf-ab-smoke", fan_in["body"])
        # the jobs that run the pull request's code hold a read-only token; only the fan-in comments
        for name in ("perf-plan", "perf-smoke-shard"):
            self.assertIn("contents: read", jobs[name]["body"])
            self.assertNotIn("contents: write", jobs[name]["body"])
            self.assertNotIn("pull-requests: write", jobs[name]["body"])
        self.assertIn("pull-requests: write", fan_in["body"])

    def test_the_visual_workflow_captures_every_page_on_a_baseline(self):
        text = (WORKFLOWS / "visual-smoke.yaml").read_text()
        jobs = parse_jobs(text)
        body = jobs["visual-plan"]["body"] + jobs["visual-smoke"]["body"]
        full = sel("visual-smoke/full")
        self.assertIn("timeout-minutes: ${{ (needs.scope.outputs.baseline == 'true' || %s) && 45 || 25 }}" % full,
                      jobs["visual-shard"]["body"])
        self.assertIn("BASELINE: ${{ needs.scope.outputs.baseline == 'true' || %s }}" % full, jobs["visual-plan"]["body"])
        self.assertIn("BASELINE: ${{ needs.scope.outputs.baseline == 'true' || %s }}" % full, jobs["visual-smoke"]["body"])
        self.assertIn('plan.py --all --base "$BASE" --out "$OUT/plan.json"', body)
        self.assertIn('plan.py --base "$BASE" --out "$OUT/plan.json"', body)
        self.assertIn("name: visual-baseline-candidate", body)
        self.assertIn("scope", jobs["visual-comment"]["needs"])
        # A full baseline can exceed the publisher's bounded file count; only
        # montages selected by the trusted base-branch renderer are published.
        publish = jobs["visual-publish"]
        self.assertIn("scope", publish["needs"])
        self.assertNotIn("needs.scope.outputs.baseline != 'true'", publish["if"])
        self.assertIn("ref: ${{ github.event.pull_request.base.sha }}", publish["body"])
        self.assertIn("--images /tmp/visual-publish-images.json", publish["body"])
        self.assertIn("--names-file /tmp/visual-publish-images.json", publish["body"])
        self.assertIn("--mode branch", publish["body"])
        self.assertIn("/tmp/visual-publish-selected", publish["body"])
        # An older base uses its own renderer and publisher with a staged <=80 image set.
        self.assertIn("!cancelled()", jobs["visual-comment"]["if"])
        self.assertIn("visual-publish", jobs["visual-comment"]["needs"])
        self.assertIn("replaceAll('<details><summary>', '<details open><summary>')", jobs["visual-comment"]["body"])

    def test_a_label_that_does_not_rerun_visual_smoke_cannot_cancel_its_run(self):
        # `gh pr create --label ci-full` fires `opened` and `labeled`: the second, all-skipped run used to
        # share the pull request's concurrency group and cancel the first.
        text = (WORKFLOWS / "visual-smoke.yaml").read_text()
        group = re.search(r"(?m)^concurrency:\n  group: (.*)\n  cancel-in-progress: true$", text)
        self.assertIsNotNone(group)
        self.assertIn("github.event.label.name != 'visual-change-expected' && '-label' || ''", group.group(1))
        self.assertIn("github.event.action == 'labeled' || github.event.action == 'unlabeled'", group.group(1))

    def test_visual_artifacts_replace_previous_attempt_before_commenting(self):
        text = (WORKFLOWS / "visual-smoke.yaml").read_text()
        for name in ("visual-montage", "visual-baseline-candidate", "visual-quality-evidence"):
            self.assertRegex(text, rf"name: {name}\n\s+overwrite: true")

    def test_the_vl_matrix_runs_on_a_baseline_and_otherwise_as_before(self):
        jobs = parse_jobs((WORKFLOWS / "compat-vl.yaml").read_text())
        matrix = jobs["vl-matrix"]
        # (the job parser strips the quotes around an unbraced condition, the last one too)
        self.assertEqual(matrix["if"].rstrip("'"), ("github.event_name == 'schedule' || "
                                                    "github.event_name == 'workflow_dispatch' || "
                                                    "needs.scope.outputs.baseline == 'true'").rstrip("'"))
        self.assertEqual(matrix["needs"], ["vl-matrix-source", "scope"])
        # The pinned tuple still runs on every pull request in the stack lane.
        self.assertIn("needs.scope.outputs.stack != 'false'", jobs["vl-pinned"]["if"])
        self.assertNotIn("baseline", jobs["vl-pinned"]["if"])

    def test_a_baseline_run_writes_only_bounded_visual_proof(self):
        for name in self.HEAVY:
            with self.subTest(name):
                text = (WORKFLOWS / name).read_text()
                for job_name, job in parse_jobs(text).items():
                    if "baseline" not in job["body"].lower():
                        continue
                    if name == "visual-smoke.yaml" and job_name == "visual-publish":
                        self.assertIn("ref: ${{ github.event.pull_request.base.sha }}", job["body"])
                        self.assertIn("--names-file /tmp/visual-publish-images.json", job["body"])
                        continue  # only trusted base-branch code publishes bounded proof PNGs
                    for forbidden in ("git push", "git commit", "contents: write", "commit_via_api", "gh pr create"):
                        self.assertNotIn(forbidden, job["body"], f"{name}:{job_name}")

    def test_the_checks_catch_broken_wiring(self):
        text = self.text

        def gate_of(name):
            return "    if: " + expected_gate(name) + "\n"

        def without_selection(name):
            """The job's gate with the part that tests its own id removed."""
            old = expected_gate(name)
            lane = "product" if "product" in old else "stack"
            new = old.replace(" || " + sel("ci/" + name), "").replace("(needs.scope.outputs.%s != 'false')" % lane,
                                                                      "needs.scope.outputs.%s != 'false'" % lane)
            return text.replace("    if: " + old, "    if: " + new, 1)

        mutations = {
            "ungated test": text.replace("    needs: [scope, test-shard]\n" + gate_of("test"), "    needs: [scope, test-shard]\n", 1),
            "scope missing from needs": text.replace("  bench:\n    needs: [scope]", "  bench:\n    needs: []", 1),
            "weaker gate": text.replace(expected_gate("test-shard"), "${{ needs.scope.outputs.product != 'false' }}", 1),
            "test job no longer selectable": without_selection("test"),
            "image no longer selectable": without_selection("image"),
            "bench no longer selectable": without_selection("bench"),
            "ordered job no longer selectable": text.replace(
                " || " + sel("ci/fuzz-smoke-shard"), "", 1),
            "image gated on the product lane": text.replace(
                "  image:\n    needs: [scope]\n" + gate_of("image"),
                "  image:\n    needs: [scope]\n    if: " + CANONICAL_GATE % sel("ci/image") + "\n", 1),
            "stack job runs after a failed test": text.replace(
                gate_of("e2e-ui"), gate_of("e2e-ui").replace(TEST_OK, "needs.test.result != 'cancelled'"), 1),
            "ordered job runs after a failed test": text.replace(
                gate_of("docker"), gate_of("docker").replace(TEST_OK, "needs.test.result != 'cancelled'"), 1),
            "ordered job needs a green test": text.replace(
                gate_of("tuple-contract"), gate_of("tuple-contract").replace(TEST_OK, "needs.test.result == 'success'"), 1),
            "stack job without scope in needs": text.replace(
                "  tuple-smoke:\n    runs-on: ubuntu-latest\n    needs: [scope, test, image]",
                "  tuple-smoke:\n    runs-on: ubuntu-latest\n    needs: [test, image]", 1),
            "stack job ignores the image": text.replace(
                "  e2e-ui:\n    runs-on: ubuntu-latest\n    needs: [scope, test, image]\n" + gate_of("e2e-ui"),
                "  e2e-ui:\n    runs-on: ubuntu-latest\n    needs: [scope, test, image]\n    if: " + CANONICAL_GATE % sel("ci/e2e-ui") + "\n", 1),
            "other job downstream of test uses the stack route": text.replace(
                "  docker:\n    runs-on: ubuntu-latest\n    needs: [scope, test]\n" + gate_of("docker"),
                "  docker:\n    runs-on: ubuntu-latest\n    needs: [scope, test]\n    if: " + STACK_AFTER_TEST_GATE % sel("ci/docker") + "\n", 1),
            "aggregator ignores the stack lane": text.replace(
                ' && [ "$STACK" = "false" ] && [ "$SELECTED" != "true" ]; then', ' && [ "$SELECTED" != "true" ]; then', 1),
            "aggregator passes a selected group that was skipped": text.replace(
                ' && [ "$SELECTED" != "true" ]; then\n            echo "Skipped: no product path changed"',
                '; then\n            echo "Skipped: no product path changed"', 1),
            "aggregator ignores its group's selection": text.replace(
                "SELECTED: ${{ %s }}" % sel("ci/race-stress-shard"), "SELECTED: false", 1),
            "condition on lint": text.replace(
                "  lint:\n    runs-on: ubuntu-latest\n",
                "  lint:\n    if: ${{ needs.scope.outputs.product != 'false' }}\n    runs-on: ubuntu-latest\n", 1),
            "conformance no longer reads the test results": text.replace(
                "skipped when `test` fails or is skipped.\n    needs: [scope, test]\n",
                "skipped when `test` fails or is skipped.\n    needs: [scope]\n", 1),
            "conformance gets a status function": text.replace(
                gate_of("conformance"), "    if: ${{ !cancelled() && needs.scope.outputs.product != 'false' }}\n", 1),
            "aggregator ignores scope": text.replace(
                "PRODUCT: ${{ needs.scope.outputs.product }}", "PRODUCT: true", 1),
            "a job missing from the registry": text.replace("  lint:\n", "  brand-new-job:\n    runs-on: ubuntu-latest\n    steps: []\n\n  lint:\n", 1),
        }
        for label, mutated in mutations.items():
            with self.subTest(label):
                self.assertNotEqual(mutated, text, "mutation did not apply")
                self.assertNotEqual(ci_problems(mutated), [], label)


if __name__ == "__main__":
    unittest.main()
