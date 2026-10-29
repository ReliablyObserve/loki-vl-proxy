"""Guards the wiring of the change-scope gate in the CI workflows.

A product job that skips is reported as a pass, so a mistake in the gate lets a
broken product pull request go green. These tests read the workflow text (stdlib
only; the parser understands the block layout the workflows use) and assert:

- lint and helm always run, so the infra checks they carry are never skipped;
- every job that reads `needs.scope` lists `scope` in `needs`, and in ci.yaml
  uses the gate of its lane (product, stack, or the stack gate of a job that
  follows `test`) or, for the two aggregators, `if: always()`;
- every job of every workflow gates on the lane it belongs to, and on no other;
- every ci.yaml job is gated directly or through a gated job;
- no job downstream of `test` can run when `test` failed, and only the stack
  jobs, with exactly the gate below, can run when `test` was skipped;
- both aggregators pass on skipped groups only when scope says `false`;
- the baseline output reaches only the three heavy workflows, only through
  `needs.scope.outputs.baseline == 'true'`, only for pull requests into main,
  and never commits anything.
"""

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO / ".github" / "workflows"
CI = WORKFLOWS / "ci.yaml"

CANONICAL_GATE = "${{ !cancelled() && needs.scope.outputs.product != 'false' }}"
STACK_GATE = "${{ !cancelled() && needs.scope.outputs.stack != 'false' }}"
# A stack job that follows `test`: it runs after a green `test`, and also when
# `test` was skipped because no product path changed but a stack path did. `==`
# on both lane outputs, so an empty output can never take the skipped route.
STACK_AFTER_TEST_GATE = (
    "${{ !cancelled() && needs.image.result == 'success' && (needs.test.result == 'success'"
    " || (needs.test.result == 'skipped' && needs.scope.outputs.product == 'false'"
    " && needs.scope.outputs.stack == 'true')) }}"
)
# ci.yaml jobs whose gate is not the product one.
CI_GATES = {
    "image": STACK_GATE,
    "tuple-smoke": STACK_AFTER_TEST_GATE,
    "e2e-compat-group": STACK_AFTER_TEST_GATE,
    "e2e-fleet": STACK_AFTER_TEST_GATE,
    "e2e-ui": STACK_AFTER_TEST_GATE,
}
# The lane output every other workflow's gated job reads: (file, job) -> lane.
WORKFLOW_LANES = {
    ("perf-ab.yaml", "perf-smoke"): "perf",
    ("visual-smoke.yaml", "visual-smoke"): "stack",
    ("compat-loki.yaml", "loki-pinned"): "stack",
    ("compat-vl.yaml", "vl-pinned"): "stack",
    ("compat-drilldown.yaml", "drilldown-pinned-runtime"): "stack",
    ("compat-drilldown.yaml", "drilldown-grafana-pr-matrix"): "stack",
    ("e2e-pipeline.yml", "e2e-pipeline"): "stack",
    ("security-pr.yaml", "security-runtime"): "stack",
    ("pr-quality-report.yaml", "report"): "product",
}
LANES = ("product", "stack", "perf")
ALWAYS_ON = ("lint", "helm")
AGGREGATORS = ("e2e-compat", "race-stress")
STATUS_FUNCTIONS = re.compile(r"\b(always|cancelled|failure)\(\)")

JOBS_KEY = re.compile(r"^jobs:\s*$")
JOB_KEY = re.compile(r"^  ([A-Za-z0-9_-]+):\s*(?:#.*)?$")
JOB_FIELD = re.compile(r"^    (needs|if|uses):\s*(.*?)\s*$")
LIST_ITEM = re.compile(r"^      -\s+(\S+)")


def parse_jobs(text):
    """Return {job id: {"needs", "if", "uses", "body"}} for a workflow's jobs."""
    jobs = {}
    lines = text.splitlines()
    in_jobs = False
    current = None
    for index, line in enumerate(lines):
        if not in_jobs:
            in_jobs = bool(JOBS_KEY.match(line))
            continue
        if line and not line.startswith((" ", "#")):
            break  # next top-level key
        key = JOB_KEY.match(line)
        if key:
            current = {"needs": [], "if": None, "uses": None, "body": []}
            jobs[key.group(1)] = current
            continue
        if current is None:
            continue
        current["body"].append(line)
        field = JOB_FIELD.match(line)
        if not field:
            continue
        name, value = field.groups()
        value = re.sub(r"\s+#.*$", "", value)
        if name == "needs":
            if value.startswith("["):
                current["needs"] = [n.strip() for n in value.strip("[]").split(",") if n.strip()]
            elif value:
                current["needs"] = [value]
            else:
                for follow in lines[index + 1:]:
                    item = LIST_ITEM.match(follow)
                    if not item:
                        break
                    current["needs"].append(item.group(1))
        elif name == "if":
            current["if"] = value.strip("\"'") if not value.startswith(("\"", "'")) else value[1:-1]
        else:
            current["uses"] = value
    for job in jobs.values():
        job["body"] = "\n".join(job["body"])
    return jobs


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

    for name, job in jobs.items():
        if name == "scope":
            continue
        if reads_scope(job) and "scope" not in job["needs"]:
            found.append(f"{name} reads needs.scope but does not list scope in needs")
        if name in AGGREGATORS:
            if job["if"] != "always()":
                found.append(f"{name} must use if: always(), got {job['if']}")
            needles = ["needs.scope.outputs.product", '"skipped"', '= "false"']
            if name == "e2e-compat":
                # Stack jobs run on a stack path, so a skipped group is a pass only
                # when the stack lane says false as well.
                needles += ["needs.scope.outputs.stack", '"$STACK" = "false"']
            for needle in needles:
                if needle not in job["body"]:
                    found.append(f"{name} must test the scope output: missing {needle}")
        elif "scope" in job["needs"] and job["if"] != CI_GATES.get(name, CANONICAL_GATE):
            found.append(f"{name} must use its lane's gate, got if: {job['if']}")

    gated = {n for n, j in jobs.items() if n != "scope" and "scope" in j["needs"]}
    for name, job in jobs.items():
        if name in ("scope", *ALWAYS_ON, *AGGREGATORS) or name in gated:
            continue
        if not reaches(jobs, name, gated):
            found.append(f"{name} is neither gated on scope nor downstream of a gated job")
    # `conformance` reads the unit results of `test`, so it is downstream of a gated job (checked above)
    # and skips with it; it carries no gate of its own.
    for name in ("test", "bench"):
        if name in jobs and name not in gated:
            found.append(f"{name} must be gated on scope")
    if "conformance" in jobs and not reaches(jobs, "conformance", {"test"}):
        found.append("conformance must need test: it reads the unit results of test")

    # A job downstream of `test` keeps the implicit success(): it must skip
    # when `test` skips and must not run when `test` failed. The stack jobs are
    # the one exception, and only with their exact gate (checked above).
    for name, job in jobs.items():
        if name in AGGREGATORS or name == "test":
            continue
        if name in CI_GATES and CI_GATES[name] == STACK_AFTER_TEST_GATE and job["if"] == STACK_AFTER_TEST_GATE:
            continue
        if reaches(jobs, name, {"test"}) and job["if"] and STATUS_FUNCTIONS.search(job["if"]):
            found.append(f"{name} is downstream of test but its if: bypasses success(): {job['if']}")
    for name, gate in CI_GATES.items():
        if gate == STACK_AFTER_TEST_GATE and name in jobs:
            needs = set(jobs[name]["needs"])
            if not {"scope", "test", "image"} <= needs:
                found.append(f"{name} must need scope, test and image, got {sorted(needs)}")
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
        self.assertEqual(self.jobs["docker"]["needs"], ["test"])

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
        )
        for command in moved:
            self.assertIn(command, self.jobs["lint"]["body"], command)
        for command in moved[:3] + moved[6:]:
            self.assertNotIn(command, self.jobs["test"]["body"], command)
        self.assertNotIn("python3 -m unittest discover -s bench/ab/tests", self.jobs["conformance"]["body"])

    def test_stack_jobs_follow_test_with_the_exact_gate(self):
        for name, gate in CI_GATES.items():
            with self.subTest(name):
                self.assertEqual(self.jobs[name]["if"], gate)
                if gate == STACK_AFTER_TEST_GATE:
                    self.assertEqual(self.jobs[name]["needs"], ["scope", "test", "image"])
        self.assertEqual(self.jobs["image"]["needs"], ["scope"])

    def test_other_workflows_gate_on_their_lane(self):
        for (filename, name), lane in WORKFLOW_LANES.items():
            with self.subTest(f"{filename}:{name}"):
                jobs = parse_jobs((WORKFLOWS / filename).read_text())
                condition = jobs[name]["if"] or ""
                self.assertIn("scope", jobs[name]["needs"])
                self.assertIn(f"needs.scope.outputs.{lane} != 'false'", condition)
                for other in LANES:
                    if other != lane:
                        self.assertNotIn(f"needs.scope.outputs.{other}", condition)

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
        self.assertIn("elif printf '%s\\n' \"$labels\" | grep -qx 'baseline'", text)
        # The baseline can only come from a classifier that ran on a pull request diff.
        self.assertIn('[ "$DIFF" = "true" ]', text)

    def test_the_perf_workflow_runs_every_shape_on_a_baseline_and_keeps_its_budget_otherwise(self):
        text = (WORKFLOWS / "perf-ab.yaml").read_text()
        job = parse_jobs(text)["perf-smoke"]
        self.assertIn("BASELINE: ${{ needs.scope.outputs.baseline }}", job["body"])
        self.assertIn("timeout-minutes: ${{ needs.scope.outputs.baseline == 'true' && 90 || 30 }}", job["body"])
        self.assertIn('if [ "$BASELINE" = "true" ]; then\n            python3 bench/ab/selection.py --all', job["body"])
        self.assertIn('selection.py --base "$BASE"', job["body"])  # an ordinary pull request keeps the selection
        self.assertIn('--baseline-candidate "$OUT/baseline-candidate"', job["body"])
        self.assertIn("name: perf-baseline-candidate", job["body"])
        # The gate is unchanged: a baseline never makes the job run when its lane says no.
        self.assertEqual(job["if"], "${{ !cancelled() && needs.scope.outputs.perf != 'false' }}")

    def test_the_visual_workflow_captures_every_page_on_a_baseline(self):
        text = (WORKFLOWS / "visual-smoke.yaml").read_text()
        jobs = parse_jobs(text)
        body = jobs["visual-smoke"]["body"]
        self.assertIn("timeout-minutes: ${{ needs.scope.outputs.baseline == 'true' && 90 || 30 }}", body)
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
        gate = f"    if: {CANONICAL_GATE}\n"
        mutations = {
            "ungated test": self.text.replace("    needs: [scope]\n" + gate + "    runs-on", "    runs-on", 1),
            "scope missing from needs": self.text.replace("  bench:\n    needs: [scope]", "  bench:\n    needs: []", 1),
            "weaker gate": self.text.replace(CANONICAL_GATE, "${{ needs.scope.outputs.product != 'false' }}", 1),
            "image gated on the product lane": self.text.replace(
                "  image:\n    needs: [scope]\n    if: " + STACK_GATE, "  image:\n    needs: [scope]\n    if: " + CANONICAL_GATE, 1
            ),
            "stack job runs after a failed test": self.text.replace(
                STACK_AFTER_TEST_GATE,
                STACK_AFTER_TEST_GATE.replace("needs.test.result == 'success' ||", "needs.test.result != 'cancelled' ||"),
                1,
            ),
            "skipped test route on an empty output": self.text.replace(
                STACK_AFTER_TEST_GATE,
                STACK_AFTER_TEST_GATE.replace("needs.scope.outputs.stack == 'true'", "needs.scope.outputs.stack != 'false'"),
                1,
            ),
            "stack job without scope in needs": self.text.replace(
                "  tuple-smoke:\n    runs-on: ubuntu-latest\n    needs: [scope, test, image]",
                "  tuple-smoke:\n    runs-on: ubuntu-latest\n    needs: [test, image]",
                1,
            ),
            "stack job ignores the image": self.text.replace(
                "  e2e-ui:\n    runs-on: ubuntu-latest\n    needs: [scope, test, image]\n    if: " + STACK_AFTER_TEST_GATE,
                "  e2e-ui:\n    runs-on: ubuntu-latest\n    needs: [scope, test, image]\n    if: " + CANONICAL_GATE,
                1,
            ),
            "other job downstream of test uses the stack route": self.text.replace(
                "  docker:\n    runs-on: ubuntu-latest\n    needs: [test]\n",
                "  docker:\n    if: " + STACK_AFTER_TEST_GATE + "\n    runs-on: ubuntu-latest\n    needs: [test]\n",
                1,
            ),
            "aggregator ignores the stack lane": self.text.replace(
                ' && [ "$STACK" = "false" ]; then', "; then", 1
            ),
            "dependent bypasses success()": self.text.replace(
                "  docker:\n    runs-on: ubuntu-latest\n    needs: [test]\n",
                "  docker:\n    if: always()\n    runs-on: ubuntu-latest\n    needs: [test]\n",
                1,
            ),
            "condition on lint": self.text.replace(
                "  lint:\n    runs-on: ubuntu-latest\n",
                "  lint:\n    if: ${{ needs.scope.outputs.product != 'false' }}\n    runs-on: ubuntu-latest\n",
                1,
            ),
            "conformance no longer reads the test results": self.text.replace(
                "skipped when `test` fails or is skipped.\n    needs: [test]\n",
                "skipped when `test` fails or is skipped.\n    needs: [scope]\n",
                1,
            ),
            "aggregator ignores scope": self.text.replace(
                "PRODUCT: ${{ needs.scope.outputs.product }}", "PRODUCT: true", 1
            ),
        }
        for label, mutated in mutations.items():
            with self.subTest(label):
                self.assertNotEqual(mutated, self.text, "mutation did not apply")
                self.assertNotEqual(ci_problems(mutated), [], label)


if __name__ == "__main__":
    unittest.main()
