"""Guards the wiring of the change-scope gate in the CI workflows.

A product job that skips is reported as a pass, so a mistake in the gate lets a
broken product pull request go green. These tests read the workflow text (stdlib
only; the parser understands the block layout the workflows use) and assert:

- lint and helm always run, so the infra checks they carry are never skipped;
- every job that reads `needs.scope` lists `scope` in `needs`, and in ci.yaml
  uses the canonical gate or, for the two aggregators, `if: always()`;
- every ci.yaml job is gated directly or through a gated job;
- no job downstream of `test` can run when `test` failed or was skipped;
- both aggregators pass on skipped groups only when scope says `false`.
"""

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO / ".github" / "workflows"
CI = WORKFLOWS / "ci.yaml"

CANONICAL_GATE = "${{ !cancelled() && needs.scope.outputs.product != 'false' }}"
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
            for needle in ("needs.scope.outputs.product", '"skipped"', '= "false"'):
                if needle not in job["body"]:
                    found.append(f"{name} must test the scope output: missing {needle}")
        elif "scope" in job["needs"] and job["if"] != CANONICAL_GATE:
            found.append(f"{name} must use the canonical gate, got if: {job['if']}")

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
    # when `test` skips and must not run when `test` failed.
    for name, job in jobs.items():
        if name in AGGREGATORS or name == "test":
            continue
        if reaches(jobs, name, {"test"}) and job["if"] and STATUS_FUNCTIONS.search(job["if"]):
            found.append(f"{name} is downstream of test but its if: bypasses success(): {job['if']}")
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

    def test_infra_checks_run_in_lint_not_in_test(self):
        moved = (
            "./scripts/ci/sync_observability_assets.sh --check",
            "go run ./cmd/configdoc -check",
            "scripts.ci.tests.test_change_scope",
            "scripts.ci.tests.test_scope_wiring",
            "-run '^TestVictoriaLogsSupportPolicy$'",
            "./scripts/ci/validate_compose.sh",
        )
        for command in moved:
            self.assertIn(command, self.jobs["lint"]["body"], command)
        for command in moved[:3]:
            self.assertNotIn(command, self.jobs["test"]["body"], command)

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

    def test_the_checks_catch_broken_wiring(self):
        gate = f"    if: {CANONICAL_GATE}\n"
        mutations = {
            "ungated test": self.text.replace("    needs: [scope]\n" + gate + "    runs-on", "    runs-on", 1),
            "scope missing from needs": self.text.replace("  bench:\n    needs: [scope]", "  bench:\n    needs: []", 1),
            "weaker gate": self.text.replace(CANONICAL_GATE, "${{ needs.scope.outputs.product != 'false' }}", 1),
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
