"""Exercise tracked-file discovery through the actual Compose validation script."""
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "validate_compose.sh"
REGISTRY_CASE = (
    "conformance/registry/cases/semantics/"
    "composed-parser-count-rate-bytes-vector-fold-values.yaml"
)


class ValidateComposeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        script = self.root / "scripts/ci/validate_compose.sh"
        script.parent.mkdir(parents=True)
        source = SCRIPT.read_text()
        script.write_text(source)
        self.script = script
        stacks = re.search(r"^STACKS=\(\n(.*?)^\)", source, re.M | re.S)
        self.assertIsNotNone(stacks)
        self.stacks = [shlex.split(line)[0] for line in stacks[1].splitlines() if line.strip()]
        for stack in self.stacks:
            for filename in stack.split():
                path = self.root / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("services: {}\n")
        binary = self.root / "bin"
        binary.mkdir()
        for name, body in {
            "git": '#!/bin/sh\ncat "$TRACKED_FIXTURE"\n',
            "docker": '#!/bin/sh\nprintf "%s\\n" "$*" >> "$DOCKER_CALLS"\nexit "${DOCKER_EXIT:-0}"\n',
        }.items():
            path = binary / name
            path.write_text(body)
            path.chmod(0o755)
        self.tracked = self.root / "tracked.txt"
        self.calls = self.root / "docker-calls.txt"
        self.env = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ["PATH"],
                        TRACKED_FIXTURE=str(self.tracked), DOCKER_CALLS=str(self.calls))

    def run_validator(self, tracked):
        self.tracked.write_text("\n".join(tracked) + "\n")
        self.calls.unlink(missing_ok=True)
        return subprocess.run(["bash", str(self.script)], cwd=self.root, env=self.env,
                              capture_output=True, text=True, timeout=10)

    def test_real_registry_composed_case_is_not_a_compose_file(self):
        self.assertTrue((SCRIPT.parents[2] / REGISTRY_CASE).is_file())
        result = self.run_validator(["docker-compose.yml", REGISTRY_CASE])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.calls.read_text().splitlines()
        self.assertEqual(len(calls), len(self.stacks))
        self.assertTrue(all(call.startswith("compose -f ") and call.endswith(" config -q")
                            for call in calls), calls)

    def test_similar_names_are_not_compose_files(self):
        result = self.run_validator([
            "docs/composed.yaml", "docs/composeguide.yml", "docs/docker-composed-parser.yaml",
            "compose-fixtures/ordinary.yaml", "nested/compose/.yaml",
        ])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_unlisted_conventional_compose_names_fail_before_docker(self):
        for filename in [
            "new/compose.yml", "new/compose.yaml", "new/docker-compose.yml",
            "new/docker-compose.yaml", "new/compose.review.yml", "new/compose-review.yaml",
            "new/docker-compose.review.yml", "new/docker-compose-review.yaml",
        ]:
            with self.subTest(filename=filename):
                result = self.run_validator([filename])
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("file=" + filename + "::tracked compose file is not listed", result.stdout)
                self.assertFalse(self.calls.exists(), "Unlisted Compose must fail before Docker runs")

    def test_invalid_listed_stack_remains_a_failure(self):
        self.env["DOCKER_EXIT"] = "1"
        result = self.run_validator(["docker-compose.yml"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docker compose config failed", result.stdout)
        self.assertEqual(len(self.calls.read_text().splitlines()), len(self.stacks))


if __name__ == "__main__":
    unittest.main()
