import os
import re
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "ci" / "scan_release_image.sh"
WORKFLOWS_DIR = REPO / ".github" / "workflows"
RELEASE_WORKFLOWS = ("auto-release.yaml", "release.yaml")

IMAGE_REF = "ghcr.io/acme/loki-vl-proxy@sha256:" + "a" * 64
TRIVY_PIN = re.compile(r"aquasec/trivy:[\w.]+@sha256:[0-9a-f]{64}")

# A stand-in for docker that records its arguments and answers from a script of
# exit codes, one per call; "F" prints a Trivy fatal error line before failing.
FAKE_DOCKER = """#!/bin/sh
echo "$@" >> "$FAKE_LOG"
n=$(wc -l < "$FAKE_LOG" | tr -d ' ')
code=$(echo "$FAKE_EXITS" | cut -d, -f"$n")
case "$code" in
  F) echo "FATAL	Fatal error	init error: db download failed"; exit 1 ;;
  *) [ "$code" = 0 ] || echo "Total: 1 (HIGH: 1, CRITICAL: 0)"; exit "$code" ;;
esac
"""


def steps(name):
    text = (WORKFLOWS_DIR / name).read_text()
    parts = re.split(r"(?m)^      - (?:name: |uses: )", text)
    return [p.splitlines()[0] for p in parts[1:]]


class ScanReleaseImageScriptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        bindir = self.root / "bin"
        bindir.mkdir()
        for name, body in (("docker", FAKE_DOCKER), ("sleep", "#!/bin/sh\nexit 0\n")):
            path = bindir / name
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        self.log = self.root / "docker.log"
        self.ignore = self.root / ".trivyignore.yaml"
        self.ignore.write_text("misconfigurations: []\n")
        self.env = {
            "PATH": f"{bindir}:{os.environ['PATH']}",
            "FAKE_LOG": str(self.log),
            "IMAGE_REF": IMAGE_REF,
            "IGNORE_FILE": str(self.ignore),
            "TRIVY_CACHE_DIR": str(self.root / "cache"),
        }

    def run_script(self, exits="0,0", **overrides):
        env = {**self.env, "FAKE_EXITS": exits, **overrides}
        env = {k: v for k, v in env.items() if v is not None}
        self.log.write_text("")
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    def calls(self):
        return [line for line in self.log.read_text().splitlines() if line]

    def test_scans_both_platforms_with_the_pull_request_scan_settings(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.calls()
        self.assertEqual(len(calls), 2)
        self.assertIn("--platform linux/amd64", calls[0])
        self.assertIn("--platform linux/arm64", calls[1])
        for call in calls:
            for flag in (IMAGE_REF, "--severity HIGH,CRITICAL", "--ignore-unfixed", "--exit-code 1", "--ignorefile /config/.trivyignore.yaml"):
                self.assertIn(flag, call)
            self.assertIn("--scanners vuln,misconfig,secret", call)

    def test_uses_the_trivy_image_of_the_pull_request_scan(self):
        pr_pin = TRIVY_PIN.search((WORKFLOWS_DIR / "security-pr.yaml").read_text()).group(0)
        self.assertEqual(TRIVY_PIN.search(SCRIPT.read_text()).group(0), pr_pin)
        self.run_script()
        self.assertTrue(all(pr_pin in call for call in self.calls()))

    def test_a_finding_fails_without_retry(self):
        result = self.run_script(exits="1,0")
        self.assertEqual(result.returncode, 1)
        # amd64 failed once, arm64 was still scanned; neither was retried.
        self.assertEqual(len(self.calls()), 2)
        self.assertIn("Trivy gate failed", result.stdout)

    def test_a_failed_arm64_scan_fails_the_gate(self):
        self.assertEqual(self.run_script(exits="0,1").returncode, 1)

    def test_a_fatal_error_is_retried(self):
        result = self.run_script(exits="F,F,0,0")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.calls()), 4)

    def test_a_persistent_fatal_error_fails_after_three_attempts(self):
        result = self.run_script(exits="F,F,F,0")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(self.calls()), 4)  # 3 amd64 attempts + 1 arm64

    def test_requires_a_digest_reference(self):
        for ref in ("", "ghcr.io/acme/loki-vl-proxy:1.2.3", "ghcr.io/acme/loki-vl-proxy@sha256:abc"):
            result = self.run_script(IMAGE_REF=ref)
            self.assertEqual(result.returncode, 1, ref)
            self.assertEqual(self.calls(), [], ref)

    def test_requires_the_ignore_file(self):
        result = self.run_script(IGNORE_FILE=str(self.root / "missing.yaml"))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.calls(), [])


class ReleaseWorkflowOrderTests(unittest.TestCase):
    def index(self, order, prefix):
        matches = [i for i, name in enumerate(order) if name.startswith(prefix)]
        self.assertEqual(len(matches), 1, f"{prefix!r} in {matches}")
        return matches[0]

    def test_the_gate_runs_after_the_push_and_before_anything_is_signed_tagged_or_published(self):
        for name in RELEASE_WORKFLOWS:
            order = steps(name)
            gate = self.index(order, "Scan the pushed image with Trivy")
            self.assertEqual(gate, self.index(order, "Build and push Docker image") + 1, name)
            for later in (
                "Sign the container image",
                "Attest container image provenance (GHCR)",
                "Publish the image tags",
                "Publish Helm chart to GHCR",
                "Create GitHub Release",
            ):
                self.assertLess(gate, self.index(order, later), f"{name}: {later}")

    def test_the_release_tag_is_created_only_after_the_gate(self):
        order = steps("auto-release.yaml")
        tag = self.index(order, "Create release tag")
        self.assertEqual(tag, self.index(order, "Scan the pushed image with Trivy") + 1)
        self.assertLess(tag, self.index(order, "Sign the container image"))

    def test_the_scan_target_is_the_pushed_digest(self):
        for name in RELEASE_WORKFLOWS:
            text = (WORKFLOWS_DIR / name).read_text()
            self.assertIn("IMAGE_REF: ${{ steps.image-tags.outputs.ghcr_image }}@${{ steps.build.outputs.digest }}", text, name)


if __name__ == "__main__":
    unittest.main()
