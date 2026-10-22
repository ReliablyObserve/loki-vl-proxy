import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "ci" / "collect_release_assets.sh"
WORKFLOWS = [REPO / ".github" / "workflows" / name for name in ("auto-release.yaml", "release.yaml")]

DIGEST = "sha256:" + "a" * 64
CHART_DIGEST = "sha256:" + "b" * 64
DH_CHART_DIGEST = "sha256:" + "c" * 64
BUNDLES = [
    "BUNDLE_BINARIES",
    "BUNDLE_CHART",
    "BUNDLE_IMAGE_GHCR",
    "BUNDLE_IMAGE_DH",
    "BUNDLE_SBOM_LINUX_AMD64",
    "BUNDLE_SBOM_LINUX_ARM64",
    "BUNDLE_SBOM_DARWIN_AMD64",
    "BUNDLE_SBOM_DARWIN_ARM64",
]


def bundle(label):
    return {"mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json", "dsseEnvelope": {"payload": label}}


@unittest.skipUnless(shutil.which("jq"), "jq is required")
class CollectReleaseAssetsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.dist = self.root / "dist"
        self.dist.mkdir()
        self.env = {
            "PATH": os.environ["PATH"],
            "DIST_DIR": str(self.dist),
            "VERSION": "1.2.3",
            "GHCR_IMAGE": "ghcr.io/acme/loki-vl-proxy",
            "IMAGE_DIGEST": DIGEST,
            "CHART_GHCR_REF": "ghcr.io/acme/charts/loki-vl-proxy",
            "CHART_GHCR_DIGEST": CHART_DIGEST,
            "HAS_DOCKERHUB": "true",
            "IMAGE_SBOM_JSON": str(self.write_sbom()),
        }
        for name in BUNDLES:
            # The attest actions end the file with a newline.
            path = self.root / f"{name}.json"
            path.write_text(json.dumps(bundle(name)) + "\n")
            self.env[name] = str(path)

    def write_sbom(self, platforms=("linux/amd64", "linux/arm64")):
        path = self.root / "image-sbom.json"
        path.write_text(
            json.dumps({p: {"SPDX": {"spdxVersion": "SPDX-2.3", "name": p}} for p in platforms}, indent=2)
        )
        return path

    def run_script(self, **overrides):
        env = {**self.env, **overrides}
        env = {k: v for k, v in env.items() if v is not None}
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    def test_bundles_become_one_json_object_per_line(self):
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        lines = (self.dist / "loki-vl-proxy-1.2.3.intoto.jsonl").read_text().splitlines()
        self.assertEqual([json.loads(line)["dsseEnvelope"]["payload"] for line in lines],
                         ["BUNDLE_BINARIES", "BUNDLE_CHART", "BUNDLE_IMAGE_GHCR", "BUNDLE_IMAGE_DH",
                          "BUNDLE_SBOM_LINUX_AMD64", "BUNDLE_SBOM_LINUX_ARM64",
                          "BUNDLE_SBOM_DARWIN_AMD64", "BUNDLE_SBOM_DARWIN_ARM64"])

    def test_a_bundle_file_without_a_trailing_newline_still_gets_its_own_line(self):
        Path(self.env["BUNDLE_CHART"]).write_text(json.dumps(bundle("BUNDLE_CHART")))
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        lines = (self.dist / "loki-vl-proxy-1.2.3.intoto.jsonl").read_text().splitlines()
        self.assertEqual(len(lines), 8)

    def test_docker_hub_bundle_is_skipped_when_docker_hub_is_not_pushed(self):
        proc = self.run_script(HAS_DOCKERHUB="false", BUNDLE_IMAGE_DH="")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        lines = (self.dist / "loki-vl-proxy-1.2.3.intoto.jsonl").read_text().splitlines()
        self.assertEqual(len(lines), 7)
        self.assertNotIn("docker.io", (self.dist / "image-digests.txt").read_text())

    def test_missing_or_malformed_bundles_fail_the_run(self):
        for name in BUNDLES:
            with self.subTest(missing=name):
                proc = self.run_script(**{name: str(self.root / "absent.json")})
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(name, proc.stdout)
        notabundle = self.root / "notabundle.json"
        notabundle.write_text(json.dumps({"hello": "world"}))
        proc = self.run_script(BUNDLE_CHART=str(notabundle))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("BUNDLE_CHART", proc.stdout)

    def test_docker_hub_bundle_is_required_when_docker_hub_is_pushed(self):
        proc = self.run_script(BUNDLE_IMAGE_DH="")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("BUNDLE_IMAGE_DH", proc.stdout)

    def test_image_sbom_is_written_per_platform(self):
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        for arch in ("amd64", "arm64"):
            doc = json.loads((self.dist / f"loki-vl-proxy-image-1.2.3-linux-{arch}.spdx.json").read_text())
            self.assertEqual(doc, {"spdxVersion": "SPDX-2.3", "name": f"linux/{arch}"})

    def test_an_image_without_an_sbom_for_a_platform_fails_the_run(self):
        proc = self.run_script(IMAGE_SBOM_JSON=str(self.write_sbom(platforms=("linux/amd64",))))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("linux/arm64", proc.stdout)
        empty = self.root / "empty.json"
        empty.write_text("null")
        self.assertNotEqual(self.run_script(IMAGE_SBOM_JSON=str(empty)).returncode, 0)

    def test_digests_list_image_and_chart_references(self):
        proc = self.run_script(CHART_DH_REF="docker.io/acme/charts/loki-vl-proxy", CHART_DH_DIGEST=DH_CHART_DIGEST)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            (self.dist / "image-digests.txt").read_text().splitlines(),
            [
                f"ghcr.io/acme/loki-vl-proxy@{DIGEST}",
                f"docker.io/reliablyobserve/loki-vl-proxy@{DIGEST}",
                f"ghcr.io/acme/charts/loki-vl-proxy@{CHART_DIGEST}",
                f"docker.io/acme/charts/loki-vl-proxy@{DH_CHART_DIGEST}",
            ],
        )

    def test_digests_without_docker_hub(self):
        proc = self.run_script(HAS_DOCKERHUB="false", BUNDLE_IMAGE_DH="")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(
            (self.dist / "image-digests.txt").read_text().splitlines(),
            [f"ghcr.io/acme/loki-vl-proxy@{DIGEST}", f"ghcr.io/acme/charts/loki-vl-proxy@{CHART_DIGEST}"],
        )

    def test_a_file_holding_two_bundles_is_rejected(self):
        Path(self.env["BUNDLE_CHART"]).write_text(json.dumps(bundle("a")) + "\n" + json.dumps(bundle("b")) + "\n")
        proc = self.run_script()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("BUNDLE_CHART", proc.stdout)

    def test_bad_digests_and_versions_fail_the_run(self):
        for overrides in (
            {"IMAGE_DIGEST": "latest"},
            {"CHART_GHCR_DIGEST": ""},
            {"CHART_DH_REF": "docker.io/acme/charts/loki-vl-proxy", "CHART_DH_DIGEST": "sha256:xyz"},
            {"VERSION": "dev"},
        ):
            with self.subTest(overrides=overrides):
                self.assertNotEqual(self.run_script(**overrides).returncode, 0)


class ReleaseWorkflowWiringTests(unittest.TestCase):
    """Both release workflows must attach the same assets, built by the same script."""

    def test_new_assets_are_attached_by_both_workflows(self):
        for path in WORKFLOWS:
            text = path.read_text()
            with self.subTest(workflow=path.name):
                files = text.split("          files: |\n", 1)[1].split("\n\n", 1)[0].split()
                for glob in (
                    "dist/loki-vl-proxy-*.tgz.sigstore.json",
                    "dist/loki-vl-proxy-*.intoto.jsonl",
                    "dist/loki-vl-proxy-image-*.spdx.json",
                    "dist/image-digests.txt",
                ):
                    self.assertIn(glob, files)
                # Collecting happens before the release is created.
                self.assertLess(text.index("scripts/ci/collect_release_assets.sh"), text.index("- name: Create GitHub Release"))
                self.assertLess(text.index("cosign sign-blob --yes --bundle \"dist/loki-vl-proxy-${VERSION}.tgz.sigstore.json\""),
                                text.index("- name: Create GitHub Release"))

    def test_every_bundle_env_names_an_attest_step_with_an_id(self):
        for path in WORKFLOWS:
            text = path.read_text()
            with self.subTest(workflow=path.name):
                referenced = set(re.findall(r"steps\.(attest-[\w-]+)\.outputs\.bundle-path", text))
                self.assertEqual(len(referenced), 8)
                for step_id in referenced:
                    self.assertRegex(text, rf"(?m)^        id: {re.escape(step_id)}$")

    def test_release_yaml_runs_the_collector_from_the_workflow_commit_not_the_tag_checkout(self):
        text = (REPO / ".github" / "workflows" / "release.yaml").read_text()
        self.assertNotIn("bash scripts/ci/collect_release_assets.sh", text)
        self.assertIn('git show "${WORKFLOW_SHA}:scripts/ci/collect_release_assets.sh"', text)
        self.assertIn("WORKFLOW_SHA: ${{ github.sha }}", text)

    def test_the_image_sbom_read_is_retried(self):
        for path in WORKFLOWS:
            with self.subTest(workflow=path.name):
                self.assertIn("for ATTEMPT in 1 2 3; do", path.read_text())

    def test_no_expression_is_pasted_into_the_collection_scripts(self):
        for path in WORKFLOWS:
            text = path.read_text()
            start = text.index("- name: Collect provenance bundles, image SBOMs and digests")
            end = text.index("- name: Append verification details to the release notes")
            run = text[start:end].split("        run: |\n", 1)[1]
            with self.subTest(workflow=path.name):
                self.assertNotIn("${{", run)


if __name__ == "__main__":
    unittest.main()
