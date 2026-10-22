import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

# Registries and owners whose images CI runs through `docker run` or a script default.
TOOL_IMAGE = re.compile(
    r"(?<![\w./])(?:ghcr\.io/(?!reliablyobserve/)|docker\.io/(?!reliablyobserve/)|aquasec/|hadolint/|rhysd/|projectdiscovery/|zaproxy/)"
    r"[\w./-]+:[\w.-]+(?P<digest>@sha256:[0-9a-f]{64})?"
)
# `image: nginx:1.27-alpine` style references to the helper images of the test stacks and
# the Helm test pods; Loki, VictoriaLogs and Grafana are pinned by the compatibility matrix.
HELPER_IMAGE = re.compile(r"^\s*image:\s*(?P<ref>(?:busybox|nginx|python):\S+)")
DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")

TOOL_SOURCES = [
    *sorted((REPO / ".github" / "workflows").glob("*.y*ml")),
    *sorted((REPO / "scripts" / "ci").glob("*.sh")),
]
HELPER_SOURCES = [
    REPO / "charts" / "loki-vl-proxy" / "templates" / "tests" / "test-connection.yaml",
    *sorted((REPO / "test").glob("*/docker-compose*.yml")),
]


def unpinned_tool_images(text):
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if line.lstrip().startswith("#"):
            continue
        for match in TOOL_IMAGE.finditer(line):
            if not match.group("digest"):
                found.append((number, match.group(0)))
    return found


def unpinned_helper_images(text):
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        match = HELPER_IMAGE.match(line)
        if match and not DIGEST.search(match.group("ref")):
            found.append((number, match.group("ref")))
    return found


def pip_without_hashes(text):
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        if line.lstrip().startswith("#"):
            continue
        if re.search(r"\bpip3?\s+install\b", line) and "--require-hashes" not in line:
            found.append((number, line.strip()))
    return found


class ToolPinTests(unittest.TestCase):
    def test_sources_are_found(self):
        self.assertGreater(len(TOOL_SOURCES), 0)
        self.assertTrue(all(path.exists() for path in HELPER_SOURCES))

    def test_tool_images_carry_a_digest(self):
        problems = [
            f"{path.relative_to(REPO)}:{number}: {ref}"
            for path in TOOL_SOURCES
            for number, ref in unpinned_tool_images(path.read_text())
        ]
        self.assertEqual(problems, [], "\n" + "\n".join(problems))

    def test_helper_images_carry_a_digest(self):
        problems = [
            f"{path.relative_to(REPO)}:{number}: {ref}"
            for path in HELPER_SOURCES
            for number, ref in unpinned_helper_images(path.read_text())
        ]
        self.assertEqual(problems, [], "\n" + "\n".join(problems))

    def test_pip_installs_require_hashes(self):
        problems = [
            f"{path.relative_to(REPO)}:{number}: {line}"
            for path in TOOL_SOURCES
            for number, line in pip_without_hashes(path.read_text())
        ]
        self.assertEqual(problems, [], "\n" + "\n".join(problems))

    def test_requirement_files_are_fully_hashed(self):
        for name in ("security/requirements-semgrep.txt", "bench/visual/requirements.txt"):
            text = (REPO / name).read_text()
            blocks = re.split(r"\n(?=[A-Za-z0-9][\w.-]*==)", text)[1:]
            self.assertGreater(len(blocks), 0, name)
            for block in blocks:
                self.assertIn("--hash=sha256:", block, f"{name}: {block.splitlines()[0]}")

    def test_violations_are_reported(self):
        digest = "a" * 64
        self.assertEqual(
            unpinned_tool_images(
                "\n".join(
                    [
                        f"  ghcr.io/gitleaks/gitleaks:v8.28.0@sha256:{digest} \\",
                        "  aquasec/trivy:0.71.0 \\",
                        "  # projectdiscovery/nuclei:latest",
                        'IMAGE="${ZAP_IMAGE:-ghcr.io/zaproxy/zaproxy:stable}"',
                    ]
                )
            ),
            [(2, "aquasec/trivy:0.71.0"), (4, "ghcr.io/zaproxy/zaproxy:stable")],
        )
        self.assertEqual(
            unpinned_helper_images(f"image: busybox:1.36@sha256:{digest}\n  image: nginx:alpine\n"),
            [(2, "nginx:alpine")],
        )
        self.assertEqual(
            pip_without_hashes("pip install semgrep==1.0\npip install --require-hashes -r r.txt\n"),
            [(1, "pip install semgrep==1.0")],
        )


if __name__ == "__main__":
    unittest.main()
