import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOWS = sorted((REPO / ".github" / "workflows").glob("*.y*ml"))

# Workflows that build the artifacts users receive. A Go version they pin
# changes the released binaries, yet .github/ never ships by itself, so these
# pins are tied to go.mod and the Dockerfile, which do.
RELEASE_BUILDING = ("auto-release.yaml", "release.yaml")

GO_DIRECTIVE = re.compile(r"^go\s+(?P<version>\d+(?:\.\d+)*)\s*$", re.MULTILINE)
TOOLCHAIN_DIRECTIVE = re.compile(r"^toolchain\s+go(?P<version>\d+(?:\.\d+)*)\s*$", re.MULTILINE)
DOCKER_BUILDER = re.compile(r"^FROM\s+golang:(?P<version>\d+(?:\.\d+)*)[-@\s]", re.MULTILINE)
SETUP_GO_VERSION = re.compile(r"^\s*go-version:\s*[\"']?(?P<version>[^\"'\s#]+)", re.MULTILINE)


def go_mod_versions(text):
    """The go directive, then the toolchain directive when there is one."""
    found = {}
    go = GO_DIRECTIVE.search(text)
    if go:
        found["go.mod go directive"] = go.group("version")
    toolchain = TOOLCHAIN_DIRECTIVE.search(text)
    if toolchain:
        found["go.mod toolchain directive"] = toolchain.group("version")
    return found


def dockerfile_versions(text):
    return {
        "Dockerfile builder image #%d" % index: match.group("version")
        for index, match in enumerate(DOCKER_BUILDER.finditer(text), start=1)
    }


def workflow_versions(name, text):
    return {
        "%s go-version #%d" % (name, index): match.group("version")
        for index, match in enumerate(SETUP_GO_VERSION.finditer(text), start=1)
    }


def pinned_versions():
    versions = {}
    versions.update(go_mod_versions((REPO / "go.mod").read_text(encoding="utf-8")))
    versions.update(dockerfile_versions((REPO / "Dockerfile").read_text(encoding="utf-8")))
    for path in WORKFLOWS:
        versions.update(workflow_versions(path.name, path.read_text(encoding="utf-8")))
    return versions


def disagreements(versions):
    """The pins that differ from the go.mod go directive."""
    reference = versions.get("go.mod go directive")
    return {k: v for k, v in sorted(versions.items()) if v != reference}


class ParserTests(unittest.TestCase):
    def test_go_mod(self):
        text = "module x\n\ngo 1.27.3\n\ntoolchain go1.27.4\n"
        self.assertEqual(
            go_mod_versions(text),
            {"go.mod go directive": "1.27.3", "go.mod toolchain directive": "1.27.4"},
        )
        self.assertEqual(go_mod_versions("module x\n\ngo 1.27.3\n"), {"go.mod go directive": "1.27.3"})

    def test_dockerfile(self):
        text = "FROM golang:1.27.3-alpine3.24@sha256:abc AS builder\nFROM gcr.io/distroless/static\n"
        self.assertEqual(dockerfile_versions(text), {"Dockerfile builder image #1": "1.27.3"})

    def test_workflow(self):
        text = '  with:\n    go-version: "1.27.3"\n  - with:\n      go-version: 1.27.4  # note\n'
        self.assertEqual(
            workflow_versions("w.yaml", text),
            {"w.yaml go-version #1": "1.27.3", "w.yaml go-version #2": "1.27.4"},
        )

    def test_a_workflow_only_bump_is_caught(self):
        versions = {
            "go.mod go directive": "1.27.3",
            "Dockerfile builder image #1": "1.27.3",
            "release.yaml go-version #1": "1.27.4",
        }
        self.assertEqual(disagreements(versions), {"release.yaml go-version #1": "1.27.4"})

    def test_a_toolchain_directive_must_match_too(self):
        versions = {"go.mod go directive": "1.27.3", "go.mod toolchain directive": "1.27.4"}
        self.assertEqual(disagreements(versions), {"go.mod toolchain directive": "1.27.4"})

    def test_agreement_is_clean(self):
        self.assertEqual(disagreements({"go.mod go directive": "1.27.3", "a go-version #1": "1.27.3"}), {})


class GoToolchainPinTests(unittest.TestCase):
    def test_the_pins_are_found(self):
        versions = pinned_versions()
        self.assertIn("go.mod go directive", versions)
        self.assertIn("Dockerfile builder image #1", versions)
        for name in RELEASE_BUILDING:
            self.assertTrue(
                [k for k in versions if k.startswith(name + " go-version")],
                "%s has no go-version pin" % name,
            )

    def test_every_workflow_that_sets_up_go_pins_a_version(self):
        # An unpinned or file-based setup-go would bypass the check above.
        for path in WORKFLOWS:
            text = path.read_text(encoding="utf-8")
            uses = len(re.findall(r"uses:\s*actions/setup-go@", text))
            pinned = len(SETUP_GO_VERSION.findall(text))
            with self.subTest(path.name):
                self.assertEqual(uses, pinned, "setup-go steps without a literal go-version")
                self.assertNotIn("go-version-file", text)

    def test_one_go_version_everywhere(self):
        versions = pinned_versions()
        off = disagreements(versions)
        detail = "\n".join("  %s = %s" % item for item in off.items())
        self.assertEqual(
            off,
            {},
            "Go toolchain pins disagree with go.mod (%s):\n%s\n"
            "A Go bump changes the shipped binary, so it must touch go.mod and the "
            "Dockerfile, and every workflow must follow."
            % (versions.get("go.mod go directive"), detail),
        )


if __name__ == "__main__":
    unittest.main()
