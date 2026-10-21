import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

# `uses: <value>` as a YAML key on its own line or at the start of a list item.
USES_LINE = re.compile(r"^\s*(?:-\s+)?uses:\s*(?P<value>\S+)(?P<rest>.*)$")
# owner/repo or owner/repo/path pinned to a full commit SHA.
ACTION_REF = re.compile(r"^[\w.-]+/[\w.-]+(?:/[\w./-]+)?@[0-9a-f]{40}$")
DOCKER_DIGEST = re.compile(r"^docker://\S+@sha256:[0-9a-f]{64}$")
# The version the SHA corresponds to, kept on the same line: `# v7`, `# v5.0.1`.
VERSION_COMMENT = re.compile(r"^\s*#\s*v\d")


def workflow_files():
    files = []
    for pattern in ("*.yaml", "*.yml"):
        files += sorted((REPO / ".github" / "workflows").glob(pattern))
    for pattern in ("action.yaml", "action.yml"):
        files += sorted((REPO / ".github" / "actions").rglob(pattern))
    return files


def pin_violations(text):
    """Return (line number, line, reason) for every `uses:` that is not pinned."""
    found = []
    for number, line in enumerate(text.splitlines(), start=1):
        match = USES_LINE.match(line)
        if not match:
            continue
        value = match.group("value").strip("\"'")
        rest = match.group("rest")
        if value.startswith("./"):
            continue
        if value.startswith("docker://"):
            if not DOCKER_DIGEST.match(value):
                found.append((number, line.strip(), "docker:// reference without an @sha256 digest"))
            continue
        if not ACTION_REF.match(value):
            found.append((number, line.strip(), "not owner/repo[/path]@<40-hex commit SHA>"))
        elif not VERSION_COMMENT.match(rest):
            found.append((number, line.strip(), "no '# v<version>' comment on the same line"))
    return found


class WorkflowPinTests(unittest.TestCase):
    def test_workflow_files_are_found(self):
        self.assertGreater(len(workflow_files()), 0)

    def test_every_uses_is_pinned_with_a_version_comment(self):
        problems = []
        for path in workflow_files():
            for number, line, reason in pin_violations(path.read_text()):
                problems.append(f"{path.relative_to(REPO)}:{number}: {reason}: {line}")
        self.assertEqual(problems, [], "\n" + "\n".join(problems))

    def test_unpinned_references_are_reported(self):
        sha = "0123456789abcdef0123456789abcdef01234567"
        sample = "\n".join(
            [
                "steps:",
                f"  - uses: actions/checkout@{sha}  # v7",  # good
                f"  - uses: github/codeql-action/init@{sha} # v4.1.2",  # good, subpath
                "  - uses: ./.github/actions/local",  # good, local
                f"  - uses: docker://alpine:3@sha256:{'a' * 64}",  # good, digest
                "  - uses: actions/checkout@v4  # v4",  # tag, not a SHA
                f"  - uses: actions/setup-go@{sha}",  # no comment
                f"  - uses: actions/setup-go@{sha}  # latest",  # comment without a version
                f"  - uses: actions/cache@{sha[:39]}  # v6",  # short SHA
                "  - uses: docker://alpine:3",  # no digest
                f"  - uses: docker://alpine:3@sha256:{'b' * 63}",  # short digest
                "  - run: echo 'uses: actions/checkout@v4'",  # not a uses key
            ]
        )
        bad = [number for number, _, _ in pin_violations(sample)]
        self.assertEqual(bad, [6, 7, 8, 9, 10, 11])

    def test_good_references_pass(self):
        sha = "0123456789abcdef0123456789abcdef01234567"
        sample = f"- uses: actions/checkout@{sha}  # v7\n  uses: \"owner/repo@{sha}\" # v1.2.3\n"
        self.assertEqual(pin_violations(sample), [])


if __name__ == "__main__":
    unittest.main()
