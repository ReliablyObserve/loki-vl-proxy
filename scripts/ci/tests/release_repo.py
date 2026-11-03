"""A throwaway git repository that behaves like main for the release tests."""

import os
import subprocess
import tempfile
from pathlib import Path

from scripts.ci import changelog_release

README = (
    "# Proxy\n\n"
    "[![Tests](https://img.shields.io/badge/tests-1%20passed-brightgreen)](#tests)\n"
    "[![Coverage](https://img.shields.io/badge/coverage-80%25-green)](#tests)\n"
    "[![Lines of Code](https://img.shields.io/badge/go%20loc-1.0k-blue)](https://github.com/x/y)\n"
)
CHART = 'apiVersion: v2\nname: x\nversion: {v}\nappVersion: "{v}"\n'
BASE_CHANGELOG = "# Changelog\n\n## [Unreleased]\n\n## [2.62.0] - 2026-11-03\n\n### Fixed\n\n- Old entry.\n\n"


def sh(repo, *args, env=None, check=True):
    result = subprocess.run(args, cwd=repo, env={**os.environ, **(env or {})}, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}: {result.stderr}{result.stdout}")
    return result.stdout.strip()


def add_entry(text, category, entry):
    """Append a bullet under `### category` of [Unreleased] (heading made if absent)."""
    lines = text.split("\n")
    head, end = next((h, e) for n, h, e in changelog_release.split_sections(lines) if n == "Unreleased")
    body = [l for l in lines[head + 1 : end]]
    while body and not body[0].strip():
        body.pop(0)
    while body and not body[-1].strip():
        body.pop()
    marker = f"### {category}"
    if marker in body:
        start = body.index(marker)
        stop = start + 1
        while stop < len(body) and not body[stop].startswith("### "):
            stop += 1
        group = body[start:stop]
        while group and not group[-1].strip():
            group.pop()
        body = body[:start] + group + ["", entry] + ([""] if stop < len(body) else []) + body[stop:]
    else:
        body += ([""] if body else []) + [marker, "", entry]
    return "\n".join(lines[: head + 1] + [""] + body + [""] + lines[end:])


class Repo:
    def __init__(self, chart="2.62.0"):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = self._tmp.name
        sh(self.path, "git", "init", "-q", "-b", "main")
        sh(self.path, "git", "config", "user.email", "t@example.com")
        sh(self.path, "git", "config", "user.name", "t")
        sh(self.path, "git", "config", "commit.gpgsign", "false")
        sh(self.path, "git", "config", "tag.gpgsign", "false")
        self.write("CHANGELOG.md", BASE_CHANGELOG)
        self.write("README.md", README)
        self.write("charts/loki-vl-proxy/Chart.yaml", CHART.format(v=chart))
        self.write("internal/proxy/seed.go", "package proxy\n")
        self.commit("seed", date="2026-11-03T08:00:00Z")
        sh(self.path, "git", "tag", "v2.62.0")
        self.clock = 0

    def close(self):
        self._tmp.cleanup()

    def write(self, name, content):
        full = Path(self.path) / name
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")

    def read(self, name):
        return (Path(self.path) / name).read_text(encoding="utf-8")

    def commit(self, subject, date=None, files=None, entry=None, category="Fixed"):
        for name, content in (files or {}).items():
            self.write(name, content)
        if entry:
            self.write("CHANGELOG.md", add_entry(self.read("CHANGELOG.md"), category, entry))
        if date is None:
            self.clock = getattr(self, "clock", 0) + 1
            date = f"2026-11-03T10:{self.clock:02d}:00Z"
        env = {"GIT_AUTHOR_DATE": date, "GIT_COMMITTER_DATE": date}
        sh(self.path, "git", "add", "-A")
        sh(self.path, "git", "commit", "-q", "--allow-empty", "-m", subject, env=env)
        return self.head()

    def head(self):
        return sh(self.path, "git", "rev-parse", "HEAD")

    def tag(self, name, rev="HEAD"):
        sh(self.path, "git", "tag", name, rev)

    def checkout(self, rev):
        sh(self.path, "git", "checkout", "-q", rev)

    def merge_metadata(self, version, subject=None):
        """What the merged metadata pull request leaves on main (regenerated on its tip)."""
        text = changelog_release.apply_missing(self.read("CHANGELOG.md"), self.path, version)
        self.write("CHANGELOG.md", text)
        chart = self.read("charts/loki-vl-proxy/Chart.yaml")
        cur = next(l.split(": ")[1] for l in chart.splitlines() if l.startswith("version:"))
        if changelog_release.version_tuple(version) > changelog_release.version_tuple(cur):
            self.write("charts/loki-vl-proxy/Chart.yaml", CHART.format(v=version))
        return self.commit(subject or f"docs: sync release metadata for v{version} (#9)")
