#!/usr/bin/env python3

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from typing import Iterable


ROOT = pathlib.Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"

RELEASE_PREFIXES = (
    "feat",
    "fix",
    "perf",
    "revert",
)

IMPACTFUL_PATHS = (
    "cmd/",
    "internal/",
    "pkg/",
    "charts/",
)

UNIT_TEST_PATH_PREFIXES = (
    "cmd/",
    "internal/",
    "pkg/",
)

IMPACTFUL_FILES = {
    "Dockerfile",
    "go.mod",
    "go.sum",
}

NON_RELEASE_PATH_PREFIXES = (
    "docs/",
    "scripts/ci/tests/",
)

NON_RELEASE_FILES = {
    "README.md",
    "CHANGELOG.md",
    "LICENSE",
    "scripts/ci/check_changelog_pr.py",
}

RELEASE_METADATA_FILES = {
    "CHANGELOG.md",
    "README.md",
    "docs/observability.md",
    "charts/loki-vl-proxy/Chart.yaml",
}


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def extract_unreleased_section(text: str) -> str:
    match = re.search(
        r"^## \[Unreleased\]\s*\n(?P<body>.*?)(?=^## \[|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        return ""
    return match.group("body").strip()


def has_meaningful_changelog_content(section: str) -> bool:
    if not section.strip():
        return False
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("### "):
            continue
        if stripped.startswith("- "):
            return True
        return True
    return False


def is_release_commit(subject: str) -> bool:
    lowered = subject.strip().lower()
    if "breaking change" in lowered:
        return True
    return any(
        lowered.startswith(prefix + ":") or lowered.startswith(prefix + "(")
        for prefix in RELEASE_PREFIXES
    )


def is_release_path(path: str) -> bool:
    if is_unit_test_only_path(path):
        return False
    if path in IMPACTFUL_FILES:
        return True
    return any(path.startswith(prefix) for prefix in IMPACTFUL_PATHS)


def is_non_release_path(path: str) -> bool:
    if is_unit_test_only_path(path):
        return True
    if path in NON_RELEASE_FILES:
        return True
    return any(path.startswith(prefix) for prefix in NON_RELEASE_PATH_PREFIXES)


def is_unit_test_only_path(path: str) -> bool:
    return path.endswith("_test.go") and any(
        path.startswith(prefix) for prefix in UNIT_TEST_PATH_PREFIXES
    )


def should_require_changelog(commits: Iterable[str], files: Iterable[str]) -> bool:
    commit_list = [c for c in commits if c.strip()]
    file_list = [f for f in files if f.strip()]

    if any(is_release_commit(subject) for subject in commit_list):
        return True

    impactful = [f for f in file_list if is_release_path(f)]
    if impactful:
        return True

    non_release = [f for f in file_list if is_non_release_path(f)]
    return len(file_list) > 0 and len(non_release) != len(file_list)


def is_release_metadata_sync(files: Iterable[str]) -> bool:
    file_list = [f for f in files if f.strip()]
    if not file_list or "CHANGELOG.md" not in file_list:
        return False
    return all(path in RELEASE_METADATA_FILES for path in file_list)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()

    files = run_git("diff", "--name-only", f"{args.base}..{args.head}").splitlines()
    commits = run_git("log", "--pretty=format:%s", f"{args.base}..{args.head}").splitlines()

    base_text = run_git("show", f"{args.base}:CHANGELOG.md")
    # In PR workflows, checkout can point at the synthetic merge ref instead of
    # the PR head commit. Read changelog directly from --head to avoid false
    # negatives when base moved after the PR was opened.
    head_text = run_git("show", f"{args.head}:CHANGELOG.md")
    base_unreleased = extract_unreleased_section(base_text)
    head_unreleased = extract_unreleased_section(head_text)

    if is_release_metadata_sync(files):
        if head_unreleased.strip() == base_unreleased.strip():
            print(
                "changelog gate: release metadata sync must materialize Unreleased into a version section",
                file=sys.stderr,
            )
            return 1
        print("changelog gate: ok (release metadata sync)")
        return 0

    if not should_require_changelog(commits, files):
        print("changelog gate: skipped (no releasable changes detected)")
        return 0

    if "CHANGELOG.md" not in files:
        print(
            "changelog gate: CHANGELOG.md must be updated for feature/fix/perf or release-impacting PRs",
            file=sys.stderr,
        )
        return 1

    if not has_meaningful_changelog_content(head_unreleased):
        print(
            "changelog gate: Unreleased section must contain at least one changelog entry",
            file=sys.stderr,
        )
        return 1

    if head_unreleased.strip() == base_unreleased.strip():
        print(
            "changelog gate: Unreleased section was not updated in this PR",
            file=sys.stderr,
        )
        return 1

    print("changelog gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
