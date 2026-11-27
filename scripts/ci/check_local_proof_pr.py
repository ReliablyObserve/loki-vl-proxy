#!/usr/bin/env python3
"""Fail a pull request whose description has no filled-in "Local proof" section.

The gate reads the description from PR_BODY, the author from PR_AUTHOR and the
draft state from PR_DRAFT. It checks that something was written, not what: the
reviewer judges the evidence. Drafts and the pull requests that automation opens
pass.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / ".github" / "pull_request_template.md"

HEADING = "local proof"

# Logins that open generated pull requests: releases, scheduled reports, dependency updates.
AUTOMATION_AUTHORS = frozenset({"reliablyobserve-bot", "github-actions[bot]", "dependabot[bot]"})

# A line that says nothing on its own.
FILLER = re.compile(r"(?i)^(?:todo|tbd|n/?a|none|not applicable|pending|\.\.\.|@@\w+@@)?[.:]?$")
MARKER = re.compile(r"^(?:(?:[-*+>]|\d+\.)(?:\s+|$))?(?:\[[ xX]\](?:\s+|$))?")


def section(body: str) -> str | None:
    """The text under the "## Local proof" heading, or None without that heading.

    HTML comments do not count. A "## " line inside a code fence is not a heading.
    """
    body = re.sub(r"<!--.*?(?:-->|\Z)", "", body.replace("\r\n", "\n"), flags=re.DOTALL)
    found: list[str] | None = None
    inside = fenced = False
    for line in body.split("\n"):
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
        elif not fenced and re.match(r"#{1,2}\s", line):
            title = line.lstrip("#").strip().lower()
            inside = line.startswith("## ") and (title == HEADING or title.startswith(HEADING + " ("))
            if inside and found is None:
                found = []
            continue
        if inside:
            found.append(line)
    return None if found is None else "\n".join(found)


def content_lines(text: str) -> list[str]:
    lines = []
    for raw in text.split("\n"):
        line = MARKER.sub("", raw.strip()).strip()
        if line.startswith(("```", "~~~", "###")) or FILLER.match(line):
            continue
        lines.append(line)
    return lines


def problem(body: str, template: str) -> str | None:
    """Why the description fails the gate, or None when it passes."""
    text = section(body)
    if text is None:
        return 'the description has no "## Local proof" section'
    placeholders = set(content_lines(section(template) or ""))
    if all(line in placeholders for line in content_lines(text)):
        return 'the "## Local proof" section is empty or still holds only the template text'
    return None


def main() -> int:
    if os.environ.get("PR_DRAFT", "").lower() == "true":
        print("local proof gate: skipped (draft; it runs when the pull request is ready for review)")
        return 0
    author = os.environ.get("PR_AUTHOR", "")
    if author in AUTOMATION_AUTHORS:
        print(f"local proof gate: skipped (opened by {author})")
        return 0
    why = problem(os.environ.get("PR_BODY", ""), TEMPLATE.read_text(encoding="utf-8"))
    if why:
        print(
            f"local proof gate: {why}. Add the evidence from your own build and test of the branch "
            '(see "Local proof in the pull request before CI" in AGENTS.md), then save the description.',
            file=sys.stderr,
        )
        return 1
    print("local proof gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
