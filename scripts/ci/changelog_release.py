#!/usr/bin/env python3
"""Which CHANGELOG entries belong to a release, and how they move into it.

A release is the set of `[Unreleased]` entries added between the previous tag
and its own commit. Reading that set from git, entry by entry, makes the
release notes and the CHANGELOG move independent of when the metadata pull
request is made or merged:

  entries(tag) = Unreleased@tag - Unreleased@previous tag
                 - every entry already filed under a released section@tag

so a release run that starts while the previous release's metadata pull
request is still open does not repeat that release's entries, and entries added
to `[Unreleased]` after the tag are never part of it.

The move itself (`apply`) works on the CHANGELOG of the branch it is given
(current main): it takes exactly the tagged entries out of `[Unreleased]`, files
them under `## [x.y.z]` in version order and leaves every other line alone. A
tag whose section is already there is skipped, so the move is idempotent.

Usage:
    changelog_release.py notes --repo . --rev <sha> --prev-tag vX.Y.Z --version A.B.C
    changelog_release.py apply --repo . --changelog CHANGELOG.md --version A.B.C
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field

HEADER_RE = re.compile(r"^## \[([^\]]+)\]")
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
UNRELEASED = "Unreleased"


@dataclass(frozen=True)
class Entry:
    """One bullet (or paragraph) of a section, with the heading it sits under.

    `start` and `end` are line indexes in the text it was parsed from (end is
    exclusive and excludes trailing blank lines). They are not part of an
    entry's identity: two entries are the same when their lines are.
    """

    category: str | None
    lines: tuple[str, ...]
    start: int = field(default=-1, compare=False)
    end: int = field(default=-1, compare=False)
    # Blank lines the entry had above it in its source (tight lists have none).
    gap: int = field(default=1, compare=False)

    @property
    def key(self) -> tuple[str, ...]:
        return tuple(line.rstrip() for line in self.lines)


def version_tuple(version: str) -> tuple[int, int, int] | None:
    match = SEMVER_RE.match(version.lstrip("v"))
    return tuple(int(g) for g in match.groups()) if match else None  # type: ignore[return-value]


def split_sections(lines: list[str]) -> list[tuple[str, int, int]]:
    """[(name, header line, end line)] for every `## [name]` section."""
    headers = [(i, m.group(1)) for i, line in enumerate(lines) if (m := HEADER_RE.match(line))]
    out = []
    for n, (i, name) in enumerate(headers):
        end = headers[n + 1][0] if n + 1 < len(headers) else len(lines)
        out.append((name, i, end))
    return out


def parse_entries(lines: list[str], start: int, end: int) -> list[Entry]:
    """Entries of lines[start:end], the body of one section."""
    entries: list[Entry] = []
    category: str | None = None
    cur_start: int | None = None
    cur_cat: str | None = None

    def close(upto: int) -> None:
        nonlocal cur_start
        if cur_start is None:
            return
        stop = upto
        while stop > cur_start and not lines[stop - 1].strip():
            stop -= 1
        gap = 0
        j = cur_start
        while j > start and not lines[j - 1].strip():
            gap += 1
            j -= 1
        entries.append(Entry(cur_cat, tuple(lines[cur_start:stop]), cur_start, stop, gap))
        cur_start = None

    for i in range(start, end):
        line = lines[i]
        if line.startswith("### "):
            close(i)
            category = line[4:].strip()
        elif line.startswith(("- ", "* ")):
            close(i)
            cur_start, cur_cat = i, category
        elif not line.strip():
            continue
        elif line[0] in " \t":
            if cur_start is None:
                cur_start, cur_cat = i, category
        else:
            # Unindented text that is not a bullet: a lazy continuation when it
            # follows entry text directly, otherwise an entry of its own.
            if cur_start is None or not lines[i - 1].strip():
                close(i)
                cur_start, cur_cat = i, category
    close(end)
    return entries


def section_entries(text: str, name: str) -> list[Entry]:
    lines = text.split("\n")
    for sec, header, end in split_sections(lines):
        if sec == name:
            return parse_entries(lines, header + 1, end)
    return []


def unreleased_entries(text: str) -> list[Entry]:
    return section_entries(text, UNRELEASED)


def released_keys(text: str, newer_than: tuple[int, int, int] | None = None, names: set[str] | None = None,
                  skip: str | None = None) -> Counter:
    """Entry keys of the released (versioned) sections of text, counted.

    `newer_than` keeps only sections of a later version, `names` only sections
    with one of those names, `skip` leaves one section out.
    """
    lines = text.split("\n")
    keys: Counter = Counter()
    for name, header, end in split_sections(lines):
        if name == UNRELEASED or name == skip:
            continue
        if names is not None and name not in names:
            continue
        if newer_than is not None:
            version = version_tuple(name)
            if version is None or version <= newer_than:
                continue
        keys.update(e.key for e in parse_entries(lines, header + 1, end))
    return keys


def new_entries(prev_text: str, cur_text: str) -> list[Entry]:
    """Entries of `[Unreleased]` in cur_text that the release before it did not carry.

    The previous release carried what its `[Unreleased]` held. Those entries are
    still in cur_text's `[Unreleased]` or, once the metadata was merged, moved
    into a section added since: a moved entry is not new, and an identical entry
    written again afterwards is. An entry also filed under a section added since
    (a sweep or a hand repair) is not new either. Text equal to an entry of an
    older release is new when it is written again.
    """
    prev_names = {n for n, _, _ in split_sections(prev_text.split("\n"))}
    cur_names = {n for n, _, _ in split_sections(cur_text.split("\n")) if n != UNRELEASED} - prev_names
    added = released_keys(cur_text, names=cur_names)
    previous = Counter(e.key for e in unreleased_entries(prev_text))
    moved = previous & added
    seen = previous - moved
    filed = added - moved
    out = []
    for entry in unreleased_entries(cur_text):
        if seen[entry.key] > 0:
            seen[entry.key] -= 1
        elif filed[entry.key] > 0:
            filed[entry.key] -= 1
        else:
            out.append(entry)
    return out


def render_section(version: str, date: str, entries: list[Entry]) -> str:
    """`## [version] - date` followed by the entries, grouped under their headings."""
    groups: dict[str | None, list[Entry]] = {}
    for entry in entries:
        groups.setdefault(entry.category, []).append(entry)
    out = [f"## [{version}] - {date}", ""]
    for category, items in groups.items():
        if category is not None:
            out.append(f"### {category}")
        for n, item in enumerate(items):
            # The blank line under the section line stands for the first gap.
            out += [""] * (0 if category is None and n == 0 else item.gap) + list(item.lines)
        out.append("")
    return "\n".join(out) + "\n"


def _cleanup_unreleased(body: list[str]) -> list[str]:
    """Drop headings left without entries and collapse blank runs."""
    groups: list[list[str]] = [[]]
    for line in body:
        if line.startswith("### "):
            groups.append([line])
        else:
            groups[-1].append(line)
    out: list[str] = []
    for group in groups:
        if group and group[0].startswith("### ") and not any(l.strip() for l in group[1:]):
            continue
        out += group
    collapsed: list[str] = []
    for line in out:
        if not line.strip() and (not collapsed or not collapsed[-1].strip()):
            continue
        collapsed.append(line)
    while collapsed and not collapsed[-1].strip():
        collapsed.pop()
    return collapsed


SAME_ENTRY_RATIO = 0.7


def _same_entry(tag_entry: Entry, current: Entry) -> bool:
    """A current entry that is the tag's entry reworded or extended since."""
    if tag_entry.key[0] == current.key[0]:  # same first line: text was added under it
        return True
    ratio = difflib.SequenceMatcher(None, "\n".join(tag_entry.key), "\n".join(current.key)).ratio()
    return ratio >= SAME_ENTRY_RATIO


def apply_release(text: str, version: str, date: str, entries: list[Entry],
                  since: tuple[int, int, int] | None = None, warnings: list[str] | None = None) -> str:
    """Move `entries` out of `[Unreleased]` into a new `## [version]` section.

    Nothing changes when the section exists or there is nothing to move. An entry
    that is in `[Unreleased]` only reworded or extended since the tag moves with
    its current text (never filed twice); one found nowhere is filed from the tag
    text. Both are reported in `warnings`. Entries that a section newer than
    `since` (the previous release) already holds are not filed again. The result
    is checked: the new section holds exactly what was filed, `[Unreleased]`
    keeps every other entry, and each released section is byte-identical.
    """
    warnings = warnings if warnings is not None else []
    lines = text.split("\n")
    if lines[-1] == "":
        lines.pop()
    sections = split_sections(lines)
    if any(name == version for name, _, _ in sections):
        return text
    target = version_tuple(version)
    if target is None:
        raise ValueError(f"not a plain semver: {version!r}")
    unreleased = next(((h, e) for n, h, e in sections if n == UNRELEASED), None)
    if unreleased is None:
        raise ValueError("no [Unreleased] section")
    u_head, u_end = unreleased

    filed = released_keys(text, newer_than=since) if since is not None else Counter()
    wanted = []
    for entry in entries:
        if filed[entry.key] > 0:
            filed[entry.key] -= 1
        else:
            wanted.append(entry)
    if not wanted:
        return text

    pool = parse_entries(lines, u_head + 1, u_end)
    drop: list[Entry] = []
    placed: list[Entry | None] = [None] * len(wanted)
    for n, entry in enumerate(wanted):  # the same text, still there
        hit = next((c for c in pool if c.key == entry.key), None)
        if hit is not None:
            pool.remove(hit)
            drop.append(hit)
            placed[n] = entry
    for n, entry in enumerate(wanted):
        if placed[n] is not None:
            continue
        hit = next((c for c in pool if _same_entry(entry, c)), None)
        if hit is not None:
            pool.remove(hit)
            drop.append(hit)
            placed[n] = hit
            warnings.append(f"v{version}: `{entry.key[0][:80]}` was reworded or extended in [Unreleased] after the tag; "
                            "it is filed with its current text")
        else:
            placed[n] = entry
            warnings.append(f"v{version}: `{entry.key[0][:80]}` is not in [Unreleased] any more; "
                            "it is filed from the tagged text")
    moved = [e for e in placed if e is not None]

    removed = {i for e in drop for i in range(e.start, e.end)}
    body = [l for i, l in enumerate(lines[u_head + 1 : u_end], start=u_head + 1) if i not in removed]
    body = _cleanup_unreleased(body)
    new_unreleased = [lines[u_head], ""] + ([*body, ""] if body else [])

    # File the new section above the first released section that is older.
    insert_before = len(lines)
    for name, header, _ in sections:
        other = version_tuple(name)
        if name != UNRELEASED and other is not None and other < target:
            insert_before = header
            break
    if insert_before < u_end:
        raise ValueError("[Unreleased] is not the newest section")
    section = render_section(version, date, moved).rstrip("\n").split("\n")
    tail = lines[insert_before:]
    if tail:
        section.append("")
    out = lines[:u_head] + new_unreleased + lines[u_end:insert_before] + section + tail
    result = "\n".join(out) + "\n"
    _verify_move(text, result, version, moved, drop)
    return result


def _verify_move(before: str, after: str, version: str, entries: list[Entry], dropped: list[Entry]) -> None:
    moved = Counter(e.key for e in section_entries(after, version))
    if moved != Counter(e.key for e in entries):
        raise ValueError(f"the [{version}] section does not hold exactly the release entries")
    left = Counter(e.key for e in unreleased_entries(after))
    expect = Counter(e.key for e in unreleased_entries(before))
    expect.subtract(Counter(e.key for e in dropped))
    if +left != +expect:
        raise ValueError("[Unreleased] after the move is not [Unreleased] before it minus the release entries")
    b, a = before.split("\n"), after.split("\n")
    old = {n: "\n".join(b[h:e]).rstrip("\n") for n, h, e in split_sections(b) if n != UNRELEASED}
    new = {n: "\n".join(a[h:e]).rstrip("\n") for n, h, e in split_sections(a) if n != UNRELEASED and n != version}
    if old != new:
        raise ValueError("a released section changed")


# ---- git -------------------------------------------------------------------


def git(repo: str, *args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", "-c", "core.quotepath=false", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def release_tags(repo: str) -> list[str]:
    """Plain vMAJOR.MINOR.PATCH tags, oldest first."""
    tags = [t for t in git(repo, "tag", "--list").split() if TAG_RE.match(t)]
    return sorted(tags, key=version_tuple)  # type: ignore[arg-type,return-value]


def previous_tag(tags: list[str], version: str) -> str | None:
    target = version_tuple(version)
    lower = [t for t in tags if version_tuple(t) < target]  # type: ignore[operator]
    return lower[-1] if lower else None


def changelog_at(repo: str, rev: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{rev}:CHANGELOG.md"], cwd=repo, capture_output=True, text=True, check=False
    )
    return result.stdout if result.returncode == 0 else ""


def release_entries(repo: str, rev: str, prev_tag: str | None) -> list[Entry]:
    prev = changelog_at(repo, prev_tag) if prev_tag else ""
    return new_entries(prev, changelog_at(repo, rev))


def tag_date(repo: str, tag: str) -> str:
    out = git(repo, "log", "-1", "--format=%cd", "--date=format-local:%Y-%m-%d", tag)
    return out.strip()


def missing_releases(repo: str, text: str, version: str) -> list[tuple[str, str, list[Entry]]]:
    """[(version, date, entries)] for the tags the CHANGELOG has no section for.

    Those are the tags newer than the newest section already in `text`, up to
    and including `version`, oldest first. Filing them together keeps the
    sections in order whatever order the metadata pull requests were merged in.
    """
    present = [version_tuple(n) for n, _, _ in split_sections(text.split("\n")) if n != UNRELEASED]
    newest = max((p for p in present if p), default=(0, 0, 0))
    target = version_tuple(version)
    tags = release_tags(repo)
    out = []
    for tag in tags:
        tv = version_tuple(tag)
        if tv is None or tv <= newest or tv > target:  # type: ignore[operator]
            continue
        out.append((tag[1:], tag_date(repo, tag), release_entries(repo, tag, previous_tag(tags, tag[1:]))))
    return out


def apply_missing(text: str, repo: str, version: str, warnings: list[str] | None = None) -> str:
    tags = release_tags(repo)
    for ver, date, entries in missing_releases(repo, text, version):
        prev = previous_tag(tags, ver)
        since = version_tuple(prev) if prev else (0, 0, 0)
        text = apply_release(text, ver, date, entries, since, warnings)
    return text


# ---- CLI -------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    notes = sub.add_parser("notes", help="print the release section for a commit")
    notes.add_argument("--repo", default=".")
    notes.add_argument("--rev", required=True)
    notes.add_argument("--prev-tag", default="")
    notes.add_argument("--version", required=True)
    notes.add_argument("--date", default="")
    apply_p = sub.add_parser("apply", help="file the tagged releases the CHANGELOG lacks")
    apply_p.add_argument("--repo", default=".")
    apply_p.add_argument("--changelog", default="CHANGELOG.md")
    apply_p.add_argument("--version", required=True)
    apply_p.add_argument("--warnings-file", default="", help="write what needed a decision here, one line each")
    args = parser.parse_args(argv)

    if args.cmd == "notes":
        entries = release_entries(args.repo, args.rev, args.prev_tag or None)
        if not entries:
            print("no new [Unreleased] entries since the previous release", file=sys.stderr)
            return 1
        date = args.date or git(args.repo, "log", "-1", "--format=%cd", "--date=format-local:%Y-%m-%d", args.rev).strip()
        sys.stdout.write(render_section(args.version, date, entries))
        return 0

    with open(args.changelog, encoding="utf-8") as fh:
        text = fh.read()
    if not TAG_RE.match(f"v{args.version}") or f"v{args.version}" not in release_tags(args.repo):
        print(f"tag v{args.version} not found; the CHANGELOG move needs the tagged commit", file=sys.stderr)
        return 1
    warnings: list[str] = []
    updated = apply_missing(text, args.repo, args.version, warnings)
    for line in warnings:
        print(f"::warning::{line}", file=sys.stderr)
    if args.warnings_file:
        with open(args.warnings_file, "w", encoding="utf-8") as fh:
            fh.write("".join(f"{line}\n" for line in warnings))
    if updated != text:
        with open(args.changelog, "w", encoding="utf-8") as fh:
            fh.write(updated)
    return 0


if __name__ == "__main__":
    sys.exit(main())
