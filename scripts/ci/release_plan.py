#!/usr/bin/env python3
"""Decide whether main holds a release, and which version it is.

The decision looks at everything that changed since the latest release tag, not
at the commit that started the run. GitHub keeps one pending run per
concurrency group, so a newer push cancels an older queued run; whichever run
survives then releases what the cancelled ones would have. That makes a lost run
harmless: even a run started by a docs-only push, or by a release metadata
merge, cuts a release for earlier shipped changes that are still untagged.

A release is due when, since the latest tag,
  * a path that ships changed (change_scope.ships; the chart version that the
    metadata sync rewrites does not count), and
  * `[Unreleased]` holds entries the previous release did not carry
    (changelog_release.new_entries), and
  * at least one shipping pull request is not labelled no-release.

The bump is the largest one over the shipping pull requests merged since the
tag, using the labels and size rules the bot has always applied to the pull
request that triggered it; pull requests that ship nothing (CI, tests, docs)
do not move it.

Usage: release_plan.py plan --repo . --tip HEAD --github-repo owner/name \
           [--github-output "$GITHUB_OUTPUT"]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request

try:
    from scripts.ci import change_scope, changelog_release
except ImportError:  # run as a script: scripts/ci is sys.path[0]
    import change_scope  # type: ignore[no-redef]
    import changelog_release  # type: ignore[no-redef]

CHART = "charts/loki-vl-proxy/Chart.yaml"
# What the metadata sync rewrites; a commit that touches only these ships nothing.
METADATA_FILES = {"CHANGELOG.md", "README.md", "docs/observability.md", CHART}
ORDER = {"none": 0, "patch": 1, "minor": 2, "breaking": 3, "major": 4}
PR_SUFFIX = re.compile(r"\(#(\d+)\)$")


def git(repo, *args):
    result = subprocess.run(
        ["git", "-c", "core.quotepath=false", *args], cwd=repo, capture_output=True, text=True, check=False
    )
    return result.stdout if result.returncode == 0 else None


def chart_version(repo, rev):
    text = git(repo, "show", f"{rev}:{CHART}") or ""
    match = re.search(r"^version:\s*[\"']?([^\s\"']+)", text, re.M)
    return match.group(1) if match else ""


def pr_bump(pr):
    """(bump, no_release) for one pull request, by the bot's long-standing rules."""
    labels = {l.get("name", "").lower() for l in pr.get("labels", [])}
    if "no-release" in labels:
        return "none", True
    # The order the bot has always applied: an explicit release:* label wins over
    # the labels the labeler adds from the title (feature, breaking-change).
    # A breaking change moves the minor to the next multiple of ten (2.62.x ->
    # 2.70.0); only an explicit release:major label bumps the major.
    bump = "none"
    for label, value in (("release:major", "major"), ("release:minor", "minor"), ("release:patch", "patch"),
                         ("breaking-change", "breaking"), ("feature", "minor"), ("bugfix", "patch"),
                         ("performance", "patch")):
        if label in labels:
            bump = value
            break
    if bump != "major":
        files = int(pr.get("changed_files") or 0)
        churn = int(pr.get("additions") or 0) + int(pr.get("deletions") or 0)
        xl = bool(labels & {"size/xl", "size/xxl"})
        runtime = any(l.startswith("scope/") and l[6:] in {"proxy", "cache", "translator", "cli", "helm", "api"} for l in labels)
        if (xl and runtime) or files >= 20 or churn >= 1200:
            if bump in ("none", "patch"):
                bump = "minor"
    return bump, False


def subject_bump(subject):
    if re.search(r"BREAKING CHANGE|^feat.*!:|^fix.*!:", subject):
        return "breaking"
    if re.match(r"^feat(\(|:)", subject):
        return "minor"
    if re.match(r"^(fix|perf|refactor|revert)(\(|:)", subject):
        return "patch"
    return "none"


def fetch_pr_from_github(repo_slug, token):
    def fetch(number):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{repo_slug}/pulls/{number}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - fixed https host
                return json.load(resp)
        except Exception as exc:  # a missing PR must not stop a release
            print(f"warning: PR #{number} not readable: {exc}", file=sys.stderr)
            return None

    return fetch


def plan(repo, tip="HEAD", fetch_pr=None):
    """The release decision for `tip`: a dict with should_release and why."""
    fetch_pr = fetch_pr or (lambda number: None)
    tags = changelog_release.release_tags(repo)
    latest = tags[-1] if tags else None
    out = {"should_release": "false", "next_version": "", "latest_tag": latest or "v0.0.0", "bump": "none", "reason": ""}

    def no(reason):
        out["reason"] = reason
        return out

    base = latest or ""
    rng = f"{latest}..{tip}" if latest else tip
    names = git(repo, "diff", "--name-only", latest, tip) if latest else git(repo, "ls-tree", "-r", "--name-only", tip)
    files = [f for f in (names or "").split("\n") if f]

    # The sync that follows each release rewrites the chart version. While the
    # chart still carries the released version that edit says nothing about what
    # changed since the tag; a chart ahead of the tag is a deliberate request.
    cur_chart = chart_version(repo, tip)
    latest_version = latest[1:] if latest else "0.0.0"
    scope = files
    if latest and cur_chart == latest_version:
        edits = git(repo, "diff", "-U0", latest, tip, "--", CHART) or ""
        other = [l for l in edits.split("\n") if l[:1] in "+-" and not l.startswith(("+++", "---"))
                 and not re.match(r"^[+-](version|appVersion):", l)]
        if not other:
            scope = [f for f in files if f != CHART]
    if files and not scope:
        return no(f"nothing that ships changed since {latest}")
    if scope and not change_scope.any_ships(scope):
        return no(f"nothing that ships changed since {latest}")

    entries = changelog_release.release_entries(repo, tip, latest)
    if not entries:
        return no(f"[Unreleased] holds no entry that {latest or 'the start'} did not carry")

    if re.match(r"^\d+\.\d+\.\d+$", cur_chart) and latest:
        if changelog_release.version_tuple(cur_chart) > changelog_release.version_tuple(latest_version):
            # The pull request that moved the chart ahead is the request; its
            # no-release label withdraws it.
            asked = (git(repo, "log", "-1", "--first-parent", "--format=%s", f"{latest}..{tip}", "--", CHART) or "").strip()
            match = PR_SUFFIX.search(asked)
            asked_pr = fetch_pr(int(match.group(1))) if match else None
            if asked_pr is not None and pr_bump(asked_pr)[1]:
                return no(f"the pull request that moved the chart to {cur_chart} is labelled no-release")
            out.update(should_release="true", next_version=cur_chart, bump="chart",
                       reason=f"chart version override {cur_chart} (latest tag is {latest_version})")
            return out

    # Shipping pull requests since the tag.
    log = git(repo, "log", "--first-parent", "--format=%H%x1f%s", rng) or ""
    bump, considered, skipped = "none", 0, 0
    for row in [r for r in log.split("\n") if r]:
        sha, _, subject = row.partition("\x1f")
        parent = git(repo, "rev-parse", "--verify", "-q", f"{sha}^1")
        changed = (git(repo, "diff", "--name-only", parent.strip(), sha) if parent else "") or ""
        shipped = [f for f in changed.split("\n") if f and f not in METADATA_FILES and change_scope.ships(f)]
        if not shipped:
            continue
        match = PR_SUFFIX.search(subject.strip())
        pr = fetch_pr(int(match.group(1))) if match else None
        if pr is not None:
            this, no_release = pr_bump(pr)
            if no_release:
                skipped += 1
                continue
        else:
            this = "none"
        if this == "none":
            this = subject_bump(subject)
        considered += 1
        if ORDER[this] > ORDER[bump]:
            bump = this
    if skipped and not considered:
        return no("every shipping pull request since the last tag is labelled no-release")
    if bump == "none":
        bump = "patch"

    maj, mnr, pat = changelog_release.version_tuple(latest_version)
    if bump == "major":
        maj, mnr, pat = maj + 1, 0, 0
    elif bump == "breaking":
        mnr, pat = (mnr // 10 + 1) * 10, 0
    elif bump == "minor":
        mnr, pat = mnr + 1, 0
    else:
        pat += 1
    version = f"{maj}.{mnr}.{pat}"
    if f"v{version}" in tags:
        return no(f"v{version} already exists")
    out.update(should_release="true", next_version=version, bump=bump,
               reason=f"{len(entries)} new entries, {considered} shipping pull requests, {bump} bump")
    return out


def write_outputs(result, path):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        for key in ("should_release", "next_version", "latest_tag"):
            fh.write(f"{key}={result[key]}\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--repo", default=".")
    p.add_argument("--tip", default="HEAD")
    p.add_argument("--github-repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    p.add_argument("--github-output", default="")
    args = parser.parse_args(argv)
    token = os.environ.get("GH_TOKEN", "")
    fetch = fetch_pr_from_github(args.github_repo, token) if args.github_repo and token else None
    result = plan(args.repo, args.tip, fetch)
    print(json.dumps(result, indent=2))
    write_outputs(result, args.github_output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
