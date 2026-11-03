#!/usr/bin/env python3
"""Build and keep up to date the release metadata branch.

The metadata for a release (the CHANGELOG move, the chart version, the README
badges) is generated against the current tip of main, never against the commit
that was tagged. The CHANGELOG move itself reads the tagged commit
(changelog_release.py), so it carries exactly the entries that release shipped
and leaves every entry added since in `[Unreleased]`.

  prepare --version A.B.C   generate against main, commit the files to
                            release/metadata-vA.B.C through the API (a signed
                            commit on top of main), and generate again when main
                            moved while it ran, so the branch never starts
                            from an old main.
  refresh                   for the open release/metadata-v* pull requests:
                            regenerate the newest one when main has moved past
                            its base (or its files differ from what main now
                            needs), and close the older ones. The newest
                            branch files every tag section main lacks, so an
                            older pull request has nothing left to do.

Both commands need GH_TOKEN (the bot token) and GITHUB_REPOSITORY, and run in a
checkout with every tag.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
FILES = ["CHANGELOG.md", "README.md", "docs/observability.md", "charts/loki-vl-proxy/Chart.yaml"]
BRANCH_RE = re.compile(r"^release/metadata-v(\d+)\.(\d+)\.(\d+)$")
BADGE_PREFIXES = ("[![Tests](", "[![Coverage](", "[![Lines of Code](", "[![Lines](")
ATTEMPTS = 3


def run(args, *, cwd=".", env=None, check=True, capture=True):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=capture, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {(result.stderr or result.stdout or '').strip()}")
    return result.stdout.strip() if capture else ""


def git_as_bot(repo, token, *args, check=True):
    """git against the remote with the token handed over by a credential helper."""
    helper = '!f() { test "$1" = get && printf "username=x-access-token\\npassword=%s\\n" "${GIT_AUTH_TOKEN}"; }; f'
    env = {**os.environ, "GIT_AUTH_TOKEN": token}
    return run(["git", "-c", "credential.helper=", "-c", f"credential.helper={helper}", *args], cwd=repo, env=env, check=check)


def branch_name(version):
    return f"release/metadata-v{version}"


def copy_badges(source, target):
    """Put the Tests/Coverage/Lines badge lines of `source` into `target`."""
    wanted = {}
    for line in source.split("\n"):
        for prefix in BADGE_PREFIXES:
            if line.startswith(prefix):
                wanted.setdefault(prefix, line)
    out = []
    for line in target.split("\n"):
        for prefix, new in wanted.items():
            if line.startswith(prefix):
                line = new
                break
        out.append(line)
    return "\n".join(out)


def generate(repo, version, repo_slug, tests="", coverage="", code_lines=""):
    """Rewrite the metadata files in the working tree; return the files that changed."""
    run(["bash", str(HERE / "sync_release_metadata.sh"), version, "", tests, coverage, code_lines, ".", repo_slug], cwd=repo)
    changed = run(["git", "diff", "--name-only", "--", *FILES], cwd=repo)
    return [f for f in changed.split("\n") if f]


def commit(repo, slug, branch, base, version, files, token):
    script = os.environ.get("COMMIT_VIA_API", str(HERE / "commit_via_api.sh"))
    env = {**os.environ, "GH_TOKEN": token}
    return run(["bash", script, slug, branch, base, f"docs: sync release metadata for v{version}", *files], cwd=repo, env=env)


def fetch_main(repo, token):
    git_as_bot(repo, token, "fetch", "--force", "--tags", "origin", "main")
    return run(["git", "rev-parse", "origin/main"], cwd=repo)


def prepare(repo, version, slug, token, tests="", coverage="", code_lines=""):
    """Returns (has_changes, branch). Regenerates while main keeps moving."""
    branch = branch_name(version)
    pushed = False
    for attempt in range(1, ATTEMPTS + 1):
        try:
            base = fetch_main(repo, token)
            run(["git", "reset", "--hard", "-q", base], cwd=repo)
            files = generate(repo, version, slug, tests, coverage, code_lines)
            if not files:
                print(f"release metadata for v{version} is already on main {base[:8]}")
                return pushed, branch
            sha = commit(repo, slug, branch, base, version, files, token)
            pushed = True
            print(f"release metadata commit {sha} on {branch}, on main {base[:8]} (attempt {attempt})")
            if fetch_main(repo, token) == base:
                return True, branch
            print("main moved while the branch was built; regenerating from the new main")
        except RuntimeError as exc:
            if not pushed:
                raise
            # The branch exists from an earlier attempt: keep it, so its pull
            # request is opened and the refresh job brings it up to date.
            print(f"::warning::regenerating the release metadata failed ({exc}); keeping the branch as pushed")
            return True, branch
    print("::warning::main kept moving; the refresh job will bring the branch up to date")
    return True, branch


def gh_json(args, token):
    env = {**os.environ, "GH_TOKEN": token}
    return json.loads(run(["gh", *args], env=env) or "null")


def open_metadata_prs(slug, token):
    prs = gh_json(["pr", "list", "--repo", slug, "--base", "main", "--state", "open", "--limit", "100",
                   "--json", "number,headRefName"], token) or []
    found = []
    for pr in prs:
        match = BRANCH_RE.match(pr["headRefName"])
        if match:
            found.append((tuple(int(g) for g in match.groups()), pr["number"], pr["headRefName"]))
    return sorted(found)


def close_pr(slug, token, number, why):
    env = {**os.environ, "GH_TOKEN": token}
    run(["gh", "pr", "close", str(number), "--repo", slug, "--delete-branch", "--comment", why], env=env, check=False)


def refresh(repo, slug, token):
    """Bring the newest open metadata pull request up to date with main."""
    prs = open_metadata_prs(slug, token)
    if not prs:
        print("no open release metadata pull request")
        return
    version_t, number, branch = prs[-1]
    version = ".".join(str(p) for p in version_t)
    for _, older, _ in prs[:-1]:
        close_pr(slug, token, older, f"Superseded by #{number}, which files every release section main lacks.")

    base = fetch_main(repo, token)
    git_as_bot(repo, token, "fetch", "--force", "origin", f"{branch}:refs/remotes/origin/{branch}")
    run(["git", "reset", "--hard", "-q", base], cwd=repo)
    # The badge numbers come from the release run's tests; keep the branch's.
    branch_readme = run(["git", "show", f"origin/{branch}:README.md"], cwd=repo, check=False)
    files = generate(repo, version, slug)
    if branch_readme:
        readme = Path(repo) / "README.md"
        text = readme.read_text(encoding="utf-8")
        updated = copy_badges(branch_readme, text)
        if updated != text:
            readme.write_text(updated, encoding="utf-8")
        files = [f for f in run(["git", "diff", "--name-only", "--", *FILES], cwd=repo).split("\n") if f]
    if not files:
        close_pr(slug, token, number, f"main already carries the v{version} release metadata.")
        return
    parent = run(["git", "rev-parse", f"origin/{branch}^"], cwd=repo, check=False)
    stale = subprocess.run(["git", "diff", "--quiet", f"origin/{branch}", "--", *FILES], cwd=repo).returncode != 0
    if parent == base and not stale:
        print(f"#{number} ({branch}) is on main {base[:8]} and matches it")
        return
    sha = commit(repo, slug, branch, base, version, files, token)
    print(f"#{number}: regenerated {branch} on main {base[:8]} as {sha}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default=".")
    sub = parser.add_subparsers(dest="cmd", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--version", required=True)
    prep.add_argument("--tests", default="")
    prep.add_argument("--coverage", default="")
    prep.add_argument("--lines", default="")
    prep.add_argument("--github-output", default="")
    sub.add_parser("refresh")
    args = parser.parse_args(argv)

    slug = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    if args.cmd == "refresh":
        refresh(args.repo, slug, token)
        return 0
    if not re.match(r"^\d+\.\d+\.\d+$", args.version):
        print(f"::error::release version '{args.version}' is not MAJOR.MINOR.PATCH", file=sys.stderr)
        return 1
    changes, branch = prepare(args.repo, args.version, slug, token, args.tests, args.coverage, args.lines)
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as fh:
            fh.write(f"has_changes={'true' if changes else 'false'}\nbranch={branch}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
